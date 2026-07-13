// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tree-model session (device/timeseries paths).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::client::dataset::SessionDataSet;
use crate::client::redirect::{self, RedirectCache, RedirectCacheStats};
use crate::connection::{Connection, ConnectionOptions, Endpoint, RpcProtocol};
use crate::data::record::serialize_record_values;
use crate::data::{Tablet, Value};
use crate::error::{Error, Result};
use crate::protocol::client::{
    TIClientRPCServiceSyncClient, TSCloseOperationReq, TSCloseSessionReq, TSExecuteStatementReq,
    TSFetchResultsReq, TSInsertRecordReq, TSInsertRecordsOfOneDeviceReq, TSInsertRecordsReq,
    TSInsertTabletReq, TSInsertTabletsReq, TSOpenSessionReq, TSProtocolVersion,
};
use crate::protocol::common::TSStatus;

/// TSStatus codes the client special-cases (see protocol spec §2).
pub mod status_code {
    pub const SUCCESS_STATUS: i32 = 200;
    pub const MULTIPLE_ERROR: i32 = 302;
    /// The write succeeded; `redirectNode` merely recommends a better endpoint.
    pub const REDIRECTION_RECOMMEND: i32 = 400;
}

/// Rotating start index shared by all sessions so connections spread across
/// nodes (mirrors the C# SDK's round-robin-with-failover endpoint selection).
static ENDPOINT_START_INDEX: AtomicUsize = AtomicUsize::new(0);

/// Configuration for opening a [`Session`].
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub endpoints: Vec<Endpoint>,
    pub username: String,
    pub password: String,
    /// `tree` (default) or `table` — sent as `sql_dialect` at open time.
    pub sql_dialect: String,
    pub fetch_size: i32,
    pub zone_id: String,
    /// TCP connect timeout per endpoint attempt.
    pub connect_timeout: Duration,
    /// Per-query server-side timeout in milliseconds (request body field).
    pub query_timeout_ms: i64,
    /// Database to select at open time (table dialect; sent as config key `db`).
    pub database: Option<String>,
    /// Reopen the connection and retry an op once when an RPC fails at the
    /// Thrift/transport level (C# `Reconnect` behavior). Default `true`.
    pub enable_auto_reconnect: bool,
    /// Full round-robin passes over the endpoints during a reconnect before
    /// giving up (C# `RetryNum`). Default 3.
    pub max_reconnect_attempts: usize,
    /// Pause between reconnect passes. Default 1 s.
    pub retry_interval: Duration,
    /// Harvest status-400 `redirectNode` hints from insert responses into
    /// the per-session [`RedirectCache`]. Default `true`.
    pub enable_redirection: bool,
    /// Speak TCompactProtocol instead of TBinaryProtocol ("RPC compression"
    /// in IoTDB terms). Must match the **server** setting
    /// `dn_rpc_thrift_compression_enable` (default `false`) — there is no
    /// per-connection negotiation; a mismatch fails at the first RPC.
    /// Default `false`.
    pub enable_rpc_compression: bool,
    /// Wrap connections in TLS (cargo feature `tls`). The server must have
    /// `enable_thrift_ssl=true`. Default `false`.
    #[cfg(feature = "tls")]
    pub use_ssl: bool,
    /// PEM certificate added as trusted root (private CA / self-signed
    /// server cert). `None` → platform trust store only.
    #[cfg(feature = "tls")]
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// Skip certificate verification (self-signed test certs).
    /// **Dangerous** outside tests. Default `false`.
    #[cfg(feature = "tls")]
    pub accept_invalid_certs: bool,
    /// Hostname for SNI + certificate validation instead of the endpoint
    /// host (e.g. when connecting by IP).
    #[cfg(feature = "tls")]
    pub domain_override: Option<String>,
    /// PEM client certificate for mutual TLS (server has
    /// `thrift_ssl_client_auth=true`); set together with
    /// `client_key_path`. Mirrors the Node.js `sslOptions.cert`.
    #[cfg(feature = "tls")]
    pub client_cert_path: Option<std::path::PathBuf>,
    /// PEM PKCS#8 private key for the client certificate; set together
    /// with `client_cert_path`. Mirrors the Node.js `sslOptions.key`.
    #[cfg(feature = "tls")]
    pub client_key_path: Option<std::path::PathBuf>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            endpoints: vec![Endpoint::new("localhost", 6667)],
            username: "root".into(),
            password: "root".into(),
            sql_dialect: "tree".into(),
            fetch_size: 1024,
            zone_id: "UTC+8".into(),
            connect_timeout: Duration::from_secs(10),
            query_timeout_ms: 60_000,
            database: None,
            enable_auto_reconnect: true,
            max_reconnect_attempts: 3,
            retry_interval: Duration::from_secs(1),
            enable_redirection: true,
            enable_rpc_compression: false,
            #[cfg(feature = "tls")]
            use_ssl: false,
            #[cfg(feature = "tls")]
            ca_cert_path: None,
            #[cfg(feature = "tls")]
            accept_invalid_certs: false,
            #[cfg(feature = "tls")]
            domain_override: None,
            #[cfg(feature = "tls")]
            client_cert_path: None,
            #[cfg(feature = "tls")]
            client_key_path: None,
        }
    }
}

impl SessionConfig {
    /// Set endpoints from `"host:port"` node-url strings (IPv6 hosts may be
    /// bracketed, e.g. `"[::1]:6667"`).
    pub fn with_node_urls<S: AsRef<str>>(mut self, node_urls: &[S]) -> Result<Self> {
        self.endpoints = node_urls
            .iter()
            .map(|u| Endpoint::parse(u.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(self)
    }

    /// Resolve the connection-level options (timeout, wire protocol, TLS)
    /// this config implies.
    pub fn connection_options(&self) -> ConnectionOptions {
        ConnectionOptions {
            connect_timeout: self.connect_timeout,
            protocol: if self.enable_rpc_compression {
                RpcProtocol::Compact
            } else {
                RpcProtocol::Binary
            },
            #[cfg(feature = "tls")]
            tls: self.use_ssl.then(|| crate::connection::TlsOptions {
                ca_cert_path: self.ca_cert_path.clone(),
                accept_invalid_certs: self.accept_invalid_certs,
                domain_override: self.domain_override.clone(),
                client_cert_path: self.client_cert_path.clone(),
                client_key_path: self.client_key_path.clone(),
            }),
        }
    }
}

/// Raw result of a query statement: response metadata plus undecoded TsBlocks.
///
/// TsBlock decoding lives in the data layer; this handle only carries the
/// bytes and the bookkeeping needed to fetch more pages and close the query.
#[derive(Debug)]
pub struct QueryHandle {
    pub query_id: i64,
    pub statement: String,
    pub columns: Vec<String>,
    pub data_type_list: Vec<String>,
    pub ignore_time_stamp: bool,
    /// Serialized TsBlocks (decoded by the data layer).
    pub query_result: Vec<Vec<u8>>,
    pub more_data: bool,
    /// Output column ordinal → physical TsBlock column index (`-1` = time
    /// column); identity mapping when absent.
    pub column_index2_ts_block_column_index_list: Option<Vec<i32>>,
}

/// A tree-model session against an IoTDB cluster.
pub struct Session {
    config: SessionConfig,
    connection: Option<Connection>,
    session_id: i64,
    statement_id: i64,
    /// Current database, tracked from `USE <db>` responses (table dialect).
    database: Option<String>,
    /// Endpoint of the most recent (possibly dead) connection — reconnect
    /// starts its round-robin here, like the C# SDK.
    last_endpoint: Option<Endpoint>,
    /// Device → endpoint hints harvested from status-400 insert responses.
    redirect_cache: RedirectCache,
}

impl Session {
    pub fn new(config: SessionConfig) -> Self {
        let database = config.database.clone();
        Self {
            config,
            connection: None,
            session_id: -1,
            statement_id: -1,
            database,
            last_endpoint: None,
            redirect_cache: RedirectCache::default(),
        }
    }

    /// Connect (trying endpoints from a rotating start index; first success
    /// wins), open the session and request the connection's statement id.
    pub fn open(&mut self) -> Result<()> {
        if self.connection.is_some() {
            return Err(Error::Client("session already open".into()));
        }
        let n = self.config.endpoints.len();
        if n == 0 {
            return Err(Error::Client("no endpoints configured".into()));
        }

        let start = ENDPOINT_START_INDEX.fetch_add(1, Ordering::Relaxed) % n;
        let options = self.config.connection_options();
        let mut connection = None;
        let mut last_err: Option<Error> = None;
        for i in 0..n {
            let endpoint = self.config.endpoints[(start + i) % n].clone();
            match Connection::open(endpoint, &options) {
                Ok(c) => {
                    connection = Some(c);
                    break;
                }
                Err(e) => last_err = Some(e),
            }
        }
        let mut connection = connection.ok_or_else(|| {
            last_err.unwrap_or_else(|| Error::Client("no endpoints configured".into()))
        })?;

        let (session_id, statement_id) = self.authenticate(&mut connection)?;
        self.session_id = session_id;
        self.statement_id = statement_id;
        self.last_endpoint = Some(connection.endpoint().clone());
        self.connection = Some(connection);
        Ok(())
    }

    /// Handshake on a fresh connection: `openSession` (dialect + current
    /// database via the `db` config key) followed by `requestStatementId`.
    /// Shared by [`Session::open`] and reconnect, so a reopened session
    /// lands back in the database its `USE <db>` had selected.
    fn authenticate(&self, connection: &mut Connection) -> Result<(i64, i64)> {
        let mut configuration = BTreeMap::new();
        configuration.insert("sql_dialect".to_string(), self.config.sql_dialect.clone());
        if let Some(db) = &self.database {
            // ⚠️ config key is literally "db", not "database".
            configuration.insert("db".to_string(), db.clone());
        }
        let req = TSOpenSessionReq::new(
            TSProtocolVersion::IotdbServiceProtocolV3,
            self.config.zone_id.clone(),
            self.config.username.clone(),
            self.config.password.clone(),
            configuration,
        );
        let resp = connection.client_mut().open_session(req)?;
        check_status(&resp.status)?;
        if resp.server_protocol_version != TSProtocolVersion::IotdbServiceProtocolV3 {
            log::warn!(
                "server protocol version mismatch: expected V3, got {:?}",
                resp.server_protocol_version
            );
        }
        let session_id = resp
            .session_id
            .ok_or_else(|| Error::Client("openSession response missing sessionId".into()))?;
        let statement_id = connection.client_mut().request_statement_id(session_id)?;
        Ok((session_id, statement_id))
    }

    /// Reopen after a transport-level failure: drop the dead connection
    /// (closing its transport), then try a **full** handshake — connect +
    /// openSession + requestStatementId — round-robin over all endpoints,
    /// starting at the one that just died, for up to
    /// `max_reconnect_attempts` passes with `retry_interval` between passes
    /// (mirroring the C# SDK's `Reconnect`).
    fn reconnect(&mut self) -> Result<()> {
        self.connection = None; // drop closes the old transport
        let n = self.config.endpoints.len();
        if n == 0 {
            return Err(Error::Client("no endpoints configured".into()));
        }
        let start = self
            .last_endpoint
            .as_ref()
            .and_then(|ep| self.config.endpoints.iter().position(|e| e == ep))
            .unwrap_or(0);
        let attempts = self.config.max_reconnect_attempts.max(1);
        let options = self.config.connection_options();
        let mut last_err: Option<Error> = None;
        for attempt in 0..attempts {
            if attempt > 0 {
                std::thread::sleep(self.config.retry_interval);
            }
            for i in 0..n {
                let endpoint = self.config.endpoints[(start + i) % n].clone();
                let result = Connection::open(endpoint, &options).and_then(|mut connection| {
                    let ids = self.authenticate(&mut connection)?;
                    Ok((connection, ids))
                });
                match result {
                    Ok((connection, (session_id, statement_id))) => {
                        log::info!("reconnected to {}", connection.endpoint());
                        self.session_id = session_id;
                        self.statement_id = statement_id;
                        self.last_endpoint = Some(connection.endpoint().clone());
                        self.connection = Some(connection);
                        return Ok(());
                    }
                    Err(e) => {
                        log::warn!("reconnect attempt {}/{attempts} failed: {e}", attempt + 1);
                        last_err = Some(e);
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Client("reconnect failed".into())))
    }

    /// Run an RPC op; on a transport-level failure ([`Error::Thrift`]) with
    /// auto-reconnect enabled, reopen the session and retry the op exactly
    /// once (C# `ExecuteClientOperationAsync`). Server status errors pass
    /// through untouched. If the reconnect itself fails, the **original**
    /// error is surfaced. Ops must rebuild their request inside the closure:
    /// reconnecting swaps the connection and refreshes
    /// `session_id`/`statement_id`.
    fn with_retry<T>(&mut self, mut op: impl FnMut(&mut Self) -> Result<T>) -> Result<T> {
        let original = match op(self) {
            Err(e @ Error::Thrift(_))
                if self.config.enable_auto_reconnect && self.connection.is_some() =>
            {
                e
            }
            other => return other,
        };
        log::warn!("RPC failed at transport level ({original}); reconnecting");
        match self.reconnect() {
            Ok(()) => op(self),
            Err(reconnect_err) => {
                log::warn!("reconnect failed ({reconnect_err}); surfacing the original error");
                Err(original)
            }
        }
    }

    pub fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    /// The database currently selected on this session, if any.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// The endpoint this session is currently connected to, if open.
    pub fn current_endpoint(&self) -> Option<&Endpoint> {
        self.connection.as_ref().map(Connection::endpoint)
    }

    /// The cached redirect endpoint for `device_id`, if a status-400 insert
    /// response recommended one and the hint has not expired.
    ///
    /// The session itself does **not** act on these hints (it holds a
    /// single connection); [`crate::SessionPool::acquire_for_device`]
    /// consults them to prefer a matching idle session. See
    /// [`crate::client::redirect`] for the routing-honesty note.
    pub fn redirect_hint(&mut self, device_id: &str) -> Option<Endpoint> {
        self.redirect_cache.get(device_id)
    }

    /// Occupancy/config snapshot of the redirect cache.
    pub fn redirect_cache_stats(&self) -> RedirectCacheStats {
        self.redirect_cache.stats()
    }

    /// Drop all cached redirect hints.
    pub fn clear_redirect_cache(&mut self) {
        self.redirect_cache.clear();
    }

    /// Inspect an insert response for redirect hints before collapsing it
    /// into a `Result` — `check_status` treats 400 as plain success and
    /// would discard the recommended node.
    fn check_insert_status(&mut self, devices: &[&str], status: &TSStatus) -> Result<()> {
        if self.config.enable_redirection {
            redirect::record_redirects(&mut self.redirect_cache, devices, status);
        }
        check_status(status)
    }

    fn connection_mut(&mut self) -> Result<&mut Connection> {
        self.connection
            .as_mut()
            .ok_or_else(|| Error::Client("session is not open".into()))
    }

    /// Execute a non-query statement (DDL/DML). Tracks `USE <db>` via the
    /// response's `database` field.
    pub fn execute_non_query(&mut self, sql: &str) -> Result<()> {
        let resp = self.with_retry(|session| {
            let req = session.statement_req(sql);
            Ok(session
                .connection_mut()?
                .client_mut()
                .execute_update_statement_v2(req)?)
        })?;
        check_status(&resp.status)?;
        if let Some(db) = resp.database {
            self.database = Some(db);
        }
        Ok(())
    }

    /// Execute a query statement, returning a [`SessionDataSet`] that
    /// borrows this session until it is dropped or closed — fetches and
    /// closeOperation must reach the node that owns the query id (spec
    /// gotcha #13), so the connection stays pinned to the result set.
    pub fn execute_query(&mut self, sql: &str) -> Result<SessionDataSet<'_>> {
        let handle = self.execute_query_raw(sql)?;
        Ok(SessionDataSet::new(self, handle))
    }

    /// Execute a query statement, returning raw TsBlock bytes plus metadata.
    /// Low-level path: decoding and pagination are the caller's problem —
    /// prefer [`Session::execute_query`].
    pub fn execute_query_raw(&mut self, sql: &str) -> Result<QueryHandle> {
        // Retry covers only this initial execute RPC: once a query id
        // exists, the result set is pinned to its node and cannot be
        // migrated by a reconnect (spec gotcha #13).
        let resp = self.with_retry(|session| {
            let req = session.statement_req(sql);
            Ok(session
                .connection_mut()?
                .client_mut()
                .execute_query_statement_v2(req)?)
        })?;
        check_status(&resp.status)?;
        let query_id = resp
            .query_id
            .ok_or_else(|| Error::Client("query response missing queryId".into()))?;
        Ok(QueryHandle {
            query_id,
            statement: sql.to_string(),
            columns: resp.columns.unwrap_or_default(),
            data_type_list: resp.data_type_list.unwrap_or_default(),
            ignore_time_stamp: resp.ignore_time_stamp.unwrap_or(false),
            query_result: resp.query_result.unwrap_or_default(),
            more_data: resp.more_data.unwrap_or(false),
            column_index2_ts_block_column_index_list: resp.column_index2_ts_block_column_index_list,
        })
    }

    /// Fetch the next page of TsBlocks for an open query. Returns the raw
    /// blocks and whether more data remains; empty when the set is exhausted.
    pub fn fetch_results(&mut self, query_id: i64, sql: &str) -> Result<(Vec<Vec<u8>>, bool)> {
        let req = TSFetchResultsReq::new(
            self.session_id,
            sql.to_string(),
            self.config.fetch_size,
            query_id,
            true, // isAlign — always true on the V2/TsBlock path
            self.config.query_timeout_ms,
            self.statement_id,
        );
        let resp = self.connection_mut()?.client_mut().fetch_results_v2(req)?;
        check_status(&resp.status)?;
        if !resp.has_result_set {
            return Ok((Vec::new(), false));
        }
        Ok((
            resp.query_result.unwrap_or_default(),
            resp.more_data.unwrap_or(false),
        ))
    }

    /// Close an open query result set. Best-effort: errors are swallowed,
    /// matching the Node.js and C# SDKs.
    pub fn close_query(&mut self, query_id: i64) {
        let (session_id, statement_id) = (self.session_id, self.statement_id);
        if let Ok(connection) = self.connection_mut() {
            let req = TSCloseOperationReq::new(session_id, query_id, statement_id, None);
            if let Err(e) = connection.client_mut().close_operation(req) {
                log::debug!("closeOperation for query {query_id} failed (ignored): {e}");
            }
        }
    }

    /// Insert a [`Tablet`] (tree or table model). Serializes per protocol
    /// spec §3 — column-major values with trailing null bitmaps, i64-BE
    /// timestamps — sorting rows by timestamp first (spec §3.5). Table-model
    /// tablets are sent with `writeToTable=true` plus their column
    /// categories, and are never aligned (spec §6).
    pub fn insert_tablet(&mut self, tablet: &Tablet) -> Result<()> {
        // Serialization sorts in place; clone so the caller's tablet order
        // is untouched (the clone is cheap relative to the RPC).
        let mut tablet = tablet.clone();
        let values = tablet.serialize_values();
        let timestamps = tablet.serialize_timestamps();
        let (write_to_table, column_categories) = match tablet.column_categories() {
            Some(categories) => (
                Some(true),
                Some(categories.iter().map(|c| c.code()).collect()),
            ),
            None => (None, None),
        };
        self.insert_tablet_raw(
            tablet.target(),
            tablet.measurements().to_vec(),
            tablet.types().iter().map(|t| t.code()).collect(),
            values,
            timestamps,
            tablet.row_count() as i32,
            tablet.is_aligned(),
            write_to_table,
            column_categories,
        )
    }

    /// Insert a tablet from pre-serialized buffers (see protocol spec §3).
    ///
    /// `values` is the column-major value buffer with trailing null bitmaps;
    /// `timestamps` is the `size × 8-byte i64 BE` buffer. Prefer the typed
    /// [`Session::insert_tablet`]; this is the low-level escape hatch.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_tablet_raw(
        &mut self,
        prefix_path: &str,
        measurements: Vec<String>,
        types: Vec<i32>,
        values: Vec<u8>,
        timestamps: Vec<u8>,
        size: i32,
        is_aligned: bool,
        write_to_table: Option<bool>,
        column_categories: Option<Vec<i8>>,
    ) -> Result<()> {
        let req = TSInsertTabletReq::new(
            self.session_id,
            prefix_path.to_string(),
            measurements,
            values,
            timestamps,
            types,
            size,
            is_aligned,
            write_to_table,
            column_categories,
            None,
            None,
            None,
        );
        let status = self.with_retry(|session| {
            // Clone per attempt: a reconnect refreshes the session id.
            let mut req = req.clone();
            req.session_id = session.session_id;
            Ok(session.connection_mut()?.client_mut().insert_tablet(req)?)
        })?;
        self.check_insert_status(&[prefix_path], &status)
    }

    /// Insert a batch of [`Tablet`]s in one `insertTablets` RPC (spec §3.6).
    /// Each tablet is serialized exactly like [`Session::insert_tablet`] —
    /// column-major values with trailing null bitmaps, i64-BE timestamps,
    /// rows sorted by timestamp. `is_aligned` applies to the **whole batch**
    /// (the RPC carries a single flag); the per-tablet aligned flag is
    /// ignored, matching the Java client's `insertTablets` /
    /// `insertAlignedTablets` split.
    ///
    /// Tree model only: the batch request has no `writeToTable` /
    /// `columnCategories` fields, so table-model tablets are rejected —
    /// send those one at a time via [`Session::insert_tablet`].
    pub fn insert_tablets(&mut self, tablets: &[Tablet], is_aligned: bool) -> Result<()> {
        let req = build_insert_tablets_req(self.session_id, tablets, is_aligned)?;
        let status = self.with_retry(|session| {
            // Clone per attempt: a reconnect refreshes the session id.
            let mut req = req.clone();
            req.session_id = session.session_id;
            Ok(session.connection_mut()?.client_mut().insert_tablets(req)?)
        })?;
        let devices: Vec<&str> = req.prefix_paths.iter().map(String::as_str).collect();
        self.check_insert_status(&devices, &status)
    }

    /// [`Session::insert_tablets`] against aligned devices.
    pub fn insert_aligned_tablets(&mut self, tablets: &[Tablet]) -> Result<()> {
        self.insert_tablets(tablets, true)
    }

    /// Insert one row for one device via `insertRecord`. `values[i]` pairs
    /// with `measurements[i]`. The value buffer is row-oriented (per-cell
    /// type marker + big-endian payload), unlike the tablet's column-major
    /// layout.
    ///
    /// [`Value::Null`] cells are dropped together with their measurement
    /// before sending (matching the Java client — the server rejects null
    /// cells against registered series); an all-null row is an error.
    pub fn insert_record(
        &mut self,
        device_id: &str,
        timestamp: i64,
        measurements: Vec<String>,
        values: &[Value],
        is_aligned: bool,
    ) -> Result<()> {
        check_record_arity(&measurements, values)?;
        let (measurements, values) = filter_null_cells(measurements, values);
        if values.is_empty() {
            return Err(Error::Client("all insert values are null".into()));
        }
        let req = TSInsertRecordReq::new(
            self.session_id,
            device_id.to_string(),
            measurements,
            serialize_record_values(&values),
            timestamp,
            is_aligned,
            None,
            None,
        );
        let status = self.with_retry(|session| {
            let mut req = req.clone();
            req.session_id = session.session_id;
            Ok(session.connection_mut()?.client_mut().insert_record(req)?)
        })?;
        self.check_insert_status(&[device_id], &status)
    }

    /// Insert one row per device via `insertRecords` (multi-device batch).
    /// `device_ids`, `timestamps`, `measurements_list` and `values_list`
    /// must have equal length; row `i` targets `device_ids[i]`.
    ///
    /// Null cells are dropped per row; rows that end up all-null are dropped
    /// entirely (Java behavior). An all-null batch is an error.
    pub fn insert_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: &[Vec<Value>],
        is_aligned: bool,
    ) -> Result<()> {
        let n = device_ids.len();
        if timestamps.len() != n || measurements_list.len() != n || values_list.len() != n {
            return Err(Error::Client(format!(
                "insert_records length mismatch: {} devices, {} timestamps, \
                 {} measurement lists, {} value lists",
                n,
                timestamps.len(),
                measurements_list.len(),
                values_list.len()
            )));
        }
        let mut kept_devices = Vec::with_capacity(n);
        let mut kept_timestamps = Vec::with_capacity(n);
        let mut kept_measurements = Vec::with_capacity(n);
        let mut kept_buffers = Vec::with_capacity(n);
        for (((device, ts), measurements), values) in device_ids
            .into_iter()
            .zip(timestamps)
            .zip(measurements_list)
            .zip(values_list)
        {
            check_record_arity(&measurements, values)?;
            let (measurements, values) = filter_null_cells(measurements, values);
            if values.is_empty() {
                continue; // fully-null row: drop, like the Java client
            }
            kept_devices.push(device);
            kept_timestamps.push(ts);
            kept_measurements.push(measurements);
            kept_buffers.push(serialize_record_values(&values));
        }
        if kept_devices.is_empty() {
            return Err(Error::Client("all insert values are null".into()));
        }
        let req = TSInsertRecordsReq::new(
            self.session_id,
            kept_devices,
            kept_measurements,
            kept_buffers,
            kept_timestamps,
            is_aligned,
        );
        let status = self.with_retry(|session| {
            let mut req = req.clone();
            req.session_id = session.session_id;
            Ok(session.connection_mut()?.client_mut().insert_records(req)?)
        })?;
        let devices: Vec<&str> = req.prefix_paths.iter().map(String::as_str).collect();
        self.check_insert_status(&devices, &status)
    }

    /// Insert multiple rows for one device via `insertRecordsOfOneDevice`.
    /// Rows are stably sorted by timestamp client-side first (the server
    /// requires ascending time), matching the Java/Python clients.
    ///
    /// Null cells are dropped per row; rows that end up all-null are dropped
    /// entirely (Java behavior). An all-null batch is an error.
    pub fn insert_records_of_one_device(
        &mut self,
        device_id: &str,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: &[Vec<Value>],
        is_aligned: bool,
    ) -> Result<()> {
        let n = timestamps.len();
        if measurements_list.len() != n || values_list.len() != n {
            return Err(Error::Client(format!(
                "insert_records_of_one_device length mismatch: {} timestamps, \
                 {} measurement lists, {} value lists",
                n,
                measurements_list.len(),
                values_list.len()
            )));
        }
        let mut kept_timestamps = Vec::with_capacity(n);
        let mut kept_measurements = Vec::with_capacity(n);
        let mut kept_values = Vec::with_capacity(n);
        for ((ts, measurements), values) in timestamps
            .into_iter()
            .zip(measurements_list)
            .zip(values_list)
        {
            check_record_arity(&measurements, values)?;
            let (measurements, values) = filter_null_cells(measurements, values);
            if values.is_empty() {
                continue; // fully-null row: drop, like the Java client
            }
            kept_timestamps.push(ts);
            kept_measurements.push(measurements);
            kept_values.push(values);
        }
        if kept_timestamps.is_empty() {
            return Err(Error::Client("all insert values are null".into()));
        }
        let (timestamps, measurements_list, values_buffers) =
            sort_one_device_rows(kept_timestamps, kept_measurements, &kept_values);
        let req = TSInsertRecordsOfOneDeviceReq::new(
            self.session_id,
            device_id.to_string(),
            measurements_list,
            values_buffers,
            timestamps,
            is_aligned,
        );
        let status = self.with_retry(|session| {
            let mut req = req.clone();
            req.session_id = session.session_id;
            Ok(session
                .connection_mut()?
                .client_mut()
                .insert_records_of_one_device(req)?)
        })?;
        self.check_insert_status(&[device_id], &status)
    }

    /// [`Session::insert_record`] against an aligned device
    /// (`isAligned=true` on the same RPC).
    pub fn insert_aligned_record(
        &mut self,
        device_id: &str,
        timestamp: i64,
        measurements: Vec<String>,
        values: &[Value],
    ) -> Result<()> {
        self.insert_record(device_id, timestamp, measurements, values, true)
    }

    /// [`Session::insert_records`] against aligned devices.
    pub fn insert_aligned_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: &[Vec<Value>],
    ) -> Result<()> {
        self.insert_records(device_ids, timestamps, measurements_list, values_list, true)
    }

    /// [`Session::insert_records_of_one_device`] against an aligned device.
    pub fn insert_aligned_records_of_one_device(
        &mut self,
        device_id: &str,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: &[Vec<Value>],
    ) -> Result<()> {
        self.insert_records_of_one_device(
            device_id,
            timestamps,
            measurements_list,
            values_list,
            true,
        )
    }

    /// Close the session: best-effort `closeSession` RPC, then drop the
    /// connection.
    pub fn close(&mut self) -> Result<()> {
        if let Some(mut connection) = self.connection.take() {
            let req = TSCloseSessionReq::new(self.session_id);
            if let Err(e) = connection.client_mut().close_session(req) {
                log::debug!("closeSession failed (ignored): {e}");
            }
        }
        self.session_id = -1;
        self.statement_id = -1;
        Ok(())
    }

    fn statement_req(&self, sql: &str) -> TSExecuteStatementReq {
        TSExecuteStatementReq::new(
            self.session_id,
            sql.to_string(),
            self.statement_id,
            self.config.fetch_size,
            self.config.query_timeout_ms,
            true,  // enableRedirectQuery
            false, // jdbcQuery=false forces TsBlock queryResult responses
        )
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
impl Session {
    /// Test hook: install a raw connection (no handshake) so transport
    /// failure paths can be exercised without a live server.
    pub(crate) fn test_inject_connection(&mut self, connection: Connection) {
        self.last_endpoint = Some(connection.endpoint().clone());
        self.connection = Some(connection);
        self.session_id = 1;
        self.statement_id = 1;
    }

    /// Test hook: seed a redirect hint as if a status-400 insert had.
    pub(crate) fn test_inject_redirect_hint(&mut self, device_id: &str, endpoint: Endpoint) {
        self.redirect_cache.put(device_id, endpoint);
    }
}

/// Assembles a `TSInsertTabletsReq` (spec §3.6): one entry per tablet in
/// each parallel list, every buffer serialized exactly like the
/// single-tablet path. Kept as a free function so request assembly is
/// testable without a connection. Rejects empty batches and table-model
/// tablets (the batch RPC has no table-model fields).
fn build_insert_tablets_req(
    session_id: i64,
    tablets: &[Tablet],
    is_aligned: bool,
) -> Result<TSInsertTabletsReq> {
    if tablets.is_empty() {
        return Err(Error::Client(
            "insert_tablets called with no tablets".into(),
        ));
    }
    let n = tablets.len();
    let mut prefix_paths = Vec::with_capacity(n);
    let mut measurements_list = Vec::with_capacity(n);
    let mut values_list = Vec::with_capacity(n);
    let mut timestamps_list = Vec::with_capacity(n);
    let mut types_list = Vec::with_capacity(n);
    let mut size_list = Vec::with_capacity(n);
    for tablet in tablets {
        if tablet.is_table_model() {
            return Err(Error::Client(format!(
                "insert_tablets is tree-model only; tablet for table {:?} \
                 must go through insert_tablet",
                tablet.target()
            )));
        }
        // Serialization sorts in place; clone so the caller's tablet order
        // is untouched (the clone is cheap relative to the RPC).
        let mut tablet = tablet.clone();
        values_list.push(tablet.serialize_values());
        timestamps_list.push(tablet.serialize_timestamps());
        prefix_paths.push(tablet.target().to_string());
        measurements_list.push(tablet.measurements().to_vec());
        types_list.push(tablet.types().iter().map(|t| t.code()).collect());
        size_list.push(tablet.row_count() as i32);
    }
    Ok(TSInsertTabletsReq::new(
        session_id,
        prefix_paths,
        measurements_list,
        values_list,
        timestamps_list,
        types_list,
        size_list,
        is_aligned,
    ))
}

/// Stably sorts one-device record rows by timestamp — reordering the
/// measurement lists in step and serializing each row's value buffer in the
/// sorted order (Java `genTSInsertRecordsOfOneDeviceReq`; the server
/// requires ascending time).
fn sort_one_device_rows(
    timestamps: Vec<i64>,
    measurements_list: Vec<Vec<String>>,
    values_list: &[Vec<Value>],
) -> (Vec<i64>, Vec<Vec<String>>, Vec<Vec<u8>>) {
    let mut order: Vec<usize> = (0..timestamps.len()).collect();
    order.sort_by_key(|&i| timestamps[i]); // stable
    if order.iter().enumerate().all(|(pos, &i)| pos == i) {
        let buffers = values_list
            .iter()
            .map(|v| serialize_record_values(v))
            .collect();
        return (timestamps, measurements_list, buffers);
    }
    (
        order.iter().map(|&i| timestamps[i]).collect(),
        order
            .iter()
            .map(|&i| measurements_list[i].clone())
            .collect(),
        order
            .iter()
            .map(|&i| serialize_record_values(&values_list[i]))
            .collect(),
    )
}

/// Drops [`Value::Null`] cells together with their measurements (Java
/// `filterNullValueAndMeasurement`): the server rejects a null cell against
/// a registered series, and its bare `-2` marker carries no type to
/// auto-create one — omitting the measurement is the protocol's way of
/// writing "no value".
fn filter_null_cells(measurements: Vec<String>, values: &[Value]) -> (Vec<String>, Vec<Value>) {
    if !values.iter().any(Value::is_null) {
        return (measurements, values.to_vec());
    }
    measurements
        .into_iter()
        .zip(values.iter().cloned())
        .filter(|(_, v)| !v.is_null())
        .unzip()
}

/// One record row must pair each measurement with exactly one value.
fn check_record_arity(measurements: &[String], values: &[Value]) -> Result<()> {
    if measurements.len() != values.len() {
        return Err(Error::Client(format!(
            "record has {} values for {} measurements",
            values.len(),
            measurements.len()
        )));
    }
    Ok(())
}

/// Map a `TSStatus` to success or [`Error::Server`] (protocol spec §2):
/// 200 OK; 400 is a **successful** write with a redirect hint; 302 succeeds
/// iff every subStatus is itself OK.
pub fn check_status(status: &TSStatus) -> Result<()> {
    match status.code {
        status_code::SUCCESS_STATUS | status_code::REDIRECTION_RECOMMEND => Ok(()),
        status_code::MULTIPLE_ERROR => {
            let failed = status
                .sub_status
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|s| check_status(s).is_err());
            match failed {
                None => Ok(()),
                Some(s) => Err(Error::Server {
                    code: s.code,
                    message: s.message.clone().unwrap_or_default(),
                }),
            }
        }
        code => Err(Error::Server {
            code,
            message: status.message.clone().unwrap_or_default(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the live tests that create/delete databases: concurrent
    /// `DELETE DATABASE` on a small single-node server can transiently
    /// leave no available DataRegionGroups (server error 906) for other
    /// tests' inserts.
    static LIVE_DB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn status(code: i32) -> TSStatus {
        TSStatus::new(code, None, None, None, None, None)
    }

    #[test]
    fn default_config() {
        let cfg = SessionConfig::default();
        assert_eq!(cfg.endpoints[0], Endpoint::new("localhost", 6667));
        assert_eq!(cfg.username, "root");
        assert_eq!(cfg.password, "root");
        assert_eq!(cfg.sql_dialect, "tree");
        assert_eq!(cfg.fetch_size, 1024);
        assert_eq!(cfg.query_timeout_ms, 60_000);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
        assert!(cfg.database.is_none());
        assert!(cfg.enable_auto_reconnect);
        assert_eq!(cfg.max_reconnect_attempts, 3);
        assert_eq!(cfg.retry_interval, Duration::from_secs(1));
        assert!(cfg.enable_redirection);
        assert!(!cfg.enable_rpc_compression);
        #[cfg(feature = "tls")]
        {
            assert!(!cfg.use_ssl);
            assert!(cfg.ca_cert_path.is_none());
            assert!(!cfg.accept_invalid_certs);
            assert!(cfg.domain_override.is_none());
            assert!(cfg.client_cert_path.is_none());
            assert!(cfg.client_key_path.is_none());
        }
    }

    /// Config → connection options mapping: compression selects the compact
    /// protocol, `use_ssl` (feature `tls`) carries the TLS fields through.
    #[test]
    fn connection_options_from_config() {
        use crate::connection::RpcProtocol;

        let cfg = SessionConfig::default();
        let options = cfg.connection_options();
        assert_eq!(options.connect_timeout, cfg.connect_timeout);
        assert_eq!(options.protocol, RpcProtocol::Binary);
        #[cfg(feature = "tls")]
        assert!(options.tls.is_none());

        let cfg = SessionConfig {
            enable_rpc_compression: true,
            ..Default::default()
        };
        assert_eq!(cfg.connection_options().protocol, RpcProtocol::Compact);

        #[cfg(feature = "tls")]
        {
            let cfg = SessionConfig {
                use_ssl: true,
                ca_cert_path: Some("/certs/ca.pem".into()),
                accept_invalid_certs: true,
                domain_override: Some("iotdb.internal".into()),
                client_cert_path: Some("/certs/client.pem".into()),
                client_key_path: Some("/certs/client-key.pem".into()),
                ..Default::default()
            };
            let tls = cfg.connection_options().tls.expect("tls options");
            assert_eq!(tls.ca_cert_path.as_deref(), Some("/certs/ca.pem".as_ref()));
            assert!(tls.accept_invalid_certs);
            assert_eq!(tls.domain_override.as_deref(), Some("iotdb.internal"));
            assert_eq!(
                tls.client_cert_path.as_deref(),
                Some("/certs/client.pem".as_ref())
            );
            assert_eq!(
                tls.client_key_path.as_deref(),
                Some("/certs/client-key.pem".as_ref())
            );

            // Protocol × TLS are independent axes: every combination maps
            // through, ConnectionOptions carries both.
            for (compression, ssl) in [(false, false), (false, true), (true, false), (true, true)] {
                let cfg = SessionConfig {
                    enable_rpc_compression: compression,
                    use_ssl: ssl,
                    ..Default::default()
                };
                let options = cfg.connection_options();
                assert_eq!(
                    options.protocol,
                    if compression {
                        RpcProtocol::Compact
                    } else {
                        RpcProtocol::Binary
                    }
                );
                assert_eq!(options.tls.is_some(), ssl);
            }
        }
    }

    #[test]
    fn config_from_node_urls() {
        let cfg = SessionConfig::default()
            .with_node_urls(&["10.0.0.1:6667", "[::1]:6668"])
            .unwrap();
        assert_eq!(
            cfg.endpoints,
            vec![Endpoint::new("10.0.0.1", 6667), Endpoint::new("::1", 6668)]
        );
        assert!(SessionConfig::default()
            .with_node_urls(&["nohost"])
            .is_err());
    }

    #[test]
    fn status_200_is_ok() {
        assert!(check_status(&status(200)).is_ok());
    }

    #[test]
    fn status_400_redirect_is_success() {
        assert!(check_status(&status(400)).is_ok());
    }

    #[test]
    fn status_302_all_sub_ok_is_success() {
        let mut s = status(302);
        s.sub_status = Some(vec![Box::new(status(200)), Box::new(status(400))]);
        assert!(check_status(&s).is_ok());
    }

    #[test]
    fn status_302_mixed_sub_is_error() {
        let mut s = status(302);
        s.sub_status = Some(vec![Box::new(status(200)), Box::new(status(500))]);
        match check_status(&s) {
            Err(Error::Server { code, .. }) => assert_eq!(code, 500),
            other => panic!("expected server error, got {other:?}"),
        }
    }

    #[test]
    fn status_500_is_error() {
        let mut s = status(500);
        s.message = Some("boom".into());
        match check_status(&s) {
            Err(Error::Server { code, message }) => {
                assert_eq!(code, 500);
                assert_eq!(message, "boom");
            }
            other => panic!("expected server error, got {other:?}"),
        }
    }

    #[test]
    fn calls_on_closed_session_fail() {
        let mut session = Session::new(SessionConfig::default());
        assert!(!session.is_open());
        assert!(session.execute_non_query("SHOW DATABASES").is_err());
        assert!(session.execute_query("SELECT 1").is_err());
        assert!(session.close().is_ok()); // close on never-opened is fine
    }

    #[test]
    fn record_arity_is_validated() {
        use crate::data::Value;
        assert!(check_record_arity(&["s1".into()], &[Value::Int32(1)]).is_ok());
        assert!(check_record_arity(&["s1".into(), "s2".into()], &[Value::Int32(1)]).is_err());
        // Validation fires before any connection use — errors on a closed
        // session must be the arity error, not "session is not open".
        let mut session = Session::new(SessionConfig::default());
        let err = session
            .insert_record("root.sg.d1", 1, vec!["s1".into()], &[], false)
            .unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("0 values for 1 measurements")));
    }

    #[test]
    fn null_cells_are_filtered_with_their_measurements() {
        use crate::data::Value;
        let (m, v) = filter_null_cells(
            vec!["a".into(), "b".into(), "c".into()],
            &[Value::Int32(1), Value::Null, Value::Boolean(true)],
        );
        assert_eq!(m, ["a", "c"]);
        assert_eq!(v, [Value::Int32(1), Value::Boolean(true)]);

        // No nulls: passthrough.
        let (m, v) = filter_null_cells(vec!["a".into()], &[Value::Int32(1)]);
        assert_eq!(m, ["a"]);
        assert_eq!(v, [Value::Int32(1)]);

        // All-null rows are rejected before touching the connection.
        let mut session = Session::new(SessionConfig::default());
        let err = session
            .insert_record("root.sg.d1", 1, vec!["s1".into()], &[Value::Null], false)
            .unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("all insert values are null")));
        let err = session
            .insert_records_of_one_device(
                "root.sg.d1",
                vec![1],
                vec![vec!["s1".into()]],
                &[vec![Value::Null]],
                false,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("all insert values are null")));
    }

    #[test]
    fn one_device_rows_are_sorted_by_timestamp() {
        use crate::data::record::serialize_record_values;
        use crate::data::Value;
        let rows = [
            vec![Value::Int32(30)],
            vec![Value::Int32(10)],
            vec![Value::Int32(20)],
        ];
        let (ts, ms, bufs) = sort_one_device_rows(
            vec![3, 1, 2],
            vec![vec!["a".into()], vec!["b".into()], vec!["c".into()]],
            &rows,
        );
        assert_eq!(ts, [1, 2, 3]);
        assert_eq!(
            ms,
            [vec!["b".to_string()], vec!["c".into()], vec!["a".into()]]
        );
        assert_eq!(
            bufs,
            [
                serialize_record_values(&[Value::Int32(10)]),
                serialize_record_values(&[Value::Int32(20)]),
                serialize_record_values(&[Value::Int32(30)]),
            ]
        );

        // Already-sorted input passes through unchanged (fast path), and the
        // sort is stable for equal timestamps.
        let (ts, ms, _) = sort_one_device_rows(
            vec![5, 5],
            vec![vec!["first".into()], vec!["second".into()]],
            &[vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
        assert_eq!(ts, [5, 5]);
        assert_eq!(ms, [vec!["first".to_string()], vec!["second".into()]]);
    }

    /// insert_tablets request assembly: parallel lists pair 1:1 with the
    /// input tablets, and every buffer matches what the single-tablet path
    /// (`serialize_values`/`serialize_timestamps`) produces for the same
    /// tablet — including the timestamp sort and null bitmaps.
    #[test]
    fn insert_tablets_request_assembly() {
        use crate::data::{tablet::Tablet, TSDataType, Value};

        let mut t1 = Tablet::new(
            "root.sg.d1",
            vec!["i".into(), "s".into()],
            vec![TSDataType::Int32, TSDataType::Text],
        )
        .unwrap();
        // Unsorted rows + a null: serialization must sort and bitmap.
        t1.add_row(2, vec![Some(Value::Int32(20)), None]).unwrap();
        t1.add_row(1, vec![None, Some(Value::Text("a".into()))])
            .unwrap();
        let mut t2 = Tablet::new("root.sg.d2", vec!["b".into()], vec![TSDataType::Boolean]) //
            .unwrap();
        t2.add_row(5, vec![Some(Value::Boolean(true))]).unwrap();

        let req = build_insert_tablets_req(7, &[t1.clone(), t2.clone()], false).unwrap();
        assert_eq!(req.session_id, 7);
        assert_eq!(req.prefix_paths, ["root.sg.d1", "root.sg.d2"]);
        assert_eq!(
            req.measurements_list,
            [vec!["i".to_string(), "s".into()], vec!["b".into()]]
        );
        assert_eq!(req.types_list, [vec![1, 5], vec![0]]); // Int32+Text, Boolean
        assert_eq!(req.size_list, [2, 1]);
        assert_eq!(req.is_aligned, Some(false));
        // Byte-identical to the single-tablet serialization helpers.
        assert_eq!(
            req.values_list,
            [t1.serialize_values(), t2.serialize_values()]
        );
        assert_eq!(
            req.timestamps_list,
            [t1.serialize_timestamps(), t2.serialize_timestamps()]
        );
        // Known bytes for t1 after sorting (ts 1 first): int col null@row0,
        // text col null@row1.
        assert_eq!(
            req.values_list[0],
            [
                0x80, 0x00, 0x00, 0x00, // i row0: null placeholder (i32::MIN)
                0x00, 0x00, 0x00, 0x14, // i row1: 20
                0x00, 0x00, 0x00, 0x01, b'a', // s row0: "a"
                0x00, 0x00, 0x00, 0x00, // s row1: null placeholder (empty)
                0x01, 0x01, // i bitmap: flag + row 0 null (LSB-first)
                0x01, 0x02, // s bitmap: flag + row 1 null
            ]
        );
        assert_eq!(
            req.timestamps_list[0],
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
        );

        // The batch flag is a single RPC-level field.
        let req = build_insert_tablets_req(7, &[t2], true).unwrap();
        assert_eq!(req.is_aligned, Some(true));
    }

    #[test]
    fn insert_tablets_rejects_empty_and_table_model() {
        use crate::data::{tablet::Tablet, ColumnCategory, TSDataType};

        let err = build_insert_tablets_req(1, &[], false).unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("no tablets")));

        let tree = Tablet::new("root.sg.d1", vec!["s".into()], vec![TSDataType::Int32]).unwrap();
        let table = Tablet::new_table(
            "sensors",
            vec!["f1".into()],
            vec![TSDataType::Double],
            vec![ColumnCategory::Field],
        )
        .unwrap();
        // A table-model tablet anywhere in the batch fails the whole call.
        let err = build_insert_tablets_req(1, &[tree, table], false).unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("tree-model only")));

        // The errors fire before any connection use.
        let mut session = Session::new(SessionConfig::default());
        let err = session.insert_tablets(&[], false).unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("no tablets")));
    }

    #[test]
    fn insert_records_length_mismatch_is_client_error() {
        use crate::data::Value;
        let mut session = Session::new(SessionConfig::default());
        let err = session
            .insert_records(
                vec!["root.sg.d1".into(), "root.sg.d2".into()],
                vec![1], // 2 devices but 1 timestamp
                vec![vec!["s1".into()], vec!["s1".into()]],
                &[vec![Value::Int32(1)], vec![Value::Int32(2)]],
                false,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("length mismatch")));

        let err = session
            .insert_records_of_one_device(
                "root.sg.d1",
                vec![1, 2],
                vec![vec!["s1".into()]], // 2 timestamps but 1 measurement list
                &[vec![Value::Int32(1)]],
                false,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Client(m) if m.contains("length mismatch")));
    }

    #[test]
    fn insert_status_records_redirect_hint() {
        use crate::protocol::common::TEndPoint;
        let mut session = Session::new(SessionConfig::default());
        let mut s = status(400);
        s.redirect_node = Some(TEndPoint::new("10.1.1.1".to_string(), 6667));
        // Status 400 is still a successful write…
        assert!(session.check_insert_status(&["root.sg.d1"], &s).is_ok());
        // …and its redirect node is now cached for the device.
        assert_eq!(
            session.redirect_hint("root.sg.d1"),
            Some(Endpoint::new("10.1.1.1", 6667))
        );
        assert_eq!(session.redirect_hint("root.sg.other"), None);
        assert_eq!(session.redirect_cache_stats().size, 1);
        session.clear_redirect_cache();
        assert_eq!(session.redirect_hint("root.sg.d1"), None);

        // With redirection disabled nothing is recorded (but 400 still OK).
        let mut session = Session::new(SessionConfig {
            enable_redirection: false,
            ..Default::default()
        });
        assert!(session.check_insert_status(&["root.sg.d1"], &s).is_ok());
        assert_eq!(session.redirect_hint("root.sg.d1"), None);
    }

    /// Accept-then-drop listener: TCP connects succeed, but every RPC on
    /// the connection dies at the Thrift level. Returns the endpoint and a
    /// shared accept counter (the acceptor thread is leaked; it ends with
    /// the test process).
    fn accept_then_drop_listener() -> (Endpoint, std::sync::Arc<AtomicUsize>) {
        use std::net::TcpListener;
        use std::sync::Arc;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = accepts.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        drop(s); // immediate close ⇒ peer reads EOF
                    }
                    Err(_) => break,
                }
            }
        });
        (Endpoint::new("127.0.0.1", port), accepts)
    }

    /// A transport-level RPC failure with auto-reconnect enabled must drive
    /// `max_reconnect_attempts` full reconnect passes (visible as fresh TCP
    /// connects) and, when they all fail, surface the **original** error.
    #[test]
    fn transport_failure_reconnects_then_surfaces_original_error() {
        let (endpoint, accepts) = accept_then_drop_listener();
        let mut session = Session::new(SessionConfig {
            endpoints: vec![endpoint.clone()],
            connect_timeout: Duration::from_millis(500),
            max_reconnect_attempts: 2,
            retry_interval: Duration::from_millis(10),
            ..Default::default()
        });
        let connection = Connection::open(
            endpoint.clone(),
            &ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                ..Default::default()
            },
        )
        .expect("connect to listener");
        session.test_inject_connection(connection);
        assert_eq!(session.current_endpoint(), Some(&endpoint));

        let err = session.execute_non_query("SHOW DATABASES").unwrap_err();
        assert!(matches!(err, Error::Thrift(_)), "got {err:?}");
        // 1 initial connection + 2 reconnect passes × 1 endpoint. Each
        // failed openSession implies its connection was accepted+dropped,
        // so the counter is settled once the error is back.
        assert_eq!(accepts.load(Ordering::SeqCst), 3);
        // The failed reconnect left the session without a connection.
        assert!(!session.is_open());
    }

    /// With auto-reconnect disabled the op fails once: no reconnect
    /// connects, original error surfaced directly.
    #[test]
    fn no_reconnect_when_disabled() {
        let (endpoint, accepts) = accept_then_drop_listener();
        let mut session = Session::new(SessionConfig {
            endpoints: vec![endpoint.clone()],
            connect_timeout: Duration::from_millis(500),
            enable_auto_reconnect: false,
            ..Default::default()
        });
        let connection = Connection::open(
            endpoint,
            &ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                ..Default::default()
            },
        )
        .expect("connect to listener");
        session.test_inject_connection(connection);

        let err = session.execute_non_query("SHOW DATABASES").unwrap_err();
        assert!(matches!(err, Error::Thrift(_)), "got {err:?}");
        assert_eq!(accepts.load(Ordering::SeqCst), 1, "no reconnect attempts");
    }

    #[test]
    fn open_without_endpoints_fails() {
        let mut session = Session::new(SessionConfig {
            endpoints: vec![],
            ..Default::default()
        });
        assert!(session.open().is_err());
        assert!(!session.is_open());
    }

    /// End-to-end smoke test against a live server; skipped when no IoTDB
    /// instance is reachable on localhost:6667.
    #[test]
    fn live_server_roundtrip() {
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_server_roundtrip: no IoTDB server on 127.0.0.1:6667");
            return;
        }

        let mut session = Session::new(SessionConfig::default());
        session.open().expect("open session");
        assert!(session.is_open());

        {
            let mut dataset = session.execute_query("SHOW DATABASES").expect("query");
            assert!(!dataset.columns().is_empty());
            while let Some(row) = dataset.next_row().expect("next_row") {
                assert_eq!(row.values.len(), dataset.columns().len());
            }
        } // dataset drop closes the query and releases the session borrow

        session.close().expect("close session");
        assert!(!session.is_open());
    }

    /// RPC compression (TCompactProtocol) against a live server.
    ///
    /// Verified 2026-07-13 against apache/iotdb:2.0.6-standalone: the server
    /// speaks exactly **one** protocol, fixed by its config
    /// `dn_rpc_thrift_compression_enable` (default `false` → binary) — there
    /// is no per-connection auto-detection (server source:
    /// `AbstractThriftServiceThread.getProtocolFactory(compress)` picks a
    /// single factory). Matrix observed live:
    /// compact↔compression-enabled server: OK; compact↔default server: EOF
    /// at openSession; binary↔compression-enabled server: EOF.
    ///
    /// So this test adapts: if the compact open succeeds the server has
    /// compression enabled and we assert a full insert+query roundtrip;
    /// if it fails at the transport level (the expected outcome against the
    /// default docker image) we skip with a message — the clean transport
    /// failure is itself the verified mismatch behavior.
    #[test]
    fn live_rpc_compression_roundtrip() {
        use crate::data::{tablet::Tablet, TSDataType, Value};
        use std::net::TcpStream;
        // IOTDB_COMPACT_URL points at a compression-enabled server (e.g.
        // docker run -e dn_rpc_thrift_compression_enable=true …) to force
        // the positive roundtrip; default is the standard test server.
        let url = std::env::var("IOTDB_COMPACT_URL").unwrap_or_else(|_| "127.0.0.1:6667".into());
        let endpoint = Endpoint::parse(&url).expect("IOTDB_COMPACT_URL");
        if TcpStream::connect_timeout(
            &format!("{}:{}", endpoint.host, endpoint.port)
                .parse()
                .unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_rpc_compression_roundtrip: no IoTDB server on {url}");
            return;
        }

        const DB: &str = "root.rusttest_compact";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let mut session = Session::new(SessionConfig {
            endpoints: vec![endpoint],
            enable_rpc_compression: true,
            // Fail fast: a protocol mismatch dies at the first RPC, and
            // reconnect passes cannot fix it.
            enable_auto_reconnect: false,
            max_reconnect_attempts: 1,
            connect_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        match session.open() {
            Err(Error::Thrift(e)) => {
                eprintln!(
                    "skipping live_rpc_compression_roundtrip: server rejected the compact \
                     protocol ({e}) — expected when dn_rpc_thrift_compression_enable=false \
                     (the default); IoTDB has no per-connection protocol auto-detection"
                );
                return;
            }
            other => other.expect("open compact session"),
        }

        // The server accepted the compact handshake ⇒ compression is
        // enabled server-side; the whole roundtrip must work.
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");
        let mut tablet = Tablet::new(
            format!("{DB}.d1"),
            vec!["v".into()],
            vec![TSDataType::Int32],
        )
        .expect("tablet");
        tablet
            .add_row(1, vec![Some(Value::Int32(42))])
            .expect("add_row");
        session.insert_tablet(&tablet).expect("insert_tablet");
        let mut rows = Vec::new();
        {
            let mut dataset = session
                .execute_query(&format!("SELECT v FROM {DB}.d1"))
                .expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                rows.push((row.timestamp.expect("timestamp"), row.values[0].clone()));
            }
        }
        assert_eq!(rows, [(1, Value::Int32(42))]);
        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// Live TLS against a real IoTDB requires a server with
    /// `enable_thrift_ssl=true` plus its certificate — not part of the
    /// standard docker test topology. Opt in by pointing
    /// `IOTDB_TLS_URL` (`host:port`) at such a server; otherwise this
    /// skips. The TLS handshake/transport path itself is covered by the
    /// loopback tests in `connection::tls_tests`.
    #[cfg(feature = "tls")]
    #[test]
    fn live_tls_roundtrip() {
        let Ok(url) = std::env::var("IOTDB_TLS_URL") else {
            eprintln!(
                "skipping live_tls_roundtrip: set IOTDB_TLS_URL=host:port to a TLS-enabled \
                 IoTDB (enable_thrift_ssl=true); the standard test server is plain TCP"
            );
            return;
        };
        let mut session = Session::new(SessionConfig {
            endpoints: vec![Endpoint::parse(&url).expect("IOTDB_TLS_URL")],
            use_ssl: true,
            ca_cert_path: std::env::var_os("IOTDB_TLS_CA").map(Into::into),
            accept_invalid_certs: std::env::var_os("IOTDB_TLS_INSECURE").is_some(),
            domain_override: std::env::var("IOTDB_TLS_DOMAIN").ok(),
            // A client identity is only *verified* by a server with
            // thrift_ssl_client_auth=true (not available in 2.0.6, whose
            // RPC service hardcodes requireClientAuth(false)); setting these
            // against any TLS server still proves the identity loads and
            // the handshake tolerates it.
            client_cert_path: std::env::var_os("IOTDB_TLS_CLIENT_CERT").map(Into::into),
            client_key_path: std::env::var_os("IOTDB_TLS_CLIENT_KEY").map(Into::into),
            ..Default::default()
        });
        session.open().expect("open TLS session");

        // Full write+read roundtrip over the TLS transport, not just a
        // metadata query.
        const DB: &str = "root.rusttest_tls";
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");
        session
            .execute_non_query(&format!(
                "CREATE TIMESERIES {DB}.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN"
            ))
            .expect("create timeseries");
        session
            .execute_non_query(&format!(
                "INSERT INTO {DB}.d1(timestamp, s1) VALUES (1, 42)"
            ))
            .expect("insert");
        let mut rows = Vec::new();
        {
            let mut dataset = session
                .execute_query(&format!("SELECT s1 FROM {DB}.d1"))
                .expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                rows.push((row.timestamp.expect("timestamp"), row.values[0].clone()));
            }
        }
        assert_eq!(rows, [(1, crate::data::Value::Int32(42))]);
        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// DATE wire-format adjudication test (goal V1). Inserts one DATE row via
    /// the tablet binary path (i32 yyyyMMdd) and one via a SQL date literal
    /// (parsed server-side), then reads both back: if the tablet encoding is
    /// correct, both rows decode to the same i32 for the same calendar date.
    /// This breaks the write-read circularity a plain roundtrip would have.
    /// Skipped when no IoTDB instance is reachable on localhost:6667.
    #[test]
    fn live_date_encoding_adjudication() {
        use crate::data::{tablet::Tablet, TSDataType, Value};
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!(
                "skipping live_date_encoding_adjudication: no IoTDB server on 127.0.0.1:6667"
            );
            return;
        }

        const DB: &str = "root.rusttest_date";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // 2026-07-13 as yyyyMMdd; deliberately not near epoch so a
        // days-since-epoch misinterpretation cannot coincide.
        const DATE_YYYYMMDD: i32 = 20260713;

        let mut session = Session::new(SessionConfig::default());
        session.open().expect("open session");
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");

        // Row at ts=1: tablet binary path.
        let mut tablet = Tablet::new(
            format!("{DB}.d1"),
            vec!["dt".into()],
            vec![TSDataType::Date],
        )
        .expect("tablet");
        tablet
            .add_row(1, vec![Some(Value::Date(DATE_YYYYMMDD))])
            .expect("add_row");
        session.insert_tablet(&tablet).expect("insert_tablet");

        // Row at ts=2: SQL literal path — server parses the calendar date itself.
        session
            .execute_non_query(&format!(
                "INSERT INTO {DB}.d1(timestamp, dt) VALUES (2, '2026-07-13')"
            ))
            .expect("insert via SQL literal");

        // Read both back; they must decode identically.
        let mut got: Vec<(i64, Value)> = Vec::new();
        {
            let mut dataset = session
                .execute_query(&format!("SELECT dt FROM {DB}.d1 ORDER BY time"))
                .expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                got.push((row.timestamp.expect("timestamp"), row.values[0].clone()));
            }
        }
        assert_eq!(got.len(), 2, "expected both rows back");
        assert_eq!(
            got[0].1,
            Value::Date(DATE_YYYYMMDD),
            "tablet-path DATE readback"
        );
        assert_eq!(
            got[1].1,
            Value::Date(DATE_YYYYMMDD),
            "SQL-literal DATE must decode to the same i32 as the tablet path — \
             proves yyyyMMdd is the server's wire semantics"
        );

        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// V3 regression: with auto-reconnect and redirection at their default
    /// (enabled) settings, normal write/read ops behave exactly as before.
    /// On a single-node server no status 400 is ever issued, so the
    /// redirect cache must stay empty. Skipped when no IoTDB instance is
    /// reachable on localhost:6667.
    #[test]
    fn live_ops_with_retry_and_redirection_enabled() {
        use crate::data::{tablet::Tablet, TSDataType, Value};
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!(
                "skipping live_ops_with_retry_and_redirection_enabled: \
                 no IoTDB server on 127.0.0.1:6667"
            );
            return;
        }

        const DB: &str = "root.rusttest_retry";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let cfg = SessionConfig::default();
        assert!(cfg.enable_auto_reconnect && cfg.enable_redirection);

        let mut session = Session::new(cfg);
        session.open().expect("open session");
        assert_eq!(
            session.current_endpoint(),
            Some(&Endpoint::new("localhost", 6667))
        );

        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");

        // Tablet + record inserts, both passing through check_insert_status.
        let mut tablet = Tablet::new(
            format!("{DB}.d1"),
            vec!["v".into()],
            vec![TSDataType::Int32],
        )
        .expect("tablet");
        tablet
            .add_row(1, vec![Some(Value::Int32(7))])
            .expect("add_row");
        session.insert_tablet(&tablet).expect("insert_tablet");
        session
            .insert_record(
                &format!("{DB}.d1"),
                2,
                vec!["v".into()],
                &[Value::Int32(8)],
                false,
            )
            .expect("insert_record");

        // Single node ⇒ the server never recommends a redirect.
        assert_eq!(session.redirect_cache_stats().size, 0);
        assert_eq!(session.redirect_hint(&format!("{DB}.d1")), None);

        let mut rows = 0;
        {
            let mut dataset = session
                .execute_query(&format!("SELECT v FROM {DB}.d1"))
                .expect("query");
            while dataset.next_row().expect("next_row").is_some() {
                rows += 1;
            }
        }
        assert_eq!(rows, 2);

        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// Value-asserting live roundtrip: unsorted input, nulls, and a row count
    /// that is a multiple of 8 (stresses the rows/8+1 bitmap padding byte).
    /// Skipped when no IoTDB instance is reachable on localhost:6667.
    #[test]
    fn live_insert_tablet_readback() {
        use crate::data::{tablet::Tablet, TSDataType, Value};
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_insert_tablet_readback: no IoTDB server on 127.0.0.1:6667");
            return;
        }

        const DB: &str = "root.rusttest_readback";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        const ROWS: i64 = 16;

        let mut session = Session::new(SessionConfig::default());
        session.open().expect("open session");
        // Fresh database (ignore error if it doesn't exist yet).
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");

        let mut tablet = Tablet::new(
            format!("{DB}.d1"),
            vec!["ival".into(), "dval".into(), "sval".into()],
            vec![TSDataType::Int32, TSDataType::Double, TSDataType::Text],
        )
        .expect("tablet");
        // Insert in reverse timestamp order; serialization must sort.
        for ts in (0..ROWS).rev() {
            let ival = if ts % 3 == 0 {
                None
            } else {
                Some(Value::Int32(ts as i32 * 10))
            };
            let dval = if ts % 5 == 0 {
                None
            } else {
                Some(Value::Double(ts as f64 + 0.5))
            };
            let sval = Some(Value::Text(format!("row-{ts}")));
            tablet.add_row(ts, vec![ival, dval, sval]).expect("add_row");
        }
        session.insert_tablet(&tablet).expect("insert_tablet");

        // Read back all rows and assert every cell.
        let mut seen = 0i64;
        {
            let mut dataset = session
                .execute_query(&format!("SELECT ival, dval, sval FROM {DB}.d1"))
                .expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                let ts = row.timestamp.expect("timestamp");
                assert_eq!(ts, seen, "rows must come back in ascending time order");
                let expect_ival = if ts % 3 == 0 {
                    Value::Null
                } else {
                    Value::Int32(ts as i32 * 10)
                };
                let expect_dval = if ts % 5 == 0 {
                    Value::Null
                } else {
                    Value::Double(ts as f64 + 0.5)
                };
                assert_eq!(row.values[0], expect_ival, "ival at ts={ts}");
                assert_eq!(row.values[1], expect_dval, "dval at ts={ts}");
                assert_eq!(
                    row.values[2],
                    Value::Text(format!("row-{ts}")),
                    "sval at ts={ts}"
                );
                seen += 1;
            }
        }
        assert_eq!(seen, ROWS, "row count");

        // Filtered query must honor the predicate.
        let mut filtered = 0i64;
        {
            let mut dataset = session
                .execute_query(&format!("SELECT sval FROM {DB}.d1 WHERE time >= 10"))
                .expect("filtered query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                assert!(row.timestamp.expect("timestamp") >= 10);
                filtered += 1;
            }
        }
        assert_eq!(filtered, ROWS - 10, "filtered row count");

        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// Live roundtrip for the whole `insertRecord(s)` family: single record
    /// (with null + all-null-marker coverage), multi-device records, one-device
    /// batch given unsorted (client must sort), and aligned variants on an
    /// aligned device. Every write is read back with SELECT and each cell
    /// asserted. Skipped when no IoTDB instance is reachable on
    /// localhost:6667.
    #[test]
    fn live_insert_records_readback() {
        use crate::data::Value;
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_insert_records_readback: no IoTDB server on 127.0.0.1:6667");
            return;
        }

        const DB: &str = "root.rusttest_records";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let read_rows = |session: &mut Session, sql: &str| -> Vec<(i64, Vec<Value>)> {
            let mut rows = Vec::new();
            let mut dataset = session.execute_query(sql).expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                rows.push((row.timestamp.expect("timestamp"), row.values.clone()));
            }
            rows
        };

        let mut session = Session::new(SessionConfig::default());
        session.open().expect("open session");
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");

        // --- insert_record: mixed types plus an explicit null ---
        session
            .insert_record(
                &format!("{DB}.d1"),
                11,
                vec!["i".into(), "b".into()],
                &[Value::Int32(43), Value::Boolean(true)],
                false,
            )
            .expect("insert_record");
        session
            .insert_record(
                &format!("{DB}.d1"),
                10,
                vec!["i".into(), "d".into(), "s".into(), "b".into()],
                &[
                    Value::Int32(42),
                    Value::Double(2.5),
                    Value::Text("hello".into()),
                    Value::Null, // filtered out client-side with its measurement
                ],
                false,
            )
            .expect("insert_record row with null");
        let rows = read_rows(
            &mut session,
            &format!("SELECT i, d, s, b FROM {DB}.d1 ORDER BY time"),
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 10);
        assert_eq!(
            rows[0].1,
            [
                Value::Int32(42),
                Value::Double(2.5),
                Value::Text("hello".into()),
                Value::Null,
            ]
        );
        assert_eq!(rows[1].0, 11);
        assert_eq!(
            rows[1].1,
            [
                Value::Int32(43),
                Value::Null,
                Value::Null,
                Value::Boolean(true),
            ]
        );

        // --- insert_records: one row each on two devices ---
        session
            .insert_records(
                vec![format!("{DB}.d2"), format!("{DB}.d3")],
                vec![20, 21],
                vec![vec!["x".into()], vec!["x".into()]],
                &[vec![Value::Int64(-7)], vec![Value::Float(1.5)]],
                false,
            )
            .expect("insert_records");
        let rows = read_rows(&mut session, &format!("SELECT x FROM {DB}.d2"));
        assert_eq!(rows, [(20, vec![Value::Int64(-7)])]);
        let rows = read_rows(&mut session, &format!("SELECT x FROM {DB}.d3"));
        assert_eq!(rows, [(21, vec![Value::Float(1.5)])]);

        // --- insert_records_of_one_device: unsorted input, client sorts ---
        session
            .insert_records_of_one_device(
                &format!("{DB}.d4"),
                vec![32, 30, 31],
                vec![
                    vec!["v".into()],
                    vec!["v".into()],
                    vec!["v".into(), "w".into()],
                ],
                &[
                    vec![Value::Int32(320)],
                    vec![Value::Int32(300)],
                    vec![Value::Int32(310), Value::Text("mid".into())],
                ],
                false,
            )
            .expect("insert_records_of_one_device");
        let rows = read_rows(
            &mut session,
            &format!("SELECT v, w FROM {DB}.d4 ORDER BY time"),
        );
        assert_eq!(
            rows,
            [
                (30, vec![Value::Int32(300), Value::Null]),
                (31, vec![Value::Int32(310), Value::Text("mid".into())]),
                (32, vec![Value::Int32(320), Value::Null]),
            ]
        );

        // --- aligned variants on a fresh aligned device ---
        session
            .execute_non_query(&format!(
                "CREATE ALIGNED TIMESERIES {DB}.a1(s1 INT32, s2 DOUBLE)"
            ))
            .expect("create aligned timeseries");
        session
            .insert_aligned_record(
                &format!("{DB}.a1"),
                40,
                vec!["s1".into(), "s2".into()],
                &[Value::Int32(400), Value::Double(4.5)],
            )
            .expect("insert_aligned_record");
        session
            .insert_aligned_records(
                vec![format!("{DB}.a1")],
                vec![41],
                vec![vec!["s1".into()]],
                &[vec![Value::Int32(410)]],
            )
            .expect("insert_aligned_records");
        session
            .insert_aligned_records_of_one_device(
                &format!("{DB}.a1"),
                vec![43, 42],
                vec![vec!["s1".into()], vec!["s2".into()]],
                &[vec![Value::Int32(430)], vec![Value::Double(4.2)]],
            )
            .expect("insert_aligned_records_of_one_device");
        let rows = read_rows(
            &mut session,
            &format!("SELECT s1, s2 FROM {DB}.a1 ORDER BY time"),
        );
        assert_eq!(
            rows,
            [
                (40, vec![Value::Int32(400), Value::Double(4.5)]),
                (41, vec![Value::Int32(410), Value::Null]),
                (42, vec![Value::Null, Value::Double(4.2)]),
                (43, vec![Value::Int32(430), Value::Null]),
            ]
        );

        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }

    /// Live roundtrip for `insertTablets`: one batch of three tablets across
    /// two devices (with nulls and unsorted rows), plus an aligned batch on
    /// an aligned device. Every written cell is read back and asserted.
    /// Skipped when no IoTDB instance is reachable on localhost:6667.
    #[test]
    fn live_insert_tablets_readback() {
        use crate::data::{tablet::Tablet, TSDataType, Value};
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_insert_tablets_readback: no IoTDB server on 127.0.0.1:6667");
            return;
        }

        const DB: &str = "root.rusttest_tablets";
        let _guard = LIVE_DB_LOCK.lock().unwrap_or_else(|p| p.into_inner());

        let read_rows = |session: &mut Session, sql: &str| -> Vec<(i64, Vec<Value>)> {
            let mut rows = Vec::new();
            let mut dataset = session.execute_query(sql).expect("query");
            while let Some(row) = dataset.next_row().expect("next_row") {
                rows.push((row.timestamp.expect("timestamp"), row.values.clone()));
            }
            rows
        };

        let mut session = Session::new(SessionConfig::default());
        session.open().expect("open session");
        let _ = session.execute_non_query(&format!("DELETE DATABASE {DB}"));
        session
            .execute_non_query(&format!("CREATE DATABASE {DB}"))
            .expect("create database");

        // Three tablets, two devices, one aligned=false batch. t1 and t3
        // both target d1 over disjoint time ranges; t1 has a null and
        // unsorted rows (the client must sort per tablet).
        let mut t1 = Tablet::new(
            format!("{DB}.d1"),
            vec!["i".into(), "s".into()],
            vec![TSDataType::Int32, TSDataType::Text],
        )
        .expect("tablet t1");
        t1.add_row(2, vec![Some(Value::Int32(20)), None])
            .expect("add_row");
        t1.add_row(1, vec![None, Some(Value::Text("one".into()))])
            .expect("add_row");
        let mut t2 = Tablet::new(
            format!("{DB}.d2"),
            vec!["d".into()],
            vec![TSDataType::Double],
        )
        .expect("tablet t2");
        t2.add_row(10, vec![Some(Value::Double(1.5))])
            .expect("add_row");
        t2.add_row(11, vec![Some(Value::Double(-2.5))])
            .expect("add_row");
        let mut t3 = Tablet::new(
            format!("{DB}.d1"),
            vec!["i".into(), "s".into()],
            vec![TSDataType::Int32, TSDataType::Text],
        )
        .expect("tablet t3");
        t3.add_row(
            3,
            vec![Some(Value::Int32(30)), Some(Value::Text("three".into()))],
        )
        .expect("add_row");
        session
            .insert_tablets(&[t1, t2, t3], false)
            .expect("insert_tablets");

        let rows = read_rows(
            &mut session,
            &format!("SELECT i, s FROM {DB}.d1 ORDER BY time"),
        );
        assert_eq!(
            rows,
            [
                (1, vec![Value::Null, Value::Text("one".into())]),
                (2, vec![Value::Int32(20), Value::Null]),
                (3, vec![Value::Int32(30), Value::Text("three".into())]),
            ]
        );
        let rows = read_rows(
            &mut session,
            &format!("SELECT d FROM {DB}.d2 ORDER BY time"),
        );
        assert_eq!(
            rows,
            [
                (10, vec![Value::Double(1.5)]),
                (11, vec![Value::Double(-2.5)]),
            ]
        );

        // Aligned batch on an aligned device.
        session
            .execute_non_query(&format!(
                "CREATE ALIGNED TIMESERIES {DB}.a1(s1 INT32, s2 DOUBLE)"
            ))
            .expect("create aligned timeseries");
        let mut a1 = Tablet::new_aligned(
            format!("{DB}.a1"),
            vec!["s1".into(), "s2".into()],
            vec![TSDataType::Int32, TSDataType::Double],
        )
        .expect("aligned tablet");
        a1.add_row(40, vec![Some(Value::Int32(400)), None])
            .expect("add_row");
        a1.add_row(41, vec![Some(Value::Int32(410)), Some(Value::Double(4.1))])
            .expect("add_row");
        session
            .insert_aligned_tablets(&[a1])
            .expect("insert_aligned_tablets");
        let rows = read_rows(
            &mut session,
            &format!("SELECT s1, s2 FROM {DB}.a1 ORDER BY time"),
        );
        assert_eq!(
            rows,
            [
                (40, vec![Value::Int32(400), Value::Null]),
                (41, vec![Value::Int32(410), Value::Double(4.1)]),
            ]
        );

        session
            .execute_non_query(&format!("DELETE DATABASE {DB}"))
            .expect("cleanup");
        session.close().expect("close session");
    }
}
