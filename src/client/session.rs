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
use crate::connection::{Connection, Endpoint};
use crate::data::Tablet;
use crate::error::{Error, Result};
use crate::protocol::client::{
    TIClientRPCServiceSyncClient, TSCloseOperationReq, TSCloseSessionReq, TSExecuteStatementReq,
    TSFetchResultsReq, TSInsertTabletReq, TSOpenSessionReq, TSProtocolVersion,
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
        let mut connection = None;
        let mut last_err: Option<Error> = None;
        for i in 0..n {
            let endpoint = self.config.endpoints[(start + i) % n].clone();
            match Connection::open(endpoint, self.config.connect_timeout) {
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
        self.session_id = resp
            .session_id
            .ok_or_else(|| Error::Client("openSession response missing sessionId".into()))?;
        self.statement_id = connection
            .client_mut()
            .request_statement_id(self.session_id)?;
        self.connection = Some(connection);
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    /// The database currently selected on this session, if any.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    fn connection_mut(&mut self) -> Result<&mut Connection> {
        self.connection
            .as_mut()
            .ok_or_else(|| Error::Client("session is not open".into()))
    }

    /// Execute a non-query statement (DDL/DML). Tracks `USE <db>` via the
    /// response's `database` field.
    pub fn execute_non_query(&mut self, sql: &str) -> Result<()> {
        let req = self.statement_req(sql);
        let resp = self
            .connection_mut()?
            .client_mut()
            .execute_update_statement_v2(req)?;
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
        let req = self.statement_req(sql);
        let resp = self
            .connection_mut()?
            .client_mut()
            .execute_query_statement_v2(req)?;
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
        let status = self.connection_mut()?.client_mut().insert_tablet(req)?;
        check_status(&status)
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
}
