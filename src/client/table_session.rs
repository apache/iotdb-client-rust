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

//! Table-model (relational) session — a thin wrapper over [`Session`] with
//! `sql_dialect="table"` enforced at open time (protocol spec §6).

use std::time::Duration;

use crate::client::dataset::SessionDataSet;
use crate::client::session::{Session, SessionConfig};
use crate::connection::Endpoint;
use crate::data::Tablet;
use crate::error::{Error, Result};

/// Builder for [`TableSession`]. Defaults mirror [`SessionConfig`] except
/// the SQL dialect, which is always `"table"`.
///
/// ```no_run
/// use iotdb_client::TableSession;
///
/// let mut session = TableSession::builder()
///     .node_urls(&["127.0.0.1:6667"])?
///     .database("mydb")
///     .build()?;
/// session.execute_non_query("SHOW TABLES")?;
/// # Ok::<(), iotdb_client::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct TableSessionBuilder {
    config: SessionConfig,
}

impl Default for TableSessionBuilder {
    fn default() -> Self {
        Self {
            config: SessionConfig {
                sql_dialect: "table".into(),
                ..SessionConfig::default()
            },
        }
    }
}

impl TableSessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Endpoints from `"host:port"` node-url strings.
    pub fn node_urls<S: AsRef<str>>(mut self, node_urls: &[S]) -> Result<Self> {
        self.config = self.config.with_node_urls(node_urls)?;
        Ok(self)
    }

    pub fn endpoints(mut self, endpoints: Vec<Endpoint>) -> Self {
        self.config.endpoints = endpoints;
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.config.username = username.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = password.into();
        self
    }

    /// Database to select at open time (sent as config key `"db"`).
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = Some(database.into());
        self
    }

    pub fn fetch_size(mut self, fetch_size: i32) -> Self {
        self.config.fetch_size = fetch_size;
        self
    }

    pub fn zone_id(mut self, zone_id: impl Into<String>) -> Self {
        self.config.zone_id = zone_id.into();
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    pub fn query_timeout_ms(mut self, timeout_ms: i64) -> Self {
        self.config.query_timeout_ms = timeout_ms;
        self
    }

    /// Speak TCompactProtocol ("RPC compression"). Must match the server's
    /// `dn_rpc_thrift_compression_enable` setting — see
    /// [`SessionConfig::enable_rpc_compression`].
    pub fn enable_rpc_compression(mut self, enable: bool) -> Self {
        self.config.enable_rpc_compression = enable;
        self
    }

    /// Wrap connections in TLS (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn use_ssl(mut self, use_ssl: bool) -> Self {
        self.config.use_ssl = use_ssl;
        self
    }

    /// PEM certificate added as trusted root (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn ca_cert_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.config.ca_cert_path = Some(path.into());
        self
    }

    /// Skip certificate verification — **dangerous** outside tests
    /// (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn accept_invalid_certs(mut self, accept: bool) -> Self {
        self.config.accept_invalid_certs = accept;
        self
    }

    /// Hostname for SNI + certificate validation (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn domain_override(mut self, domain: impl Into<String>) -> Self {
        self.config.domain_override = Some(domain.into());
        self
    }

    /// PEM client certificate for mutual TLS; set together with
    /// [`client_key_path`](Self::client_key_path) (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn client_cert_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.config.client_cert_path = Some(path.into());
        self
    }

    /// PEM PKCS#8 private key for the client certificate; set together with
    /// [`client_cert_path`](Self::client_cert_path) (cargo feature `tls`).
    #[cfg(feature = "tls")]
    pub fn client_key_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.config.client_key_path = Some(path.into());
        self
    }

    /// The [`SessionConfig`] this builder resolves to (dialect always
    /// `"table"`).
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Open the session against the configured endpoints.
    pub fn build(mut self) -> Result<TableSession> {
        // Enforce the dialect even if the config was mutated externally.
        self.config.sql_dialect = "table".into();
        let mut session = Session::new(self.config);
        session.open()?;
        Ok(TableSession { session })
    }
}

/// A table-model (relational) session. All statements run in the `"table"`
/// SQL dialect; inserts require table-model tablets
/// ([`Tablet::new_table`]).
pub struct TableSession {
    session: Session,
}

impl TableSession {
    pub fn builder() -> TableSessionBuilder {
        TableSessionBuilder::new()
    }

    /// The database currently selected on this session, if any — tracked
    /// from the open-time `"db"` config key and `USE <db>` responses.
    pub fn database(&self) -> Option<&str> {
        self.session.database()
    }

    pub fn is_open(&self) -> bool {
        self.session.is_open()
    }

    /// Insert a table-model tablet (`writeToTable=true` + column
    /// categories on the wire). Rejects tree-model tablets.
    pub fn insert(&mut self, tablet: &Tablet) -> Result<()> {
        if !tablet.is_table_model() {
            return Err(Error::Client(
                "TableSession::insert requires a table-model tablet (Tablet::new_table)".into(),
            ));
        }
        self.session.insert_tablet(tablet)
    }

    /// Execute a query; the returned [`SessionDataSet`] borrows this
    /// session until closed or dropped.
    pub fn execute_query(&mut self, sql: &str) -> Result<SessionDataSet<'_>> {
        self.session.execute_query(sql)
    }

    /// Execute a non-query statement (DDL/DML), tracking `USE <db>`.
    pub fn execute_non_query(&mut self, sql: &str) -> Result<()> {
        self.session.execute_non_query(sql)
    }

    pub fn close(&mut self) -> Result<()> {
        self.session.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::TSDataType;

    #[test]
    fn builder_defaults_to_table_dialect() {
        let b = TableSessionBuilder::new()
            .node_urls(&["10.0.0.1:6667"])
            .unwrap()
            .username("u")
            .password("p")
            .database("db1")
            .fetch_size(500)
            .zone_id("UTC")
            .connect_timeout(Duration::from_secs(3))
            .query_timeout_ms(1_000);
        let cfg = b.config();
        assert_eq!(cfg.sql_dialect, "table");
        assert_eq!(cfg.endpoints, vec![Endpoint::new("10.0.0.1", 6667)]);
        assert_eq!(cfg.username, "u");
        assert_eq!(cfg.password, "p");
        assert_eq!(cfg.database.as_deref(), Some("db1"));
        assert_eq!(cfg.fetch_size, 500);
        assert_eq!(cfg.zone_id, "UTC");
        assert_eq!(cfg.connect_timeout, Duration::from_secs(3));
        assert_eq!(cfg.query_timeout_ms, 1_000);
    }

    #[test]
    fn builder_passes_transport_options_through() {
        let b = TableSessionBuilder::new().enable_rpc_compression(true);
        assert!(b.config().enable_rpc_compression);

        #[cfg(feature = "tls")]
        {
            let b = TableSessionBuilder::new()
                .use_ssl(true)
                .ca_cert_path("/certs/ca.pem")
                .accept_invalid_certs(true)
                .domain_override("iotdb.internal")
                .client_cert_path("/certs/client.pem")
                .client_key_path("/certs/client-key.pem");
            let cfg = b.config();
            assert!(cfg.use_ssl);
            assert_eq!(cfg.ca_cert_path.as_deref(), Some("/certs/ca.pem".as_ref()));
            assert!(cfg.accept_invalid_certs);
            assert_eq!(cfg.domain_override.as_deref(), Some("iotdb.internal"));
            assert_eq!(
                cfg.client_cert_path.as_deref(),
                Some("/certs/client.pem".as_ref())
            );
            assert_eq!(
                cfg.client_key_path.as_deref(),
                Some("/certs/client-key.pem".as_ref())
            );
        }
    }

    #[test]
    fn insert_rejects_tree_model_tablets() {
        let mut session = TableSession {
            session: Session::new(SessionConfig::default()),
        };
        let tablet = Tablet::new("root.sg.d1", vec!["s1".into()], vec![TSDataType::Int32]).unwrap();
        match session.insert(&tablet) {
            Err(Error::Client(msg)) => assert!(msg.contains("table-model")),
            other => panic!("expected client error, got {other:?}"),
        }
    }

    /// Live-server test; skipped when no IoTDB instance is reachable.
    #[test]
    fn live_table_session_roundtrip() {
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_err()
        {
            eprintln!("skipping live_table_session_roundtrip: no IoTDB server on 127.0.0.1:6667");
            return;
        }

        let mut session = TableSession::builder().build().expect("open");
        assert!(session.is_open());
        session
            .execute_non_query("CREATE DATABASE IF NOT EXISTS rust_client_test")
            .expect("create db");
        session
            .execute_non_query("USE rust_client_test")
            .expect("use db");
        assert_eq!(session.database(), Some("rust_client_test"));
        session.close().expect("close");
    }
}
