//! Tree-model session (device/timeseries paths).

use crate::connection::{Connection, Endpoint};
use crate::error::{Error, Result};

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
        }
    }
}

/// A tree-model session against an IoTDB cluster.
pub struct Session {
    config: SessionConfig,
    connection: Option<Connection>,
}

impl Session {
    pub fn new(config: SessionConfig) -> Self {
        Self { config, connection: None }
    }

    pub fn open(&mut self) -> Result<()> {
        let endpoint = self
            .config
            .endpoints
            .first()
            .ok_or_else(|| Error::Client("no endpoints configured".into()))?
            .clone();
        self.connection = Some(Connection::open(endpoint)?);
        // TODO(codegen): call openSession RPC (username/password/zoneId/sql_dialect),
        // store sessionId + statementId.
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    pub fn close(&mut self) -> Result<()> {
        // TODO(codegen): call closeSession RPC before dropping the connection.
        self.connection = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_targets_localhost() {
        let cfg = SessionConfig::default();
        assert_eq!(cfg.endpoints[0], Endpoint::new("localhost", 6667));
        assert_eq!(cfg.sql_dialect, "tree");
    }

    #[test]
    fn open_without_endpoints_fails() {
        let mut session = Session::new(SessionConfig { endpoints: vec![], ..Default::default() });
        assert!(session.open().is_err());
        assert!(!session.is_open());
    }
}
