//! Apache IoTDB client for Rust.
//!
//! Provides two data models over Apache Thrift RPC (default port 6667):
//! - **Tree model**: [`Session`] / `SessionPool` — device/timeseries paths
//! - **Table model**: `TableSession` / `TableSessionPool` — SQL dialect
//!
//! Architecture mirrors the other IoTDB client SDKs (Node.js, C#):
//! Pool (round-robin over node urls) → Session → Connection (framed transport + binary protocol).

pub mod client;
pub mod connection;
pub mod data;
pub mod error;
pub mod protocol;

pub use client::dataset::{Row, SessionDataSet};
pub use client::pool::{PooledSession, SessionPool, SessionPoolConfig, TableSessionPool};
pub use client::session::{QueryHandle, Session, SessionConfig};
pub use client::table_session::{TableSession, TableSessionBuilder};
pub use connection::Endpoint;
pub use data::{ColumnCategory, TSDataType, Tablet, TsBlock, Value};
pub use error::{Error, Result};
