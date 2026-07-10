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

pub use client::session::Session;
pub use error::{Error, Result};
