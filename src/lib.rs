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
pub use client::redirect::{RedirectCache, RedirectCacheStats};
pub use client::session::{QueryHandle, Session, SessionConfig};
pub use client::table_session::{TableSession, TableSessionBuilder};
pub use connection::Endpoint;
pub use data::{ColumnCategory, TSDataType, Tablet, TsBlock, Value};
pub use error::{Error, Result};
