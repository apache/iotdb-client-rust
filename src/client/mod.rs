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

//! Session-layer API: tree model ([`Session`]), table model
//! ([`TableSession`]), pooled variants and query result iteration —
//! mirroring the Node.js and C# SDKs.

pub mod dataset;
pub mod pool;
pub mod redirect;
pub mod session;
pub mod table_session;

pub use dataset::{Row, SessionDataSet};
pub use pool::{PooledSession, SessionPool, SessionPoolConfig, TableSessionPool};
pub use redirect::{RedirectCache, RedirectCacheStats};
pub use session::{QueryHandle, Session, SessionConfig};
pub use table_session::{TableSession, TableSessionBuilder};
