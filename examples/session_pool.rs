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

//! Session pool walkthrough: share a [`SessionPool`] across threads, each
//! acquiring a session to insert and query concurrently; then the same idea
//! with a [`TableSessionPool`].
//!
//! Run against a local IoTDB (e.g. `docker compose up -d`):
//!
//! ```sh
//! cargo run --example session_pool
//! ```

use std::sync::Arc;
use std::thread;

use iotdb_client::{
    Result, SessionPool, SessionPoolConfig, TSDataType, TableSessionPool, Tablet, Value,
};

const DB: &str = "root.rust_pool_example";
const THREADS: usize = 4;

fn main() -> Result<()> {
    env_logger::init();

    // --- Tree-model pool ---------------------------------------------------
    let config = SessionPoolConfig {
        max_size: THREADS,
        min_size: 1, // one session opened eagerly
        ..SessionPoolConfig::default()
    }
    .with_node_urls(&["127.0.0.1:6667"])?;
    let pool = Arc::new(SessionPool::new(config)?);
    println!("pool created ({} live)", pool.live_count());

    pool.execute_non_query(&format!("CREATE DATABASE {DB}"))?;

    // Each thread acquires its own session, inserts a tablet for its device
    // and queries the row count back. The RAII guard returns the session to
    // the pool on drop.
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let pool = Arc::clone(&pool);
            thread::spawn(move || -> Result<usize> {
                let device = format!("{DB}.d{t}");
                let mut session = pool.acquire()?;

                let mut tablet =
                    Tablet::new(&device, vec!["value".into()], vec![TSDataType::Int64])?;
                let base_ts = 1_720_000_000_000i64;
                for i in 0..5i64 {
                    tablet.add_row(base_ts + i, vec![Some(Value::Int64(t as i64 * 100 + i))])?;
                }
                session.insert_tablet(&tablet)?;

                let mut rows = 0usize;
                let mut dataset = session.execute_query(&format!("SELECT value FROM {device}"))?;
                while dataset.next_row()?.is_some() {
                    rows += 1;
                }
                Ok(rows)
            })
        })
        .collect();
    for (t, handle) in handles.into_iter().enumerate() {
        let rows = handle.join().expect("thread panicked")?;
        println!("thread {t}: inserted and read back {rows} rows");
    }

    pool.execute_non_query(&format!("DELETE DATABASE {DB}"))?;
    pool.close();
    println!("tree pool closed");

    // --- Table-model pool ----------------------------------------------------
    let config = SessionPoolConfig::default().with_node_urls(&["127.0.0.1:6667"])?;
    let table_pool = TableSessionPool::new(config)?;
    table_pool.execute_non_query("CREATE DATABASE IF NOT EXISTS rust_pool_example")?;
    table_pool.execute_non_query("USE rust_pool_example")?;
    {
        let mut session = table_pool.acquire()?;
        let mut dataset = session.execute_query("SHOW TABLES")?;
        println!("tables in rust_pool_example:");
        while let Some(row) = dataset.next_row()? {
            println!("  {:?}", row.values);
        }
    }
    table_pool.execute_non_query("DROP DATABASE rust_pool_example")?;
    table_pool.close();
    println!("table pool closed");

    Ok(())
}
