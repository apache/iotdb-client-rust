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

//! Tree-model walkthrough: open a [`Session`], create a database and
//! timeseries, insert a [`Tablet`] (several data types, some null cells),
//! query it back row by row, then clean up.
//!
//! Run against a local IoTDB (e.g. `docker compose up -d`):
//!
//! ```sh
//! cargo run --example tree_session
//! ```

use iotdb_client::{Result, Session, SessionConfig, TSDataType, Tablet, Value};

const DB: &str = "root.rust_example";
const DEVICE: &str = "root.rust_example.d1";

fn main() -> Result<()> {
    env_logger::init();

    let config = SessionConfig::default().with_node_urls(&["127.0.0.1:6667"])?;
    let mut session = Session::new(config);
    session.open()?;
    println!("session opened");

    // --- DDL: database + timeseries -------------------------------------
    session.execute_non_query(&format!("CREATE DATABASE {DB}"))?;
    for (name, dtype) in [
        ("temperature", "DOUBLE"),
        ("humidity", "FLOAT"),
        ("status", "BOOLEAN"),
        ("counter", "INT64"),
        ("tag", "TEXT"),
    ] {
        session.execute_non_query(&format!(
            "CREATE TIMESERIES {DEVICE}.{name} WITH DATATYPE={dtype}, ENCODING=PLAIN"
        ))?;
    }
    println!("database and timeseries created");

    // --- Insert a tablet (column-major batch, nulls allowed) ------------
    let mut tablet = Tablet::new(
        DEVICE,
        vec![
            "temperature".into(),
            "humidity".into(),
            "status".into(),
            "counter".into(),
            "tag".into(),
        ],
        vec![
            TSDataType::Double,
            TSDataType::Float,
            TSDataType::Boolean,
            TSDataType::Int64,
            TSDataType::Text,
        ],
    )?;
    let base_ts = 1_720_000_000_000i64; // epoch milliseconds
    for i in 0..10i64 {
        tablet.add_row(
            base_ts + i * 1_000,
            vec![
                Some(Value::Double(20.0 + i as f64 * 0.5)),
                // Every third humidity reading is missing.
                (i % 3 != 0).then_some(Value::Float(40.0 + i as f32)),
                Some(Value::Boolean(i % 2 == 0)),
                Some(Value::Int64(i * 100)),
                // Only some rows carry a text tag.
                (i % 4 == 0).then(|| Value::Text(format!("batch-{i}"))),
            ],
        )?;
    }
    session.insert_tablet(&tablet)?;
    println!("inserted {} rows into {DEVICE}", tablet.row_count());

    // --- Query and iterate ----------------------------------------------
    {
        let mut dataset = session.execute_query(&format!(
            "SELECT temperature, humidity, status, counter, tag FROM {DEVICE}"
        ))?;
        println!("columns: {:?}", dataset.columns());
        println!("types:   {:?}", dataset.column_types());
        while let Some(row) = dataset.next_row()? {
            println!("ts={:?} values={:?}", row.timestamp, row.values);
        }
    } // dataset drop closes the query and releases the session borrow

    // --- Cleanup ----------------------------------------------------------
    session.execute_non_query(&format!("DELETE DATABASE {DB}"))?;
    println!("database deleted");

    session.close()?;
    println!("session closed");
    Ok(())
}
