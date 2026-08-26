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

//! Table-model (relational) walkthrough: open a [`TableSession`] via the
//! builder, create a database and table, insert a table-model [`Tablet`]
//! (TAG + FIELD columns), query it back with SQL, then clean up.
//!
//! Run against a local IoTDB (e.g. `docker compose up -d`):
//!
//! ```sh
//! cargo run --example table_session
//! ```

use iotdb_client::{ColumnCategory, Result, TSDataType, TableSession, Tablet, Value};

const DB: &str = "rust_example_db";

fn main() -> Result<()> {
    env_logger::init();

    // The builder opens the session with sql_dialect="table".
    let mut session = TableSession::builder()
        .node_urls(&["127.0.0.1:6667"])?
        .username("root")
        .password("root")
        .build()?;
    println!("table session opened");

    // --- DDL: database + table ------------------------------------------
    session.execute_non_query(&format!("CREATE DATABASE IF NOT EXISTS {DB}"))?;
    session.execute_non_query(&format!("USE {DB}"))?;
    println!("current database: {:?}", session.database());
    session.execute_non_query(
        "CREATE TABLE IF NOT EXISTS sensors (\
           region STRING TAG, \
           device_id STRING TAG, \
           temperature DOUBLE FIELD, \
           status BOOLEAN FIELD)",
    )?;
    println!("table `sensors` created");

    // --- Insert a table-model tablet (TAG/FIELD categories) --------------
    let mut tablet = Tablet::new_table(
        "sensors",
        vec![
            "region".into(),
            "device_id".into(),
            "temperature".into(),
            "status".into(),
        ],
        vec![
            TSDataType::String,
            TSDataType::String,
            TSDataType::Double,
            TSDataType::Boolean,
        ],
        vec![
            ColumnCategory::Tag,
            ColumnCategory::Tag,
            ColumnCategory::Field,
            ColumnCategory::Field,
        ],
    )?;
    let base_ts = 1_720_000_000_000i64; // epoch milliseconds
    for i in 0..8i64 {
        tablet.add_row(
            base_ts + i * 1_000,
            vec![
                Some(Value::String(
                    if i % 2 == 0 { "east" } else { "west" }.into(),
                )),
                Some(Value::String(format!("dev-{}", i % 3))),
                Some(Value::Double(21.0 + i as f64 * 0.25)),
                // Some status readings are missing (null cells).
                (i % 3 != 0).then_some(Value::Boolean(i % 2 == 0)),
            ],
        )?;
    }
    session.insert(&tablet)?;
    println!("inserted {} rows into `sensors`", tablet.row_count());

    // --- Query with SQL ----------------------------------------------------
    {
        let mut dataset = session.execute_query(
            "SELECT time, region, device_id, temperature, status \
             FROM sensors ORDER BY time",
        )?;
        println!("columns: {:?}", dataset.columns());
        while let Some(row) = dataset.next_row()? {
            println!("{:?}", row.values);
        }
    } // dataset drop closes the query and releases the session borrow

    // --- OBJECT column demo (needs IoTDB 2.0.8+; skipped on older servers) -
    // OBJECT writes use Tablet::set_object_value_at with a 9-byte segment
    // header (isEOF + big-endian offset). SELECT file renders the size
    // summary; SELECT READ_OBJECT(file) returns the raw BLOB.
    if let Err(e) = session.execute_non_query(
        "CREATE TABLE IF NOT EXISTS objects (\
           region STRING TAG, \
           file OBJECT FIELD)",
    ) {
        eprintln!("skipping OBJECT demo (server does not support OBJECT): {e}");
    } else {
        let object_bytes: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        let mut tablet = Tablet::new_table(
            "objects",
            vec!["region".into(), "file".into()],
            vec![TSDataType::String, TSDataType::Object],
            vec![ColumnCategory::Tag, ColumnCategory::Field],
        )?;
        tablet.add_row(base_ts, vec![Some(Value::String("east".into())), None])?;
        tablet.set_object_value_at(true, 0, &object_bytes, 1, 0)?;
        session.insert(&tablet)?;
        println!("inserted an OBJECT row into `objects`");

        {
            let mut dataset = session.execute_query("SELECT file FROM objects")?;
            while let Some(row) = dataset.next_row()? {
                println!("{:?}", row.values); // e.g. [String("(Object) 1.00 KB")]
            }
        }
        session.execute_non_query("DROP TABLE objects")?;
    }

    // --- Cleanup ----------------------------------------------------------
    session.execute_non_query(&format!("DROP DATABASE {DB}"))?;
    println!("database dropped");

    session.close()?;
    println!("session closed");
    Ok(())
}
