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

//! Write-performance benchmark for the tree and table models, modeled on the
//! Node.js client's `benchmark/` suite and on
//! [thulab/iot-benchmark](https://github.com/thulab/iot-benchmark). The
//! **measurement semantics mirror iot-benchmark** (`DBWrapper` /
//! `Measurement`):
//!
//! - the per-operation latency span starts **before** batch preparation for
//!   that batch (Java times `genTablet` + submit + `Future.get`,
//!   `SessionStrategy.insertOneBatchByTablet`), not just the insert RPC;
//! - failed operations record **no latency sample** — they only count toward
//!   `failOperation` / `failPoint` (`DBWrapper.measureOneBatch`);
//! - throughput = okPoint / elapsed, where elapsed is the wall time of the
//!   whole data phase (schema creation excluded), matching
//!   `BaseMode.measure`;
//! - output includes an iot-benchmark-style "Result Matrix" and
//!   "Latency (ms) Matrix" (AVG MIN P10 P25 MEDIAN P75 P90 P95 P99 P999 MAX
//!   SLOWEST_THREAD) so runs diff side-by-side with Java logs. Percentiles
//!   here are exact over the sorted samples (p = ceil(p% × n) − 1);
//!   iot-benchmark uses a t-digest approximation. SLOWEST_THREAD is the max
//!   over threads of that thread's latency sum (`Measurement.mergeMeasurement`).
//!
//! Schema setup happens **outside** the timed section; timestamps are
//! sequential per device from a fixed base, so runs are deterministic.
//!
//! Sensor data types follow the Node.js default distribution:
//! 30% FLOAT, 20% DOUBLE, 20% INT32, 10% INT64, 10% TEXT, 10% BOOLEAN.
//!
//! Run against a live IoTDB (release mode, or the client dominates):
//!
//! ```sh
//! cargo run --release --example benchmark -- --mode tree \
//!     --devices 20 --sensors 10 --batches 100 --batch-size 100 --clients 8
//! cargo run --release --example benchmark -- --mode table --cleanup
//! ```
//!
//! Connection defaults honor `IOTDB_HOST` / `IOTDB_PORT` / `IOTDB_USER` /
//! `IOTDB_PASSWORD` like the e2e tests; CLI flags override env vars.

use std::env;
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use iotdb_client::{
    ColumnCategory, PooledSession, Result, SessionPool, SessionPoolConfig, TSDataType,
    TableSessionPool, Tablet, Value,
};

const TREE_DB: &str = "root.benchmark";
const TABLE_DB: &str = "benchmark_db";
const TABLE_NAME: &str = "benchmark_table";
/// Node.js `STRING_LENGTH` default.
const TEXT_LEN: usize = 16;
/// Progress report interval (Node.js `REPORT_INTERVAL` default 5000 ms).
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Tree,
    Table,
}

struct BenchConfig {
    mode: Mode,
    devices: usize,
    sensors: usize,
    batches: usize,
    batch_size: usize,
    clients: usize,
    host: String,
    port: u16,
    user: String,
    password: String,
    /// Fixed base timestamp (epoch ms) — deterministic, no wall clock in
    /// the data path.
    base_ts: i64,
    /// Time interval between consecutive points per device (ms), Node.js
    /// `POINT_STEP`.
    point_step: i64,
    /// When > 0, pre-generate only this many tablets per worker and re-send
    /// them cyclically, rebasing timestamps per batch so every insert still
    /// lands on a fresh, disjoint time range (values repeat; timestamps
    /// don't). Bounds pre-generation memory for very large point counts.
    /// 0 = pre-generate everything (default, original behavior).
    reuse_tablets: usize,
    /// Tree model only: batch this many tablets (different devices) into one
    /// `insert_tablets` RPC. 1 = one `insert_tablet` per RPC (default,
    /// original behavior). Latency percentiles are then per multi-tablet RPC.
    tablets_per_rpc: usize,
    cleanup: bool,
}

impl BenchConfig {
    fn total_points(&self) -> u64 {
        (self.devices * self.sensors * self.batches * self.batch_size) as u64
    }
}

const USAGE: &str = "IoTDB Rust client write benchmark

USAGE:
    cargo run --release --example benchmark -- [OPTIONS]

OPTIONS:
    --mode <tree|table>   Data model to benchmark (default: tree)
    --devices <N>         Number of devices (default: 100)
    --sensors <N>         Sensors (FIELD columns) per device (default: 10)
    --batches <N>         Batches (tablets) per device (default: 20)
    --batch-size <N>      Rows per tablet (default: 1000)
    --clients <N>         Worker threads = session pool size (default: 8)
    --host <HOST>         Server host (default: $IOTDB_HOST or 127.0.0.1)
    --port <PORT>         Server port (default: $IOTDB_PORT or 6667)
    --user <USER>         Username (default: $IOTDB_USER or root)
    --password <PASS>     Password (default: $IOTDB_PASSWORD or root)
    --base-ts <MS>        Base epoch-ms timestamp (default: 1720000000000)
    --point-step <MS>     Interval between points per device (default: 1000)
    --reuse-tablets <N>   Pre-generate only N tablets per worker and re-send
                          them with rebased timestamps (bounds memory for
                          very large runs; 0 = pre-generate all, default)
    --tablets-per-rpc <N> Tree model only: send N tablets per RPC via
                          insert_tablets (default: 1 = insert_tablet)
    --cleanup             Drop the benchmark database after the run
    --help                Print this help

Total points = devices x sensors x batches x batch-size
(defaults: 100 x 10 x 20 x 1000 = 20,000,000).";

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_args() -> BenchConfig {
    let mut config = BenchConfig {
        mode: Mode::Tree,
        devices: 100,
        sensors: 10,
        batches: 20,
        batch_size: 1000,
        clients: 8,
        host: env_or("IOTDB_HOST", "127.0.0.1"),
        port: env_or("IOTDB_PORT", "6667").parse().unwrap_or(6667),
        user: env_or("IOTDB_USER", "root"),
        password: env_or("IOTDB_PASSWORD", "root"),
        base_ts: 1_720_000_000_000,
        point_step: 1000,
        reuse_tablets: 0,
        tablets_per_rpc: 1,
        cleanup: false,
    };

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    let value = |i: &mut usize, flag: &str| -> String {
        *i += 1;
        args.get(*i).cloned().unwrap_or_else(|| {
            eprintln!("missing value for {flag}\n\n{USAGE}");
            exit(2);
        })
    };
    while i < args.len() {
        let flag = args[i].as_str();
        match flag {
            "--mode" => {
                config.mode = match value(&mut i, flag).as_str() {
                    "tree" => Mode::Tree,
                    "table" => Mode::Table,
                    other => {
                        eprintln!("invalid --mode `{other}` (expected tree|table)");
                        exit(2);
                    }
                }
            }
            "--devices" => config.devices = parse_num(&value(&mut i, flag), flag),
            "--sensors" => config.sensors = parse_num(&value(&mut i, flag), flag),
            "--batches" => config.batches = parse_num(&value(&mut i, flag), flag),
            "--batch-size" => config.batch_size = parse_num(&value(&mut i, flag), flag),
            "--clients" => config.clients = parse_num(&value(&mut i, flag), flag),
            "--host" => config.host = value(&mut i, flag),
            "--port" => config.port = parse_num(&value(&mut i, flag), flag) as u16,
            "--user" => config.user = value(&mut i, flag),
            "--password" => config.password = value(&mut i, flag),
            "--base-ts" => config.base_ts = parse_num(&value(&mut i, flag), flag) as i64,
            "--point-step" => config.point_step = parse_num(&value(&mut i, flag), flag) as i64,
            "--reuse-tablets" => config.reuse_tablets = parse_num(&value(&mut i, flag), flag),
            "--tablets-per-rpc" => config.tablets_per_rpc = parse_num(&value(&mut i, flag), flag),
            "--cleanup" => config.cleanup = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                exit(0);
            }
            other => {
                eprintln!("unknown flag `{other}`\n\n{USAGE}");
                exit(2);
            }
        }
        i += 1;
    }

    for (name, v) in [
        ("--devices", config.devices),
        ("--sensors", config.sensors),
        ("--batches", config.batches),
        ("--batch-size", config.batch_size),
        ("--clients", config.clients),
        ("--tablets-per-rpc", config.tablets_per_rpc),
    ] {
        if v == 0 {
            eprintln!("{name} must be positive");
            exit(2);
        }
    }
    if config.tablets_per_rpc > 1 && config.mode == Mode::Table {
        eprintln!("--tablets-per-rpc requires --mode tree (insert_tablets is tree-model only)");
        exit(2);
    }
    config
}

fn parse_num(s: &str, flag: &str) -> usize {
    s.parse().unwrap_or_else(|_| {
        eprintln!("invalid numeric value `{s}` for {flag}");
        exit(2);
    })
}

// ---------------------------------------------------------------------------
// Deterministic data generation (Node.js data-generator.js distribution)
// ---------------------------------------------------------------------------

/// Sensor `i` of `n` gets its type from the Node.js default
/// `INSERT_DATATYPE_PROPORTION`: FLOAT 0.3, DOUBLE 0.2, INT32 0.2,
/// INT64 0.1, TEXT 0.1, BOOLEAN 0.1.
fn sensor_type(i: usize, n: usize) -> TSDataType {
    let f = i as f64 / n as f64;
    if f < 0.3 {
        TSDataType::Float
    } else if f < 0.5 {
        TSDataType::Double
    } else if f < 0.7 {
        TSDataType::Int32
    } else if f < 0.8 {
        TSDataType::Int64
    } else if f < 0.9 {
        TSDataType::Text
    } else {
        TSDataType::Boolean
    }
}

fn type_name(ty: TSDataType) -> &'static str {
    match ty {
        TSDataType::Float => "FLOAT",
        TSDataType::Double => "DOUBLE",
        TSDataType::Int32 => "INT32",
        TSDataType::Int64 => "INT64",
        TSDataType::Text => "TEXT",
        TSDataType::Boolean => "BOOLEAN",
        other => unreachable!("benchmark does not generate {other:?}"),
    }
}

/// Deterministic cell value from (device, sensor, row) — no RNG dependency.
fn cell_value(ty: TSDataType, device: usize, sensor: usize, row: usize) -> Value {
    let seed = (row as u64)
        .wrapping_mul(31)
        .wrapping_add((sensor as u64).wrapping_mul(7))
        .wrapping_add(device as u64);
    match ty {
        TSDataType::Float => Value::Float((seed % 1000) as f32 * 0.1),
        TSDataType::Double => Value::Double((seed % 10_000) as f64 * 0.01),
        TSDataType::Int32 => Value::Int32((seed % 100_000) as i32),
        TSDataType::Int64 => Value::Int64(seed as i64),
        TSDataType::Text => Value::Text(format!(
            "v{:0width$}",
            seed % 1_000_000_000,
            width = TEXT_LEN - 1
        )),
        TSDataType::Boolean => Value::Boolean(seed % 2 == 0),
        other => unreachable!("benchmark does not generate {other:?}"),
    }
}

/// One tablet = one batch for one device. Timestamps are sequential per
/// device: `base_ts + (batch*batch_size + row) * point_step` — already
/// sorted, so the client's sort pass is a no-op.
fn build_tablet(
    config: &BenchConfig,
    device: usize,
    batch: usize,
    sensor_names: &[String],
    sensor_types: &[TSDataType],
) -> Result<Tablet> {
    let mut tablet = match config.mode {
        Mode::Tree => Tablet::new(
            format!("{TREE_DB}.d{device}"),
            sensor_names.to_vec(),
            sensor_types.to_vec(),
        )?,
        Mode::Table => {
            let mut names = Vec::with_capacity(sensor_names.len() + 1);
            names.push("device_id".to_string());
            names.extend_from_slice(sensor_names);
            let mut types = Vec::with_capacity(sensor_types.len() + 1);
            types.push(TSDataType::String);
            types.extend_from_slice(sensor_types);
            let mut categories = vec![ColumnCategory::Tag];
            categories.extend(vec![ColumnCategory::Field; sensor_types.len()]);
            Tablet::new_table(TABLE_NAME, names, types, categories)?
        }
    };

    for r in 0..config.batch_size {
        let row_index = batch * config.batch_size + r;
        let ts = config.base_ts + row_index as i64 * config.point_step;
        let mut row: Vec<Option<Value>> = Vec::with_capacity(sensor_types.len() + 1);
        if config.mode == Mode::Table {
            row.push(Some(Value::String(format!("d{device}"))));
        }
        for (s, &ty) in sensor_types.iter().enumerate() {
            row.push(Some(cell_value(ty, device, s, row_index)));
        }
        tablet.add_row(ts, row)?;
    }
    Ok(tablet)
}

// ---------------------------------------------------------------------------
// Pool abstraction (tree vs table) — both hand out the same PooledSession
// ---------------------------------------------------------------------------

enum Pool {
    Tree(SessionPool),
    Table(TableSessionPool),
}

impl Pool {
    fn acquire(&self) -> Result<PooledSession<'_>> {
        match self {
            Pool::Tree(p) => p.acquire(),
            Pool::Table(p) => p.acquire(),
        }
    }

    fn execute_non_query(&self, sql: &str) -> Result<()> {
        match self {
            Pool::Tree(p) => p.execute_non_query(sql),
            Pool::Table(p) => p.execute_non_query(sql),
        }
    }

    fn close(&self) {
        match self {
            Pool::Tree(p) => p.close(),
            Pool::Table(p) => p.close(),
        }
    }
}

fn create_pool(config: &BenchConfig) -> Result<Pool> {
    let mut pool_config = SessionPoolConfig {
        // One dedicated session per worker; open all of them eagerly so
        // connection setup stays outside the timed section.
        max_size: config.clients,
        min_size: config.clients,
        ..SessionPoolConfig::default()
    }
    .with_node_urls(&[format!("{}:{}", config.host, config.port)])?;
    pool_config.session.username = config.user.clone();
    pool_config.session.password = config.password.clone();
    Ok(match config.mode {
        Mode::Tree => Pool::Tree(SessionPool::new(pool_config)?),
        Mode::Table => Pool::Table(TableSessionPool::new(pool_config)?),
    })
}

// ---------------------------------------------------------------------------
// Schema setup / cleanup (outside the timed section)
// ---------------------------------------------------------------------------

fn setup_schema(
    pool: &Pool,
    config: &BenchConfig,
    sensor_names: &[String],
    sensor_types: &[TSDataType],
) -> Result<()> {
    match config.mode {
        Mode::Tree => {
            // Fresh database each run; ignore "does not exist" on the drop.
            let _ = pool.execute_non_query(&format!("DELETE DATABASE {TREE_DB}"));
            pool.execute_non_query(&format!("CREATE DATABASE {TREE_DB}"))?;
            // Pre-register every timeseries so metadata creation cost stays
            // out of the write path (Node.js schema-manager behavior).
            let mut session = pool.acquire()?;
            for device in 0..config.devices {
                for (name, &ty) in sensor_names.iter().zip(sensor_types) {
                    session.execute_non_query(&format!(
                        "CREATE TIMESERIES {TREE_DB}.d{device}.{name} WITH DATATYPE={}, ENCODING=PLAIN",
                        type_name(ty)
                    ))?;
                }
            }
        }
        Mode::Table => {
            let _ = pool.execute_non_query(&format!("DROP DATABASE IF EXISTS {TABLE_DB}"));
            pool.execute_non_query(&format!("CREATE DATABASE {TABLE_DB}"))?;
            // The pool replays the last USE on every acquire, so all worker
            // sessions land in the right database.
            pool.execute_non_query(&format!("USE {TABLE_DB}"))?;
            let columns: Vec<String> = std::iter::once("device_id STRING TAG".to_string())
                .chain(
                    sensor_names
                        .iter()
                        .zip(sensor_types)
                        .map(|(name, &ty)| format!("{name} {} FIELD", type_name(ty))),
                )
                .collect();
            pool.execute_non_query(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({})",
                columns.join(", ")
            ))?;
        }
    }
    Ok(())
}

fn cleanup_schema(pool: &Pool, mode: Mode) -> Result<()> {
    match mode {
        Mode::Tree => pool.execute_non_query(&format!("DELETE DATABASE {TREE_DB}")),
        Mode::Table => pool.execute_non_query(&format!("DROP DATABASE {TABLE_DB}")),
    }
}

/// Post-run sanity check: read back the row count for the whole run.
fn verify_row_count(pool: &Pool, config: &BenchConfig) -> Result<()> {
    let expected_rows = (config.devices * config.batches * config.batch_size) as i64;
    let sql = match config.mode {
        // COUNT over one representative sensor across all devices.
        Mode::Tree => format!("SELECT COUNT(s_0) FROM {TREE_DB}.*"),
        Mode::Table => format!("SELECT COUNT(*) FROM {TABLE_NAME}"),
    };
    let mut session = pool.acquire()?;
    let mut dataset = session.execute_query(&sql)?;
    let mut total: i64 = 0;
    while let Some(row) = dataset.next_row()? {
        for v in &row.values {
            if let Value::Int64(n) = v {
                total += n;
            }
        }
    }
    let status = if total == expected_rows {
        "OK"
    } else {
        "MISMATCH"
    };
    println!("[Verify] rows on server: {total} (expected {expected_rows}) — {status}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Metrics (semantics mirror iot-benchmark's Measurement / DBWrapper)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct WorkerStats {
    /// One entry per **successful** operation (ms). The timed span covers
    /// batch preparation + the insert RPC (iot-benchmark's DBWrapper times
    /// genTablet + submit + Future.get; failures record no latency).
    latencies_ms: Vec<f64>,
    /// All operations attempted (ok + failed).
    ops: u64,
    /// Failed operations (iot-benchmark failOperation).
    fail_operations: u64,
    /// Points from successful operations only (iot-benchmark okPoint).
    points: u64,
    /// Points belonging to failed operations (iot-benchmark failPoint).
    fail_points: u64,
    error_samples: Vec<String>,
}

/// Node.js `getPercentile`: index = ceil(p/100 × n) − 1 over ascending samples.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((p / 100.0 * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

fn print_summary(config: &BenchConfig, wall: Duration, workers: &[WorkerStats]) {
    let mut latencies: Vec<f64> = workers
        .iter()
        .flat_map(|w| w.latencies_ms.iter().copied())
        .collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).expect("latency is never NaN"));
    let ops: u64 = workers.iter().map(|w| w.ops).sum();
    let fail_operations: u64 = workers.iter().map(|w| w.fail_operations).sum();
    let ok_operations = ops - fail_operations;
    let ok_points: u64 = workers.iter().map(|w| w.points).sum();
    let fail_points: u64 = workers.iter().map(|w| w.fail_points).sum();
    let secs = wall.as_secs_f64();
    // AVG = sum(ok latencies) / okOperations (Measurement.calculateMetrics).
    let avg = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    };
    // SLOWEST_THREAD = max over threads of that thread's latency sum
    // (Measurement.mergeMeasurement → MAX_THREAD_LATENCY_SUM).
    let slowest_thread: f64 = workers
        .iter()
        .map(|w| w.latencies_ms.iter().sum::<f64>())
        .fold(0.0, f64::max);

    let sep = "=".repeat(80);
    println!("\n{sep}\nBENCHMARK RESULTS\n{sep}");
    println!("\n[Execution Time]");
    println!(
        "  Duration:              {:.2}s ({:.0}ms)",
        secs,
        secs * 1000.0
    );
    println!("\n[Operations]");
    println!("  Total Operations:      {ops}");
    println!("  Successful:            {ok_operations}");
    println!("  Failed:                {fail_operations}");
    println!(
        "  Success Rate:          {:.2}%",
        if ops == 0 {
            0.0
        } else {
            ok_operations as f64 / ops as f64 * 100.0
        }
    );
    println!("\n[Data Points]");
    println!("  Total Points Written:  {ok_points}");
    if fail_points > 0 {
        println!("  Failed Points:         {fail_points}");
    }
    println!("\n[Throughput]");
    println!("  Operations/sec:        {:.2}", ops as f64 / secs);
    println!("  Points/sec:            {:.0}", ok_points as f64 / secs);
    println!("\n[Latency (ms)] (successful operations only; span includes batch preparation)");
    println!(
        "  Min:                   {:.2}ms",
        latencies.first().copied().unwrap_or(0.0)
    );
    println!(
        "  Max:                   {:.2}ms",
        latencies.last().copied().unwrap_or(0.0)
    );
    println!("  Average:               {avg:.2}ms");
    println!(
        "  P50 (Median):          {:.2}ms",
        percentile(&latencies, 50.0)
    );
    println!(
        "  P90:                   {:.2}ms",
        percentile(&latencies, 90.0)
    );
    println!(
        "  P95:                   {:.2}ms",
        percentile(&latencies, 95.0)
    );
    println!(
        "  P99:                   {:.2}ms",
        percentile(&latencies, 99.0)
    );

    let samples: Vec<&String> = workers.iter().flat_map(|w| &w.error_samples).collect();
    if !samples.is_empty() {
        println!("\n[Error Samples]");
        for (i, err) in samples.iter().take(5).enumerate() {
            println!("  {}. {err}", i + 1);
        }
    }
    println!("\n{sep}");
    println!(
        "Config: mode={} devices={} sensors={} batches={} batch-size={} clients={} → {} points",
        if config.mode == Mode::Tree {
            "tree"
        } else {
            "table"
        },
        config.devices,
        config.sensors,
        config.batches,
        config.batch_size,
        config.clients,
        config.total_points(),
    );

    // --- iot-benchmark-style matrices (columns/order match Measurement) ----
    // %-25s per Result-Matrix cell, %-12s per Latency-Matrix metric cell.
    println!("\nTest elapsed time (not include schema creation): {secs:.2} second");
    println!(
        "----------------------------------------------------------Result Matrix----------------------------------------------------------"
    );
    println!(
        "{:<25}{:<25}{:<25}{:<25}{:<25}{:<25}",
        "Operation", "okOperation", "okPoint", "failOperation", "failPoint", "throughput(point/s)"
    );
    println!(
        "{:<25}{:<25}{:<25}{:<25}{:<25}{:<25}",
        "INGESTION",
        ok_operations,
        ok_points,
        fail_operations,
        fail_points,
        format!(
            "{:.2}",
            if secs > 0.0 {
                ok_points as f64 / secs
            } else {
                0.0
            }
        )
    );
    println!(
        "---------------------------------------------------------------------------------------------------------------------------------"
    );
    println!(
        "--------------------------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------------------------"
    );
    print!("{:<25}", "Operation");
    let metrics: [(&str, f64); 12] = [
        ("AVG", avg),
        ("MIN", latencies.first().copied().unwrap_or(0.0)),
        ("P10", percentile(&latencies, 10.0)),
        ("P25", percentile(&latencies, 25.0)),
        ("MEDIAN", percentile(&latencies, 50.0)),
        ("P75", percentile(&latencies, 75.0)),
        ("P90", percentile(&latencies, 90.0)),
        ("P95", percentile(&latencies, 95.0)),
        ("P99", percentile(&latencies, 99.0)),
        ("P999", percentile(&latencies, 99.9)),
        ("MAX", latencies.last().copied().unwrap_or(0.0)),
        ("SLOWEST_THREAD", slowest_thread),
    ];
    for (name, _) in &metrics {
        print!("{name:<12}");
    }
    println!();
    print!("{:<25}", "INGESTION");
    for (_, value) in &metrics {
        print!("{:<12}", format!("{value:.2}"));
    }
    println!();
    println!(
        "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------"
    );
    println!("Note: exact percentiles (iot-benchmark uses t-digest approximation)");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    env_logger::init();
    let config = parse_args();

    let sensor_names: Vec<String> = (0..config.sensors).map(|i| format!("s_{i}")).collect();
    let sensor_types: Vec<TSDataType> = (0..config.sensors)
        .map(|i| sensor_type(i, config.sensors))
        .collect();

    let sep = "=".repeat(80);
    println!("{sep}\nIoTDB Rust Client Write Benchmark\n{sep}");
    println!(
        "  Mode:        {}",
        if config.mode == Mode::Tree {
            "tree"
        } else {
            "table"
        }
    );
    println!("  Server:      {}:{}", config.host, config.port);
    println!("  Devices:     {}", config.devices);
    println!(
        "  Sensors:     {} ({})",
        config.sensors,
        sensor_types
            .iter()
            .map(|&t| type_name(t))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Batches:     {} per device", config.batches);
    println!("  Batch size:  {} rows", config.batch_size);
    println!("  Clients:     {} (pool size)", config.clients);
    println!("  Total:       {} points", config.total_points());
    println!("{sep}\n");

    // --- Pool + schema (untimed) -----------------------------------------
    println!(
        "[Setup] opening session pool ({} eager sessions)...",
        config.clients
    );
    let pool = create_pool(&config)?;
    println!("[Setup] creating schema...");
    let t0 = Instant::now();
    setup_schema(&pool, &config, &sensor_names, &sensor_types)?;
    println!("[Setup] schema ready in {:.2}s", t0.elapsed().as_secs_f64());

    // --- Pre-generate all tablets (untimed) -------------------------------
    // Worker w owns devices where device % clients == w and walks them
    // batch-major, i.e. round-robin over its devices.
    println!("[Setup] pre-generating test data...");
    let t0 = Instant::now();
    // Each worker's full schedule is `batches × its devices` inserts. With
    // --reuse-tablets N only the first N tablets are materialized; the worker
    // cycles over them and rebases timestamps per iteration (see worker loop).
    let mut worker_tablets: Vec<(Vec<Tablet>, usize)> = Vec::with_capacity(config.clients);
    for w in 0..config.clients {
        let devices: Vec<usize> = (0..config.devices)
            .filter(|d| d % config.clients == w)
            .collect();
        let schedule_len = devices.len() * config.batches;
        let materialized = if config.reuse_tablets > 0 {
            schedule_len.min(config.reuse_tablets)
        } else {
            schedule_len
        };
        let mut tablets = Vec::with_capacity(materialized);
        'gen: for batch in 0..config.batches {
            for &device in &devices {
                if tablets.len() == materialized {
                    break 'gen;
                }
                tablets.push(build_tablet(
                    &config,
                    device,
                    batch,
                    &sensor_names,
                    &sensor_types,
                )?);
            }
        }
        worker_tablets.push((tablets, schedule_len));
    }
    println!(
        "[Setup] {} tablets generated in {:.2}s{}",
        worker_tablets.iter().map(|(t, _)| t.len()).sum::<usize>(),
        t0.elapsed().as_secs_f64(),
        if config.reuse_tablets > 0 {
            format!(
                " (reused cyclically over {} inserts)",
                worker_tablets.iter().map(|(_, n)| n).sum::<usize>()
            )
        } else {
            String::new()
        }
    );

    // --- Timed insert phase ------------------------------------------------
    println!(
        "\n[Test Phase] running with {} concurrent clients...\n",
        config.clients
    );
    let ops_done = AtomicU64::new(0);
    let points_done = AtomicU64::new(0);
    let stop_reporter = AtomicBool::new(false);

    let started = Instant::now();
    let worker_stats: Vec<WorkerStats> = thread::scope(|scope| {
        // Progress reporter (Node.js ProgressReporter equivalent).
        let reporter = scope.spawn(|| {
            let mut last_ops = 0u64;
            let mut last_tick = Instant::now();
            while !stop_reporter.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(200));
                if last_tick.elapsed() >= REPORT_INTERVAL {
                    let ops = ops_done.load(Ordering::Relaxed);
                    let rate = (ops - last_ops) as f64 / last_tick.elapsed().as_secs_f64();
                    println!(
                        "[Progress] Operations: {ops}, Rate: {rate:.2} ops/s, Total Points: {}",
                        points_done.load(Ordering::Relaxed)
                    );
                    last_ops = ops;
                    last_tick = Instant::now();
                }
            }
        });

        let handles: Vec<_> = worker_tablets
            .iter_mut()
            .map(|(tablets, schedule_len)| {
                let schedule_len = *schedule_len;
                let pool = &pool;
                let ops_done = &ops_done;
                let points_done = &points_done;
                scope.spawn(move || -> Result<WorkerStats> {
                    let mut session = pool.acquire()?;
                    let mut stats = WorkerStats {
                        latencies_ms: Vec::with_capacity(schedule_len),
                        ..WorkerStats::default()
                    };
                    let materialized = tablets.len();
                    let mut i = 0;
                    while i < schedule_len {
                        // Per-op timing mirrors iot-benchmark: the timer
                        // starts BEFORE batch preparation, as Java's
                        // DBWrapper.insertOneBatch times genTablet + submit
                        // + Future.get. Mapping per mode:
                        //   * reuse mode  — timestamp rebase (the per-batch
                        //     work a streaming producer would do) + the RPC
                        //     are inside the timed span;
                        //   * non-reuse   — tablets are pre-built (Java's
                        //     pre-built workload), so the span covers
                        //     borrowing the tablet slice + the RPC.
                        let start = Instant::now();
                        // A chunk never wraps the materialized ring, so it
                        // always maps to one contiguous slice.
                        let idx = i % materialized;
                        let chunk = config
                            .tablets_per_rpc
                            .min(schedule_len - i)
                            .min(materialized - idx);
                        if config.reuse_tablets > 0 {
                            // Rebase each reused tablet onto its iteration's
                            // disjoint time window so every insert writes
                            // fresh timestamps.
                            for (k, tablet) in tablets[idx..idx + chunk].iter_mut().enumerate() {
                                let window = config.base_ts
                                    + ((i + k) * config.batch_size) as i64 * config.point_step;
                                for (r, ts) in tablet.timestamps_mut().iter_mut().enumerate() {
                                    *ts = window + r as i64 * config.point_step;
                                }
                            }
                        }
                        let batch = &tablets[idx..idx + chunk];
                        let points: u64 = batch
                            .iter()
                            .map(|t| (t.row_count() * config.sensors) as u64)
                            .sum();
                        let outcome = if chunk == 1 {
                            session.insert_tablet(&batch[0])
                        } else {
                            session.insert_tablets(batch, false)
                        };
                        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                        stats.ops += 1;
                        ops_done.fetch_add(1, Ordering::Relaxed);
                        match outcome {
                            Ok(()) => {
                                // Only successful operations record a latency
                                // sample (DBWrapper.measureOneBatch).
                                stats.latencies_ms.push(elapsed_ms);
                                stats.points += points;
                                points_done.fetch_add(points, Ordering::Relaxed);
                            }
                            Err(e) => {
                                stats.fail_operations += 1;
                                stats.fail_points += points;
                                if stats.error_samples.len() < 5 {
                                    stats.error_samples.push(e.to_string());
                                }
                            }
                        }
                        i += chunk;
                    }
                    Ok(stats)
                })
            })
            .collect();

        let stats = handles
            .into_iter()
            .map(|h| h.join().expect("worker thread panicked"))
            .collect::<Result<Vec<_>>>();
        stop_reporter.store(true, Ordering::Relaxed);
        reporter.join().expect("reporter thread panicked");
        stats
    })?;
    let wall = started.elapsed();

    // --- Results + verification + cleanup ---------------------------------
    print_summary(&config, wall, &worker_stats);
    verify_row_count(&pool, &config)?;

    if config.cleanup {
        println!("[Cleanup] dropping benchmark database...");
        cleanup_schema(&pool, config.mode)?;
    }
    pool.close();
    Ok(())
}
