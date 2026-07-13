# Apache IoTDB Rust Client

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

[English](./README.md) | [中文](./README_ZH.md)

Rust client SDK for [Apache IoTDB](https://iotdb.apache.org/), speaking Apache Thrift RPC (default port 6667). Supports both IoTDB data models, mirroring the architecture of the Node.js and C# SDKs:

- **Tree model** — `Session` / `SessionPool`: device/timeseries paths (`root.sg.d1.s1`)
- **Table model** — `TableSession` / `TableSessionPool`: relational SQL dialect

## Status

Working client: session management (with multi-node failover), tablet writes (`insertTablet`) for both models, TsBlock query decoding with paging iteration, and thread-safe session pools. Not yet published to crates.io.

## Requirements

- Rust 1.75+
- Apache IoTDB 2.x (examples and integration tests use `apache/iotdb:2.0.6-standalone`)

## Installation

Once published to crates.io:

```toml
[dependencies]
iotdb-client = "0.1"
```

Until then, use a git dependency:

```toml
[dependencies]
iotdb-client = { git = "https://github.com/apache/iotdb-client-rust" }
```

## Quick start

### Tree model

```rust
use iotdb_client::{Result, Session, SessionConfig, TSDataType, Tablet, Value};

fn main() -> Result<()> {
    let config = SessionConfig::default().with_node_urls(&["127.0.0.1:6667"])?;
    let mut session = Session::new(config);
    session.open()?;

    session.execute_non_query("CREATE DATABASE root.demo")?;
    session.execute_non_query(
        "CREATE TIMESERIES root.demo.d1.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
    )?;

    // Batch write via a column-major tablet (nulls allowed).
    let mut tablet = Tablet::new(
        "root.demo.d1",
        vec!["temperature".into()],
        vec![TSDataType::Double],
    )?;
    tablet.add_row(1_720_000_000_000, vec![Some(Value::Double(21.5))])?;
    tablet.add_row(1_720_000_001_000, vec![None])?; // null cell
    session.insert_tablet(&tablet)?;
    // Multiple tablets in one RPC: insert_tablets(&[t1, t2], false)
    // (tree model only; insert_aligned_tablets for aligned devices).

    // Or write a single row via insertRecord (row-oriented; aligned variants
    // and multi-row insert_records / insert_records_of_one_device also exist).
    session.insert_record(
        "root.demo.d1",
        1_720_000_002_000,
        vec!["temperature".into()],
        &[Value::Double(22.0)],
        false, // is_aligned
    )?;

    // Query with row iteration; the dataset borrows the session until dropped.
    {
        let mut dataset = session.execute_query("SELECT temperature FROM root.demo.d1")?;
        while let Some(row) = dataset.next_row()? {
            println!("ts={:?} values={:?}", row.timestamp, row.values);
        }
    }

    session.execute_non_query("DELETE DATABASE root.demo")?;
    session.close()
}
```

### Table model

```rust
use iotdb_client::{ColumnCategory, Result, TSDataType, TableSession, Tablet, Value};

fn main() -> Result<()> {
    let mut session = TableSession::builder()
        .node_urls(&["127.0.0.1:6667"])?
        .username("root")
        .password("root")
        .build()?;

    session.execute_non_query("CREATE DATABASE IF NOT EXISTS demo")?;
    session.execute_non_query("USE demo")?;
    session.execute_non_query(
        "CREATE TABLE IF NOT EXISTS sensors (device_id STRING TAG, temperature DOUBLE FIELD)",
    )?;

    let mut tablet = Tablet::new_table(
        "sensors",
        vec!["device_id".into(), "temperature".into()],
        vec![TSDataType::String, TSDataType::Double],
        vec![ColumnCategory::Tag, ColumnCategory::Field],
    )?;
    tablet.add_row(
        1_720_000_000_000,
        vec![
            Some(Value::String("dev-1".into())),
            Some(Value::Double(21.5)),
        ],
    )?;
    session.insert(&tablet)?;

    {
        let mut dataset = session.execute_query("SELECT time, device_id, temperature FROM sensors")?;
        while let Some(row) = dataset.next_row()? {
            println!("{:?}", row.values);
        }
    }

    session.execute_non_query("DROP DATABASE demo")?;
    session.close()
}
```

### Session pool

```rust
use std::sync::Arc;
use iotdb_client::{Result, SessionPool, SessionPoolConfig};

fn main() -> Result<()> {
    let config = SessionPoolConfig {
        max_size: 4,
        ..SessionPoolConfig::default()
    }
    .with_node_urls(&["127.0.0.1:6667"])?;
    let pool = Arc::new(SessionPool::new(config)?);

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || -> Result<()> {
                let mut session = pool.acquire()?; // RAII guard, released on drop
                session.execute_non_query("SHOW DATABASES")?;
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("thread panicked")?;
    }

    pool.close();
    Ok(())
}
```

Full runnable versions live in [`examples/`](./examples):

```sh
cargo run --example tree_session
cargo run --example table_session
cargo run --example session_pool
```

## TLS & RPC compression

**RPC compression** (IoTDB's term for the Thrift *compact protocol*) is a plain config flag:

```rust
let config = SessionConfig { enable_rpc_compression: true, ..Default::default() };
// or: TableSession::builder().enable_rpc_compression(true)...
```

It must match the **server** setting `dn_rpc_thrift_compression_enable` (default `false`). The server speaks exactly one protocol — there is no per-connection negotiation, so a mismatch in either direction fails at the first RPC with a transport error.

**TLS** is behind the `tls` cargo feature (platform-native TLS via [`native-tls`](https://crates.io/crates/native-tls)):

```toml
iotdb-client = { version = "0.1", features = ["tls"] }
```

```rust
let config = SessionConfig {
    use_ssl: true,
    ca_cert_path: Some("ca.pem".into()),  // trust a private CA / self-signed cert
    accept_invalid_certs: false,          // true skips verification (tests only!)
    domain_override: None,                // SNI/validation hostname when connecting by IP
    ..Default::default()
};
// or: TableSession::builder().use_ssl(true).ca_cert_path("ca.pem")...
```

For **mutual TLS** (server has `thrift_ssl_client_auth=true`), add a PEM client certificate and its PKCS#8 key — the analogue of the Node.js `sslOptions.cert`/`sslOptions.key`:

```rust
let config = SessionConfig {
    use_ssl: true,
    ca_cert_path: Some("ca.pem".into()),
    client_cert_path: Some("client.crt".into()),  // must be set together
    client_key_path: Some("client.key".into()),   // with client_cert_path
    ..Default::default()
};
// or: TableSession::builder().use_ssl(true).client_cert_path("client.crt").client_key_path("client.key")...
```

The server needs Thrift SSL enabled (`enable_thrift_ssl=true` + key store; see `tests/fixtures/tls/README.md` for a throwaway docker setup). Pool configs pass all options through their embedded `session` config.

## Thrift codegen

Generated stubs live in `src/protocol/` (`client.rs`, `common.rs`); never hand-edit them. The IDL sources in `thrift/` are synced from the IoTDB repo's `iotdb-protocol/` (`thrift-datanode/src/main/thrift/client.thrift`, `thrift-commons/src/main/thrift/common.thrift`).

Regenerate with:

```sh
./tools/generate-thrift.sh
```

The script picks the Thrift compiler in order of preference:

1. `$THRIFT_BIN` if set
2. the IoTDB repo's Maven build output (`$IOTDB_REPO`, default `../iotdb`): `iotdb-protocol/*/target/thrift/bin/thrift` — run `./mvnw generate-sources -pl iotdb-protocol/thrift-datanode -am` there first. This guarantees the exact Thrift version pinned by the IoTDB pom.
3. `thrift` on `PATH` (version must match the IoTDB pom's `thrift.version`)

When `$IOTDB_REPO` is present, the IDL files are re-synced from it before generation, and the Apache license headers are re-prepended to the generated files.

## Development

```sh
cargo build                              # build
cargo test                               # unit tests (live tests self-skip without a server)
cargo test test_name                     # single test
cargo fmt --check                        # format check
cargo clippy --all-targets -- -D warnings  # lint
./tools/check-license.sh                 # license header check
```

Integration tests need a running IoTDB; the live tests detect it on `127.0.0.1:6667` and skip gracefully when absent:

```sh
docker compose up -d   # standalone IoTDB (see docker-compose-1c1d.yml for a 1C1D cluster)
cargo test             # now includes the live-server tests
```

## Benchmark

`examples/benchmark.rs` is a write-performance benchmark modeled on the Node.js client's `benchmark/` suite (which follows [thulab/iot-benchmark](https://github.com/thulab/iot-benchmark)); metric definitions match, so results are comparable across the SDKs. Tablets are pre-generated outside the timed section; N worker threads each own a pooled session and insert `insert_tablet` batches round-robin over their devices. Timestamps are sequential per device from a fixed base, so runs are deterministic.

```sh
# tree model, defaults: 100 devices × 10 sensors × 20 batches × 1000 rows = 20M points, 8 clients
cargo run --release --example benchmark -- --mode tree

# table model at a custom scale, dropping the database afterwards
cargo run --release --example benchmark -- --mode table \
    --devices 20 --sensors 10 --batches 100 --batch-size 100 --clients 8 --cleanup
```

Knobs: `--mode tree|table`, `--devices`, `--sensors`, `--batches` (per device), `--batch-size` (rows per tablet), `--clients` (worker threads = pool size), `--host/--port/--user/--password` (also via `IOTDB_HOST/PORT/USER/PASSWORD`), `--base-ts`, `--point-step`, `--reuse-tablets` (pre-generate only N tablets per worker and re-send them with rebased timestamps — bounds memory for very large runs; the per-batch timestamp rewrite happens inside the timed loop, like a real streaming producer), `--tablets-per-rpc` (tree model: batch N tablets into one `insert_tablets` RPC), `--cleanup`. Sensor types follow the Node.js default distribution (30% FLOAT, 20% DOUBLE, 20% INT32, 10% INT64, 10% TEXT, 10% BOOLEAN). The report includes total points, wall time, points/sec, per-batch latency p50/p90/p95/p99/max, error count, and a read-back row-count verification.

Measured on an Apple M2 Pro (10 cores), IoTDB 2.0.6 standalone in Docker on the same machine (Docker VM: all 10 CPUs / 8 GB; JVM heap 1 GB), release build:

| Mode | Devices × Sensors × Batches × Rows | Clients | Points | Throughput | p50 / p99 latency |
| --- | --- | --- | --- | --- | --- |
| tree | 20 × 10 × 100 × 100 | 8 | 2M | ~1.98M pts/s | 2.46 ms / 8.38 ms |
| table | 20 × 10 × 100 × 100 | 8 | 2M | ~1.97M pts/s | 2.13 ms / 9.97 ms |
| tree | 100 × 10 × 20 × 1000 | 8 | 20M | ~12.4M pts/s | 4.45 ms / 27.03 ms |
| tree | 100 × 100 × 4 × 1000 | 10 | 40M | ~15–20M pts/s | 31 ms / 156 ms |
| tree | 100 × 100 × 25 × 1000, `--tablets-per-rpc 4` | 10 | 250M | **~21–22.5M pts/s** | 105 ms / 907 ms |

Throughput scales with points per RPC: wider tablets (100 sensors = 100k points per 1000-row tablet) and multi-tablet `insert_tablets` batching lift the same hardware from ~12M to ~22M pts/s sustained (250M points, `--reuse-tablets`). Beyond ~400k points per RPC — or more clients than cores — throughput plateaus and tail latency grows; during peak runs the server JVM bursts to ~3 cores then stalls on memtable flushes while the Rust client sits at ~30% of one core, so the ceiling here is the co-located dockerized server (1 GB heap), not the client. Numbers are client+server on one machine — treat them as an upper bound on client overhead, not a server capacity measurement.

## Project layout

| Path | Contents |
| --- | --- |
| `src/client/` | `Session`, `TableSession`, `SessionPool`, `TableSessionPool`, `SessionDataSet` |
| `src/connection/` | Low-level Thrift transport (framed transport + binary protocol) |
| `src/data/` | `Tablet`, `Value`, `TSDataType` (official TSFile codes 0–11), TsBlock decoding, bitmaps |
| `src/protocol/` | Generated Thrift stubs (do not edit) |
| `thrift/` | Thrift IDL sources, synced from the IoTDB repo |
| `examples/` | Runnable examples for both models and the pools |
| `tools/` | Codegen and license-check scripts |

## License

[Apache License 2.0](./LICENSE)
