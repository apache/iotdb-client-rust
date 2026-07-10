# Apache IoTDB Rust Client

Rust client SDK for [Apache IoTDB](https://iotdb.apache.org/), speaking Apache Thrift RPC (default port 6667). Supports the **tree model** (`Session`, device/timeseries paths) with the **table model** (`TableSession`, SQL dialect) planned — mirroring the architecture of the Node.js and C# SDKs.

## Status

Early scaffold. Thrift stubs are not yet generated; session layer is a skeleton.

## Build & Test

```sh
cargo build
cargo test                 # unit tests
cargo test test_name       # single test
cargo fmt && cargo clippy  # format + lint
```

## Thrift codegen

IDL sources in `thrift/` (`client.thrift`, `common.thrift`) originate from the IoTDB repo's `iotdb-protocol/`. Regenerate stubs (requires the `thrift` compiler ≥ 0.23):

```sh
thrift --gen rs -out src/protocol thrift/common.thrift
thrift --gen rs -out src/protocol thrift/client.thrift
```

Never hand-edit generated files.

## Layout

- `src/client/` — Session / pool layer (tree & table model)
- `src/connection/` — low-level Thrift transport (framed + binary protocol)
- `src/data/` — Tablet, SessionDataSet, TSDataType (official TSFile codes 0–11)
- `src/protocol/` — generated Thrift stubs
- `thrift/` — Thrift IDL sources
