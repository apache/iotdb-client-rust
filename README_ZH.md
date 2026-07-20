# Apache IoTDB Rust 客户端

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

[English](./README.md) | [中文](./README_ZH.md)

[Apache IoTDB](https://iotdb.apache.org/) 的 Rust 客户端 SDK，基于 Apache Thrift RPC（默认端口 6667）。支持 IoTDB 的两种数据模型，架构与 Node.js、C# SDK 保持一致：

- **树模型** — `Session` / `SessionPool`：设备/时间序列路径（`root.sg.d1.s1`）
- **表模型** — `TableSession` / `TableSessionPool`：关系型 SQL 方言

## 状态

可用的客户端：会话管理（支持多节点故障转移）、两种模型的 Tablet 批量写入（`insertTablet`）、TsBlock 查询解码及分页迭代、线程安全的会话池。尚未发布到 crates.io。

## 环境要求

- Rust 1.75+
- Apache IoTDB 2.x — 完整的服务器版本兼容矩阵、每个 release 对应的 IDL/Thrift 工具链版本，以及 SemVer/弃用政策见 [COMPATIBILITY.md](./COMPATIBILITY.md)（CI 针对 2.0.6 与 2.0.10 测试）

## 安装

发布到 crates.io 之后：

```toml
[dependencies]
iotdb-client-rust = "0.1"
```

在此之前，可使用 git 依赖：

```toml
[dependencies]
iotdb-client = { git = "https://github.com/apache/iotdb-client-rust" }
```

## 快速开始

### 树模型

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

    // 通过列式 Tablet 批量写入（允许 null）。
    let mut tablet = Tablet::new(
        "root.demo.d1",
        vec!["temperature".into()],
        vec![TSDataType::Double],
    )?;
    tablet.add_row(1_720_000_000_000, vec![Some(Value::Double(21.5))])?;
    tablet.add_row(1_720_000_001_000, vec![None])?; // null 单元格
    session.insert_tablet(&tablet)?;
    // 一次 RPC 批量写入多个 tablet：insert_tablets(&[t1, t2], false)
    //（仅树模型；aligned 设备用 insert_aligned_tablets）。

    // 也可以通过 insertRecord 写入单行（行式编码；另有 aligned 变体
    // 以及多行的 insert_records / insert_records_of_one_device）。
    session.insert_record(
        "root.demo.d1",
        1_720_000_002_000,
        vec!["temperature".into()],
        &[Value::Double(22.0)],
        false, // is_aligned
    )?;

    // 逐行迭代查询结果；数据集在被 drop 之前持有会话的借用。
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

### 表模型

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

### 会话池

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
                let mut session = pool.acquire()?; // RAII 守卫，drop 时归还
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

完整可运行版本见 [`examples/`](./examples)：

```sh
cargo run --example tree_session
cargo run --example table_session
cargo run --example session_pool
```

## TLS 与 RPC 压缩

**RPC 压缩**（IoTDB 术语，实为 Thrift *compact 协议*）只是一个配置开关：

```rust
let config = SessionConfig { enable_rpc_compression: true, ..Default::default() };
// 或：TableSession::builder().enable_rpc_compression(true)...
```

必须与**服务端**配置 `dn_rpc_thrift_compression_enable`（默认 `false`）一致。服务端只讲一种协议——没有按连接协商的机制，任意方向的不匹配都会在第一个 RPC 上以传输错误失败。

**TLS** 位于 `tls` cargo feature 之后（基于 [`native-tls`](https://crates.io/crates/native-tls) 的平台原生 TLS）：

```toml
iotdb-client-rust = { version = "0.1", features = ["tls"] }
```

```rust
let config = SessionConfig {
    use_ssl: true,
    ca_cert_path: Some("ca.pem".into()),  // 信任私有 CA / 自签名证书
    accept_invalid_certs: false,          // true 跳过证书校验（仅限测试！）
    domain_override: None,                // 按 IP 连接时用于 SNI/校验的主机名
    ..Default::default()
};
// 或：TableSession::builder().use_ssl(true).ca_cert_path("ca.pem")...
```

**双向 TLS**（服务端 `thrift_ssl_client_auth=true`）需额外提供 PEM 客户端证书及其 PKCS#8 私钥 —— 对应 Node.js 的 `sslOptions.cert`/`sslOptions.key`：

```rust
let config = SessionConfig {
    use_ssl: true,
    ca_cert_path: Some("ca.pem".into()),
    client_cert_path: Some("client.crt".into()),  // 必须与 client_key_path
    client_key_path: Some("client.key".into()),   // 同时设置
    ..Default::default()
};
// 或：TableSession::builder().use_ssl(true).client_cert_path("client.crt").client_key_path("client.key")...
```

服务端需开启 Thrift SSL（`enable_thrift_ssl=true` + 密钥库；一次性 docker 环境见 `tests/fixtures/tls/README.md`）。连接池配置通过其内嵌的 `session` 配置透传全部选项。

## Thrift 代码生成

生成的桩代码位于 `src/protocol/`（`client.rs`、`common.rs`），请勿手动编辑。`thrift/` 中的 IDL 源文件从 IoTDB 仓库的 `iotdb-protocol/` 同步（`thrift-datanode/src/main/thrift/client.thrift`、`thrift-commons/src/main/thrift/common.thrift`）。

重新生成：

```sh
./tools/generate-thrift.sh
```

脚本按以下优先级选择 Thrift 编译器：

1. `$THRIFT_BIN`（若已设置）
2. IoTDB 仓库 Maven 构建产物（`$IOTDB_REPO`，默认 `../iotdb`）：`iotdb-protocol/*/target/thrift/bin/thrift` — 需先在该仓库执行 `./mvnw generate-sources -pl iotdb-protocol/thrift-datanode -am`。这可保证 Thrift 版本与 IoTDB pom 中固定的版本完全一致。
3. `PATH` 上的 `thrift`（版本须与 IoTDB pom 的 `thrift.version` 匹配）

当 `$IOTDB_REPO` 存在时，生成前会先从其重新同步 IDL 文件，并在生成后为生成文件重新添加 Apache 许可证头。

## 开发

```sh
cargo build                              # 构建
cargo test                               # 单元测试（无服务器时在线测试自动跳过）
cargo test test_name                     # 运行单个测试
cargo fmt --check                        # 格式检查
cargo clippy --all-targets -- -D warnings  # 静态检查
./tools/check-license.sh                 # 许可证头检查
```

集成测试需要一个运行中的 IoTDB；在线测试会探测 `127.0.0.1:6667`，服务器不可达时自动跳过：

```sh
docker compose up -d   # 单机版 IoTDB（1C1D 集群拓扑见 docker-compose-1c1d.yml）
cargo test             # 此时包含在线测试
```

## 性能基准测试

`examples/benchmark.rs` 是写入性能基准，参考 Node.js 客户端的 `benchmark/` 套件与 [thulab/iot-benchmark](https://github.com/thulab/iot-benchmark)。Tablet 在计时区间之外预先生成；N 个工作线程各持有一个池化会话，按批次轮询各自的设备执行 `insert_tablet`。时间戳从固定基准按设备顺序递增，运行结果可复现。

> **统计口径现已对齐 iot-benchmark：**单次操作的计时区间包含批次准备（而非仅 insert RPC）；失败操作不计入延迟样本（单独统计 `failOperation`/`failPoint`）；输出包含 iot-benchmark 风格的 Result Matrix 与 Latency (ms) Matrix（AVG…P999/MAX/SLOWEST_THREAD；百分位为精确值，iot-benchmark 使用 t-digest 近似）。旧版基准（仅计 RPC 时间，含下表数据）的数字与新输出**不可直接对比**。

```sh
# 树模型，默认规模：100 设备 × 10 传感器 × 20 批 × 1000 行 = 2000 万点，8 客户端
cargo run --release --example benchmark -- --mode tree

# 表模型，自定义规模，结束后删除数据库
cargo run --release --example benchmark -- --mode table \
    --devices 20 --sensors 10 --batches 100 --batch-size 100 --clients 8 --cleanup
```

参数：`--mode tree|table`、`--devices`、`--sensors`、`--batches`（每设备批数）、`--batch-size`（每 tablet 行数）、`--clients`（工作线程数 = 池大小）、`--host/--port/--user/--password`（亦支持 `IOTDB_HOST/PORT/USER/PASSWORD` 环境变量）、`--base-ts`、`--point-step`、`--reuse-tablets`（每工作线程仅预生成 N 个 tablet 并循环重发，每批重写时间戳基准——为超大规模运行限定内存占用；时间戳重写在计时循环内进行，等同真实流式生产者的按批打时间戳）、`--tablets-per-rpc`（树模型：每次 RPC 通过 `insert_tablets` 批量发送 N 个 tablet）、`--cleanup`。传感器类型分布沿用 Node.js 默认比例（30% FLOAT、20% DOUBLE、20% INT32、10% INT64、10% TEXT、10% BOOLEAN）。报告包含人类可读摘要、iot-benchmark 风格的 Result/Latency 矩阵，以及读回行数校验。

实测环境：Apple M2 Pro（10 核），IoTDB 2.0.6 standalone（Docker，与客户端同机；Docker VM 全部 10 核 / 8 GB，JVM 堆 1 GB），release 构建，**采用旧的仅计 RPC 口径**（见上方说明——新口径下吞吐会略低、延迟会略高）：

| 模式 | 设备 × 传感器 × 批数 × 行数 | 客户端 | 总点数 | 吞吐量 | p50 / p99 延迟 |
| --- | --- | --- | --- | --- | --- |
| tree | 20 × 10 × 100 × 100 | 8 | 200 万 | ~198 万 pts/s | 2.46 ms / 8.38 ms |
| table | 20 × 10 × 100 × 100 | 8 | 200 万 | ~197 万 pts/s | 2.13 ms / 9.97 ms |
| tree | 100 × 10 × 20 × 1000 | 8 | 2000 万 | ~1240 万 pts/s | 4.45 ms / 27.03 ms |
| tree | 100 × 100 × 4 × 1000 | 10 | 4000 万 | ~1500–2000 万 pts/s | 31 ms / 156 ms |
| tree | 100 × 100 × 25 × 1000，`--tablets-per-rpc 4` | 10 | 2.5 亿 | **~2100–2250 万 pts/s** | 105 ms / 907 ms |

吞吐量随每 RPC 点数增长：更宽的 tablet（100 传感器 × 1000 行 = 每 tablet 10 万点）加上 `insert_tablets` 多 tablet 批量，可将同一硬件从 ~1200 万提升到 ~2200 万 pts/s 持续吞吐（2.5 亿点，`--reuse-tablets`）。每 RPC 超过约 40 万点、或客户端数超过核数后，吞吐进入平台期且尾延迟上升；峰值运行期间服务器 JVM 写入突发时占 ~3 核、随后阻塞在 memtable 刷盘上，而 Rust 客户端仅占单核 ~30%——瓶颈在同机 Docker 内的服务器（1 GB 堆），不在客户端。数据为客户端与服务器同机测得——应视为客户端开销的上界，而非服务器容量。

## 项目结构

| 路径 | 内容 |
| --- | --- |
| `src/client/` | `Session`、`TableSession`、`SessionPool`、`TableSessionPool`、`SessionDataSet` |
| `src/connection/` | 底层 Thrift 传输（帧传输 + 二进制协议） |
| `src/data/` | `Tablet`、`Value`、`TSDataType`（官方 TSFile 编码 0–11）、TsBlock 解码、位图 |
| `src/protocol/` | 生成的 Thrift 桩代码（勿编辑） |
| `thrift/` | Thrift IDL 源文件，从 IoTDB 仓库同步 |
| `examples/` | 两种模型及会话池的可运行示例 |
| `tools/` | 代码生成与许可证检查脚本 |

## 许可证

[Apache License 2.0](./LICENSE)
