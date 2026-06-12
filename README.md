# timslite

> 高性能 Rust 时序数据存储库: mmap 分段存储、Block 聚合、延迟压缩、持久化队列、Journal 变更日志、C ABI FFI 和 Python wrapper。

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/github/snower/timslite/graph/badge.svg?token=OSZNPBORFL)](https://codecov.io/github/snower/timslite)

timslite 是一个可嵌入到其它项目中的本地时序数据存储引擎。它面向需要高性能本地写入、精确时间戳读取、范围查询、持久化消费队列和轻量变更日志的应用，不需要单独部署数据库服务。

你可以把它作为 Rust library 使用，也可以编译为 `cdylib` 后通过 C ABI 接入 C/C++/Go 等宿主，还可以通过 `wrapper/python` 在 Python 项目中使用。

## 当前状态

timslite 仍处于首次正式发布前的开发阶段。Rust API、C ABI、Python wrapper 和磁盘格式仍可能调整，因此更适合评估、内部集成实验和受控场景使用。

当前可用能力:

- Rust 核心存储引擎已实现。
- C ABI 头文件维护在 [include/timslite.h](include/timslite.h)。
- Python wrapper 位于 [wrapper/python](wrapper/python)。
- CI 已覆盖 Rust format、clippy、unit/integration tests 和 Python wrapper tests。
- 性能基准仍在后续完善中；`benches/` 目录已存在，但 benchmark target 还不是必过验证项。

## 适用场景

适合:

- 在 Rust 服务或本地工具中嵌入时序数据存储。
- 高频 timestamp 写入、精确 timestamp 读取和范围查询。
- 希望用 mmap 分段文件保存本地数据，并对历史 block 做压缩。
- 需要基于 dataset 的持久化 queue 消费。
- 需要辅助热迁移、审计或同步的变更日志。

暂不适合:

- 需要严格事务、WAL 或强一致 crash recovery 的场景。
- 需要稳定 1.0 API/ABI 的生产外部依赖。
- 需要任意 UTF-8 dataset 名称的场景。公开 dataset name、dataset type、queue group name 必须匹配 `^[0-9A-Za-z_-]+$`。

## 安装与依赖

### Rust 依赖

在 crate 发布前，可以通过本地 path 或 git 依赖接入:

```toml
[dependencies]
timslite = { path = "../timslite" }
```

或:

```toml
[dependencies]
timslite = { git = "https://github.com/<owner>/timslite" }
```

### 运行依赖

timslite 不需要外部数据库进程。主要 Rust 依赖:

- `memmap2`: mmap 文件 I/O。
- `zstd`: 默认 block 压缩算法。
- `miniz_oxide`: deflate 压缩支持。
- `log`: 日志门面。
- `libc`: C ABI 内存分配和释放边界。

## 快速开始

```rust
use timslite::{DataSetConfigBuilder, Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let store_config = StoreConfig::builder()
        .enable_background_thread(true)
        .enable_journal(true)
        .build();

    let mut store = Store::open("./data/timslite", store_config.clone())?;

    let dataset_config = DataSetConfigBuilder::from_store(&store_config)
        .index_continuous(0)
        .retention_window(0)
        .build();

    let dataset = store.create_dataset_with_config(
        "sensor",
        "temperature",
        Some(dataset_config),
    )?;

    store.write_dataset(dataset, 1_700_000_001, b"21.5")?;
    store.write_dataset(dataset, 1_700_000_002, b"21.7")?;

    let row = store.read_dataset(dataset, 1_700_000_001)?;
    assert_eq!(row.unwrap().1, b"21.5");

    let rows = store.query_dataset(dataset, 1_700_000_001, 1_700_000_010)?;
    assert_eq!(rows.len(), 2);

    store.close()?;
    Ok(())
}
```

打开已有 dataset:

```rust
use timslite::{Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let mut store = Store::open("./data/timslite", StoreConfig::default())?;
    let dataset = store.open_dataset("sensor", "temperature")?;

    let latest = store.read_dataset(dataset, -1)?;
    println!("latest: {:?}", latest);

    Ok(())
}
```

`read_dataset(handle, -1)` 会读取 `latest_written_timestamp` 对应的精确 timestamp。如果该 timestamp 已删除或已过期，会返回 `Ok(None)`，不会自动向前搜索上一条有效数据。

## 核心概念

### Store

`Store` 是顶层入口，负责:

- dataset registry 和 dataset handle。
- 全局 immutable compressed-block cache。
- 可选后台维护线程。
- 可选内置 `.journal/logs` dataset。
- 新建 dataset 时使用的默认配置。

### Dataset

dataset 由 `(name, dataset_type)` 标识，例如 `("sensor", "temperature")`。

常用操作:

- `write(timestamp, data)`: 写入或修正一个 timestamp。
- `append(timestamp, data)`: 追加到最新 tail record，或创建新的未来 timestamp。
- `delete(timestamp)`: 标记一个 timestamp 为删除。
- `read(timestamp)`: 精确读取一个 timestamp。
- `query(start, end)`: 范围查询，左右闭区间。
- `read_exist` / `query_exist` / `read_length` / `query_length`: 轻量存在性和长度查询。

单条逻辑 record 最大 4 MiB。普通聚合 block payload 上限为 64 KiB；更大的 record 会使用 single-record block。

### Timestamp

timestamp 是应用传入的 `i64`，timslite 不要求它必须是系统时间。

如果业务 timestamp 使用秒，`retention_window` 也应使用秒。如果业务使用其它单位，retention 也必须使用同一单位。

### Index 模式

`index_continuous = 0`: 稀疏索引，通常作为默认选择。

`index_continuous = 1`: 连续 timestamp grid，gap 使用 filler entry 表示。适合需要固定 timestamp slot 的场景，但稀疏写入时会产生更多索引项。

### 压缩与缓存

pending block 保持 raw 且可变。下一次写入导致 pending block overflow 时，旧 block 会 seal、compress，并变为 immutable。只有 immutable compressed block 会进入全局读缓存。

默认压缩算法是 zstd。`compress_type = 1` 时可使用 deflate。

## 配置示例

```rust
use std::time::Duration;
use timslite::StoreConfig;

let config = StoreConfig::builder()
    .data_segment_size(64 * 1024 * 1024)
    .index_segment_size(4 * 1024 * 1024)
    .initial_data_segment_size(256 * 1024)
    .initial_index_segment_size(4 * 1024)
    .compress_type(0) // 0=zstd, 1=deflate
    .compress_level(6)
    .cache_max_memory(256 * 1024 * 1024)
    .flush_interval(Duration::from_secs(15))
    .idle_timeout(Duration::from_secs(30 * 60))
    .retention_check_hour(0) // UTC hour
    .enable_background_thread(true)
    .enable_journal(true)
    .build();
```

新建 dataset 时，可以基于 store 默认配置创建 dataset 专属配置:

```rust
use timslite::DataSetConfigBuilder;

let dataset_config = DataSetConfigBuilder::from_store(&config)
    .index_continuous(0)
    .retention_window(30 * 86400)
    .build();
```

## Append 语义

`append(timestamp, data)` 用于扩展最新记录:

- `timestamp < latest_written_timestamp`: 返回错误。
- `timestamp > latest_written_timestamp`: 创建新 record。
- `timestamp == latest_written_timestamp`: 仅当 latest record 仍是未压缩 tail record 时允许原地追加。
- 空 append 会先执行 timestamp 顺序和 retention 校验，再作为 no-op。
- 追加后的逻辑 record 仍不能超过 4 MiB。

追加到已有 latest record 不会再次通知普通 dataset queue；创建新 timestamp 时会通知 queue。

## Queue

每个 dataset 可以打开一个持久化 queue。每个 consumer group 使用独立的 4 KiB state file 保存消费进度。

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let mut store = Store::open("./data/timslite-queue", StoreConfig::default())?;
    let dataset = store.create_dataset_with_config("jobs", "default", None)?;

    let queue = store.open_queue(dataset)?;
    let consumer = queue.open_consumer("worker_1")?;

    let ts = queue.push(b"job payload")?;

    if let Some((polled_ts, payload)) = consumer.poll(Duration::from_secs(1))? {
        assert_eq!(polled_ts, ts);
        assert_eq!(payload, b"job payload");
        consumer.ack(polled_ts)?;
    }

    Ok(())
}
```

如果 consumer 需要收到后续 push 的数据，应先打开 consumer 再 push。新 consumer 的初始位置从当前 `latest_written_timestamp` 开始。

## Journal

Journal 由 `StoreConfig.enable_journal(true)` 控制，默认开启。

内置 journal dataset 固定为 `.journal/logs`，记录:

- `0x01`: create dataset。
- `0x02`: drop dataset。
- `0x11`: dataset write。
- `0x12`: dataset delete。
- `0x13`: dataset append。

Journal timestamp 是从 `1` 开始递增的 sequence，不是系统时间。

Journal v1 是辅助变更日志，不是严格 WAL。write/delete/append record 中保存的是源 dataset 的 index pointer。消费者如果需要 payload，必须在源 dataset 仍可访问时读取:

```rust
let journal_queue = store.open_journal_queue()?;
let consumer = journal_queue.open_consumer("migrator_1")?;
```

如果源 dataset 已删除、已被 retention 回收、已 checkpoint、已 correction 或已覆盖，旧 journal record 可能无法单独精确 replay。

## Retention

`retention_window` 是 dataset 级配置，单位与业务 timestamp 相同:

```rust
let dataset_config = timslite::DataSetConfigBuilder::from_store(&store_config)
    .retention_window(30 * 86400) // timestamp 为秒时表示 30 天
    .build();
```

启用 retention 后:

- 过期 timestamp 的读取返回 `None`。
- 过期 timestamp 不允许 delete、out-of-order rewrite 或 correction。
- 回收只删除整个时间范围都已过期的 data/index segment。
- `retention_check_hour` 使用 UTC hour，范围 `0..=23`。

## C ABI

公开 C 头文件位于 [include/timslite.h](include/timslite.h)。

主要 API 组:

- Store lifecycle: `tmsl_store_open`, `tmsl_store_open_with_config`, `tmsl_store_close`。
- Dataset lifecycle: `tmsl_dataset_create`, `tmsl_dataset_create_with_config`, `tmsl_dataset_open`, `tmsl_dataset_close`, `tmsl_dataset_drop`。
- Data operations: `tmsl_dataset_write`, `tmsl_dataset_append`, `tmsl_dataset_delete`, `tmsl_dataset_read`, `tmsl_dataset_query`。
- Queue operations: `tmsl_queue_open`, `tmsl_queue_consumer_open`, `tmsl_queue_poll`, `tmsl_queue_ack`。
- Background helpers: `tmsl_store_tick_background_tasks`, `tmsl_store_next_background_delay`。

read/query/queue API 返回的 buffer 必须使用头文件中声明的 timslite free 函数释放。

## Python Wrapper

Python wrapper 位于 [wrapper/python](wrapper/python)，暴露与 Rust 类似的 Store、DataSet、Query、Queue 概念。

本地安装和测试:

```bash
cd wrapper/python
pip install maturin pytest
maturin develop
python -m pytest tests/ -v
```

Python 例子见 [wrapper/python/README.md](wrapper/python/README.md)。

## 使用注意事项

- Dataset name、dataset type、queue group name 必须匹配 `^[0-9A-Za-z_-]+$`，最长 255 字节。
- `.journal/logs` 是保留路径。Journal 开启时可以只读打开并 query/queue 消费。
- 磁盘上的多字节 integer 使用 little-endian。
- 后台 flush 默认 15 秒，只执行 mmap sync，不 seal、不压缩 pending block。
- timslite 优先高读写性能，不提供严格事务保证。crash 后最近未 flush 的写入可能丢失。
- 在本仓库运行测试时建议使用 `cargo test -- --test-threads=1`，因为文件系统测试共享临时路径。

## 从源码构建与验证

作为依赖集成时，通常只需要正常 `cargo build`。如果你从源码验证:

```bash
cargo build
cargo build --release
cargo test -- --test-threads=1
```

更严格的本地检查:

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

## 更多文档

设计文档入口是 [design.md](design.md)。常用专题:

- [Architecture](docs/design/architecture.md)
- [Data model](docs/design/data-model.md)
- [Dataset operations](docs/design/dataset-operations.md)
- [Data segment](docs/design/data-segment.md)
- [Time index](docs/design/time-index.md)
- [Query iterator](docs/design/query-iterator.md)
- [Queue overview](docs/design/queue-overview.md)
- [Journal](docs/design/journal.md)
- [Store and FFI](docs/design/store-and-ffi.md)
