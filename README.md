# timslite

> 高性能 Rust 时序数据存储动态库: mmap 分段存储、Block 聚合、延迟压缩、持久化队列、Journal 变更日志、C ABI FFI。

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)

timslite 是一个可嵌入使用的 Rust 时序数据存储库，也可编译为 `cdylib` 通过稳定 C ABI 被 C/C++/Go/Python 等语言调用。它以 `(dataset_name, dataset_type)` 为物理隔离单元，使用 mmap 数据段和索引段保存记录，支持时间索引、延迟压缩、后台维护、持久化队列消费，以及用于热迁移/辅助恢复的内置 Journal 数据集。

## 当前状态

- Rust 核心存储引擎已实现。
- C ABI 头文件维护在 [include/timslite.h](include/timslite.h)。
- Python 绑定位于 [wrapper/python](wrapper/python)。
- 详细设计入口为 [design.md](design.md)，专题文档位于 [docs/design](docs/design)。
- 开发计划入口为 [plan.md](plan.md)，阶段计划位于 [docs/plan](docs/plan)。

## 核心特性

- **mmap 分段存储**: 数据和索引文件按需打开并通过 `memmap2` 映射。
- **Block 级聚合**: 多条 record 聚合进 64 KiB payload block，降低索引和元数据开销。
- **延迟压缩**: pending block 保持 raw 可写状态，直到下一次写入溢出时 seal 并压缩。
- **可变文件头**: 数据段和索引段持久化 `header_len`，物理偏移由逻辑数据偏移和段 header 长度共同计算。
- **时间索引**: 每个 dataset 拥有独立的 18 字节 index entry，支持稀疏模式和连续模式。
- **写入修正能力**: 支持 correction、out-of-order rewrite、delete、append 等写路径。
- **单 record 上限**: 普通 write 和 append 都限制单条逻辑 record 最大 4 MiB。
- **Retention window**: 保留窗口使用与 timestamp 相同的单位，后台回收调度按 UTC hour 执行。
- **读缓存**: 全局 `BlockCache` 只缓存不可变 compressed block，不缓存 mutable pending/raw block。
- **持久化队列**: Queue consumer 使用 4 KiB mmap 状态文件保存 ack 进度和 pending 超时重投递状态。
- **Journal 变更日志**: `.journal/logs` 记录 create/drop/write/delete/append，timestamp 为连续递增 sequence。
- **FFI 与 Python**: C ABI 和 PyO3 wrapper 覆盖主要 Store、DataSet、Query、Queue 工作流。

## 数据模型要点

- Dataset name、dataset type、queue consumer group name 必须匹配 `^[0-9A-Za-z_-]+$`，且最多 255 字节。
- `.journal/logs` 是保留的内部 dataset。Journal 开启时可以受控只读打开，但 public create/write/append/delete/drop 会被拒绝。
- Record payload 编码为 `data_len: u32`、`timestamp: i64`、原始数据 bytes。
- `block_offset` 是相对数据区起点的逻辑全局偏移，指向 `BlockHeader`。
- 路由到具体段后，物理文件偏移为 `segment.header_len + (block_offset - segment.file_offset)`。
- on-disk integer 使用 little-endian；各字段 signed/unsigned 和边界校验见详细设计文档。
- `read(-1)` 读取 `latest_written_timestamp` 对应的 timestamp；如果该 timestamp 已删除或过期，返回 `None`。

## 目录结构

```text
{data_dir}/
├── {dataset_name}/
│   └── {dataset_type}/
│       ├── meta
│       ├── data/
│       │   ├── 00000000000000000000
│       │   └── 00000000000067108864
│       ├── index/
│       │   ├── 00000000000000000000
│       │   └── 0000000000001700000000
│       └── queue/
│           └── {group_name}
└── .journal/
    └── logs/
        ├── meta
        ├── data/
        ├── index/
        └── queue/
```

每个业务 dataset 按 `(name, type)` 物理隔离。数据段文件名是逻辑数据区起始 offset，索引段文件名是该段 base timestamp。连续索引模式下，中间跨度较大的空洞段不需要提前创建，只有真实写入落入该段时才创建。

## Rust 快速示例

```rust
use timslite::{DataSetConfigBuilder, Store, StoreConfig};

fn main() -> timslite::Result<()> {
    let store_config = StoreConfig::builder()
        .enable_background_thread(true)
        .enable_journal(true)
        .retention_check_hour(0) // UTC hour
        .build();

    let mut store = Store::open("./tmp/timslite", store_config.clone())?;

    let dataset_config = DataSetConfigBuilder::from_store(&store_config)
        .index_continuous(1)
        .retention_window(86_400)
        .build();

    let sensor = store.create_dataset_with_config("sensor", "logs", Some(dataset_config))?;

    store.write_dataset(sensor, 1_700_000_000, b"first")?;
    store.append_dataset(sensor, 1_700_000_000, b"+tail")?;

    let latest = store.read_dataset(sensor, -1)?;
    assert_eq!(latest.unwrap().1, b"first+tail");

    let rows = store.query_dataset(sensor, 1_700_000_000, 1_700_000_010)?;
    assert_eq!(rows.len(), 1);

    let _ = store.tick_background_tasks()?;
    Ok(())
}
```

## Queue 示例

```rust
use std::time::Duration;

# use timslite::{DataSetHandle, Store};
# fn demo(mut store: Store, handle: DataSetHandle) -> timslite::Result<()> {
let queue = store.open_queue(handle)?;
let consumer = queue.open_consumer("worker_1")?;

let ts = queue.push(b"job payload")?;

if let Some((polled_ts, payload)) = consumer.poll(Duration::from_secs(1))? {
    assert_eq!(polled_ts, ts);
    assert_eq!(payload, b"job payload");
    consumer.ack(polled_ts)?;
}
# Ok(())
# }
```

如果消费者需要接收后续 push 的数据，应先打开 consumer 再 push。新 consumer 的初始进度会从当前 `latest_written_timestamp()` 开始。

## Journal 说明

Journal 由 `StoreConfig.enable_journal` 控制，默认开启。

- 内置 dataset 固定为 `.journal/logs`。
- Journal timestamp 是从 `1` 开始的连续 sequence，不是系统时间。
- 记录类型: `0x01` create dataset、`0x02` drop dataset、`0x11` write data、`0x12` delete data、`0x13` append data。
- Journal v1 是辅助变更日志，不是严格 WAL，不保证 journal 页和业务 dataset 页以事务顺序落盘。
- 消费 `0x11/0x12/0x13` 时，应将日志里的 index pointer 作为源 dataset 读取指针，在源数据仍可访问时通过 `read_entry_at_index` 拉取数据。
- `store.open_journal_queue()` 可打开 journal queue 进行实时消费。

## 构建与测试

```bash
cargo build
cargo build --release

# 必须单线程运行，文件系统测试会共享 tmp 路径。
cargo test -- --test-threads=1

cargo fmt -- --check
cargo clippy -- -D warnings
```

修改 Python wrapper 时，还需要在 [wrapper/python](wrapper/python) 下执行对应 wrapper 检查和 Python 测试。

## FFI

C ABI 声明位于 [include/timslite.h](include/timslite.h)。主要入口包括:

- `tmsl_store_open`、`tmsl_store_open_with_config`、`tmsl_store_close`
- `tmsl_dataset_create`、`tmsl_dataset_create_with_config`、`tmsl_dataset_open`、`tmsl_dataset_close`、`tmsl_dataset_drop`
- `tmsl_dataset_write`、`tmsl_dataset_append`、`tmsl_dataset_delete`、`tmsl_dataset_read`、`tmsl_dataset_query`
- Queue open/poll/ack 相关函数
- 后台 tick 与配置辅助函数

所有 FFI 函数通过返回值和调用方提供的 error buffer 报告错误。read/query 返回的 buffer 必须通过匹配的 timslite FFI free 函数释放。

## 设计文档导航

从 [design.md](design.md) 开始阅读。常用专题:

- [architecture.md](docs/design/architecture.md)
- [data-model.md](docs/design/data-model.md)
- [data-segment.md](docs/design/data-segment.md)
- [dataset-operations.md](docs/design/dataset-operations.md)
- [time-index.md](docs/design/time-index.md)
- [background-and-cache.md](docs/design/background-and-cache.md)
- [queue-overview.md](docs/design/queue-overview.md)
- [queue-state-file.md](docs/design/queue-state-file.md)
- [journal.md](docs/design/journal.md)
- [store-and-ffi.md](docs/design/store-and-ffi.md)

修改存储行为时，应同步更新对应设计文档和计划清单。
