# AGENTS.md - timslite

> 高性能 Rust 时序数据存储动态库: mmap 分段存储、Block 聚合、延迟压缩、持久化队列、Journal 变更日志、C ABI FFI。

## 项目概览

timslite 是 Rust 2021 存储引擎，可作为 Rust 库使用，也可编译为 `cdylib` 通过 C ABI 对外暴露，并提供 Python wrapper。存储模型以 dataset 为中心，每个 `(dataset_name, dataset_type)` 拥有独立 meta、data segment、index segment 和可选 queue state。

核心能力:

- mmap-backed data/index 文件
- segment 懒打开和空闲关闭
- Block 聚合与延迟压缩
- 稀疏/连续两种 timestamp index 模式
- correction、out-of-order write、delete、read latest、query iterator、append
- 全局 immutable compressed-block cache
- 持久化 queue consumer group
- 内置 `.journal/logs` 辅助变更日志

## 目录结构

```text
src/
├── lib.rs              # 公共导出
├── config.rs           # StoreConfig/DataSetConfig builder
├── error.rs            # TmslError 和 Result
├── util.rs             # 路径校验和 endian helper
├── meta.rs             # DataSetMeta TLV
├── header.rs           # 可变长度文件头 helper
├── block.rs            # BlockHeader 序列化
├── compress.rs         # miniz_oxide deflate
├── cache.rs            # BlockCache
├── dataset.rs          # DataSet 操作
├── store.rs            # Store facade、handle、journal/cache context
├── ffi.rs              # C ABI
├── bg/                 # 后台任务执行器
├── index/              # TimeIndex 和 IndexSegment
├── journal/            # JournalManager 和 codec
├── queue/              # DatasetQueue、consumer、state file
└── segment/            # DataSegmentSet 和 DataSegment

include/timslite.h      # C 头文件
wrapper/python/         # PyO3 wrapper 和 Python tests
docs/design/            # 详细设计文档
docs/plan/              # phase 计划
docs/review/            # design review 和 TODO 追踪
```

## 构建与验证

```bash
cargo build
cargo build --release

# 本仓库文件系统测试共享 tmp 路径，必须单线程。
cargo test -- --test-threads=1

cargo fmt -- --check
cargo clippy -- -D warnings
```

修改 Python wrapper 时，还需要在 `wrapper/python` 下执行对应 cargo 检查和 Python 测试，前提是本地环境支持。

## 工作规则

- 修改行为前先阅读相关设计文档。
- 涉及设计或实现范围变化时，同步更新 [design.md](design.md)、对应 [docs/design](docs/design) 文档、[plan.md](plan.md) 和对应 [docs/plan](docs/plan) checklist。
- 用户要求仅 review 时，只创建或更新指定 review artifact，不要顺手改代码。
- 搜索优先使用 `rg` 和 `rg --files`。
- 手工编辑使用 `apply_patch`。
- 不回退工作区内与当前任务无关的用户修改。
- 代码注释保持简短，统一使用英文。
- 优先沿用现有模块模式，不轻易引入新抽象。
- 完成任务后先不要git commit，审核确认后再git commit。

## 当前存储契约

### 名称与路径

- Public dataset name、dataset type、queue consumer group name 必须匹配 `^[0-9A-Za-z_-]+$`。
- 每个 path component 最多 `PATH_COMPONENT_MAX_LEN` 字节，当前为 255。
- `.journal/logs` 是保留路径，不满足 public dataset name 规则。

### Header 与 Offset

- Data segment 和 index segment 使用可变长度 header。
- Segment state 持久化 `header_len`，不要假设固定物理数据起点。
- `block_offset` 是相对数据区起点的逻辑全局偏移，指向 `BlockHeader`。
- 物理数据偏移为 `segment.header_len + (block_offset - segment.file_offset)`。
- Data segment 文件名是逻辑数据区 base offset。
- Index segment 文件名是该段 base timestamp。

### On-Disk Integer

- on-disk integer 使用 little-endian。
- timestamp 字段为 signed `i64`。
- size、offset、length、flags、counter 默认使用 unsigned 类型，除非设计文档另有说明。
- 序列化 TLV、record length、path component、journal 字段前必须做边界校验。

### Record 与 Block

- Record header 为 `data_len: u32` 加 `timestamp: i64`。
- 普通 write 和 append 都拒绝超过 4 MiB 的逻辑 record。
- `BLOCK_MAX_SIZE` 固定为 65536 bytes，是编译期 block payload 上限。
- `single_record` 表示 block 内只有一条逻辑 record，不再等同于原始“超大 record”路径。

### 压缩与缓存

- Pending block 保持 raw 且可变。
- Idle-close 不 seal、不压缩 pending block。
- 压缩延迟到下一次 write overflow seal 当前 block 时执行。
- 被 seal 的 block 会压缩并标记 `SEALED | COMPRESSED`；正常当前设计不保留 raw sealed 状态。
- 只有 immutable compressed block 可以进入全局 `BlockCache`。
- Correction fallback、out-of-order rewrite、delete、retention 必须对受影响 cache key 做 invalidation。

### Append

- `append(timestamp, data)` 先执行 timestamp 顺序和 retention 校验，再把空 data 作为 no-op。
- `timestamp < latest_written_timestamp` 返回错误。
- `timestamp > latest_written_timestamp` 创建新 record，语义为 forward append。
- `timestamp == latest_written_timestamp` 只有在 latest record 是未压缩 tail record 时才允许原地追加。
- 追加到已有 latest record 不再迁移到 single-record block；只能原地追加到未压缩 tail record。
- 追加后如果超过普通 pending block 可承载范围，直接返回错误。
- 追加到已有 latest record 不再次通知普通 dataset queue；创建新 timestamp 时需要通知。

### Retention

- `retention_window` 使用与 dataset timestamp 相同的单位。
- `retention_window = 0` 表示不限制。
- `retention_check_hour` 是 UTC hour，范围 `0..=23`。
- `read(ts)` 对过期 timestamp 返回 `None`。
- 过期 timestamp 不允许 delete、out-of-order rewrite 或 correction。
- Reclaim 只删除整个时间范围都过期的 data/index segment；不要求 data 和 index 回收完全同步。

### Latest Timestamp

- `latest_written_timestamp` 是 dataset 已成功写入过的最大 timestamp。
- 删除 latest record 不会回退该值。
- `read(-1)` 读取该精确 timestamp；如果它已删除或过期，结果为 `None`。

## Store、DataSet、Cache、Journal Context

- Store 管理的 `DataSet` 持有 runtime context，包括 cache、journal sink 和 read-only 状态。
- 通过 Store 获取的 DataSet，其 public 行为应与 Store facade 行为一致。
- 不要在普通 read/write/delete/query public API 中要求调用方传入 cache 或 journal 参数。
- 低层 `DataSet::create/open` 绕过 Store 时没有 Store runtime context，journal hook 为 no-op。
- `.journal/logs` public access 使用 read-only runtime context；`JournalManager` 内部写入走 crate-level 路径，避免递归 journal。

## Journal 契约

- Journal 由 `StoreConfig.enable_journal` 控制，默认开启。
- Journal dataset 固定为 `{data_dir}/.journal/logs`。
- Journal timestamp 是从 `1` 开始的连续 sequence，不是 wall-clock time。
- 记录类型:
  - `0x01`: create dataset
  - `0x02`: drop dataset
  - `0x11`: dataset write data
  - `0x12`: dataset delete data
  - `0x13`: dataset append data
- Journal TLV length 为 `u16`；需要写 journal 的操作，应在主操作前校验 name/meta snapshot 是否可编码。
- Journal v1 是辅助变更日志，不是严格 WAL 或事务日志。
- 处理 `0x11`、`0x12`、`0x13` 的 consumer 必须在源数据仍存在时，用 `read_entry_at_index` 从源 dataset 拉取数据。
- `.journal/logs` 在 journal enabled 时可只读打开并正常 query。
- `Store::open_journal_queue()` 打开 journal queue；每条成功写入的 `0x13` 都以独立 journal sequence 投递。

## Queue 契约

- Consumer state 是 dataset queue 目录下的 4 KiB mmap 文件。
- Group name 校验规则与 dataset path component 一致。
- 新 consumer 从当前 `latest_written_timestamp` 初始化；如果需要消费后续 push，应先打开 consumer 再 push。
- Poll 等待 condvar 时不能持有 dataset 或 queue state 锁。
- Condvar notify mutex 只是协调原语，不属于 dataset/data/index 锁层级。
- ACK 只更新当前 consumer group 的 state file。

## 后台任务

`BackgroundTasks` 通过一个 executor state mutex 串行执行 flush、idle-close、cache eviction、retention reclaim。

- `StoreConfig.enable_background_thread = true`: Store 启动后台线程。
- `false`: 调用方需要主动调用 `Store::tick_background_tasks()`。
- 手动 tick 和后台线程共用同一个 executor mutex。
- 锁顺序保持为 executor state、dataset registry、dataset mutex、segment internals。

## 设计文档

入口为 [design.md](design.md)。高频专题:

- [docs/design/architecture.md](docs/design/architecture.md)
- [docs/design/data-model.md](docs/design/data-model.md)
- [docs/design/data-segment.md](docs/design/data-segment.md)
- [docs/design/dataset-operations.md](docs/design/dataset-operations.md)
- [docs/design/time-index.md](docs/design/time-index.md)
- [docs/design/background-and-cache.md](docs/design/background-and-cache.md)
- [docs/design/compression.md](docs/design/compression.md)
- [docs/design/query-iterator.md](docs/design/query-iterator.md)
- [docs/design/queue-overview.md](docs/design/queue-overview.md)
- [docs/design/queue-state-file.md](docs/design/queue-state-file.md)
- [docs/design/journal.md](docs/design/journal.md)
- [docs/design/store-and-ffi.md](docs/design/store-and-ffi.md)

## 未完成计划

以 [plan.md](plan.md) 为当前来源。长期未完成项包括 C 链接验证、性能基准，以及新版 phase 计划里仍未勾选的测试/增强项。
