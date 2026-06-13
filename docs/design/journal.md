# Journal 变更日志设计

## 二十五、Journal: 专用 Append Log

### 25.1 目标与边界

Journal v1 是 **pointer-based 辅助变更日志**: 用于热迁移、增量同步、审计和有限恢复工具。它不是自包含 redo log, 不携带业务 payload、payload checksum 或 record version, 因此不能独立重建业务数据。

处理 `0x11/0x12/0x13` 的 consumer 必须把 journal record 中的 `index_info` 视为读取指针, 在源 dataset 仍可访问且未被 retention/checkpoint 清除时, 通过源 dataset 的 `read_entry_at_index(index_info)` 拉取或验证业务数据。

Journal 不提供事务语义:

- 主操作成功但 journal append 前 crash: journal 可能缺少该操作。
- journal append 成功但 flush 前 crash: journal record 可能丢失。
- journal append 失败不回滚已完成的主操作。

### 25.2 存储模型

Journal 不再复用标准 `DataSet`。底层使用专用 append log, 仅保留 data segment / block / 压缩 / 扩容 / idle-close / queue 中适合 journal 的能力, 删除 TimeIndex / IndexSegment:

```text
{data_dir}/
└── .journal/
    └── logs/
        ├── meta
        ├── data/
        │   ├── 00000000000000000001
        │   └── ...
        └── queue/
            └── {group_name}
```

详细分段格式、sequence 计算、block 读取、flush/idle-close 和 crash recovery 见 [Journal 专用存储设计](journal-storage.md)。

固定标识:

```rust
const JOURNAL_DATASET_NAME: &str = ".journal";
const JOURNAL_DATASET_TYPE: &str = "logs";
```

`.journal/logs` 是 Store 内部保留路径:

- public `create_dataset` 不允许创建 `.journal/logs`。
- public `open_dataset(".journal", "logs")` 不再返回普通 DataSet handle。
- journal 读取、查询和 queue 消费使用专用 Store/FFI/Python API。
- 普通 dataset 扫描始终跳过 `.journal`。
- `enable_journal=false` 时不创建、不打开、不追加 journal, 专用 journal API 返回 `NotFound`。

### 25.3 Journal Sequence

Journal sequence 是从 `1` 开始的连续 `i64`, 不是业务 timestamp, 与当前系统时间无关。

```text
empty journal:
  next_sequence = 1

append:
  sequence = next_sequence
  next_sequence += 1
```

要求:

- 第一条 journal record 的 sequence 为 `1`。
- 每追加一条 journal record, sequence 必须等于上一条 sequence + 1。
- `0` 保留为 queue state 初始 processed position, 不作为 journal record sequence。
- 如果 `next_sequence > i64::MAX`, 返回 `InvalidData`。
- 文档和代码中统一使用 `next_sequence` 表示下一条待分配序号; 最新已写序号为 `next_sequence - 1`。

业务数据 timestamp 只出现在 `0x11/0x12/0x13` 的 `index_info` TLV 中。

### 25.4 Journal Record 二进制格式

Journal record payload:

```text
┌──────────────┬───────────────┬────────────────────────────┐
│ log_type:u8  │ length:u16 LE │ TLV 列表 (length bytes)     │
└──────────────┴───────────────┴────────────────────────────┘
```

TLV entry:

```text
┌─────────┬────────────────┬────────────────────┐
│ type:u8 │ length:u16 LE  │ value:length bytes │
└─────────┴────────────────┴────────────────────┘
```

约束:

- outer `length` 是所有 TLV entry 的总字节数, 不包含 `log_type` 和 outer `length`。
- TLV `length` 和 outer `length` 均为 `u16 LE`, 最大 65535。
- encoded journal payload 最大为 65538 字节。
- 若 encoded payload 加 record header 后超过普通 block 容量, journal segment 使用 `SINGLE_RECORD` block。
- 所有多字节整数使用 Little Endian。
- parser 遇到未知 TLV type 可跳过, 但必须拒绝越界 length。

通用 TLV:

| TLV type | 名称 | value 类型 | 适用日志 |
|----------|------|------------|----------|
| `0x01` | dataset name | UTF-8 bytes | 全部 |
| `0x02` | dataset type | UTF-8 bytes | 全部 |
| `0x03` | metadata / index info | 变体 | 全部 |
| `0x04` | append info | 8 bytes | `0x13` |

`name` 和 `dataset_type` 必须复用普通 dataset 路径安全规则: 非空、最长 255 字节, 且整体匹配 `^[0-9A-Za-z_-]+$`。

### 25.5 日志类型

Journal v1 记录五类事件:

| 日志类型 | 含义 | 触发操作 |
|----------|------|----------|
| `0x01` | 创建 dataset | `Store::create_dataset*` 成功创建普通 dataset |
| `0x02` | 删除 dataset | `Store::drop_dataset*` 成功删除普通 dataset |
| `0x11` | dataset 写入数据 | `DataSet::write*` 成功发布/更新 index entry |
| `0x12` | dataset 删除数据 | `DataSet::delete*` 成功把 index entry 标记为 filler |
| `0x13` | dataset append 数据 | `DataSet::append*` 成功创建或追加 record |

#### 25.5.1 `0x01` 创建 dataset

```text
log_type = 0x01
TLV:
  0x01 name      : UTF-8 bytes
  0x02 type      : UTF-8 bytes
  0x03 metadata  : DataSetMeta 文件除固定 header 外的数据
```

`metadata` 来源为 `{dataset}/meta` 文件去掉固定 8 字节 header 后的 TLV meta_values 原始字节。

#### 25.5.2 `0x02` 删除 dataset

```text
log_type = 0x02
TLV:
  0x01 name      : UTF-8 bytes
  0x02 type      : UTF-8 bytes
  0x03 metadata  : 删除前读取到的 DataSetMeta meta_values
```

如果 journal append 失败, 删除操作不回滚。

#### 25.5.3 `0x11` dataset 写入数据

```text
log_type = 0x11
TLV:
  0x01 name        : UTF-8 bytes
  0x02 type        : UTF-8 bytes
  0x03 index_info  : 18 bytes
```

`index_info`:

```text
Offset  Size  Type       Field
0       8     i64 LE     timestamp
8       8     u64 LE     block_offset
16      2     u16 LE     in_block_offset
```

`index_info` 使用写入完成后可读取业务 record 的最终 index entry。correction 原地覆盖时 index entry 不变, 仍写 `0x11`。

#### 25.5.4 `0x12` dataset 删除数据

```text
log_type = 0x12
TLV:
  0x01 name        : UTF-8 bytes
  0x02 type        : UTF-8 bytes
  0x03 index_info  : 18 bytes
```

`index_info` 使用删除前的真实旧 entry, 不记录删除后的 filler sentinel。

#### 25.5.5 `0x13` dataset append 数据

```text
log_type = 0x13
TLV:
  0x01 name         : UTF-8 bytes
  0x02 type         : UTF-8 bytes
  0x03 index_info   : 18 bytes
  0x04 append_info  : 8 bytes
```

`append_info`:

```text
Offset  Size  Type       Field
0       4     u32 LE     data_offset
4       4     u32 LE     data_len
```

`0x13` 不携带 append bytes。consumer 必须通过源 dataset 的 `read_entry_at_index(index_info)` 获取完整 record, 再用 `data_offset/data_len` 理解本次追加范围。

### 25.6 Store/DataSet 集成

#### Store::open

```text
Store::open(data_dir):
  1. 初始化 StoreConfig / BlockCache / BackgroundTasks
  2. if enable_journal:
       JournalManager::open_or_create(data_dir/.journal/logs)
       - 不创建 TimeIndex / IndexSegment
       - 不写递归 0x01 journal
     else:
       journal = Disabled
  3. 扫描普通 dataset registry
     - 跳过 .journal
     - 跳过非法公共目录名
```

#### Store::create_dataset / drop_dataset

创建或删除普通 dataset 成功后, 通过 `JournalManager.append_create/drop` 追加 `0x01/0x02`。journal append 失败不回滚主操作。

#### DataSet runtime context

Store 管理的业务 `DataSet` 持有 `DataSetRuntimeContext`, 其中 journal hook 指向 `JournalManager`。业务 `DataSet::write/append/delete` 在主操作成功后调用 hook:

```rust
trait DataSetJournalSink {
    fn record_write(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()>;
    fn record_delete(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()>;
    fn record_append(&self, key: &DataSetKey, entry: IndexEntry, data_offset: u32, data_len: u32) -> Result<()>;
}
```

`enable_journal=false` 时 hook 为 no-op。

### 25.7 JournalManager API

```rust
pub(crate) enum JournalManager {
    Enabled {
        log: Arc<Mutex<JournalLog>>,
        queue: Mutex<Option<JournalQueue>>,
    },
    Disabled,
}

impl JournalManager {
    pub(crate) fn open_or_create(data_dir: &Path, config: &StoreConfig) -> Result<Self>;
    pub(crate) fn append_create(&self, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_drop(&self, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_data_write(&self, key: &DataSetKey, entry: IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_delete(&self, key: &DataSetKey, old_entry: IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_append(&self, key: &DataSetKey, entry: IndexEntry, data_offset: u32, data_len: u32) -> Result<Option<i64>>;
    pub(crate) fn latest_sequence(&self) -> Result<Option<i64>>;
    pub(crate) fn read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub(crate) fn query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub(crate) fn open_queue(&self) -> Result<JournalQueue>;
    pub(crate) fn flush_dirty(&self) -> Result<()>;
    pub(crate) fn close(&self) -> Result<()>;
}
```

`append_*` 在 enabled 时返回 `Some(sequence)`, disabled 时返回 `Ok(None)`。

### 25.8 读取、查询与实时消费

Journal 使用专用 API:

```rust
let latest = store.journal_latest_sequence()?;
let one = store.journal_read(sequence)?;
let rows = store.journal_query(start, end)?;
let queue = store.open_journal_queue()?;
```

约束:

- `journal_read(-1)` 不作为特殊 latest 快捷方式; journal 专用 API 使用明确 sequence。
- `sequence <= 0` 返回 `None` 或 `InvalidData` 由 API 具体定义保持一致, 但不得读取任何文件。
- `journal_query(start,end)` 中 `start > end` 返回 `InvalidData`。
- 返回 payload 是 encoded `JournalRecord` 原始字节, 调用方可用 parser 解码。

#### JournalQueue

JournalQueue 单独实现, 不复用 `DatasetQueue`, 但复用 queue state file 格式和 at-least-once 语义。

```rust
let queue = store.open_journal_queue()?;
let consumer = queue.open_consumer("migrate-node-a")?;
while let Some((sequence, payload)) = consumer.poll(timeout)? {
    let record = JournalRecord::decode(&payload)?;
    apply(record)?;
    consumer.ack(sequence)?;
}
```

Queue 语义:

- producer 只有 `JournalManager.append_*`。
- 外部 push 返回 `InvalidData`。
- 新 consumer 默认从当前 `latest_sequence.unwrap_or(0)` 开始, 只消费后续新增记录。
- 每条成功写入的 journal record 都 notify waiting consumers。
- consumer group state 文件路径为 `{data_dir}/.journal/logs/queue/{group_name}`。

### 25.9 并发与锁顺序

Journal 是全 Store 共享串行写入点。并发写不同业务 dataset 时, 业务写入仍可并行到各自 `DataSet` mutex, 但 journal append 会在 `JournalLog` mutex 上串行。

锁顺序:

```text
Store datasets registry lock
  -> target DataSet mutex
     -> JournalManager / JournalLog mutex
```

约束:

- 不允许持有 journal mutex 后再获取普通 dataset mutex。
- journal append 内部不得调用 Store public create/open/drop/write/delete API。
- 后台任务只能通过 `JournalManager::flush_dirty()` / `close()` 操作 journal。
- JournalQueue poll 等待时不得持有 JournalLog 或 queue state 锁。

### 25.10 Crash Safety 与一致性边界

Journal 专用存储必须保证 read/query/queue 不返回半写记录:

- append 写入顺序为 record payload -> block header/state -> segment state。
- open 时扫描 block 链, 只保留最后一个完整 record 前缀。
- 如果 header state 比扫描结果更乐观, 以内存扫描结果修正。
- crash 可能丢失最近未 flush 的 journal records, 但不能读出旧数据或错误数据。

更完整的扫描规则见 [Journal 专用存储设计](journal-storage.md#crash-recovery)。

### 25.11 Retention、删除与保留策略

Journal v1 不参与普通 dataset retention。它不支持 delete 或 invalid_record_count。后续若需要日志回收, 应设计独立 journal checkpoint/retention:

- 基于所有消费组最小 ack sequence 的安全截断。
- 基于外部 snapshot checkpoint 删除旧 segment。
- 基于时间窗口保留, 但需要日志内额外时间字段或外部元数据。

### 25.12 解析与兼容性

Journal record parser:

1. 读取 `log_type`。
2. 读取 outer `length: u16 LE`。
3. 校验 payload 至少包含 `length` 字节。
4. 逐个解析 TLV, 未知 type 跳过。
5. 对已知日志类型校验必需 TLV 是否存在且长度正确。

兼容规则:

- 未来可新增 log_type, 旧 reader 可跳过未知 log_type。
- 未来可在现有 log_type 中追加 TLV, 旧 reader 跳过未知 TLV。
- 已有 TLV type 的语义和二进制类型不可变更。

### 25.13 模块归属

```text
src/journal/
├── mod.rs          # JournalManager + facade + DataSetJournalSink impl
├── record.rs       # JournalRecord encoder/decoder
├── segment.rs      # JournalSegment mmap segment, block append/read/scan
├── log.rs          # JournalLog sequence registry and read/query/append
└── queue.rs        # JournalQueue + JournalQueueConsumer
```

Journal 不放入 `DataSet` 本体。业务 DataSet 只通过 `DataSetJournalSink` hook 通知成功变更。
