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
- `StoreConfig.enable_journal=false` 时不创建、不打开、不追加 journal, 专用 journal API 返回 `NotFound`。
- 普通 dataset 还拥有自己的不可变 `DataSetConfig.enable_journal` 创建参数, 默认 `true`。只有 `StoreConfig.enable_journal && DataSetConfig.enable_journal` 同时为 true 时, 该 dataset 的 create/drop/write/delete/append 才写 journal。

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

业务数据 timestamp 只出现在 `0x11/0x12/0x13` 的 `index_info` TV field 中。

### 25.4 Journal Record 二进制格式

Journal record payload:

```text
┌──────────────┬──────────────────────────────┐
│ log_type:u8  │ TV / type-defined value list │
└──────────────┴──────────────────────────────┘
```

Journal record payload 不再包含 outer `length:u16`。底层 `JournalSegment` 的每条 record 已经有
`payload_len:u32 + sequence:i64`, 因此 `JournalRecord::decode()` 以传入 payload slice 的长度作为完整边界。

字段采用 TV / type-defined value 格式:

```text
┌─────────┬──────────────────────────────────────────────┐
│ type:u8 │ value bytes, interpreted by log_type + type  │
└─────────┴──────────────────────────────────────────────┘
```

约束:

- parser 必须先读取 `log_type`, 再按该 `log_type` 的 schema 解析后续字段。
- 字符串和 metadata 字段在 type 后自带 `u16 LE` length; 固定字段直接使用 schema 定义的固定字节数。
- field order 使用 canonical 顺序: identifier 在前, 其它字段按本节列出的 type 递增顺序写入。
- encoder 必须使用 canonical 顺序; decoder 必须拒绝缺失字段、重复字段、字段顺序错误和当前 `log_type` schema 外的字段。
- 所有多字节整数使用 Little Endian。
- 若 encoded payload 加 record header 后超过普通 block 容量, journal segment 使用 `SINGLE_RECORD` block。

通用 identifier TV:

| Type | Value | 合法范围 | 说明 |
|------|-------|----------|------|
| `0x01` | `u8` | `1..=255` | identifier 使用 1 字节 |
| `0x02` | `u16 LE` | `256..=65535` | identifier 使用 2 字节 |
| `0x03` | `u32 LE` | `65536..=u32::MAX` | identifier 使用 4 字节 |
| `0x04` | `u64 LE` | `u32::MAX+1..=u64::MAX` | identifier 使用 8 字节 |

identifier 必须使用 canonical/minimal encoding:

- `0` 非法。
- 小值必须使用最短 type。例如 identifier `42` 必须编码为 `0x01 + u8(42)`, 不允许编码为 `0x02/0x03/0x04`。
- decoder 遇到非最短编码必须返回 `InvalidData`。

schema 外的未知 type 不能安全跳过, 因为 TV 没有统一 length。当前版本遇到当前 `log_type` schema 之外的 type 必须返回 `InvalidData`。未来扩展应新增 `log_type`, 或新增明确自带 length 的 extension type。

`name` 和 `dataset_type` 只出现在 create/drop 记录中, 必须复用普通 dataset 路径安全规则: 非空、最长 255 字节, 且整体匹配 `^[0-9A-Za-z_-]+$`。虽然 name/type 字段自带 `u16` length, 仍不得放宽 path component 上限。

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
TV:
  0x01..0x04 identifier : canonical dataset identifier
  0x10 name             : u16 length + UTF-8 bytes
  0x11 type             : u16 length + UTF-8 bytes
  0x12 metadata         : u16 length + DataSetMeta 文件除固定 header 外的数据
```

`metadata` 来源为 `{dataset}/meta` 文件去掉固定 8 字节 header 后的 TLV meta_values 原始字节。
create 记录保留 `name`/`type`, 使审计、迁移和离线回放工具在只看 journal stream 时也能建立 identifier 到 dataset key 的初始映射。

#### 25.5.2 `0x02` 删除 dataset

```text
log_type = 0x02
TV:
  0x01..0x04 identifier : canonical dataset identifier
  0x10 name             : u16 length + UTF-8 bytes
  0x11 type             : u16 length + UTF-8 bytes
  0x12 metadata         : u16 length + 删除前读取到的 DataSetMeta meta_values
```

如果 journal append 失败, 删除操作不回滚。
drop 记录同样保留 `name`/`type`, 使 dataset 删除后仍能解释历史 identifier。

#### 25.5.3 `0x11` dataset 写入数据

```text
log_type = 0x11
TV:
  0x01..0x04 identifier : canonical dataset identifier
  0x10 index_info       : 18 bytes
```

`index_info`:

```text
Offset  Size  Type       Field
0       8     i64 LE     timestamp
8       8     u64 LE     block_offset
16      2     u16 LE     in_block_offset
```

`index_info` 使用写入完成后可读取业务 record 的最终 index entry。correction 原地覆盖时 index entry 不变, 仍写 `0x11`。
`0x11` 不携带 `name`/`type`; consumer 通过 Store 的 `identifier -> DataSetKey` 索引或 create/drop journal catalog 解析目标 dataset。

#### 25.5.4 `0x12` dataset 删除数据

```text
log_type = 0x12
TV:
  0x01..0x04 identifier : canonical dataset identifier
  0x10 index_info       : 18 bytes
```

`index_info` 使用删除前的真实旧 entry, 不记录删除后的 filler sentinel。
`0x12` 不携带 `name`/`type`; consumer 必须先解析 identifier。

#### 25.5.5 `0x13` dataset append 数据

```text
log_type = 0x13
TV:
  0x01..0x04 identifier : canonical dataset identifier
  0x10 index_info       : 18 bytes
  0x11 append_info      : 8 bytes
```

`append_info`:

```text
Offset  Size  Type       Field
0       4     u32 LE     data_offset
4       4     u32 LE     data_len
```

`0x13` 不携带 `name`/`type` 或 append bytes。consumer 必须先解析 identifier, 再通过源 dataset 的 `read_entry_at_index(index_info)` 获取完整 record, 并用 `data_offset/data_len` 理解本次追加范围。

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

创建或删除普通 dataset 成功后, 若该 dataset 的有效 journal 开关为 true, 通过 `JournalManager.append_create/drop` 追加 `0x01/0x02`。调用方必须传入该 dataset 的非零 identifier、`DataSetKey` 和 meta snapshot。journal append 失败不回滚主操作。

有效 journal 开关定义为:

```text
effective_dataset_journal = StoreConfig.enable_journal && DataSetMeta.enable_journal
```

`DataSetMeta.enable_journal=false` 只跳过该 dataset 的业务变更日志, 不会关闭全局 `.journal/logs`, 也不会影响其它 dataset。create/drop 的 journal 预校验也只在有效开关为 true 时执行。

#### DataSet runtime context

Store 管理的业务 `DataSet` 持有 `DataSetRuntimeContext`。当该 dataset 的有效 journal 开关为 true 时, context 中的 journal hook 指向 `JournalManager`; 否则 journal hook 为 `None`。业务 `DataSet::write/append/delete` 在主操作成功后调用 hook:

```rust
trait DataSetJournalSink {
    fn record_write(&self, identifier: u64, entry: IndexEntry) -> Result<()>;
    fn record_delete(&self, identifier: u64, entry: IndexEntry) -> Result<()>;
    fn record_append(&self, identifier: u64, entry: IndexEntry, data_offset: u32, data_len: u32) -> Result<()>;
}
```

Store 管理的业务 `DataSet` 在执行 journal hook 前必须已经持有非零 identifier。低层 `DataSet::create/open` 绕过 Store 时没有 Store runtime context, journal hook 仍为 no-op。全局 journal disabled 或 dataset journal disabled 时 hook 均为 no-op。

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
    pub(crate) fn append_create(&self, identifier: u64, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_drop(&self, identifier: u64, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_data_write(&self, identifier: u64, entry: IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_delete(&self, identifier: u64, old_entry: IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_append(&self, identifier: u64, entry: IndexEntry, data_offset: u32, data_len: u32) -> Result<Option<i64>>;
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
2. 根据 `log_type` 选择固定 schema。
3. 逐个解析 TV 字段; parser 以传入 payload slice 的末尾作为记录边界。
4. 校验 identifier 使用 canonical/minimal encoding。
5. 对已知日志类型校验必需字段存在、字段顺序正确、没有重复字段、长度正确且无剩余字节。

兼容规则:

- 未来可新增 log_type, 旧 reader 可跳过未知 log_type。
- 已知 log_type 内不能追加无长度字段, 因为 TV 没有统一 length, 旧 reader 无法安全跳过未知 type。
- 如需扩展已知事件, 应新增 log_type, 或预留一个明确自带 `u16/u32 length` 的 extension type 并在本设计中定义其跳过规则。
- 已有 type 的语义和二进制类型不可变更。

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
