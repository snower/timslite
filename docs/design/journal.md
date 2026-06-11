# Journal 变更日志设计

## 二十五、Journal: 内置 Dataset 变更日志

### 25.1 目标与边界

> Journal v1 定位为 **pointer-based 辅助日志**: 仅在源 dataset 仍可访问, 且目标数据未被 retention 或未来 checkpoint 清除时, 支撑热迁移、增量同步、审计和有限故障恢复工具。它不是自包含 redo log, 不携带业务 payload、payload checksum 或 record version, 因此不能独立重建业务数据。
>
> Consumer 处理 `0x11/0x12/0x13` 时必须把 journal record 中的 `index_info` 视为读取指针, 通过源 dataset 的 `read_entry_at_index(index_info)` 拉取当前可读数据; 如果源 dataset 不可访问、索引指向的数据已被回收/删除/覆盖, consumer 必须把该条记录视为不可重放或需要全量校验补偿。

Journal 用于记录 Store/DataSet 的关键变更操作, 后续可服务于数据热迁移、增量同步、审计和故障恢复工具。Journal 自身不引入 WAL、二阶段提交或跨 dataset 事务, 不改变 timslite 当前“高性能、允许最近写入丢失”的 crash 模型。

Journal v1 记录五类事件:

| 日志类型 | 含义 | 触发操作 |
|----------|------|----------|
| `0x01` | 创建 dataset | `Store::create_dataset*` 成功创建普通 dataset |
| `0x02` | 删除 dataset | `Store::drop_dataset*` 成功删除普通 dataset |
| `0x11` | dataset 写入数据 | `DataSet::write*` 成功发布/更新 index entry |
| `0x12` | dataset 删除数据 | `DataSet::delete*` 成功把 index entry 标记为 filler |
| `0x13` | dataset append 数据 | `DataSet::append*` 成功创建或追加 record |

Journal 只记录普通用户 dataset 的操作。内部 journal dataset 自身的创建、打开、写入、flush、retention 和删除流程不得再次写 journal, 避免递归。

### 25.2 底层存储

Journal 底层使用一个内置 dataset:

```text
{data_dir}/
└── .journal/
    └── logs/
        ├── meta
        ├── data/
        └── index/
```

固定标识:

```rust
const JOURNAL_DATASET_NAME: &str = ".journal";
const JOURNAL_DATASET_TYPE: &str = "logs";
```

`.journal` 不满足公共 dataset name 规则 `^[0-9A-Za-z_-]+$`, 因此它是 Store 内部保留名称, 但 journal 开启后允许受控读取:

- public `create_dataset` 不允许创建 `.journal/logs`。
- public `open_dataset(".journal", "logs")` 在 `StoreConfig.enable_journal=true` 时允许, 返回只读 DataSet handle, 可执行 `read/query/query_iter/latest_timestamp/open_queue`。
- public `write/append/delete/drop/close-as-drop` 不允许修改或删除 `.journal/logs`。
- `Store::open` 扫描普通 dataset 时继续跳过非法目录名; `Journal` 模块单独按固定路径打开或创建 `.journal/logs`。
- journal dataset 不进入普通可写 dataset registry, 或进入带 read-only 标记的 internal registry; 后台任务若需要 flush/idle-close 它, 必须通过 `JournalManager` 访问。

#### StoreConfig: enable_journal

```rust
pub struct StoreConfig {
    // ... existing fields ...
    /// 是否启用内置 journal (默认 true)
    pub enable_journal: bool,
}

impl StoreConfigBuilder {
    /// 设置是否启用内置 journal。
    ///
    /// - `true` (默认): `Store::open` 自动 open/create `.journal/logs`, 普通操作成功后写入 journal
    /// - `false`: 不创建、不打开、不追加 journal; `.journal/logs` public open/read/query/open_queue 均返回 NotFound
    pub fn enable_journal(mut self, enable: bool) -> Self;
}
```

`tmsl_store_open(data_dir)` 使用默认配置, 因此默认开启 journal。需要禁用时使用 `tmsl_store_open_with_config` 并设置 FFI 配置中的 `enable_journal = 0`。

Journal dataset 的创建参数:

| 参数 | v1 选择 | 说明 |
|------|---------|------|
| `index_continuous` | `0` | journal timestamp 使用连续递增 seq, 不需要连续 filler |
| `retention_window` | `0` | 默认不自动回收 journal; 未来可加独立保留策略 |
| segment size / initial size / compress_level | 继承 `StoreConfig` dataset 默认值 | 保持与普通 dataset 相同存储能力 |

### 25.3 Journal Record 时间戳

Journal 作为 dataset record 存储时, 仍需要 dataset timestamp。该 timestamp 是 journal sequence timestamp, 不是业务数据 timestamp, 与当前系统时间无关。

生成规则:

```text
last = journal_dataset.latest_written_timestamp()
journal_ts = last + 1
```

要求:

- 第一条 journal record 的 `journal_ts = 1`。
- 每追加一条 journal record, `journal_ts` 必须等于上一条 journal record 的 timestamp + 1。
- 同一 journal dataset 内 timestamp 必须连续、有序、无 gap。
- 不读取当前时间, 不使用 wall-clock/UNIX timestamp, 不受时钟回拨影响。
- 如果 `last == i64::MAX`, 返回 `InvalidData`。

业务数据 timestamp 只出现在 `0x11/0x12/0x13` 的索引信息 TLV 中。

### 25.4 日志二进制格式

每条 journal record 的 payload:

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

- outer `length` 是所有 TLV entry 的总字节数, 不包含 `log_type` 和 outer `length` 自身。
- TLV `length` 使用 `u16 LE`, 单个 value 最大 65535 字节。
- outer `length` 同样使用 `u16 LE`, 因此整条 TLV list 最大 65535 字节。
- `name` 和 `dataset_type` value 是 UTF-8 字节, 不包含 `\0` 结尾; 普通 dataset 复用路径安全规则, 必须非空、最长 255 字节且匹配 `^[0-9A-Za-z_-]+$`。
- `metadata` value 最大 65535 字节, 且还必须满足 `(3+name_len)+(3+type_len)+(3+metadata_len) <= 65535`。
- 所有多字节整数沿用文件格式统一规则: Little Endian。
- 解析器遇到未知 TLV type 可跳过, 但必须拒绝越界 length。

### 25.5 TLV 字段定义

通用 TLV:

| TLV type | 名称 | value 类型 | 适用日志 |
|----------|------|------------|----------|
| `0x01` | dataset name | UTF-8 bytes | 全部 |
| `0x02` | dataset type | UTF-8 bytes | 全部 |
| `0x03` | metadata / index info | 变体 | 全部 |
| `0x04` | append info | 8 bytes | `0x13` |

#### 25.5.1 `0x01` 创建 dataset

```text
log_type = 0x01
TLV:
  0x01 name      : UTF-8 bytes
  0x02 type      : UTF-8 bytes
  0x03 metadata  : DataSetMeta 文件除固定 header 外的数据
```

`metadata` 定义:

- 来源为 `{dataset}/meta` 文件。
- 去掉 DataSetMeta 固定 8 字节 header: `magic[4] + version[u16 LE] + meta_data_length[u16 LE]`。
- value 内容是后续 TLV meta_values 的原始字节。
- metadata value 长度必须等于 meta 文件 header 中的 `meta_data_length`。
- journal enabled 时, create/drop 主操作执行前必须预校验 name/type/metadata snapshot 可编码性; create 使用当前 DataSetMeta v1 的 meta_values 长度预估, drop 在删除目录前读取 metadata snapshot 后校验。

创建日志记录应在普通 dataset 的 meta/data/index 初始结构创建成功后写入。若 journal 写入失败, 已创建 dataset 不做回滚; API 应返回 journal 失败错误, 调用方需按“主操作可能已生效”处理。

#### 25.5.2 `0x02` 删除 dataset

```text
log_type = 0x02
TLV:
  0x01 name      : UTF-8 bytes
  0x02 type      : UTF-8 bytes
  0x03 metadata  : DataSetMeta 文件除固定 header 外的数据
```

删除日志使用删除前读取到的 metadata snapshot。推荐流程:

1. 校验目标不是 `.journal/logs`。
2. 读取并缓存目标 dataset 的 meta TLV bytes。
3. 执行 dataset close/drop/remove_dir_all。
4. 写入 `0x02` journal record。

如果第 4 步失败, 删除操作不回滚。调用方应将该错误视为“dataset 已删除但 journal 缺失/不确定”, 后续通过全量扫描或人工修复恢复一致性。

#### 25.5.3 `0x11` dataset 写入数据

```text
log_type = 0x11
TLV:
  0x01 name        : UTF-8 bytes
  0x02 type        : UTF-8 bytes
  0x03 index_info  : 18 bytes
```

`index_info` 固定 18 字节:

```text
Offset  Size  Type       Field
0       8     i64 LE     timestamp
8       8     u64 LE     block_offset
16      2     u16 LE     in_block_offset
```

记录语义:

- `timestamp` 是业务数据 timestamp。
- `block_offset` 是写入完成后 index entry 中的全局数据区逻辑 offset。
- `in_block_offset` 是写入完成后 index entry 中的 block 内偏移。
- correction 原地覆盖时 index entry 不变, 仍写 `0x11` 日志记录该 timestamp 的写入变更。
- correction 回退为乱序追加、out-of-order rewrite 或连续模式回填时, 使用更新后的 index entry。

写入日志必须在数据写入和 index 发布成功后构造, 因为只有此时才能确定最终 index entry。Journal v1 是变更日志而不是 redo WAL, 因此不要求 journal 先于业务 index 落盘。

#### 25.5.4 `0x12` dataset 删除数据

```text
log_type = 0x12
TLV:
  0x01 name        : UTF-8 bytes
  0x02 type        : UTF-8 bytes
  0x03 index_info  : 18 bytes
```

`index_info` 使用删除前的真实 index entry:

```text
Offset  Size  Type       Field
0       8     i64 LE     timestamp
8       8     u64 LE     old_block_offset
16      2     u16 LE     old_in_block_offset
```

约束:

- 只有成功删除真实 entry 才写 `0x12`。
- 如果目标不存在、已是 filler 或过期不可操作, 不写 journal。
- 删除日志记录旧位置, 便于迁移端或恢复工具理解“哪条已发布 record 被删除”。删除后业务 index 中会被覆盖为 filler sentinel, 但 journal 不记录 sentinel 作为 `index_info`。

#### 25.5.5 `0x13` dataset append 数据

```text
log_type = 0x13
TLV:
  0x01 name         : UTF-8 bytes
  0x02 type         : UTF-8 bytes
  0x03 index_info   : 18 bytes
  0x04 append_info  : 8 bytes
```

`index_info` 固定 18 字节, 使用 append 成功后的最终 index entry:

```text
Offset  Size  Type       Field
0       8     i64 LE     timestamp
8       8     u64 LE     block_offset
16      2     u16 LE     in_block_offset
```

`append_info` 固定 8 字节:

```text
Offset  Size  Type       Field
0       4     u32 LE     data_offset
4       4     u32 LE     data_len
```

记录语义:

- `timestamp` 是业务数据 timestamp。
- `block_offset` / `in_block_offset` 指向 append 完成后可读取到完整逻辑 record 的最终位置。
- `data_offset` 是本次 append 数据在逻辑 record data 内的起始偏移。
- `data_len` 是本次 append 写入的数据长度, 不是追加后的完整 record 长度。
- 当 append 因 `timestamp > latest_written_timestamp` 创建新 record 时, `data_offset=0`, `data_len=input.len()`, `index_info` 指向新 record。
- 当 append 原地追加到最新末尾 record 时, `data_offset=old_data_len`, `data_len=input.len()`, `index_info` 保持原 record 起始位置。
- `0x13` 不携带 append bytes; consumer 必须通过源 dataset 的 `read_entry_at_index(index_info)` 获取完整 record, 再按 `data_offset/data_len` 识别本次追加范围。
- append 失败不写 `0x13` journal record。

### 25.6 Store/DataSet 写入流程集成

#### Store::open

```text
Store::open(data_dir):
  1. 初始化 StoreConfig / BlockCache / BackgroundTasks
  2. if config.enable_journal:
       JournalManager::open_or_create(data_dir/.journal/logs)
       - 若不存在, 用内部 dataset create 创建
       - 若存在, 用内部 dataset open 打开
       - 不写 create journal
     else:
       journal = Disabled
  3. 扫描普通 dataset registry
     - 跳过 .journal
     - 跳过非法公共目录名
```

#### Store::create_dataset

```text
create_dataset(name, type, config):
  1. 校验 name/type 是普通合法名称, 且不是保留 journal 标识
  2. if enable_journal: 预校验 0x01 create journal record 的 name/type/meta_values 长度可编码
  3. 创建普通 dataset
  4. 从 meta 文件提取 meta_values bytes
  5. if enable_journal: journal.append_create(name, type, meta_values)
  6. 返回 dataset handle
```

#### Store::drop_dataset

```text
drop_dataset(name, type):
  1. 校验不是 journal dataset
  2. 读取并缓存 meta_values bytes
  3. if enable_journal: 预校验 0x02 drop journal record 的 name/type/meta_values 长度可编码
  4. 关闭并删除普通 dataset
  5. if enable_journal: journal.append_drop(name, type, meta_values)
  6. 返回
```

#### DataSet runtime context

Store 管理的业务 DataSet 必须持有运行时上下文:

```rust
struct DataSetRuntimeContext {
    block_cache: Option<Arc<BlockCache>>,
    journal: Option<Arc<dyn DataSetJournalSink>>,
    read_only: bool,
}
```

`Store::open/create/open_dataset` 在 DataSet 放入 registry 或返回 handle 前注入该上下文。此后调用方无论通过 `Store::write_dataset/append_dataset/delete_dataset_record` 门面, 还是通过 `Store::get_dataset` 取得 `Arc<Mutex<DataSet>>` 后直接调用 `DataSet::write/append/delete/read/query`, 都应得到一致的 cache 和 journal 语义。独立绕过 Store 直接 `DataSet::create/open` 的低层实例没有 Store 运行时上下文, journal hook 为 no-op。
`.journal/logs` 使用 `read_only=true` 的 runtime context; public DataSet mutation 必须拒绝, JournalManager 内部追加日志时走 crate-level 写入路径, 避免递归 journal 且不暴露写权限。

#### DataSet::write

业务 DataSet 写入需要把最终 `IndexEntry` 返回给自身 journal hook:

```rust
struct WriteOutcome {
    index_entry: IndexEntry,
    branch: WriteBranch, // normal / correction / out_of_order / continuous_backfill
}
```

`DataSet::write` 在主写入和 index 发布成功后调用:

```text
journal.append_data_write(dataset_key, outcome.index_entry)
```

当 `enable_journal=false` 时, 该 hook 是 no-op, 不影响主写入返回结果。

#### DataSet::append

```text
DataSet::append(timestamp, data):
  1. 从 DataSetRuntimeContext 取得 BlockCache / JournalSink
  2. 执行内部 append_with_cache_outcome(timestamp, data, cache)
       -> AppendOutcome { index_entry, data_offset, data_len }
  3. 若 journal sink 存在且本次 append 非空 no-op, 追加 0x13 journal record
```

`DataSet::append` 在 append 成功后调用:

```text
journal.append_data_append(dataset_key, outcome.index_entry, outcome.data_offset, outcome.data_len)
```

当 `enable_journal=false` 时, 该 hook 是 no-op, 不影响主 append 返回结果。`timestamp > latest_written_timestamp` 由 append API 创建新 record 时仍写 `0x13`, 不写 `0x11`, 因为外部语义是 append。

#### DataSet::delete

删除成功时需要返回旧真实 `IndexEntry`:

```rust
struct DeleteOutcome {
    old_index_entry: IndexEntry,
}
```

`DataSet::delete` 在删除成功后调用:

```text
journal.append_data_delete(dataset_key, outcome.old_index_entry)
```

当 `enable_journal=false` 时, 该 hook 是 no-op, 不影响主删除返回结果。

### 25.7 JournalManager 内部接口

```rust
pub(crate) enum JournalManager {
    Enabled {
        dataset: Arc<Mutex<DataSet>>,
        queue: Option<DatasetQueue>,
    },
    Disabled,
}

impl JournalManager {
    pub(crate) fn open_or_create(data_dir: &Path, config: &StoreConfig) -> Result<Self>;
    pub(crate) fn open_readonly_dataset(&self) -> Result<Arc<Mutex<DataSet>>>;
    pub(crate) fn open_queue(&self) -> Result<DatasetQueue>;
    pub(crate) fn append_create(&self, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_drop(&self, key: &DataSetKey, meta_values: &[u8]) -> Result<Option<i64>>;
    pub(crate) fn append_data_write(&self, key: &DataSetKey, entry: &IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_delete(&self, key: &DataSetKey, old_entry: &IndexEntry) -> Result<Option<i64>>;
    pub(crate) fn append_data_append(&self, key: &DataSetKey, entry: &IndexEntry, data_offset: u32, data_len: u32) -> Result<Option<i64>>;
    pub(crate) fn query_since(&self, after_journal_ts: i64) -> Result<QueryIterator<'_>>;
}
```

`append_*` 在 journal enabled 时返回写入 journal dataset 的 `Some(journal_ts)`, 便于调用方或测试确认日志顺序; disabled 时返回 `Ok(None)`。

### 25.8 读取、查询与实时消费

#### 25.8.1 作为普通 dataset 读取

当 `enable_journal=true` 时, `.journal/logs` 可通过受控 public open 路径打开:

```rust
let journal = store.open_dataset(".journal", "logs")?;
let latest = journal.latest_written_timestamp()?;
let one = journal.read(latest)?;
let rows = journal.query(start_journal_ts, end_journal_ts)?;
let iter = journal.query_iter(start_journal_ts, end_journal_ts)?;
```

约束:

- 返回的 handle 标记为 `read_only_internal=true`。
- 允许: `read`, `query`, `query_iter`, `latest_written_timestamp`, `open_queue`, `close`。
- 禁止: `write`, `append`, `delete`, `drop_dataset`, `create_dataset`, `queue.push`。
- `read/query` 返回的是 journal record payload 原始字节, 调用方可用 journal parser 解码为 `JournalRecord`。
- `timestamp=-1` 语义保持不变: 读取最大 journal sequence timestamp 对应的日志 record; 若最新日志被未来 retention/checkpoint 删除, 返回 `None`, 不回退。

如果 `enable_journal=false`, `open_dataset(".journal", "logs")` 返回 `NotFound`, 即使磁盘上存在旧 `.journal/logs` 目录也不主动打开。

#### 25.8.2 通过 Queue 实时 poll

Journal dataset 支持打开 queue 进行实时消费:

```rust
let queue = store.open_journal_queue()?;
let consumer = queue.open_consumer("migrate-node-a")?;
while let Some((journal_ts, payload)) = consumer.poll(timeout)? {
    let record = JournalRecord::decode(&payload)?;
    apply(record)?;
    consumer.ack(journal_ts)?;
}
```

也可以先通过 `open_dataset(".journal", "logs")` 获取只读 handle, 再 `open_queue()`。

Queue 语义:

- journal queue 的 producer 只有 `JournalManager.append_*`。
- `DatasetQueue::push` 对 journal queue 必须返回 `InvalidData`, 外部不能伪造日志。
- journal queue 以 journal sequence timestamp 作为独立递增消费序列; 每条成功写入的 `0x13` 都必须投递给 journal queue consumer, 不受普通 dataset queue 的 append 去重语义影响。
- 每次 journal append 成功后, 复用 Queue 的 notify 机制唤醒等待中的 consumer。
- 每个消费组维护自己的 4KB queue state 文件: `{data_dir}/.journal/logs/queue/{group}`。
- `poll(timeout)` 返回 `(journal_ts, payload)`; `journal_ts` 是 journal dataset timestamp, 可作为迁移端 checkpoint。
- consumer 需要自行 decode payload 并 ack; decode 失败时可以不 ack, 由后续重试或人工处理。

Queue 与 query 的关系:

| 方式 | 适用场景 |
|------|----------|
| `query/query_iter` | 批量扫描历史 journal, 补偿缺口, 离线恢复 |
| `open_queue + poll` | 实时热迁移/同步, 需要持久消费进度和等待唤醒 |

### 25.9 并发与锁顺序

Journal dataset 是全 Store 共享串行写入点。并发写入不同业务 dataset 时, 业务写入仍可并行到各自 `DataSet` mutex, 但 journal append 会在 `JournalManager.dataset` mutex 上串行。

锁顺序:

```text
Store datasets registry lock
  -> target DataSet mutex
     -> JournalManager dataset mutex
```

约束:

- 不允许持有 journal mutex 后再获取普通 dataset mutex。
- journal append 内部不得调用 Store public create/open/drop/write/delete API。
- 后台任务如果 flush/idle-close journal, 只能通过 JournalManager 直接操作 journal dataset, 不扫描 public registry 中的 `.journal`。
- journal queue consumer poll 时遵循 Queue 锁顺序, 不允许在持有 journal mutex 后反向获取普通 dataset mutex。
- drop dataset 流程不得在持有 datasets registry 写锁时执行 journal append; 应先完成 registry 更新和目录删除, 再写 journal, 避免长时间阻塞其它 Store 操作。

### 25.10 Crash Safety 与一致性边界

Journal v1 是同步 change log, 不是事务 WAL:

- 主操作成功但 journal append 前 crash: journal 可能缺少该操作。
- journal append 成功但后续 flush 前 crash: journal record 可能丢失。
- 主 dataset 与 journal dataset 分属不同文件集合, 不保证 mmap 落盘顺序。
- journal append 失败时不回滚已完成的主操作。

因此:

- 热迁移消费者应记录最后消费的 `journal_ts`, 并能通过全量扫描/校验补偿 journal 缺口。
- 恢复工具必须把 journal 视为辅助信息, 不能假设它覆盖所有已返回成功的操作。
- 若未来需要严格故障恢复, 应在独立设计中引入真正 WAL/commit marker/checksum 或二阶段协议, 不复用 v1 journal 语义。

### 25.11 Retention、删除与保留策略

Journal dataset 默认 `retention_window = 0`, 不被普通 retention 策略删除。若 `enable_journal=false`, Store 不会打开或维护 journal retention。未来可增加独立 journal retention 或 checkpoint 机制:

- 基于已消费 `journal_ts` 的安全截断。
- 基于时间窗口的日志保留。
- 基于快照 checkpoint 后删除旧 journal segment。

v1 不设计 compaction, journal 删除只能通过后续明确的 journal retention/checkpoint 功能实现。

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
- 已有 TLV type 的语义和二进制类型不可变更; 如需变更, 新增 TLV type 或新 log_type。

### 25.13 模块归属

建议新增源码模块:

```text
src/
└── journal/
    └── mod.rs
```

模块职责:

- journal record encoder/decoder。
- metadata snapshot 提取。
- journal sequence timestamp 生成。
- `JournalManager` 内部 dataset 生命周期管理。
- Store/DataSet 操作 hook 的窄接口, 包括 create/drop/write/delete/append。

Journal 不应放入 `DataSet` 本体以避免普通 dataset 依赖 Store; DataSet 可返回 `WriteOutcome/DeleteOutcome`, Store 层负责调用 JournalManager。
