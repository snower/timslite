# DataSet Inspect - 数据集详细信息查询

## 一、概述

`DataSet::inspect()` 返回一个数据集的完整配置参数和当前状态信息。返回结构分为两部分：

- **`DataSetInfo`**: 不变配置，创建时确定，生命周期内不改变。
- **`DataSetState`**: 可变状态，用于运维、监控和调试。

`DataSetState` 的统计信息必须反映整个 dataset，而不是只统计当前打开的分段。为了避免 inspect 时打开所有历史分段，dataset 目录下新增一个与 `meta` 同级的 `state` 文件，保存已经归档的分段统计缓存；inspect 时读取该缓存并叠加当前仍可能追加写入的 active tail segment 状态。

## 二、设计目标

1. **完整性**: 返回值能覆盖 dataset 配置、写入进度、分段数量、统计信息和运行时上下文。
2. **低成本**: 普通 inspect 不打开所有 data/index segment，不做全量扫描。
3. **清晰性**: 分段数量返回“总数 + 打开数”，不暴露关闭数。
4. **一致性**: Rust API、FFI、Python wrapper 字段名称和语义保持一致。
5. **非关键路径**: `state` 文件是可重建的持久化缓存，不是数据正确性的唯一真源；异常不应影响正常数据读写流程。

## 三、数据结构

### 3.1 DataSetInfo (不变配置)

```rust
/// 不变的配置参数，在数据集创建时确定。
/// 这些值从 meta 文件读取，生命周期内不会改变。
pub struct DataSetInfo {
    // ─── 标识 ─────────────────────────────────────────────────────────────
    /// 数据集名称
    pub name: String,
    /// 数据集类型
    pub dataset_type: String,
    /// 数据集目录路径
    pub base_dir: String,

    // ─── 存储配置 ─────────────────────────────────────────────────────────
    /// 数据段文件大小上限 (bytes)
    pub data_segment_size: u64,
    /// 索引段文件大小上限 (bytes)
    pub index_segment_size: u64,
    /// 初始数据段文件大小 (bytes，会自动扩展到 data_segment_size)
    pub initial_data_segment_size: u64,
    /// 初始索引段文件大小 (bytes，会自动扩展到 index_segment_size)
    pub initial_index_segment_size: u64,

    // ─── 压缩配置 ─────────────────────────────────────────────────────────
    /// 压缩算法类型 (0=zstd, 1=deflate)
    pub compress_type: u8,
    /// 压缩级别 (0-9，zstd 下 3=fast, 6=default, 9=best)
    pub compress_level: u8,

    // ─── 索引配置 ─────────────────────────────────────────────────────────
    /// 索引模式: 0=稀疏模式, 1=连续模式
    pub index_continuous: u8,

    // ─── 数据保留 ─────────────────────────────────────────────────────────
    /// 数据保留窗口 (与 timestamp 同单位，0=不限制)
    pub retention_window: u64,
    /// 是否记录本 dataset 的 journal (创建后不可变)
    pub enable_journal: bool,

    // ─── 元数据 ───────────────────────────────────────────────────────────
    /// 数据集创建时间 (Unix milliseconds)
    pub create_time: i64,
}
```

### 3.2 DataSetState (可变状态)

```rust
/// 可变的当前状态，反映数据集的实时运行状况。
pub struct DataSetState {
    // ─── 写入状态 ─────────────────────────────────────────────────────────
    /// 最大已写入 timestamp (不是最新有效 record，删除不会回退)
    pub latest_written_timestamp: i64,

    // ─── 数据段状态 ───────────────────────────────────────────────────────
    /// 当前打开的数据段数量
    pub open_data_segments: u32,
    /// 数据段总数量 (打开 + 未打开 + active tail，非关闭数)
    pub data_segments: u32,
    /// 全部数据段的总 record 数量 (包含已删除和过期)
    pub total_record_count: u64,
    /// 全部数据段的总已用空间 (bytes，不含 header)
    pub total_data_size: u64,
    /// 全部数据段的总未压缩大小 (bytes)
    pub total_uncompressed_size: u64,
    /// 全部数据段的无效 record 数量 (已删除/过期/覆盖)
    pub total_invalid_record_count: u64,
    /// 全局最小 timestamp；空 dataset 使用 timestamp sentinel
    pub min_timestamp: i64,
    /// 全局最大 timestamp；空 dataset 使用 timestamp sentinel
    pub max_timestamp: i64,

    // ─── 索引段状态 ───────────────────────────────────────────────────────
    /// 当前打开的索引段数量
    pub open_index_segments: u32,
    /// 索引段总数量 (打开 + 未打开，非关闭数)
    pub index_segments: u32,
    /// 内存中待刷新的索引 entry 数量
    pub pending_index_entries: u32,
    /// 索引基础 timestamp (第一个 entry 的 timestamp)
    pub base_timestamp: Option<i64>,

    // ─── 运行时上下文 ─────────────────────────────────────────────────────
    /// 是否只读模式
    pub read_only: bool,
    /// 是否启用 BlockCache
    pub has_block_cache: bool,
    /// 是否启用 Journal
    pub has_journal: bool,

    // ─── 队列状态 ─────────────────────────────────────────────────────────
    /// 是否有关联的 Queue
    pub has_queue: bool,
    /// Queue consumer group 数量 (仅在 has_queue=true 时有意义)
    pub queue_consumer_groups: u32,
}
```

字段语义约束：

- `data_segments` 是 data segment 总数，不等于“打开数 + 关闭数”，API 不再暴露关闭数。
- `index_segments` 是 index segment 总数，不等于“打开数 + 关闭数”，API 不再暴露关闭数。
- `open_*` 只反映当前 mmap/open 的分段数量，不能作为统计覆盖范围。
- `total_*` 必须覆盖整个 dataset：归档 state 文件统计 + active tail data segment 统计。
- `min_timestamp` / `max_timestamp` 以 index 可见范围为准：归档 state 文件中的索引范围 + active index segment 范围。

## 四、Dataset State 文件

### 4.1 路径与职责

每个普通 dataset 目录新增固定文件：

```text
{data_dir}/{name}/{type}/state
```

它与 `meta`、`identifier` 同级，用于保存已经归档的 data/index 分段统计缓存。该文件不是数据正确性的唯一真源：

- 正常读写、delete、append、query 不依赖它判断数据是否存在。
- state 文件缺失、损坏或落后时，不应让正常数据文件读写流程失败。
- 后续若需要重建 state，可提供单独维护流程打开全部分段重算；普通 inspect 不执行该流程。
- 当前首次开发阶段不考虑历史兼容。

### 4.2 active tail 定义

active tail data segment 是当前仍可能继续追加写入的最高 `file_offset` 数据分段。

- 是否计入 state 文件不能由 open/closed 状态决定。
- active tail 即使已被 idle-close，也仍是 active tail，不能被归档统计重复计入。
- 创建下一个新 data segment 时，旧 active tail 才变成归档分段，并把其统计加入 state 文件。

active index segment 使用相同思路：当前仍可能追加 index entry 的最高有效 index segment 不纳入归档 timestamp 范围，inspect 时动态叠加。

### 4.3 文件格式

`state` 文件为固定长度二进制文件，所有多字节整数均为 little-endian：

| Offset | 字段 | 类型 | 说明 |
|--------|------|------|------|
| 0 | magic | `[u8; 4]` | ASCII `DSSF` |
| 4 | version | `u32` | 当前为 `1` |
| 8 | archived_until_offset | `u64` | 归档水位，语义为已纳入 state 的 data segment `file_offset` 排他上界 |
| 16 | min_timestamp | `i64` | 已归档 index segment 的最小 timestamp；空范围使用 sentinel |
| 24 | max_timestamp | `i64` | 已归档 index segment 的最大 timestamp；空范围使用 sentinel |
| 32 | total_record_count | `u64` | 已归档 data segment 的 record 总数 |
| 40 | total_data_size | `u64` | 已归档 data segment 的数据区已用字节数，不含 header |
| 48 | total_uncompressed_size | `u64` | 已归档 data segment 的未压缩逻辑大小 |
| 56 | total_invalid_record_count | `u64` | 已归档 data segment 的无效 record 总数 |

总长度为 64 bytes。`archived_until_offset` 是排他水位：

- 初始 dataset 只有第一个 active tail 时，`archived_until_offset = 0`。
- 当从 `file_offset = X` 滚动到新 data segment `Y` 时，将 `X` 的统计加入 state，并设置 `archived_until_offset = Y`。
- 对于 `file_offset < archived_until_offset` 的 data segment，其统计必须已体现在 state 文件中，除非该 segment 后续被 retention 删除并从 state 中扣减。

### 4.4 更新规则

1. **创建新 data segment**
   创建下一个新 data segment 前，旧 active tail 已经完结。写入路径读取旧 tail 当前 header/state 中的统计值，将 `record_count`、`data_wrote_position`、`uncompressed_size`、`invalid_record_count` 加到 dataset state 文件，并推进 `archived_until_offset`。

2. **创建新 index segment**
   创建下一个新 index segment 前，旧 active index segment 的 timestamp 范围纳入 state 文件的 `min_timestamp` / `max_timestamp`。timestamp 范围由 index segment 元数据或已知 entry 范围提供，不从 data segment 推导。

3. **Retention 删除 data segment**
   删除一个已归档 data segment 时，从 state 文件的 `total_record_count`、`total_data_size`、`total_uncompressed_size`、`total_invalid_record_count` 中扣减该段统计。min/max timestamp 不在 data segment 删除路径做简单减法。

4. **Retention 删除 index segment**
   删除一个已归档 index segment 时更新 state 文件的 `min_timestamp` / `max_timestamp`。如果删除段命中当前边界，应从剩余已归档 index segment 元数据中重新计算边界；无剩余归档 index segment 时恢复为空范围 sentinel。

5. **delete 导致 invalid_record_count 变化**
   当索引删除命中非 active tail data segment，且该 data segment 已经纳入 state 文件时，同步更新 state 文件中的 `total_invalid_record_count`。如果命中 active tail，则只更新 active tail segment 自身状态，inspect 时动态叠加。

### 4.5 Flush 与 idle-close

dataset state 文件是 dirty flush queue 的一等对象：

```rust
enum SegmentFlushTarget {
    Data { file_offset: u64 },
    Index { start_timestamp: i64 },
    QueueState { group_name: String },
    DatasetState,
}
```

state 文件变更后只标记 dirty 并入队 `SegmentFlushTarget::DatasetState`，由后台 flush 周期统一 `mmap.flush()` / sync。`DataSet::flush()` 和 idle-close 应与 data/index segment、queue state file 使用一致的 flush 语义，同步当前 dataset state 文件并清理 stale target。

## 五、API 设计

### 5.1 Rust API

```rust
impl DataSet {
    /// 获取数据集的详细信息和状态。
    pub fn inspect(&self) -> Result<DataSetInspectResult> {
        Ok(DataSetInspectResult {
            info: self.build_info(),
            state: self.build_state()?,
        })
    }
}

/// inspect() 的返回结果，包含不变配置和可变状态。
pub struct DataSetInspectResult {
    pub info: DataSetInfo,
    pub state: DataSetState,
}
```

### 5.2 Store Facade

```rust
impl Store {
    /// 获取指定数据集的详细信息和状态。
    pub fn inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult> {
        let handle = DataSetHandle::new(name, dataset_type);
        let ds = self.get_dataset(&handle)?;
        let ds = ds.lock().map_err(|_| TmslError::LockPoisoned("dataset".into()))?;
        ds.inspect()
    }
}
```

### 5.3 FFI

```c
/// 数据集不变配置信息
typedef struct {
    const char *name;               // 需要调用方释放
    const char *dataset_type;       // 需要调用方释放
    const char *base_dir;           // 需要调用方释放
    uint64_t data_segment_size;
    uint64_t index_segment_size;
    uint64_t initial_data_segment_size;
    uint64_t initial_index_segment_size;
    uint8_t compress_type;
    uint8_t compress_level;
    uint8_t index_continuous;
    uint64_t retention_window;
    uint8_t enable_journal;
    int64_t create_time;
} TmslDataSetInfo;

/// 数据集可变状态
typedef struct {
    int64_t latest_written_timestamp;
    uint32_t open_data_segments;
    uint32_t data_segments;
    uint64_t total_record_count;
    uint64_t total_data_size;
    uint64_t total_uncompressed_size;
    uint64_t total_invalid_record_count;
    int64_t min_timestamp;
    int64_t max_timestamp;
    uint32_t open_index_segments;
    uint32_t index_segments;
    uint32_t pending_index_entries;
    int64_t base_timestamp;          // 0 表示无 (用 0 作为 sentinel)
    uint8_t read_only;
    uint8_t has_block_cache;
    uint8_t has_journal;
    uint8_t has_queue;
    uint32_t queue_consumer_groups;
} TmslDataSetState;

/// inspect 结果
typedef struct {
    TmslDataSetInfo info;
    TmslDataSetState state;
} TmslInspectResult;

/// 获取数据集详细信息和状态。
/// 返回 0 表示成功，非 0 表示错误。
/// 调用方需要使用 tmsl_free_inspect_result 释放内存。
int tmsl_store_inspect_dataset(
    TmslStore *store,
    const char *name,
    const char *dataset_type,
    TmslInspectResult *out_result,
    char *err_buf,
    uint32_t err_buf_len
);

/// 释放 inspect 结果的内存。
void tmsl_free_inspect_result(TmslInspectResult *result);
```

### 5.4 Python Wrapper

```python
class DataSetInfo:
    """不变的配置参数"""
    name: str
    dataset_type: str
    base_dir: str
    data_segment_size: int
    index_segment_size: int
    initial_data_segment_size: int
    initial_index_segment_size: int
    compress_type: int
    compress_level: int
    index_continuous: int
    retention_window: int
    enable_journal: bool
    create_time: int

class DataSetState:
    """可变的当前状态"""
    latest_written_timestamp: int
    open_data_segments: int
    data_segments: int
    total_record_count: int
    total_data_size: int
    total_uncompressed_size: int
    total_invalid_record_count: int
    min_timestamp: int
    max_timestamp: int
    open_index_segments: int
    index_segments: int
    pending_index_entries: int
    base_timestamp: Optional[int]
    read_only: bool
    has_block_cache: bool
    has_journal: bool
    has_queue: bool
    queue_consumer_groups: int

class DataSetInspectResult:
    """inspect() 返回结果"""
    info: DataSetInfo
    state: DataSetState

class Store:
    def inspect_dataset(self, name: str, dataset_type: str) -> DataSetInspectResult:
        """获取数据集的详细信息和状态"""
        ...
```

## 六、实现要点

### 6.1 Info 构建

Info 数据来自三个来源:

1. **DataSetMeta**: 从 meta 文件读取的持久化配置。
2. **DataSetKey**: 数据集标识 `(name, dataset_type)`。
3. **base_dir**: 数据集目录路径。

### 6.2 State 构建

State 构建聚合三个来源：

1. dataset state 文件：已归档 data segment 统计和已归档 index segment timestamp 范围。
2. data/index registry：分段总数、当前打开数、active tail data segment 统计、active index segment 范围。
3. runtime context / queue registry：read-only、cache、journal、queue 信息。

```rust
fn build_state(&self) -> Result<DataSetState> {
    let archived = self.dataset_state.snapshot();
    let data = self.segments.inspect_snapshot();
    let index = self.time_index.inspect_snapshot();
    let queue = self.get_queue_state();

    Ok(DataSetState {
        latest_written_timestamp: self.latest_written_timestamp,
        open_data_segments: data.open_segments,
        data_segments: data.total_segments,
        total_record_count: archived.total_record_count + data.active_tail_record_count,
        total_data_size: archived.total_data_size + data.active_tail_data_size,
        total_uncompressed_size: archived.total_uncompressed_size + data.active_tail_uncompressed_size,
        total_invalid_record_count: archived.total_invalid_record_count + data.active_tail_invalid_record_count,
        min_timestamp: merge_min_timestamp(archived.min_timestamp, index.active_min_timestamp),
        max_timestamp: merge_max_timestamp(archived.max_timestamp, index.active_max_timestamp),
        open_index_segments: index.open_segments,
        index_segments: index.total_segments,
        pending_index_entries: index.pending_entries,
        base_timestamp: index.base_timestamp,
        read_only: self.runtime_context.read_only,
        has_block_cache: self.runtime_context.block_cache.is_some(),
        has_journal: self.runtime_context.journal.is_some(),
        has_queue: queue.has_queue,
        queue_consumer_groups: queue.consumer_groups,
    })
}
```

`DataSegmentMeta` 不需要扩展为保存所有 inspect 汇总字段。归档汇总由 dataset state 文件承载；单段统计在分段从 active tail 转入归档、或 retention 删除归档段时读取当前已知的 segment/header 状态并更新 state 文件。

### 6.3 时间戳哨兵值处理

- `min_timestamp` 和 `max_timestamp` 使用 `TIMESTAMP_MIN_SENTINEL` / `TIMESTAMP_MAX_SENTINEL` 表示空范围。
- FFI 可使用 0 作为无范围 sentinel；Python wrapper 可转换为 `None` 或保持与当前 API 一致的 sentinel 语义。
- min/max 不从 data segment 统计中做加减推导，只随 index segment 新增/删除维护，inspect 时叠加 active index segment 范围。

### 6.4 base_timestamp 处理

- `TimeIndex.base_timestamp` 是 `Option<i64>`。
- FFI 中使用 0 作为 sentinel 表示 None。
- Python 中使用 `Optional[int]`。

## 七、性能考虑

1. **无全量分段打开**: 普通 `inspect()` 不打开全部历史分段，只读取内存中的 registry、dataset state 缓存和 active tail/index 状态。
2. **锁持有时间短**: 持有 dataset mutex 读取状态快照，不执行 rebuild 或历史分段扫描。
3. **可控 I/O**: state 文件在 dataset open/create 时加载，后续按 dirty flush queue 同步；inspect 本身不触发重建。
4. **内存分配**: Info 和 State 结构体是栈上分配，FFI 层需要堆分配字符串。

## 八、测试用例

1. **基本功能**: 创建数据集后 inspect 返回正确的 info 和 state。
2. **字段命名**: Rust/FFI/Python 均暴露 `data_segments` / `index_segments`，不再暴露关闭分段数字段。
3. **写入后状态**: 写入数据后 active tail 统计被 inspect 正确叠加。
4. **多数据段**: rollover 后旧 active tail 进入 dataset state 文件，inspect 返回归档统计 + 新 active tail 统计。
5. **Retention 删除**: 删除归档 data/index segment 后，state 文件统计和 min/max 边界正确更新。
6. **delete 更新 invalid**: 删除命中已归档 data segment 时，state 文件中的 `total_invalid_record_count` 更新。
7. **空数据集**: 空数据集的 min/max timestamp sentinel 处理正确。
8. **只读模式**: 只读打开的数据集 `read_only=true`。
9. **队列状态**: 有关联队列时 `has_queue=true`。
10. **FFI 内存管理**: `tmsl_free_inspect_result` 正确释放所有内存。

## 九、与现有 API 的关系

- `get_dataset_names()`: 只返回名称列表，不返回详情。
- `get_dataset_types()`: 只返回类型列表，不返回详情。
- `inspect()`: 返回完整的配置和状态信息，是前两者的补充。

## 十、未来扩展

可能的扩展方向:

- 添加 `last_write_time` (最后写入时间)。
- 添加 `last_read_time` (最后读取时间)。
- 添加 `healthy` 状态标志。
- 添加 `error_count` 错误计数。
- 支持批量 inspect 多个数据集。
- 增加显式 `rebuild_dataset_state` 维护接口，用于离线重建或校验 state 文件。

## 十一、变更记录

| 日期 | 版本 | 描述 |
|------|------|------|
| 2026-06-08 | v1 | 初始设计 |
| 2026-06-15 | v2 | DataSetState 分段字段改为总数+打开数；新增 dataset state 文件作为归档统计缓存 |
