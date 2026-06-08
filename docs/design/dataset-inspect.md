# DataSet Inspect - 数据集详细信息查询

## 一、概述

为 DataSet 添加 `inspect()` 接口，返回该数据集的完整配置参数和运行时状态信息。返回结构分为两部分：

- **`DataSetInfo`**: 不变的配置参数，在数据集创建时确定，生命周期内不会改变
- **`DataSetState`**: 可变的当前状态，反映数据集的实时运行状况

## 二、设计目标

1. **完整性**: 通过返回值可以全面了解数据集的配置和状态
2. **清晰性**: 明确区分不变配置和可变状态
3. **实用性**: 返回的信息对运维、监控、调试有价值
4. **一致性**: 跨 Rust API、FFI、Python wrapper 保持一致的语义

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
    /// 已关闭的数据段数量 (仅元数据在内存中)
    pub closed_data_segments: u32,
    /// 所有数据段的总 record 数量 (包含已删除和过期)
    pub total_record_count: u64,
    /// 所有数据段的总已用空间 (bytes，不含 header)
    pub total_data_size: u64,
    /// 所有数据段的总未压缩大小 (bytes)
    pub total_uncompressed_size: u64,
    /// 所有数据段的无效 record 数量 (已删除/过期/覆盖)
    pub total_invalid_record_count: u64,
    /// 全局最小 timestamp (跨所有数据段)
    pub min_timestamp: i64,
    /// 全局最大 timestamp (跨所有数据段)
    pub max_timestamp: i64,

    // ─── 索引段状态 ───────────────────────────────────────────────────────
    /// 当前打开的索引段数量
    pub open_index_segments: u32,
    /// 已关闭的索引段数量
    pub closed_index_segments: u32,
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

## 四、API 设计

### 4.1 Rust API

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

### 4.2 Store Facade

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

### 4.3 FFI

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
    int64_t create_time;
} TmslDataSetInfo;

/// 数据集可变状态
typedef struct {
    int64_t latest_written_timestamp;
    uint32_t open_data_segments;
    uint32_t closed_data_segments;
    uint64_t total_record_count;
    uint64_t total_data_size;
    uint64_t total_uncompressed_size;
    uint64_t total_invalid_record_count;
    int64_t min_timestamp;
    int64_t max_timestamp;
    uint32_t open_index_segments;
    uint32_t closed_index_segments;
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

### 4.4 Python Wrapper

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
    create_time: int

class DataSetState:
    """可变的当前状态"""
    latest_written_timestamp: int
    open_data_segments: int
    closed_data_segments: int
    total_record_count: int
    total_data_size: int
    total_uncompressed_size: int
    total_invalid_record_count: int
    min_timestamp: int
    max_timestamp: int
    open_index_segments: int
    closed_index_segments: int
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

## 五、实现要点

### 5.1 Info 构建

Info 数据来自两个来源:
1. **DataSetMeta**: 从 meta 文件读取的持久化配置
2. **DataSetKey**: 数据集标识 (name, dataset_type)
3. **base_dir**: 数据集目录路径

```rust
fn build_info(&self) -> DataSetInfo {
    DataSetInfo {
        name: self.id.name.clone(),
        dataset_type: self.id.dataset_type.clone(),
        base_dir: self.base_dir.to_string_lossy().to_string(),
        data_segment_size: self.config.data_segment_size,
        index_segment_size: self.config.index_segment_size,
        initial_data_segment_size: self.config.initial_data_segment_size,
        initial_index_segment_size: self.config.initial_index_segment_size,
        compress_type: self.config.compress_type,
        compress_level: self.config.compress_level,
        index_continuous: self.config.index_continuous,
        retention_window: self.retention_window,
        create_time: self.config.create_time, // 需要从 meta 读取
    }
}
```

### 5.2 State 构建

State 数据需要聚合多个内部结构的状态:

```rust
fn build_state(&self) -> Result<DataSetState> {
    // 聚合数据段状态
    let (open_ds, closed_ds, total_records, total_data, total_uncompressed, total_invalid, min_ts, max_ts) =
        self.aggregate_data_segment_state();

    // 聚合索引段状态
    let (open_idx, closed_idx, pending_entries, base_ts) =
        self.aggregate_index_state();

    // 队列状态
    let (has_queue, consumer_groups) = self.get_queue_state();

    Ok(DataSetState {
        latest_written_timestamp: self.latest_written_timestamp,
        open_data_segments: open_ds,
        closed_data_segments: closed_ds,
        total_record_count: total_records,
        total_data_size: total_data,
        total_uncompressed_size: total_uncompressed,
        total_invalid_record_count: total_invalid,
        min_timestamp: min_ts,
        max_timestamp: max_ts,
        open_index_segments: open_idx,
        closed_index_segments: closed_idx,
        pending_index_entries: pending_entries,
        base_timestamp: base_ts,
        read_only: self.runtime_context.read_only,
        has_block_cache: self.runtime_context.block_cache.is_some(),
        has_journal: self.runtime_context.journal.is_some(),
        has_queue,
        queue_consumer_groups: consumer_groups,
    })
}
```

### 5.3 时间戳哨兵值处理

- `min_timestamp` 和 `max_timestamp` 使用 `TIMESTAMP_MIN_SENTINEL` / `TIMESTAMP_MAX_SENTINEL` 表示空段
- FFI 和 Python 中应转换为 0 或 None 表示无有效时间范围

### 5.4 base_timestamp 处理

- `TimeIndex.base_timestamp` 是 `Option<i64>`
- FFI 中使用 0 作为 sentinel 表示 None
- Python 中使用 `Optional[int]`

## 六、性能考虑

1. **无磁盘 I/O**: `inspect()` 只读取内存中的状态，不触发任何磁盘操作
2. **锁持有时间短**: 只需要持有 dataset mutex 读取状态，不需要等待 I/O
3. **内存分配**: Info 和 State 结构体是栈上分配，FFI 层需要堆分配字符串

## 七、测试用例

1. **基本功能**: 创建数据集后 inspect 返回正确的 info 和 state
2. **写入后状态**: 写入数据后 state 中的统计数据正确更新
3. **多数据段**: 跨多个数据段的统计聚合正确
4. **空数据集**: 空数据集的 min/max timestamp 处理正确
5. **只读模式**: 只读打开的数据集 read_only=true
6. **队列状态**: 有关联队列时 has_queue=true
7. **FFI 内存管理**: tmsl_free_inspect_result 正确释放所有内存

## 八、与现有 API 的关系

- `get_dataset_names()`: 只返回名称列表，不返回详情
- `get_dataset_types()`: 只返回类型列表，不返回详情
- `inspect()`: 返回完整的配置和状态信息，是前两者的补充

## 九、未来扩展

可能的扩展方向:
- 添加 `last_write_time` (最后写入时间)
- 添加 `last_read_time` (最后读取时间)
- 添加 `healthy` 状态标志
- 添加 `error_count` 错误计数
- 支持批量 inspect 多个数据集

## 十、变更记录

| 日期 | 版本 | 描述 |
|------|------|------|
| 2026-06-08 | v1 | 初始设计 |
