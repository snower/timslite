# Phase 32: Dataset Inspect API

## 概述

为 DataSet 添加 `inspect()` 接口，返回该数据集的完整配置参数和运行时状态信息。

返回结构分为两部分：
- **DataSetInfo**: 不变的配置参数，在数据集创建时确定，生命周期内不会改变
- **DataSetState**: 可变的当前状态，反映数据集的实时运行状况

设计文档: [dataset-inspect.md](../design/dataset-inspect.md)

> Phase 40 在本接口基础上优化统计来源: `data_segments` / `index_segments` 表示分段总数, `open_data_segments` / `open_index_segments` 表示打开数; `total_*` 统计通过 dataset state 文件覆盖整个 dataset。

## 设计要点

### 数据结构

```rust
pub struct DataSetInfo {
    // 标识
    pub name: String,
    pub dataset_type: String,
    pub base_dir: String,
    // 存储配置
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    // 压缩配置
    pub compress_type: u8,
    pub compress_level: u8,
    // 索引配置
    pub index_continuous: u8,
    // 数据保留
    pub retention_window: u64,
    // Journal 配置
    pub enable_journal: bool,
    // 元数据
    pub create_time: i64,
}

pub struct DataSetState {
    // 写入状态
    pub latest_written_timestamp: Option<i64>,
    // 数据段
    pub open_data_segments: u32,
    pub data_segments: u32,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    // 索引段
    pub open_index_segments: u32,
    pub index_segments: u32,
    pub pending_index_entries: u32,
    pub base_timestamp: Option<i64>,
    // 运行时上下文
    pub read_only: bool,
    pub has_block_cache: bool,
    pub has_journal: bool,
    // 队列
    pub has_queue: bool,
    pub queue_consumer_groups: u32,
}

pub struct DataSetInspectResult {
    pub info: DataSetInfo,
    pub state: DataSetState,
}
```

### API 设计

```rust
// DataSet
impl DataSet {
    pub fn inspect(&self) -> Result<DataSetInspectResult>;
}

// Store
impl Store {
    pub fn inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult>;
}
```

### FFI 设计

```c
typedef struct {
    const char *name;
    const char *dataset_type;
    const char *base_dir;
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

typedef struct {
    uint8_t has_latest_written_timestamp;
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
    int64_t base_timestamp;
    uint8_t read_only;
    uint8_t has_block_cache;
    uint8_t has_journal;
    uint8_t has_queue;
    uint32_t queue_consumer_groups;
} TmslDataSetState;

typedef struct {
    TmslDataSetInfo info;
    TmslDataSetState state;
} TmslInspectResult;

int tmsl_store_inspect_dataset(
    TmslStore *store,
    const char *name,
    const char *dataset_type,
    TmslInspectResult *out_result,
    char *err_buf,
    uint32_t err_buf_len
);

void tmsl_free_inspect_result(TmslInspectResult *result);
```

### Python Wrapper 设计

```python
class DataSetInfo:
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
    latest_written_timestamp: Optional[int]
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
    info: DataSetInfo
    state: DataSetState

class Store:
    def inspect_dataset(self, name: str, dataset_type: str) -> DataSetInspectResult: ...
```

## 实现任务

- [x] 计划文档创建 (本文件)
- [x] plan.md 更新
- [x] DataSet inspect() 实现
  - [x] DataSetInfo 构建 (从 DataSetKey + DataSetMeta + config)
  - [x] DataSetState 构建 (聚合 DataSegmentSet + TimeIndex + runtime_context)
  - [x] 时间戳哨兵值处理 (TIMESTAMP_MIN/MAX_SENTINEL → 0/None)
- [x] Store inspect_dataset() 实现
- [x] FFI 函数实现
  - [x] TmslInspectResult 结构体
  - [x] tmsl_store_inspect_dataset
  - [x] tmsl_free_inspect_result (释放 info 字符串 + state)
- [x] C 头文件更新 (include/timslite.h)
- [x] Python wrapper 更新
  - [x] DataSetInfo PyClass
  - [x] DataSetState PyClass
  - [x] DataSetInspectResult PyClass
  - [x] store.inspect_dataset() 方法
- [x] 集成测试编写
- [x] 验证: cargo build + test + fmt + clippy

## 测试用例

### 集成测试

1. ✅ `test_inspect_basic`: 创建数据集后 inspect 返回正确的 info 和 state
2. ✅ `test_inspect_info_fields`: 验证所有 info 字段值正确
3. ✅ `test_inspect_state_after_write`: 写入数据后 state 统计正确更新
4. ✅ `test_inspect_state_multi_segment`: 跨多个数据段的统计聚合正确
5. ✅ `test_inspect_state_empty_dataset`: 空数据集 min/max timestamp 处理正确
6. ✅ `test_inspect_with_queue`: 有关联队列时 has_queue=true
7. ✅ `test_inspect_not_found`: 不存在的数据集返回错误
8. ✅ `test_inspect_after_drop`: drop 后 inspect 返回错误

## 验收标准

- [x] `cargo build` 成功
- [x] `cargo test -- --test-threads=1` 全部通过
- [x] `cargo fmt -- --check` 无格式问题
- [x] `cargo clippy -- -D warnings` 无警告
- [x] Python wrapper 编译通过
