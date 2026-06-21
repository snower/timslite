# Phase 30: Dataset 读操作优化 (read_exist, query_exist, read_length, query_length, query_length_iter)

**目标**: 为 DataSet 新增 5 个轻量级读操作接口，支持索引存在性检查和数据长度查询，避免读取完整数据以提升性能

**依赖**: Phase 4 (时间索引), Phase 5 (DataSet), Phase 13 (查询迭代器 + HotBlockCache)

**设计文档**: [数据集读操作](../design/dataset-read-operations.md)

---

## 30.1 DataSegmentSet::read_record_data_len() — 读取 record header

新增轻量级方法，仅读取 record header 获取 data_len，不读取实际数据。公共契约中的 record header 固定为 12B (`data_len: u32` + `timestamp: i64`); 实现可以在 index 已定位且无需重复 timestamp 校验的 fast path 中只解码前 4B `data_len`, 但文档口径统一按 12B header 表达。

### 实现位置
`src/segment/data.rs` 或 `src/segment/mod.rs`

### 接口签名
```rust
impl DataSegmentSet {
    /// Read only the record header to get data_len.
    /// Does not read the actual data payload.
    pub fn read_record_data_len(&self, entry: &ReadIndexEntry) -> Result<u32>;
}
```

### 实现要点
1. 根据 `entry.block_offset` 定位所属 DataSegment
2. 计算物理偏移: `segment.header_len + (block_offset - segment.file_offset)`
3. 读取 BlockHeader，校验 magic/flags
4. 若为 single-record block: 读取 block data 起始位置的 record header
5. 若为 multi-record block: 遍历到 `in_block_offset` 位置
6. Record header 格式: `data_len: u32 (4 bytes)` + `timestamp: i64 (8 bytes)`
7. 返回 `data_len`

### 测试用例
- [x] 单条记录 block 的 data_len 读取
- [x] 多条记录 block 中按 in_block_offset 定位
- [x] 压缩 block 的正确解压后读取
- [x] 无效 block_offset 返回错误

---

## 30.2 read_exist() — 单时间戳数据存在检查

检查单个时间戳当前是否存在可见数据。过期 timestamp、filler/deleted entry 均返回 false。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Check if visible data exists for the given timestamp.
    /// timestamp is an exact signed i64 business timestamp.
    pub fn read_exist(&mut self, timestamp: i64) -> Result<bool>;
}
```

### 实现要点
1. 检查 retention 是否过期，过期返回 `false`
2. 调用 `self.time_index.find_entry(timestamp)?`
3. 不存在或 `block_offset == BLOCK_OFFSET_FILLER` 返回 `false`
4. **不读取数据段** — 性能最优
5. 返回 `true` 表示当前可见数据存在，而不是底层索引 entry 物理存在

### 测试用例
- [x] 存在的时间戳返回 true
- [x] 不存在的时间戳返回 false
- [x] filler/deleted entry 返回 false
- [x] timestamp == -1 按精确时间戳检查
- [x] 过期时间戳返回 false

---

## 30.3 query_exist() — 范围数据存在性检查

范围查询当前可见数据存在性，返回位图。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Check visible data existence in [start_ts, end_ts].
    /// Returns bitmap as byte array. Bit i represents (start_ts + i).
    /// Bit is 1 if visible data exists, 0 otherwise.
    pub fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>;
}
```

### 实现要点
1. 若 `start_ts > end_ts` 返回空 Vec
2. 使用 checked arithmetic 按原请求范围计算 `count = (end_ts - start_ts + 1)` 和 `byte_count = (count + 7) / 8`
3. 保持返回 bitmap 与原请求范围对齐；过期范围 bit 保持 0
4. 初始化位图 `vec![0u8; byte_count]`
5. 若 bitmap 超过 4MiB，返回错误
6. 按 retention 可见起点裁剪实际 index 查询范围，调用 `self.time_index.query(effective_start, end_ts)?`
7. 遍历 entry，跳过 filler/deleted entry，计算 `bit_offset = (entry.timestamp - start_ts) as usize`
8. 设置对应位: `bitmap[byte_index] |= 1 << bit_index`

### 位图格式
```
start_ts = 100, end_ts = 107
时间戳:    100 101 102 103 104 105 106 107
存在性:      1   0   1   1   0   0   1   0
位图 (LE): 0b01001101 = 0x4D
返回: [0x4D]
```

### 测试用例
- [x] 空范围返回空 Vec
- [x] 单字节位图正确性
- [x] 跨字节位图正确性
- [x] 包含 filler 的位图（filler 位为 0）
- [x] retention 裁剪后的范围
- [x] 大范围位图（1000+ 时间点）
- [x] 超过 4MiB bitmap 上限返回错误

---

## 30.4 read_length() — 单时间戳数据长度读取

读取单条记录的逻辑数据长度。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Read the logical data length for a timestamp.
    /// timestamp is an exact signed i64 business timestamp.
    /// Returns Some(data_len) if record exists, None if not found or filler.
    pub fn read_length(&mut self, timestamp: i64) -> Result<Option<u32>>;
}
```

### 实现要点
1. 检查 retention 是否过期（过期返回 None）
2. `TimeIndex::find_entry()` 查找索引
3. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER` 返回 None)
4. 构造 `ReadIndexEntry`
5. 调用 `self.segments.read_record_data_len(&re)?`
6. 更新 `self.last_used_at`
7. 返回 `Ok(Some(data_len))`

### 测试用例
- [x] 正常记录返回正确的 data_len
- [x] 不存在的时间戳返回 None
- [x] filler 返回 None
- [x] 过期时间戳返回 None
- [x] timestamp == -1 按精确时间戳读取长度
- [x] 压缩 block 中的 data_len 正确读取

---

## 30.5 query_length() — 范围查询数据长度

范围查询数据长度，返回有效记录列表。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Query data lengths for timestamps in [start_ts, end_ts].
    /// Returns Vec<(timestamp, data_len)> for valid records only.
    pub fn query_length(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>;
}
```

### 实现要点
1. `clamp_query_range()` 裁剪到 retention 窗口
2. 若 `start_ts > end_ts` 返回空 Vec
3. 准备 `result: Vec<(i64, u32)>` = Vec::new()
4. 获取 `QuerySource` 列表（复用 `prepare_query_sources`）
5. 遍历 source，对每个有效 entry：
   - 跳过 filler
   - 调用 `read_record_data_len()` 获取 data_len
   - push `(entry.timestamp, data_len)` 到 result
6. 返回 result

### 测试用例
- [x] 空范围返回空 Vec
- [x] 正常范围返回正确的 (timestamp, data_len) 列表
- [x] filler 被跳过
- [x] 过期记录被跳过
- [x] 跨 segment 边界的查询
- [x] 与 query_iter 结果一致性验证

---

## 30.6 QueryLengthIterator — 惰性数据长度迭代器

创建惰性范围数据长度迭代器。

### 实现位置
`src/query/iter.rs` 或新建 `src/query/length_iter.rs`

### 接口签名
```rust
/// Virtual iterator for data lengths in [start_ts, end_ts].
/// Each next() returns (timestamp, data_len) for valid records.
pub struct QueryLengthIterator<'a> {
    sources: Vec<QuerySource>,
    segments: &'a mut DataSegmentSet,
    cache: Option<Arc<BlockCache>>,
    hot_block: HotBlockCache,
    current_source_idx: usize,
}

impl<'a> QueryLengthIterator<'a> {
    pub fn new(sources: Vec<QuerySource>, segments: &'a mut DataSegmentSet, cache: Option<Arc<BlockCache>>) -> Self;
    pub fn new_with_sources(sources: Vec<QuerySource>, segments: &'a mut DataSegmentSet, cache: Option<Arc<BlockCache>>) -> Self;
}

impl<'a> Iterator for QueryLengthIterator<'a> {
    type Item = Result<(i64, u32)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Similar to QueryIterator::next_entry()
        // But only reads record header, not full data
    }
}
```

### DataSet 接口
```rust
impl DataSet {
    /// Create a lazy iterator for data lengths in [start_ts, end_ts].
    pub fn query_length_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator>;
}
```

### 实现要点
1. 复用 `QuerySource` 枚举（与 QueryIterator 相同）
2. 复用 `HotBlockCache`（需要读取 block 获取 record header）
3. public Rust wrapper 创建时准备 source cursor, 不预先收集全部 `(timestamp, data_len)`
4. `next()` 实现：
   - 从当前 source 获取下一个 IndexEntry
   - 跳过 filler
   - 锁定当前 Store-managed dataset 并检查仍处于 open 状态
   - 检查 HotBlockCache
   - 若 miss，读取 block 并填充 hot_block
   - 从 hot_block 提取 record header 获取 data_len
   - 返回 `Some(Ok((timestamp, data_len)))`
5. 通过 Store 创建时自动注入 `Arc<BlockCache>`
6. FFI `tmsl_dataset_query_length_iter` 保持 index-entry snapshot iterator 语义, `next` 时按 snapshot entry 读取长度

### 测试用例
- [x] 迭代器正确遍历所有有效记录
- [x] filler 被跳过
- [x] HotBlockCache 命中/未命中路径
- [x] 跨 source 边界正确切换
- [x] 空范围返回 None
- [x] 与 query_length 结果一致性验证
- [x] public Rust wrapper 创建 iterator 后才在 `next()` 触达数据源

---

## 30.7 FFI 接口

新增 C ABI FFI 函数。

### 实现位置
`src/ffi.rs`

### 新增函数
```c
// read_exist: 检查当前是否存在可见数据
// 返回 0=false/1=true; 错误时返回 -1
int tmsl_dataset_read_exist(void* dataset, int64_t timestamp, char* err_buf, size_t err_buf_len);

// query_exist: 范围数据存在性快速检查，返回位图
// 过期 timestamp 与 filler/deleted entry 位为 0；bitmap 最大 4MiB
// 返回的 bitmap 由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放
// bitmap_len 写入字节数；出错时返回 -1
int tmsl_dataset_query_exist(void* dataset, int64_t start_ts, int64_t end_ts,
                             uint8_t** out_bitmap, size_t* out_bitmap_len,
                             char* err_buf, size_t err_buf_len);

// read_length: 读取数据长度
// 返回 0=成功(out_len 有效)/1=未找到/-1=错误
int tmsl_dataset_read_length(void* dataset, int64_t timestamp,
                             uint32_t* out_len,
                             char* err_buf, size_t err_buf_len);

typedef struct TmslLengthEntry {
    int64_t timestamp;
    uint32_t data_len;
} TmslLengthEntry;

// query_length: 范围查询数据长度数组
// 返回的数组由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放
// out_array_len 写入 TmslLengthEntry 元素数量而非字节数；出错时返回 -1
// 每个元素使用 C struct 普通布局，非 packed；sizeof=16，alignment=8
int tmsl_dataset_query_length(void* dataset, int64_t start_ts, int64_t end_ts,
                              TmslLengthEntry** out_array, size_t* out_array_len,
                              char* err_buf, size_t err_buf_len);

// query_length_iter: 创建数据长度迭代器
// 返回迭代器句柄，出错时返回 NULL
void* tmsl_dataset_query_length_iter(void* dataset, int64_t start_ts, int64_t end_ts,
                                     char* err_buf, size_t err_buf_len);

// 迭代器 next: 返回 0=成功/1=无更多数据/-1=错误
int tmsl_length_iter_next(void* iter, int64_t* out_ts, uint32_t* out_len,
                          char* err_buf, size_t err_buf_len);
```

### 内存管理
- `tmsl_dataset_query_exist` 返回的 bitmap 由 `libc::malloc` 分配，调用方通过 `tmsl_data_free` 释放
- `tmsl_dataset_query_length` 返回的数组由 `libc::malloc` 分配，调用方通过 `tmsl_data_free` 释放
- `tmsl_dataset_query_length_iter` 返回的迭代器由 `tmsl_iter_close` 释放

### 测试用例
- [x] FFI read_exist 正确返回 true/false
- [x] FFI query_exist 返回正确位图
- [x] FFI read_length 正确返回数据长度
- [x] FFI query_length 返回正确数组
- [x] FFI query_length_iter 迭代器正确工作
- [x] 内存泄漏验证（所有 malloc 都有对应 free）

---

## 30.8 Store 门面 API

在 Store 层暴露轻量读 snapshot 接口。`query_length_iter` 是 `DataSet` public wrapper 方法; Store facade 当前保留 `dataset_query_length` snapshot 方法, 不再声明单独的 Store-level length iterator。

### 实现位置
`src/store.rs`

### 新增方法
```rust
impl Store {
    pub fn dataset_read_exist(&self, handle: DataSetHandle, timestamp: i64) -> Result<bool>;
    pub fn dataset_query_exist(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>;
    pub fn dataset_read_length(&self, handle: DataSetHandle, timestamp: i64) -> Result<Option<u32>>;
    pub fn dataset_query_length(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>;
}
```

### 实现要点
1. 通过 `DataSetHandle` 从 registry 获取 dataset 锁
2. 调用对应的 DataSet 方法
3. 更新 `last_used_at`

---

## 30.9 C 头文件更新

### 实现位置
`include/timslite.h`

### 新增声明
```c
// 轻量级读操作 (详见 dataset-read-operations.md §5 FFI 接口)

// 检查当前是否存在可见数据。timestamp 为精确业务时间戳。
// 返回 0=false/1=true; 错误时返回 -1。
int tmsl_dataset_read_exist(void* dataset, int64_t timestamp, char* err_buf, size_t err_buf_len);

// 范围数据存在性快速检查，返回位图。位 i 代表 (start_ts + i) 当前是否有可见数据。
// 返回的 bitmap 由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
// 过期 timestamp 与 filler/deleted entry 位为 0；bitmap 最大 4MiB。
// bitmap_len 写入字节数；出错时返回 -1。
int tmsl_dataset_query_exist(void* dataset, int64_t start_ts, int64_t end_ts,
                             uint8_t** out_bitmap, size_t* out_bitmap_len,
                             char* err_buf, size_t err_buf_len);

// 读取单条记录的数据长度。timestamp 为精确业务时间戳。
// 返回 0=成功(out_len 有效)/1=未找到/-1=错误。
int tmsl_dataset_read_length(void* dataset, int64_t timestamp,
                             uint32_t* out_len,
                             char* err_buf, size_t err_buf_len);

// 范围查询数据长度数组。返回的数组由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
// out_array_len 写入 TmslLengthEntry 元素数量而非字节数；出错时返回 -1。
// 每个元素使用 C struct 普通布局，非 packed；sizeof=16，alignment=8。
int tmsl_dataset_query_length(void* dataset, int64_t start_ts, int64_t end_ts,
                              TmslLengthEntry** out_array, size_t* out_array_len,
                              char* err_buf, size_t err_buf_len);

// 创建数据长度迭代器。返回迭代器句柄，出错时返回 NULL。
void* tmsl_dataset_query_length_iter(void* dataset, int64_t start_ts, int64_t end_ts,
                                     char* err_buf, size_t err_buf_len);

// 迭代器 next。返回 0=成功/1=无更多数据/-1=错误。
int tmsl_length_iter_next(void* iter, int64_t* out_ts, uint32_t* out_len,
                          char* err_buf, size_t err_buf_len);
```

---

## 30.10 Python Wrapper 更新

### 实现位置
`wrapper/python/src/lib.rs`

### 新增方法
```python
class DataSet:
    def read_exist(self, timestamp: int) -> bool: ...
    def query_exist(self, start_ts: int, end_ts: int) -> bytes: ...
    def read_length(self, timestamp: int) -> Optional[int]: ...
    def query_length(self, start_ts: int, end_ts: int) -> List[Tuple[int, int]]: ...
    def query_length_iter(self, start_ts: int, end_ts: int) -> QueryLengthIterator: ...
```

---

## 30.11 集成测试

### 实现位置
`tests/read_operations.rs` 或 `tests/query_length.rs`

### 测试矩阵

| 测试场景 | read_exist | query_exist | read_length | query_length | query_length_iter |
|---------|------------|-------------|-------------|--------------|-------------------|
| 空数据集 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 单条记录 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 多条记录 | ✅ | ✅ | ✅ | ✅ | ✅ |
| filler entry | ✅ | ✅ | ✅ | ✅ | ✅ |
| 过期记录 | ✅ | ✅ | ✅ | ✅ | ✅ |
| timestamp=-1 精确读取 | ✅ | N/A | ✅ | N/A | N/A |
| 压缩 block | ✅ | ✅ | ✅ | ✅ | ✅ |
| 跨 segment | ✅ | ✅ | ✅ | ✅ | ✅ |
| 连续模式 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 大范围查询 | N/A | ✅ | N/A | ✅ | ✅ |

---

## 实现顺序

1. **30.1** DataSegmentSet::read_record_data_len() — 基础依赖
2. **30.2** read_exist() — 最简单，验证可见数据存在性
3. **30.4** read_length() — 依赖 30.1
4. **30.3** query_exist() — 批量可见数据存在性检查
5. **30.5** query_length() — 批量数据长度
6. **30.6** QueryLengthIterator + query_length_iter() — 迭代器
7. **30.7** FFI 接口
8. **30.8** Store 门面 API
9. **30.9** C 头文件
10. **30.10** Python Wrapper
11. **30.11** 集成测试

---

## 验证命令

```bash
cargo build
cargo test -- --test-threads=1
cargo fmt -- --check
cargo clippy -- -D warnings

# Python wrapper (if applicable)
cd wrapper/python && cargo test && cargo clippy
```

---

## 任务清单

> 以下为 `plan.md` 中 Phase 30 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — read_exist/query_exist/read_length/query_length/query_length_iter 接口规范
- [x] DataSegmentSet::read_record_data_len() — 仅读取 record header 获取 data_len
- [x] DataSet::read_exist() — 单时间戳当前可见数据存在检查
- [x] DataSet::query_exist() — 范围当前可见数据存在检查，返回位图
- [x] DataSet::read_length() — 单时间戳数据长度读取
- [x] DataSet::query_length() — 范围查询数据长度列表
- [x] QueryLengthIterator + query_length_iter() — 惰性数据长度迭代器
- [x] FFI 接口 — tmsl_dataset_read_exist/query_exist/read_length/query_length/query_length_iter
- [x] Store 门面 API — dataset_read_exist/query_exist/read_length/query_length/query_length_iter
- [x] C 头文件 — include/timslite.h 新增函数声明
- [x] Python Wrapper — DataSet 类新增方法
- [x] 集成测试 — 完整测试矩阵覆盖
- [x] 验证 — `cargo test -- --test-threads=1`, `cargo fmt -- --check`, `cargo clippy -- -D warnings`
