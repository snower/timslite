# Phase 30: Dataset 读操作优化 (read_exist, query_exist, read_length, query_length, query_length_iter)

**目标**: 为 DataSet 新增 5 个轻量级读操作接口，支持索引存在性检查和数据长度查询，避免读取完整数据以提升性能

**依赖**: Phase 4 (时间索引), Phase 5 (DataSet), Phase 13 (查询迭代器 + HotBlockCache)

**设计文档**: [数据集读操作](../design/dataset-read-operations.md)

---

## 30.1 DataSegmentSet::read_record_data_len() — 读取 record header

新增轻量级方法，仅读取 record header 获取 data_len，不读取实际数据。

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
- [ ] 单条记录 block 的 data_len 读取
- [ ] 多条记录 block 中按 in_block_offset 定位
- [ ] 压缩 block 的正确解压后读取
- [ ] 无效 block_offset 返回错误

---

## 30.2 read_exist() — 单时间戳索引存在检查

检查单个时间戳的索引是否存在（包括 filler）。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Check if index entry exists for the given timestamp.
    /// timestamp == -1 checks latest_written_timestamp.
    /// Returns true if index entry exists (including filler entries).
    pub fn read_exist(&self, timestamp: i64) -> Result<bool>;
}
```

### 实现要点
1. 若 `timestamp == -1`，使用 `latest_written_timestamp`（若 <= 0 返回 false）
2. 调用 `self.time_index.find_entry(effective_ts)?`
3. 返回 `Ok(entry.is_some())`
4. **不检查 filler** — 只要 `find_entry()` 返回 `Some` 就是 `true`
5. **不读取数据段** — 性能最优
6. **不检查 retention** — 索引存在即返回 `true`

### 测试用例
- [ ] 存在的时间戳返回 true
- [ ] 不存在的时间戳返回 false
- [ ] filler entry 返回 true（索引存在）
- [ ] timestamp == -1 且有数据返回 true
- [ ] timestamp == -1 且无数据返回 false
- [ ] 过期时间戳仍返回 true（仅检查索引）

---

## 30.3 query_exist() — 范围索引存在性检查

范围查询索引存在性，返回位图。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Check existence of index entries in [start_ts, end_ts].
    /// Returns bitmap as byte array. Bit i represents (start_ts + i).
    /// Bit is 1 if index entry exists, 0 otherwise.
    pub fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>;
}
```

### 实现要点
1. `clamp_query_range()` 裁剪到 retention 窗口
2. 若 `start_ts > end_ts` 返回空 Vec
3. 计算 `count = (end_ts - start_ts + 1)` 和 `byte_count = (count + 7) / 8`
4. 初始化位图 `vec![0u8; byte_count]`
5. 调用 `self.time_index.query(start_ts, end_ts)?` 获取所有 entry
6. 遍历 entry，计算 `bit_offset = (entry.timestamp - start_ts) as usize`
7. 设置对应位: `bitmap[byte_index] |= 1 << bit_index`

### 位图格式
```
start_ts = 100, end_ts = 107
时间戳:    100 101 102 103 104 105 106 107
存在性:      1   0   1   1   0   0   1   0
位图 (LE): 0b01001101 = 0x4D
返回: [0x4D]
```

### 测试用例
- [ ] 空范围返回空 Vec
- [ ] 单字节位图正确性
- [ ] 跨字节位图正确性
- [ ] 包含 filler 的位图（filler 位为 1）
- [ ] retention 裁剪后的范围
- [ ] 大范围位图（1000+ 时间点）

---

## 30.4 read_length() — 单时间戳数据长度读取

读取单条记录的逻辑数据长度。

### 实现位置
`src/dataset.rs`

### 接口签名
```rust
impl DataSet {
    /// Read the logical data length for a timestamp.
    /// timestamp == -1 reads latest_written_timestamp.
    /// Returns Some(data_len) if record exists, None if not found or filler.
    pub fn read_length(&mut self, timestamp: i64) -> Result<Option<u32>>;
}
```

### 实现要点
1. 若 `timestamp == -1`，使用 `latest_written_timestamp`
2. 检查 retention 是否过期（过期返回 None）
3. `TimeIndex::find_entry()` 查找索引
4. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER` 返回 None)
5. 构造 `ReadIndexEntry`
6. 调用 `self.segments.read_record_data_len(&re)?`
7. 更新 `self.last_used_at`
8. 返回 `Ok(Some(data_len))`

### 测试用例
- [ ] 正常记录返回正确的 data_len
- [ ] 不存在的时间戳返回 None
- [ ] filler 返回 None
- [ ] 过期时间戳返回 None
- [ ] timestamp == -1 读取最新记录
- [ ] timestamp == -1 无数据返回 None
- [ ] 压缩 block 中的 data_len 正确读取

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
- [ ] 空范围返回空 Vec
- [ ] 正常范围返回正确的 (timestamp, data_len) 列表
- [ ] filler 被跳过
- [ ] 过期记录被跳过
- [ ] 跨 segment 边界的查询
- [ ] 与 query_iter 结果一致性验证

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
    pub fn query_length_iter<'a>(&'a mut self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator<'a>>;
}
```

### 实现要点
1. 复用 `QuerySource` 枚举（与 QueryIterator 相同）
2. 复用 `HotBlockCache`（需要读取 block 获取 record header）
3. `next()` 实现：
   - 从当前 source 获取下一个 IndexEntry
   - 跳过 filler
   - 检查 HotBlockCache
   - 若 miss，读取 block 并填充 hot_block
   - 从 hot_block 提取 record header 获取 data_len
   - 返回 `Some(Ok((timestamp, data_len)))`
4. 通过 Store 创建时自动注入 `Arc<BlockCache>`

### 测试用例
- [ ] 迭代器正确遍历所有有效记录
- [ ] filler 被跳过
- [ ] HotBlockCache 命中/未命中路径
- [ ] 跨 source 边界正确切换
- [ ] 空范围返回 None
- [ ] 与 query_length 结果一致性验证

---

## 30.7 FFI 接口

新增 C ABI FFI 函数。

### 实现位置
`src/ffi.rs`

### 新增函数
```c
// read_exist: 检查索引是否存在 (包括 filler)
// 返回 0=false/1=true; 错误时返回 -1
int tmsl_dataset_read_exist(void* dataset, int64_t timestamp, char* err_buf, size_t err_buf_len);

// query_exist: 范围索引存在性检查，返回位图
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

// query_length: 范围查询数据长度数组
// 返回的数组由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放
// array_len 写入元素数量；出错时返回 -1
// 每个元素为 (timestamp: i64, data_len: u32)，共 12 字节
int tmsl_dataset_query_length(void* dataset, int64_t start_ts, int64_t end_ts,
                              void** out_array, size_t* out_array_len,
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
- `tmsl_dataset_query_length_iter` 返回的迭代器由 `tmsl_iter_free` 释放

### 测试用例
- [ ] FFI read_exist 正确返回 true/false
- [ ] FFI query_exist 返回正确位图
- [ ] FFI read_length 正确返回数据长度
- [ ] FFI query_length 返回正确数组
- [ ] FFI query_length_iter 迭代器正确工作
- [ ] 内存泄漏验证（所有 malloc 都有对应 free）

---

## 30.8 Store 门面 API

在 Store 层暴露新接口。

### 实现位置
`src/store.rs`

### 新增方法
```rust
impl Store {
    pub fn dataset_read_exist(&self, handle: DataSetHandle, timestamp: i64) -> Result<bool>;
    pub fn dataset_query_exist(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>;
    pub fn dataset_read_length(&self, handle: DataSetHandle, timestamp: i64) -> Result<Option<u32>>;
    pub fn dataset_query_length(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>;
    pub fn dataset_query_length_iter(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator<'_>>;
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

// 检查索引是否存在 (包括 filler)。timestamp=-1 检查 latest_written_timestamp。
// 返回 0=false/1=true; 错误时返回 -1。
int tmsl_dataset_read_exist(void* dataset, int64_t timestamp, char* err_buf, size_t err_buf_len);

// 范围索引存在性检查，返回位图。位 i 代表 (start_ts + i) 是否存在。
// 返回的 bitmap 由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
// bitmap_len 写入字节数；出错时返回 -1。
int tmsl_dataset_query_exist(void* dataset, int64_t start_ts, int64_t end_ts,
                             uint8_t** out_bitmap, size_t* out_bitmap_len,
                             char* err_buf, size_t err_buf_len);

// 读取单条记录的数据长度。timestamp=-1 读取 latest_written_timestamp。
// 返回 0=成功(out_len 有效)/1=未找到/-1=错误。
int tmsl_dataset_read_length(void* dataset, int64_t timestamp,
                             uint32_t* out_len,
                             char* err_buf, size_t err_buf_len);

// 范围查询数据长度数组。返回的数组由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
// array_len 写入元素数量；出错时返回 -1。
// 每个元素为 (timestamp: i64, data_len: u32)，共 12 字节。
int tmsl_dataset_query_length(void* dataset, int64_t start_ts, int64_t end_ts,
                              void** out_array, size_t* out_array_len,
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
| timestamp=-1 | ✅ | N/A | ✅ | N/A | N/A |
| 压缩 block | ✅ | ✅ | ✅ | ✅ | ✅ |
| 跨 segment | ✅ | ✅ | ✅ | ✅ | ✅ |
| 连续模式 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 大范围查询 | N/A | ✅ | N/A | ✅ | ✅ |

---

## 实现顺序

1. **30.1** DataSegmentSet::read_record_data_len() — 基础依赖
2. **30.2** read_exist() — 最简单，验证索引访问
3. **30.4** read_length() — 依赖 30.1
4. **30.3** query_exist() — 批量索引检查
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
