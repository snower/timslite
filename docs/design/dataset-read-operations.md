# 数据集读操作 - DataSet Read Operations

> 统一描述 DataSet 所有读相关接口: read、query、query_iter 及新增的 read_exist、query_exist、read_length、query_length、query_length_iter。

---

## 一、读操作接口总览

| 接口 | 返回类型 | 用途 | 数据读取 |
|------|---------|------|---------|
| `read(ts)` | `Option<(i64, Vec<u8>)>` | 读取单条完整记录 | 是 |
| `query(start, end)` | `Vec<(i64, Vec<u8>)>` | 范围查询完整记录 | 是 |
| `query_iter(start, end)` | `QueryIterator` | 惰性范围查询迭代器 | 是 (按需) |
| `read_exist(ts)` | `bool` | 检查单个时间戳索引是否存在 | **否** |
| `query_exist(start, end)` | `Vec<u8>` (bitmap) | 范围索引存在性检查 | **否** |
| `read_length(ts)` | `Option<u32>` | 读取单条记录数据长度 | 仅 header |
| `query_length(start, end)` | `Vec<(i64, u32)>` | 范围查询数据长度列表 | 仅 header |
| `query_length_iter(start, end)` | `QueryLengthIterator` | 惰性范围数据长度迭代器 | 仅 header (按需) |

**性能层次**:
- **最快**: `read_exist` — 仅索引查找，无数据段 I/O
- **快**: `query_exist` — 索引范围查询，无数据段 I/O
- **中**: `read_length`, `query_length`, `query_length_iter` — 需读取 record header (8 bytes)
- **慢**: `read`, `query`, `query_iter` — 需读取完整数据

---

## 二、现有接口

### 2.1 read(timestamp)

```rust
pub fn read(&mut self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>
```

读取单条完整记录。

**参数**:
- `timestamp`: 目标时间戳，`-1` 表示读取 `latest_written_timestamp`

**返回**:
- `Ok(Some((timestamp, data)))` — 记录存在且有效
- `Ok(None)` — 记录不存在、是 filler、或已过期

**流程**:
1. 若 `timestamp == -1`，使用 `latest_written_timestamp`
2. 检查 retention 是否过期
3. `TimeIndex::find_entry()` 查找索引
4. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER`)
5. `DataSegmentSet::read_at_index()` 读取完整数据

**相关文档**: [数据集操作·读取流程](dataset-operations.md#十读取流程详解)

---

### 2.2 query(start_ts, end_ts)

```rust
pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>
```

范围查询，返回所有有效记录。

**参数**:
- `start_ts`: 起始时间戳（含）
- `end_ts`: 结束时间戳（含）

**返回**: `Vec<(i64, Vec<u8>)>` — 按时间戳排序的记录列表

**流程**: 内部调用 `query_iter().collect_all()`

**相关文档**: [查询迭代器](query-iterator.md)

---

### 2.3 query_iter(start_ts, end_ts)

```rust
pub fn query_iter<'a>(&'a mut self, start_ts: i64, end_ts: i64) -> Result<QueryIterator<'a>>
```

惰性范围查询迭代器。

**参数**: 同 `query`

**返回**: `QueryIterator` — 每次 `next()` 按需读取一条记录

**特点**:
- 支持 HotBlockCache（查询级 block 缓存）
- 索引按 source cursor 逐条推进
- 通过 Store 创建时自动注入 `Arc<BlockCache>`

**相关文档**: [查询迭代器](query-iterator.md)

---

## 三、新增接口

### 3.1 read_exist(timestamp)

```rust
pub fn read_exist(&self, timestamp: i64) -> Result<bool>
```

检查单个时间戳的索引是否存在。

**参数**:
- `timestamp`: 目标时间戳，`-1` 表示检查 `latest_written_timestamp`

**返回**:
- `Ok(true)` — 索引 entry 存在（包括 filler）
- `Ok(false)` — 索引 entry 不存在，或 `timestamp == -1` 且 `latest_written_timestamp <= 0`

**流程**:
1. 若 `timestamp == -1`，使用 `latest_written_timestamp`（若 <= 0 返回 false）
2. `TimeIndex::find_entry()` 查找索引
3. 返回 `entry.is_some()`

**特点**:
- **不检查 filler** — 只要 `find_entry()` 返回 `Some` 就是 `true`
- **不读取数据段** — 性能最优
- **不检查 retention** — 索引存在即返回 `true`

**设计决策**: `read_exist` 仅表示"索引位置有 entry"，不表示数据有效。调用方如需确认数据有效性，应使用 `read()` 或 `read_length()`。

---

### 3.2 query_exist(start_ts, end_ts)

```rust
pub fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>
```

范围索引存在性检查，返回位图。

**参数**:
- `start_ts`: 起始时间戳（含）
- `end_ts`: 结束时间戳（含）

**返回**: `Vec<u8>` — 位图字节数组
- 位 `i` 代表时间戳 `(start_ts + i)` 是否存在
- `1` = 存在，`0` = 不存在
- 字节数组长度 = `(count + 7) / 8`，其中 `count = end_ts - start_ts + 1`

**示例**:
```
start_ts = 100, end_ts = 107
时间戳:    100 101 102 103 104 105 106 107
存在性:      1   0   1   1   0   0   1   0
位图 (LE): 0b01001101 = 0x4D
返回: [0x4D]
```

**流程**:
1. `clamp_query_range()` 裁剪到 retention 窗口
2. `TimeIndex::query()` 获取范围内所有 entry
3. 遍历 entry，设置对应位

**特点**:
- **不读取数据段** — 仅索引查询
- **不限制范围大小** — 调用方自行控制
- **返回原始位图** — 调用方需自行解析

---

### 3.3 read_length(timestamp)

```rust
pub fn read_length(&mut self, timestamp: i64) -> Result<Option<u32>>
```

读取单条记录的逻辑数据长度。

**参数**:
- `timestamp`: 目标时间戳，`-1` 表示读取 `latest_written_timestamp`

**返回**:
- `Ok(Some(data_len))` — 记录存在，返回逻辑数据长度
- `Ok(None)` — 记录不存在、是 filler、或已过期

**流程**:
1. 若 `timestamp == -1`，使用 `latest_written_timestamp`
2. 检查 retention 是否过期
3. `TimeIndex::find_entry()` 查找索引
4. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER`)
5. `DataSegmentSet::read_record_data_len()` 仅读取 record header

**数据长度定义**: 用户写入的原始数据长度（不含 record header、block header、压缩开销）

**新增依赖**: 需在 `DataSegmentSet` 中添加 `read_record_data_len()` 方法

---

### 3.4 query_length(start_ts, end_ts)

```rust
pub fn query_length(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>
```

范围查询数据长度，返回有效记录列表。

**参数**:
- `start_ts`: 起始时间戳（含）
- `end_ts`: 结束时间戳（含）

**返回**: `Vec<(i64, u32)>` — 有效记录列表，按时间戳排序
- 仅包含存在的有效记录（跳过 filler 和不存在的时间戳）
- 每个元素为 `(timestamp, data_len)`

**示例**:
```
start_ts = 100, end_ts = 104
存在的时间戳: 100, 102, 103
返回: [(100, 128), (102, 256), (103, 512)]
```

**流程**:
1. `clamp_query_range()` 裁剪到 retention 窗口
2. 遍历范围内索引 entry
3. 对每个有效 entry，读取 record header 获取 data_len
4. 返回 `(timestamp, data_len)` 列表

**与 query_length_iter 的区别**:
- `query_length` 一次性返回所有结果
- `query_length_iter` 惰性迭代，内存效率更高

---

### 3.5 query_length_iter(start_ts, end_ts)

```rust
pub fn query_length_iter<'a>(&'a mut self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator<'a>>
```

惰性范围数据长度迭代器。

**参数**: 同 `query_length`

**返回**: `QueryLengthIterator` — 每次 `next()` 返回 `(timestamp, data_len)`

**特点**:
- 仅返回有效记录（跳过 filler 和不存在的时间戳）
- 支持 HotBlockCache（需读取 block 获取 record header）
- 通过 Store 创建时自动注入 `Arc<BlockCache>`

**与 query_iter 的区别**:
- `query_iter` 返回完整数据 `(i64, Vec<u8>)`
- `query_length_iter` 仅返回数据长度 `(i64, u32)`

**新增依赖**: 需创建 `QueryLengthIterator` 结构体

---

## 四、实现要点

### 4.1 DataSegmentSet::read_record_data_len()

新增轻量级方法，仅读取 record header 获取 data_len:

```rust
impl DataSegmentSet {
    /// Read only the record header to get data_len.
    /// Does not read the actual data payload.
    pub fn read_record_data_len(&self, entry: &ReadIndexEntry) -> Result<u32> {
        // 1. Find data segment containing block_offset
        // 2. Seek to block start position
        // 3. Read block header (validate magic/flags)
        // 4. For single-record block: read record header at block data start
        //    For multi-record block: iterate to find target record by in_block_offset
        // 5. Return data_len from record header
    }
}
```

**Record Header 格式** (12 bytes):
```
data_len: u32 (4 bytes, little-endian)
timestamp: i64 (8 bytes, little-endian)
```

### 4.2 QueryLengthIterator

类似 `QueryIterator`，但返回类型不同:

```rust
pub struct QueryLengthIterator<'a> {
    sources: Vec<QuerySource>,
    segments: &'a mut DataSegmentSet,
    cache: Option<Arc<BlockCache>>,
    hot_block: HotBlockCache,
    current_source_idx: usize,
}

impl<'a> Iterator for QueryLengthIterator<'a> {
    type Item = Result<(i64, u32)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Similar to QueryIterator::next_entry()
        // But only reads record header, not full data
    }
}
```

**复用**: `QuerySource` 和 `HotBlockCache` 可直接复用

### 4.3 索引查询优化

`query_exist` 使用 `TimeIndex::query()` 获取范围内所有 entry。对于大范围查询，可考虑:
- 当前实现: 一次性加载所有 entry 到内存
- 未来优化: 使用 `prepare_query_sources()` + cursor 模式（需权衡复杂度）

---

## 五、FFI 接口

新增 FFI 函数:

```c
// read_exist: 检查索引是否存在
bool tmsl_dataset_read_exist(TmslStore* store, const char* name, const char* type, int64_t timestamp);

// query_exist: 范围索引存在性检查，返回位图
// 返回的 bitmap 需要调用方 free
uint8_t* tmsl_dataset_query_exist(TmslStore* store, const char* name, const char* type, 
                                   int64_t start_ts, int64_t end_ts, size_t* bitmap_len);

// read_length: 读取数据长度
bool tmsl_dataset_read_length(TmslStore* store, const char* name, const char* type, 
                              int64_t timestamp, uint32_t* out_len);

// query_length: 范围查询数据长度数组
// 返回的数组需要调用方 free
uint32_t* tmsl_dataset_query_length(TmslStore* store, const char* name, const char* type,
                                     int64_t start_ts, int64_t end_ts, size_t* array_len);

// query_length_iter: 创建数据长度迭代器
TmslIterator* tmsl_dataset_query_length_iter(TmslStore* store, const char* name, const char* type,
                                              int64_t start_ts, int64_t end_ts);

// 迭代器 next: 返回 timestamp，通过 out_len 返回 data_len
bool tmsl_length_iter_next(TmslIterator* iter, int64_t* out_ts, uint32_t* out_len);
```

**相关文档**: [Store 与 FFI](store-and-ffi.md)

---

## 六、使用场景

### 6.1 数据存在性快速检查

```rust
// 检查单个时间戳是否有数据
if dataset.read_exist(timestamp)? {
    // 有数据，继续处理
}

// 批量检查多个时间戳
let bitmap = dataset.query_exist(start_ts, end_ts)?;
for i in 0..count {
    let exists = (bitmap[i / 8] >> (i % 8)) & 1 == 1;
    // ...
}
```

### 6.2 预估查询数据量

```rust
// 查询前预估数据量，避免一次性加载过多数据
let entries = dataset.query_length(start_ts, end_ts)?;
let total_size: u64 = entries.iter()
    .map(|&(_, len)| len as u64)
    .sum();

if total_size > MAX_ALLOWED_SIZE {
    // 使用迭代器分批处理
    let iter = dataset.query_length_iter(start_ts, end_ts)?;
    // ...
}
```

### 6.3 高效数据同步

```rust
// 同步场景：先检查哪些时间戳有数据，再针对性拉取
let bitmap = dataset.query_exist(remote_start, remote_end)?;
for i in 0..bitmap.len() * 8 {
    if bitmap[i / 8] & (1 << (i % 8)) != 0 {
        let ts = remote_start + i as i64;
        let data = dataset.read(ts)?;
        // 同步到目标
    }
}
```

---

## 七、运行时约束

### 7.1 read_only 模式支持

所有新增的读操作接口均支持 `read_only` 模式：

- `read_exist()` — 只读索引，完全兼容
- `query_exist()` — 只读索引，完全兼容
- `read_length()` — 读取 record header，完全兼容
- `query_length()` — 读取 record header，完全兼容
- `query_length_iter()` — 读取 record header，完全兼容

`.journal/logs` dataset 的 `runtime_context` 标记为 `read_only=true`，这些读操作可正常调用。

### 7.2 Journal 交互

这些读操作**不会**触发 journal 记录：

- `read_exist()` — 无写入，无 journal hook
- `query_exist()` — 无写入，无 journal hook
- `read_length()` — 无写入，无 journal hook
- `query_length()` — 无写入，无 journal hook
- `query_length_iter()` — 无写入，无 journal hook

与 `read()`/`query()`/`query_iter()` 行为一致，读操作不产生 journal 条目。

### 7.3 并发安全

- `read_exist()` 使用 `&self`（不可变借用），支持并发调用
- 其他读操作使用 `&mut self`（可变借用），与写操作互斥
- 通过 Store 门面调用时，内部 `Mutex<DataSet>` 保证线程安全
