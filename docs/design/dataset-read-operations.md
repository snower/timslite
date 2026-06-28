# 数据集读操作 - DataSet Read Operations

> 统一描述 DataSet 所有读相关接口: read、query、query_iter 及新增的 read_exist、query_exist、read_length、query_length、query_length_iter。

---

## 一、读操作接口总览

| 接口 | 返回类型 | 用途 | 数据读取 |
|------|---------|------|---------|
| `read(ts)` | `Option<(i64, Vec<u8>)>` | 读取单条完整记录 | 是 |
| `read_latest()` | `Option<(i64, Vec<u8>)>` | 读取最大已写 timestamp 对应记录 | 是 |
| `query(start, end)` | `Vec<(i64, Vec<u8>)>` | 范围查询完整记录 | 是 |
| `query_iter(start, end)` | `QueryIterator` | 惰性范围查询迭代器 | 是 (按需) |
| `read_exist(ts)` | `bool` | 检查单个时间戳当前是否有可见数据 | **否** |
| `query_exist(start, end)` | `Vec<u8>` (bitmap) | 范围数据存在性快速检查 | **否** |
| `read_length(ts)` | `Option<u32>` | 读取单条记录数据长度 | 仅 header |
| `query_length(start, end)` | `Vec<(i64, u32)>` | 范围查询数据长度列表 | 仅 header |
| `query_length_iter(start, end)` | `QueryLengthIterator` | 惰性范围数据长度迭代器 | 仅 header (按需) |

**性能层次**:
- **最快**: `read_exist` — 仅索引查找，无数据段 I/O
- **快**: `query_exist` — 索引范围查询，无数据段 I/O
- **中**: `read_length`, `query_length`, `query_length_iter` — 需定位并读取 record header (12 bytes)
- **慢**: `read`, `query`, `query_iter` — 需读取完整数据

---

## 二、现有接口

本节签名描述 Store-managed public `DataSet` wrapper。Public wrapper 内部持有 `Arc<Mutex<DataSetInner>>`, 因此读操作使用 `&self` 并在方法内部获取 dataset mutex。若文档需要展示 crate-internal `DataSetInner` helper, 会明确标注为 internal; internal helper 可以继续使用 `&mut self`。

### 2.1 read(timestamp)

```rust
pub fn read(&self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>
```

读取单条完整记录。

**参数**:
- `timestamp`: 目标业务时间戳, signed `i64`; `-1` 是普通精确 timestamp, 不表示 latest

**返回**:
- `Ok(Some((timestamp, data)))` — 记录存在且有效
- `Ok(None)` — 记录不存在、是 filler、或已过期

**流程**:
1. 检查 retention 是否过期
2. `TimeIndex::find_entry()` 查找索引
3. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER`)
4. `DataSegmentSet::read_at_index()` 读取完整数据

### 2.1.1 read_latest()

```rust
pub fn read_latest(&self) -> Result<Option<(i64, Vec<u8>)>>
```

读取 `latest_written_timestamp` 对应的完整记录。

**语义**:
- `latest_written_timestamp: Option<i64>` 为 `None` 时返回 `Ok(None)`
- 最大已写 timestamp 对应 entry 不存在、已删除、为 filler 或已过期时返回 `Ok(None)`, 不回退到更早有效记录
- `read_latest()` 是唯一 latest 读取入口; `read(-1)` 读取精确 timestamp `-1`

**相关文档**: [数据集操作·读取流程](dataset-operations.md#十读取流程详解)

---

### 2.2 query(start_ts, end_ts)

```rust
pub fn query(&self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>
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
pub fn query_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryIterator>
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

检查单个时间戳当前是否有可见数据。

**参数**:
- `timestamp`: 目标业务时间戳, signed `i64`; `-1` 是普通精确 timestamp

**返回**:
- `Ok(true)` — timestamp 在 retention 可见范围内，且索引 entry 指向真实数据
- `Ok(false)` — 索引 entry 不存在、是 filler/deleted entry，或 timestamp 已过期

**流程**:
1. 检查 retention 是否过期，过期返回 `false`
2. `TimeIndex::find_entry()` 查找索引
3. entry 不存在或 `block_offset == BLOCK_OFFSET_FILLER` 返回 `false`
4. 否则返回 `true`

**特点**:
- **不读取数据段** — 性能最优
- **检查 retention 和 filler** — 表示当前可见数据存在性，不表示底层索引物理 entry 是否仍存在

**设计决策**: `read_exist` / `query_exist` 用于通过索引快速判断“数据是否当前可读”。过期索引、filler/deleted entry 与物理索引残留都返回不存在；调用方不应使用它们判断底层索引文件是否仍有 entry。

---

### 3.2 query_exist(start_ts, end_ts)

```rust
pub fn query_exist(&self, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>
```

范围数据存在性快速检查，返回位图。

**参数**:
- `start_ts`: 起始时间戳（含）
- `end_ts`: 结束时间戳（含）

**返回**: `Vec<u8>` — 位图字节数组
- 位 `i` 代表时间戳 `(start_ts + i)` 当前是否有可见数据
- `1` = retention 可见范围内存在真实数据，`0` = 不存在、过期、或为 filler/deleted entry
- 字节数组长度 = `(count + 7) / 8`，其中 `count = end_ts - start_ts + 1`
- 最大可分配 bitmap 为 4MiB；超过该上限返回错误

**示例**:
```
start_ts = 100, end_ts = 107
时间戳:    100 101 102 103 104 105 106 107
存在性:      1   0   1   1   0   0   1   0
位图 (LE): 0b01001101 = 0x4D
返回: [0x4D]
```

**流程**:
1. 若 `start_ts > end_ts` 返回空 bitmap
2. 使用 checked arithmetic 按原请求范围计算 timestamp 数量和 bitmap 字节数，保持 bit `i` 始终对应 `start_ts + i`
3. 若 bitmap 字节数超过 4MiB，返回错误
4. 计算 retention 可见起点，仅查询当前可见范围内的 index entry；过期区间在原 bitmap 中保持 0
5. 跳过 filler/deleted entry，仅对真实数据 entry 设置对应位

**特点**:
- **不读取数据段** — 仅索引查询
- **限制 bitmap 内存** — 单次调用最多分配 4MiB bitmap
- **返回原始位图** — 调用方需自行解析

---

### 3.3 read_length(timestamp)

```rust
pub fn read_length(&self, timestamp: i64) -> Result<Option<u32>>
```

读取单条记录的逻辑数据长度。

**参数**:
- `timestamp`: 目标业务时间戳, signed `i64`; `-1` 是普通精确 timestamp

**返回**:
- `Ok(Some(data_len))` — 记录存在，返回逻辑数据长度
- `Ok(None)` — 记录不存在、是 filler、或已过期

**流程**:
1. 检查 retention 是否过期
2. `TimeIndex::find_entry()` 查找索引
3. 跳过 filler (`block_offset == BLOCK_OFFSET_FILLER`)
4. `DataSegmentSet::read_record_data_len()` 仅读取 record header

**数据长度定义**: 用户写入的原始数据长度（不含 record header、block header、压缩开销）

**新增依赖**: 需在 `DataSegmentSet` 中添加 `read_record_data_len()` 方法

---

### 3.4 query_length(start_ts, end_ts)

```rust
pub fn query_length(&self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>
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
pub fn query_length_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator>
```

惰性范围数据长度迭代器。

**参数**: 同 `query_length`

**返回**: `QueryLengthIterator` — 每次 `next()` 返回 `(timestamp, data_len)`

**特点**:
- 仅返回有效记录（跳过 filler 和不存在的时间戳）
- 支持 HotBlockCache（需读取 block 获取 record header）
- 通过 Store 创建时自动注入 `Arc<BlockCache>`
- public Rust wrapper 使用 `TimeIndex::prepare_query_sources()` 构造 source cursor, 迭代期间按需打开 index segment 并读取 record header; 创建 iterator 时不预先收集全部 `(timestamp, data_len)`。

**与 query_iter 的区别**:
- `query_iter` 返回完整数据 `(i64, Vec<u8>)`
- `query_length_iter` 仅返回数据长度 `(i64, u32)`

**Rust 与 FFI 语义差异**:
- Rust `DataSet::query_length_iter()` 是 source-cursor iterator。每次 `next()` 才读取下一个 index entry 和对应 record header; 如果 dataset 在迭代期间关闭或相关 data segment 被 retention 删除, 后续 `next()` 返回错误。
- FFI `tmsl_dataset_query_length_iter()` 当前使用 index-entry snapshot iterator。创建 iterator 时 snapshot 当前可见的 `IndexEntry` 列表; `tmsl_length_iter_next()` 再逐条按 snapshot entry 读取 data length。它不会重新查询 index, 但 data segment 缺失、过期或 dataset handle 失效仍会在 next 时返回错误。

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

`read_length` / `query_length` 的公共契约按完整 12B record header 定义, 因为 record 边界和 timestamp 校验都属于 header 语义。实现可以在已经由 index 定位、且无需再次校验 timestamp 的 fast path 中只解码前 4B `data_len`, 但文档、测试和 ABI 不再使用 8B header 口径。

### 4.2 QueryLengthIterator

类似 `QueryIterator`，但返回类型不同:

```rust
pub struct QueryLengthIterator {
    sources: Vec<QuerySource>,
    dataset: Arc<Mutex<DataSetInner>>,
    hot_block: HotBlockCache,
    current_source_idx: usize,
}

impl Iterator for QueryLengthIterator {
    type Item = Result<(i64, u32)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Read the next source entry lazily, then lock the dataset
        // and read only the record header, not the full payload.
    }
}
```

**复用**: `QuerySource` 和 `HotBlockCache` 可直接复用。crate-internal `DataSetInner::query_length_iter()` 可以继续返回借用 `DataSegmentSet` 的内部 iterator; public wrapper 需要持有 dataset `Arc<Mutex<...>>` 或等价 guard/cursor 结构，避免退化为 `query_length()` snapshot。

### 4.3 索引查询优化

`query_exist` 使用 `TimeIndex::query()` 获取 retention 可见范围内的 entry，并跳过 filler/deleted entry。对于大范围查询，可考虑:
- 当前实现: bitmap 最大 4MiB，一次性加载可见范围内的 entry 到内存
- 未来优化: 使用 `prepare_query_sources()` + cursor 模式（需权衡复杂度）

---

## 五、FFI 接口

轻量读操作的 C ABI 以 [Store 与 C ABI Wrapper](store-and-ffi.md#十二c-abi-wrapper) 和 [wrapper/cffi/include/timslite.h](../../wrapper/cffi/include/timslite.h) 为权威来源。当前有效 ABI 全部使用 opaque dataset pointer (`void* dataset`) 作为入口, 返回 `int` 状态码或 opaque iterator pointer, 并通过 `err_buf` 返回错误文本。

本文件不再复制完整 C 函数签名, 避免旧式 `TmslStore* + name/type` 草案与当前 ABI 漂移。这里仅记录语义:

- `tmsl_dataset_read_exist`: 精确 timestamp 存在性检查, `0=false`, `1=true`, `-1=error`。
- `tmsl_dataset_query_exist`: 返回 malloc 分配的 bitmap, 调用方用 `tmsl_data_free` 释放。
- `tmsl_dataset_read_length`: `0=found`, `1=not found`, `-1=error`。
- `tmsl_dataset_query_length`: 返回 malloc 分配的 `TmslLengthEntry` snapshot array, 调用方用 `tmsl_data_free` 释放。
- `tmsl_dataset_query_length_iter`: 创建 index-entry snapshot iterator; `tmsl_length_iter_next` 按 snapshot entry 逐条读取 data length。

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

`.journal/logs` 不再作为普通 `DataSet` 暴露, 因此这些 DataSet 读操作不适用于 journal。Journal 使用专用 `journal_read` / `journal_query` / `open_journal_queue` API。

### 7.2 Journal 交互

这些读操作**不会**触发 journal 记录：

- `read_exist()` — 无写入，无 journal hook
- `query_exist()` — 无写入，无 journal hook
- `read_length()` — 无写入，无 journal hook
- `query_length()` — 无写入，无 journal hook
- `query_length_iter()` — 无写入，无 journal hook

与 `read()`/`query()`/`query_iter()` 行为一致，读操作不产生 journal 条目。

### 7.3 并发安全

- 低层 `DataSetInner` 读操作使用可变借用, 与写操作互斥
- 公开 `DataSet` 读操作使用 `&self`, 并由 DataSet 内部 mutex 保证线程安全
