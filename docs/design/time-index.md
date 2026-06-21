# 时间索引 - TimeIndex + IndexSegment

## 七、TimeIndex: 时间索引

### 7.1 结构

```rust
struct TimeIndex {
    base_dir: PathBuf,
    segment_size: u64,
    initial_segment_size: u64,    // 初始分配大小
    index_segments: BTreeMap<i64, IndexSegmentEntry>, // key = index segment start_timestamp
    in_memory_buffer: Vec<IndexEntry>,
    in_memory_flush_threshold: usize,               // 默认 1024
    index_continuous: bool,                         // 连续存储模式
    base_timestamp: Option<i64>,                    // 连续模式第一条真实写入 timestamp
    time_step: i64,                                 // 连续模式固定为 1 个 timestamp 单位
}

struct IndexSegmentMeta {
    path: PathBuf,
    start_timestamp: i64,   // 连续模式下为逻辑分段起点
    entries_capacity: usize,
    wrote_count: usize,     // 已物化 entry 数; 后续位置可为逻辑空洞
}
```

### 7.2 生命周期管理

```rust
impl TimeIndex {
    /// sync 所有打开的 index segment
    pub fn sync_all(&mut self) -> io::Result<()>;

    /// idle-close 所有 index segment
    pub fn idle_close_all(&mut self) -> Result<()>;

    /// 按需打开已关闭的 index segment
    fn ensure_segment_open(&mut self, start_ts: i64) -> Result<&mut IndexSegment>;

    /// 兼容查询: 收集时间范围 [start_ts, end_ts] 内的所有 entries
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<IndexEntry>>;

    /// 惰性查询准备: 返回 source cursor, segment 文件不全量加载 IndexEntry
    pub fn prepare_query_sources(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<QuerySource>>;

    /// 从磁盘加载已有 index segments
    pub fn load_existing(base_dir: &Path, segment_size: u64) -> io::Result<Self>;

    /// 连续模式: 从首个数值分段文件名恢复, 或在首次真实写入时初始化内存 base_timestamp
    fn set_or_load_base_timestamp(&mut self, first_ts: i64) -> Result<i64>;

    /// 连续模式: 由 base_timestamp + segment capacity 计算逻辑分段起点
    fn segment_start_for(&self, timestamp: i64) -> Result<i64>;

    /// 连续模式正序写: 只物化上段尾部与当前段前缀, 中间完整分段保持逻辑空洞
    fn append_sparse_continuous_entry(
        &mut self,
        prev_latest: i64,
        timestamp: i64,
        entry: IndexEntry,
    ) -> Result<()>;

    /// 连续模式回填: 目标可能是真实 entry、filler 或逻辑空洞
    fn upsert_sparse_continuous_entry(&mut self, timestamp: i64, entry: IndexEntry) -> Result<Option<IndexEntry>>;
}
```

### 7.3 IndexEntry 序列化 (14 字节)

`IndexEntry.block_offset` 字段存储 Block 在数据流中的逻辑全局偏移: 相对各数据段数据区起点, 指向 BlockHeader 起始, 不包含任何数据段 header。读取 data segment 时必须先定位所属 `segment`, 再以 `segment.header_len + (block_offset - segment.file_offset)` 定位 BlockHeader。

```rust
const INDEX_ENTRY_SIZE: usize = 14;

impl IndexEntry {
    fn to_bytes_for_segment(&self, segment_start_timestamp: i64) -> Result<[u8; INDEX_ENTRY_SIZE]> {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        let delta = self
            .timestamp
            .checked_sub(segment_start_timestamp)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(TmslError::InvalidData("index timestamp delta out of range"))?;
        buf[0..4].copy_from_slice(&delta.to_le_bytes());
        buf[4..12].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[12..14].copy_from_slice(&self.in_block_offset.to_le_bytes());
        Ok(buf)
    }

    fn from_bytes_for_segment(segment_start_timestamp: i64, buf: &[u8; INDEX_ENTRY_SIZE]) -> Result<Self> {
        let delta = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let timestamp = segment_start_timestamp
            .checked_add(delta as i64)
            .ok_or(TmslError::InvalidData("index timestamp delta overflow"))?;
        Ok(Self {
            timestamp,
            block_offset: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            in_block_offset: u16::from_le_bytes(buf[12..14].try_into().unwrap()),
        })
    }
}
```

`IndexEntry` 内存态仍保留 `timestamp: i64`; 只有 index segment 落盘格式把 timestamp 存为 `timestamp_delta: u32 LE`, 其值为 `timestamp - segment.start_timestamp`。`block_offset: u64 LE` 和 `in_block_offset: u16 LE` 不变。写入前必须校验 delta 非负且不超过 `u32::MAX`; 非连续模式下, 如果最新 index segment 尚未写满但新 timestamp 已超过该 segment 的 u32 delta 范围, 必须创建以该 timestamp 为起点的新 index segment。

这是破坏性 index entry 落盘布局调整: 当前项目尚未首次 release, 因此 index segment header version 仍保持 1, 且不保留旧 18 字节 entry 解析逻辑。JournalRecord 中的 `JournalIndexInfo` 不属于 index segment 物理格式, 仍保持 `timestamp:i64 + block_offset:u64 + in_block_offset:u16` 的 18 字节格式。

### 7.4 IndexSegment

```rust
struct IndexSegment {
    path: PathBuf,
    start_timestamp: i64,       // 连续模式下为逻辑分段起点
    entries_capacity: usize,
    wrote_count: usize,          // 已物化 entry 数; wrote_count 之后为逻辑空洞
    mmap: Option<MmapMut>,       // None = closed/unmapped
    sealed: bool,
    last_accessed_at: Instant,
    is_flushed: bool,             // 内存态: 当前 mmap 内容是否已 MS_SYNC
    queued_for_flush: bool,       // 内存态: dirty 后是否已进入 runtime flush 队列
    current_file_size: u64,      // 运行时文件实际大小 (随扩容增长)
    max_file_size: u64,          // 扩容上限 (segment_size, 不可变)
}

impl IndexSegment {
    fn append_entry(&mut self, entry: &IndexEntry) -> io::Result<()>;
    fn lower_bound(&self, target_ts: i64) -> usize;
    fn find_exact(&self, target_ts: i64) -> Option<IndexEntry>;
    fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry>;

    /// 连续模式 O(1) 查找变体
    fn lower_bound_cs(&self, target_ts: i64, index_continuous: bool) -> usize;
    fn find_exact_cs(&self, target_ts: i64, index_continuous: bool) -> Option<IndexEntry>;
    fn find_entry_index_cs(&self, target_ts: i64, index_continuous: bool) -> Option<usize>;
    fn direct_lookup(&self, target_ts: i64) -> Option<IndexEntry>;

    /// 连续模式: 物化到 target_ts 之前的位置, 缺失项写入 filler
    fn materialize_until(&mut self, target_ts: i64) -> Result<()>;

    /// 读取段内最后一条索引条目的 timestamp (用于回收和 latest 恢复, 无需完全 open)
    fn last_entry_timestamp(path: &Path) -> Result<Option<i64>>;

    /// 生命周期
    pub fn ensure_open(&mut self) -> Result<()>;
    pub fn idle_close(&mut self) -> Result<()>;
    pub fn sync(&mut self) -> Result<()>;

    /// 创建/打开/扩容
    pub fn create(base_dir: &Path, start_timestamp: i64, initial_size: u64, max_size: u64) -> Result<Self>;
    pub fn open(path: &Path, start_timestamp: i64, max_file_size: u64) -> Result<Self>;
    pub fn expand(&mut self) -> Result<()>;
}
```

索引分段的 flush 语义与 data segment 一致:

- 创建、打开、成功 `sync()` 后 `is_flushed=true`。
- `append_entry()` / `overwrite_entry()` 写 mmap 后置 `is_flushed=false`。
- dirty 状态首次从 true 变 false 时, 通过 `DataSetRuntimeContext` 引用的 Store 级共享 `flush_queue` 加入 `{ dataset_key, Index { start_timestamp } }` target。
- `TimeIndex::flush_to_disk()` 可能把内存 index buffer 写入多个 index segment; 写入完成后由 `DataSet` 收集 dirty index targets 入队。
- 创建新的 index segment 前, 对前一个已经完结或跨 grid 的 index segment 直接 `sync()`。

### 7.5 索引文件布局

```
┌──────────────────────────────────────────────┐
│ IndexFileHeader reserved area (fixed 128B)   │
│ - 固定前缀(9B): magic(4)+version(2)+         │
│   fileType(1)+meta_length(2)                 │
│ - Meta TLV: created_at, file_offset,         │
│   file_size, compress_level, compress_type   │
│ - state_length(2B): 8                        │
│ - State(8B): wrote_position (1×8B)          │
│ - Reserved padding to byte 128               │
├──────────────────────────────────────────────┤
│ Index Area (starts at absolute offset 128)   │
│ ┌─────────┬──────────┬──────┐                │
│ │ dts:4   │ block:8  │ ib:2 │ entry 1        │
│ └─────────┴──────────┴──────┘                │
│ ┌─────────┬──────────┬──────┐                │
│ │ dts:4   │ block:8  │ ib:2 │ entry 2        │
│ └─────────┴──────────┴──────┘                │
│ ...                                           │
└──────────────────────────────────────────────┘
```

> **与数据段的差异**: 索引段 state 仅保留 `wrote_position` (8 bytes), 无需 `record_count` (可计算: `(wrote_position - 128) / 14`), 无需 `pending` 相关字段 (索引无 pending 概念), 无需 `min/max_timestamp` (索引按 `start_timestamp` 路由, 无需额外范围字段)。索引段的 Meta TLV/state 仍按可变 header 解析, 但 entry area 起点固定为 128, 不随已知 header 内容长度变化。

> **发布边界**: `IndexEntry` 是 record 对查询可见的发布点。Data segment append 必须先写 payload 与 block/header state, 最后才追加或覆盖 index entry。若 crash 发生在 index 写入前, data segment 中的孤儿 payload 不可见并按丢失处理。若 crash/reopen 后出现已持久化 index 指向不完整 data 的情况, 读取路径必须通过 block 边界和 record timestamp 校验拒绝返回错误数据; 本库不通过索引事务恢复该写入。

### 7.6 查找算法

| 操作 | 非连续模式 | 连续模式 |
|------|-----------|---------|
| `lower_bound` | 二分查找 O(log n) | 直接计算 O(1) |
| `find_exact` | 二分查找 O(log n) | 直接计算 O(1) |
| `find_entry_index` | 二分查找 O(log n) | 直接计算 O(1) |
| `query_range` | O(log n + k) | O(1 + k) |

其中 `k` = 查询范围内条目数, `n` = 段内总条目数。

### 7.6.1 TimeIndex 分段注册表与定位

`TimeIndex` 使用单个 `BTreeMap<i64, IndexSegmentEntry>` 保存所有 index segment, key 为 `start_timestamp`:

- `IndexSegmentEntry::Open(IndexSegment)`: 当前 mmap/open 的索引分段。
- `IndexSegmentEntry::Closed(IndexSegmentMeta)`: 已 idle-close 或 load-existing 后尚未打开的索引分段元数据。

该注册表天然按 `start_timestamp` 升序维护。load-existing、新建 segment、lazy-open、idle-close、remove pure filler segment 与 retention reclaim 都只更新同一个 map 中的 entry 状态或删除 key。写入路径可以通过 `BTreeMap::last_key_value()` 获取最新段, 不再维护 open/closed 两个列表。

单点查找/更新/删除的 segment 定位规则:

- continuous mode: 通过 `segment_start_for(timestamp)` 直接计算目标 `start_timestamp`, 然后从 `BTreeMap` 中 O(log n) 命中。最多打开一个 closed segment。
- non-continuous mode: 通过 `range(..=timestamp).next_back()` 找 `start_timestamp <= timestamp` 的最后一个候选 segment。若候选段内不存在目标 timestamp, 该 timestamp 不存在; 不逐个打开全部 closed index segment。

范围查询仍需要访问与 `[start_ts, end_ts]` 有交集的多个 segment, 但应利用 `BTreeMap::range` 跳过明显在范围外的前缀/后缀。对于 closed segment, 只打开候选范围内的 segment。

### 7.7 连续模式稀疏分段

连续模式仍保持 O(1) 定位, 但不再要求所有缺失 timestamp 都落盘为 filler。它使用固定逻辑网格:

```text
segment_capacity = floor((index_segment_size - 128) / 14)
time_step        = 1
base_timestamp   = first real write timestamp
segment_ordinal  = floor((ts - base_timestamp) / segment_capacity)
segment_start    = base_timestamp + segment_ordinal * segment_capacity
entry_index      = ts - segment_start
```

**关键约束**:
- 第一次真实写入只初始化 `base_timestamp` 并写入真实 entry, 不从 0、Unix epoch 或其它固定起点补 filler。
- 不新增单独的 `base_timestamp` 文件; 第一次 flush 创建的首个数值 index segment 文件名即为可恢复基准。
- 跨大 gap 正序写入时, 只物化上一个真实写入所在分段的尾部和当前写入所在分段的前缀; 中间完整分段不创建、不写 filler。
- 已创建分段内 `entry_index >= wrote_count` 的位置视为逻辑空洞, `read/query/delete` 行为等价于不存在真实数据。
- 后续回填落在逻辑空洞时, 按需创建目标分段, 只物化该分段起点到目标 timestamp 前一位的 filler, 再写入真实 entry。

因此一次正序写入的最坏 filler 访问量小于两个索引分段容量之和减 2, 不随真实 timestamp 跨度增长。

### 7.8 索引保留回收

**职责**: 删除超过数据有效期的索引段文件, 与数据段回收配对进行。

**回收规则 (TimeIndex)**:
```
reclaim_expired_segments(expiration_threshold: i64):
  前置条件: 调用方已 flush() 并对 index segment 执行 idle_close_all();
           所有 index segment entry 均已 sync + unmap, 处于 closed/unmapped 状态
  for (start_timestamp, entry) in index_segments:
    if entry is not Closed(meta):
      continue
    last_ts = IndexSegment::last_entry_timestamp(meta.path)
    if last_ts < expiration_threshold:
      fs::remove_file(meta.path)
      index_segments.remove(start_timestamp)
```

**`last_entry_timestamp` 实现**:
- 仅打开文件一次 (read-only mmap, 不使用 `MmapMut::map_mut` 避免 Windows 锁定)
- 从文件 header 的 `wrote_position` 推算最后条目位置: `128 + (wrote_count - 1) * 14`
- 立即 drop(mmap) + drop(file), 检查完成后不保持打开状态
- 返回 `Ok(Some(last_ts))`, `Ok(None)` 或 `Err`; 空段/`wrote_count==0` 返回 `Ok(None)`
- `DataSet::open` 恢复 `latest_written_timestamp` 时, 只需要读取最新非空 index segment 文件的最后一条 entry; delete/filler entry 的 timestamp 仍计入 latest, 因此 `read_latest()` 不会回退到更早有效记录；没有任何 entry 时 latest 为 `None`

**关键约束**:
- 回收期间打开的索引段文件必须**检查完成后立即释放** (不用 idle-close)
- 如果 index segment 同时包含过期和未过期 timestamp, 该混合分段必须保留, 不做部分裁剪
- 索引回收不检查 entry 指向的数据段是否仍存在; 查询入口会先按 retention threshold 钳制, 再通过 data segment 边界校验避免读取异常
- 索引回收不调用 public lifecycle `DataSet::close()`, 不关闭 queue, 不移除 Store registry 中的 dataset, 也不使现有 handle 失效
- 数据段和索引段按各自分段时间范围独立回收, 不要求成对删除同一时间窗口

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [索引连续存储](index-continuous.md)

