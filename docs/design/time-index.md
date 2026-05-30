# 时间索引 - TimeIndex + IndexSegment

## 七、TimeIndex: 时间索引

### 7.1 结构

```rust
struct TimeIndex {
    base_dir: PathBuf,
    segment_size: u64,
    initial_segment_size: u64,    // 初始分配大小
    index_segments: Vec<IndexSegment>,              // 打开中的 index segment
    closed_index_segments: Vec<IndexSegmentMeta>,   // 已关闭的 index segment
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

    /// 查询时间范围 [start_ts, end_ts] 内的所有 entries
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<IndexEntry>>;

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

### 7.3 IndexEntry 序列化 (18 字节)

```rust
const INDEX_ENTRY_SIZE: usize = 18;

impl IndexEntry {
    fn to_bytes(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[16..18].copy_from_slice(&self.in_block_offset.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8; INDEX_ENTRY_SIZE]) -> Self {
        Self {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            block_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            in_block_offset: u16::from_le_bytes(buf[16..18].try_into().unwrap()),
        }
    }
}
```

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

    /// 读取段内最后一条索引条目的 timestamp (用于回收判断, 无需完全 open)
    fn last_entry_timestamp(path: &Path, max_file_size: u64) -> Result<i64>;

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

### 7.5 索引文件布局

```
┌──────────────────────────────────────────────┐
│ IndexFileHeader (variable, v1 default 52B)   │
│ - 固定前缀(9B): magic(4)+version(2)+         │
│   fileType(1)+meta_length(2)                 │
│ - Meta TLV(33B): created_at, file_offset,    │
│   file_size, compress_level                  │
│ - state_length(2B): 8                        │
│ - State(8B): wrote_position (1×8B)          │
├──────────────────────────────────────────────┤
│ Index Area                                   │
│ ┌──────────┬──────────┬──────┐               │
│ │ ts:8     │ block:8  │ ib:2 │ entry 1       │
│ └──────────┴──────────┴──────┘               │
│ ┌──────────┬──────────┬──────┐               │
│ │ ts:8     │ block:8  │ ib:2 │ entry 2       │
│ └──────────┴──────────┴──────┘               │
│ ...                                           │
└──────────────────────────────────────────────┘
```

> **与数据段的差异**: 索引段 state 仅保留 `wrote_position` (8 bytes), 无需 `record_count` (可计算: `(wrote_position - header_len) / 18`), 无需 `pending` 相关字段 (索引无 pending 概念), 无需 `min/max_timestamp` (索引按 `start_timestamp` 路由, 无需额外范围字段)。

### 7.6 查找算法

| 操作 | 非连续模式 | 连续模式 |
|------|-----------|---------|
| `lower_bound` | 二分查找 O(log n) | 直接计算 O(1) |
| `find_exact` | 二分查找 O(log n) | 直接计算 O(1) |
| `find_entry_index` | 二分查找 O(log n) | 直接计算 O(1) |
| `query_range` | O(log n + k) | O(1 + k) |

其中 `k` = 查询范围内条目数, `n` = 段内总条目数。

### 7.7 连续模式稀疏分段

连续模式仍保持 O(1) 定位, 但不再要求所有缺失 timestamp 都落盘为 filler。它使用固定逻辑网格:

```text
segment_capacity = floor((index_segment_size - index_header_len) / 18)
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
reclaim_expired_segments(expiration_threshold: i64, max_file_size: u64):
  前置条件: DataSet 已 close(), 所有 index segment 处于 closed 状态
  for meta in closed_index_segments:
    last_ts = IndexSegment::last_entry_timestamp(meta.path, max_file_size)
    if last_ts < expiration_threshold:
      fs::remove_file(meta.path)
      closed_index_segments.remove(meta)
```

**`last_entry_timestamp` 实现**:
- 仅打开文件一次 (read-only mmap, 不使用 `MmapMut::map_mut` 避免 Windows 锁定)
- 从 `meta.wrote_count` 推算最后条目位置: `header_len + (wrote_count - 1) * 18`
- 立即 drop(mmap) + drop(file), 检查完成后不保持打开状态
- 返回 `Ok(last_ts)` 或 `Err` (空段/wrote_count==0 返回 start_timestamp)

**关键约束**:
- 回收期间打开的索引段文件必须**检查完成后立即释放** (不用 idle-close)
- 数据段与索引段**必须成对回收**: 回收同一时间窗口内的索引和数据段
- 回收顺序: TimeIndex 回收 → DataSegmentSet 回收 (先清索引, 后清数据)

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [索引连续存储](index-continuous.md)
