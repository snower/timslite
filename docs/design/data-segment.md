# 数据段管理 - DataSegmentSet + DataSegment

## 五、DataSegmentSet: 数据段集合

### 5.1 职责

- 管理同一数据集下的多个 DataSegment 文件
- 按索引中的 `block_offset` 路由到正确的数据段
- 自动创建新文件 (当前文件满或 sealed 时)
- 数据读取时跨段迭代

### 5.2 结构

```rust
struct DataSegmentSet {
    base_dir: PathBuf,
    segment_size: u64,
    initial_segment_size: u64,    // 初始分配大小
    compress_level: u8,
    segments: Vec<DataSegment>,           // 打开中的 data segment
    closed_segments: Vec<DataSegmentMeta>, // 已关闭的 data segment
    next_offset: u64,
    last_used_at: Instant,
}
```

### 5.3 生命周期管理

```rust
impl DataSegmentSet {
    /// sync 所有打开的 data segment
    pub fn sync_all(&mut self) -> Result<()>;

    /// idle-close 所有 data segment
    pub fn idle_close_all(&mut self) -> Result<()>;

    /// 按需打开已关闭的 segment
    pub fn lazy_open(&mut self, file_offset: u64) -> Result<&mut DataSegment>;

    /// 加载已有的 data segment 元数据 (Store open 时)
    pub fn load_existing(base_dir: &Path, segment_size: u64,
                         compress_level: u8) -> Result<Self>;
}
```

> `DataSet::sync_all()` 需要同时调用 `segments.sync_all()` + `time_index.sync_all()`。
> `DataSet::idle_close_all()` 同理。

## 六、DataSegment: 单个数据段 (Block 管理核心)

### 6.1 结构

```rust
struct DataSegment {
    path: PathBuf,
    file_offset: u64,             // 数据区逻辑全局起点, 等于数据段文件名
    file_size: u64,              // 运行时当前文件大小 (随扩容增长)
    max_file_size: u64,          // 扩容上限 (segment_size, 不可变)
    min_timestamp: i64,          // 段内最小时间戳 (i64::MAX=空段)
    max_timestamp: i64,          // 段内最大时间戳 (i64::MIN=空段)
    header_len: u64,             // 从文件头计算出的数据区起点
    wrote_position: u64,         // 从 header_len 起算的数据区内已用字节数
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    mmap: Option<MmapMut>,       // None = closed/unmapped
    lifecycle: SegmentLifecycle,
    last_accessed_at: Instant,
    // Pending Block 状态
    pending_block_offset: Option<u64>, // block_segment_offset
    pending_wrote_position: u64,
    pending_record_count: u64,
}

enum SegmentLifecycle {
    Closed,          // 文件未打开, mmap=None
    OpenReady,       // 打开中, mmap 有效, 可读写
}

const BLOCK_HEADER_SIZE: u64 = 16;
const DATA_HEADER_SIZE: u64 = 116;  // v1 默认数据段 header_len
```

`DataSegmentSet` 接收索引中的 `block_offset`, 通过 `segment_size` 计算所属数据段:

```text
segment.file_offset = (block_offset / segment_size) * segment_size
block_segment_offset = block_offset - segment.file_offset
physical_file_offset = segment.header_len + block_segment_offset
```

`DataSegment` 内部只使用 `block_segment_offset`; 任何 mmap/seek 位置都必须再加 `header_len`。

### 6.2 文件布局

```
┌──────────────────────────────────────────────────┐
│ DataFileHeader (variable, v1 default 116 bytes)  │
│ - 固定前缀: magic(4)+version(2)+fileType(1)+     │
│   meta_length(2)                                 │
│ - Meta(TLV, 33B): created_at, file_offset,       │
│   file_size, compress_level                      │
│ - state_length: 2                                │
│ - State(72B): 9×8B                               │
│   min_timestamp, max_timestamp, wrote_position,  │
│   record_count, total_uncompressed_size,         │
│   pending_block_offset, pending_wrote_position,  │
│   pending_record_count, invalid_record_count     │
├──────────────────────────────────────────────────┤
│ Block 1 (sealed, compressed)                     │
│   BlockHeader (16 bytes)                         │
│   Payload (compressed records)                   │
├──────────────────────────────────────────────────┤
│ Block 2 (sealed, compressed)                     │
├──────────────────────────────────────────────────┤
│ Current Pending Block (未完成, 未压缩)             │
│   BlockHeader (16 bytes, flags=0)               │
│   Payload (raw records)                          │
└──────────────────────────────────────────────────┘
```

### 6.3 写入核心逻辑

```rust
impl DataSegment {
    fn append_record(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress_level: u8,
    ) -> io::Result<(u64, u16)> {
        let record_size = 4 + 8 + data.len();

        // 情况1: 单条 record 超过 BLOCK_MAX_SIZE(65536) → 独占 Block
        if record_size > BLOCK_MAX_SIZE as usize {
            if let Some(off) = self.pending_block_offset {
                self.seal_pending_block(off, compress_level)?;
                self.clear_pending();
            }
            return self.create_single_record_block(timestamp, data, compress_level);
        }

        // 情况2: 有 pending block
        if let Some(pending_off) = self.pending_block_offset {
            let new_total = self.pending_block_uncomp_size + record_size as u32;

            if new_total > BLOCK_MAX_SIZE {
                self.seal_pending_block(pending_off, compress_level)?;
                self.clear_pending();
                return self.create_pending_and_append(timestamp, data);
            }

            let in_block_offset = self.pending_block_uncomp_size;
            self.write_raw_record_to_pending(timestamp, data)?;
            self.pending_block_uncomp_size = new_total;
            self.pending_block_record_count += 1;
            return Ok((pending_off, in_block_offset));
        }

        // 情况3: 创建新 pending block
        self.create_pending_and_append(timestamp, data)
    }

    /// 密封 pending block: 压缩+写回
    fn seal_pending_block(&mut self, block_segment_offset: u64, compress_level: u8) -> io::Result<()>;
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;
    fn create_pending_and_append(&mut self, timestamp: i64, data: &[u8]) -> io::Result<(u64, u16)>;
    fn create_single_record_block(&mut self, timestamp: i64, data: &[u8], compress_level: u8) -> io::Result<(u64, u16)>;

    /// 纠正写入: 在该段最后一个 pending raw block 的最末 record 位置原地覆盖 data 字节, 支持变 size
    /// 若最后一个 block 含 SEALED 或 COMPRESSED flag → 返回错误 (不可原地修改)
    /// 若该 record 不是 block 最末 record → 返回错误
    /// 修改后需更新: block.payload_size/uncompressed_size + 段 wrote_position/total_uncompressed_size/pending_wrote_position(仅 pending)
    fn overwrite_in_last_block(
        &mut self,
        block_segment_offset: u64,
        in_block_offset: u16,
        new_data: &[u8],
    ) -> io::Result<()>;
}
```

**append 发布顺序约束**:

`DataSegment::append_record` 返回的 `(block_segment_offset, in_block_offset)` 会被 `DataSet` 与当前 `segment.file_offset` 合成为 `block_offset`, 再写入 `TimeIndex`。因此 data segment 侧必须先完成 record payload 和 block/header state 更新, 再让上层发布 index entry。对于新 pending block、追加 pending block、超大独占 block 三类路径, 设计上的可见性顺序都是:

1. 写入 record payload。
2. 写入或更新 `BlockHeader` 与 data segment state。
3. 返回 offset 给 `DataSet`, 由 `DataSet` 最后写 index。

如果 crash 发生在第 3 步之前, 已写入 data segment 但未被 index 引用的数据视为丢失; 查询不会扫描 data segment 来发现它。如果 crash 发生在 index 发布之后但 data/index 落盘顺序不完整, 读取路径必须通过 block 边界和 record timestamp 校验避免返回错位数据。

#### overwrite_in_last_block: 纠正写入 (In-Place Overwrite, 支持变 size)

纠正写入场景下 (`timestamp == latest_written_timestamp`), 该最大已写 timestamp 对应的记录只有在仍位于 **本数据段最后一个 pending raw block (`flags=0`)** 的 **最末位置** 时, 才可通过 mmap 直接修改该 record 的 data 字节, 支持 data 长度变化。只要 block 已经 sealed/compressed, 就不能再原地修改, 由 `DataSet::correct_write` 回退为乱序追加并更新索引:

```rust
fn overwrite_in_last_block(
    &mut self,
    block_segment_offset: u64,
    in_block_offset: u16,
    new_data: &[u8],
) -> io::Result<()> {
    // 1. 验证这是段内最后一个 block:
    //    block_abs_end = block_segment_offset + BLOCK_HEADER_SIZE + block.payload_size
    //    若 block_abs_end < self.wrote_position → 不是最后 block, 返回错误
    //
    // 2. 读取 block header (16B at header_len + block_segment_offset)
    //    - 检查 flags: 必须等于 0; 若含 SEALED 或 COMPRESSED → 返回错误
    //    - payload_size / uncompressed_size
    //
    // 3. 验证 record 是块内最末 record:
    //    - 读取 record.data_len (4 bytes at record_pos)
    //    - 若 in_block_offset + 12 + old_data_len != payload_size → 错误
    //
    // 4. 计算 delta = (new_data.len() + 12) - (old_data_len + 12) = new_data.len() - old_data_len
    //
    // 5. 修改 mmap 中 record 的 data_len (u32) 和 data 字节 (覆盖/扩展)
    //    record_pos = header_len + block_segment_offset + BLOCK_HEADER_SIZE + in_block_offset
    //
    // 6. 更新 block header:
    //    - payload_size       += delta  (block 内 payload 长度变化)
    //    - uncompressed_size  += delta  (block 内原始数据长度变化)
    //
    // 7. 更新段内计数字段:
    //    - self.wrote_position              += delta
    //    - self.total_uncompressed_size     += delta
    //    - if block is pending (pending_block_offset.matches):
    //        self.pending_wrote_position += delta
    //        更新 file header pending_wrote_position
    //    - 更新 file header wrote_position (update_file_wrote_position)
}
```

> **前置条件**:
> - 调用方已通过 `time_index.find_entry(timestamp)` 获取 `(block_offset, in_block_offset)`, 并在 `DataSegmentSet` 层转换为当前段的 `block_segment_offset`
> - 调用方已验证该 record 位于最新数据段
> - block.flags = 0 (pending raw)
> - record 是 block 内最末 record
>
> **不支持缩小的情况**: 若新 data 长度更小, 后续 block 需前移 (本段内只此 block 时无影响, 但通用场景复杂)。实现中允许缩小, 只需移动本 block 后的字节 (如果有) 并调整 wrote_position。


### 6.4 读取: 通过索引定位 Block 内 record (含缓存)

```rust
struct SegmentReadEntry {
    timestamp: i64,
    block_segment_offset: u64,
    in_block_offset: u16,
}

impl DataSegment {
    fn read_at_index(
        &self,
        entry: &SegmentReadEntry,
        cache: Option<&BlockCache>,
    ) -> io::Result<(i64, Vec<u8>)> {
        let m = self.mmap.as_ref().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "segment closed"))?;
        let hdr_pos = self.header_len as usize + entry.block_segment_offset as usize;
        let block_segment_offset = entry.block_segment_offset;

        // 读取 block header, 校验 SEALED/COMPRESSED 必须同时存在或同时不存在
        // 仅 sealed+compressed block 允许查询全局缓存
        // compressed 未命中 → 从 mmap 读取 + 解压 → 存入全局缓存
        // pending raw block → 直接从 mmap 复制 payload, 不进入全局缓存
        // 定位 record → 返回 (timestamp, data)
    }
}
```

> **安全性保证**: 只有 `SEALED|COMPRESSED` block 的解压 payload 才能进入全局 `BlockCache`。pending raw block 仍在写入中, 不能进入全局缓存。sealed raw block 为非法状态。compressed block 一旦写入后不允许原地修改。
>
> **Header 起点约束**: 进入 `DataSegment` 后的 `block_segment_offset` 表示相对数据区起点的偏移, 即 `block_offset - segment.file_offset`; 物理文件位置必须通过 `segment.header_len + block_segment_offset` 计算。新建 v1 文件的 `header_len` 为 116, 但打开文件时必须使用 header 中的 `meta_length/state_length` 动态计算。索引中的 `block_offset` 是数据区逻辑全局偏移, 不可直接作为文件内 seek 位置。

### 6.5 生命周期方法

```rust
impl DataSegment {
    /// 确保 mmap 有效 (closed → open + mmap + pending恢复)
    pub fn ensure_open(&mut self, compress_level: u8) -> Result<()>;

    /// sync → unmmap → close, 不 seal、不压缩、不清 pending
    pub fn idle_close(&mut self, compress_level: u8) -> Result<()>;

    /// 仅 msync (不 seal/不压缩)
    pub fn sync(&mut self) -> Result<()>;

    /// 创建新 segment (初始分配 initial_size)
    pub fn create(path: &Path, file_offset: u64, initial_size: u64, max_size: u64) -> Result<Self>;

    /// 打开已有 segment (以磁盘实际大小为准)
    pub fn open(path: &Path, file_offset: u64, max_file_size: u64) -> Result<Self>;

    /// 扩容: unmap → set_len(target) → remap → 更新内存字段
    /// header file_size 不变 (始终为 max)
    pub fn expand(&mut self) -> Result<()>;
}
```

### 6.6 扩容机制 (详见 [懒分配与扩容](lazy-allocation.md))

- 创建时: `file.set_len(initial_size)` — 仅分配初始大小
- Header `file_size`: 始终记录 `max_file_size`, 不随扩容更新
- 打开时: `fs::metadata(path)?.len()` — 以磁盘实际大小为准
- 扩容时: `current_size * 2` → 上限 `max_file_size`
- 扩容步骤: unmap → `file.set_len(target)` → remap → 更新内存字段

### 6.7 时间戳范围跟踪

**目的**: 数据段维护 `min_timestamp` 和 `max_timestamp`, 用于 DataSegmentSet 的段级过滤优化。

**更新时机**:
- `create()`: 初始化为 `min_timestamp = i64::MAX`, `max_timestamp = i64::MIN`
- `append_record()`: 每次写入时更新
  ```rust
  if timestamp < self.min_timestamp { self.min_timestamp = timestamp; }
  if timestamp > self.max_timestamp { self.max_timestamp = timestamp; }
  ```
- `open()`: 从 DataFileMetadata 读取已有值
- `ensure_open()`: 从 DataFileMetadata 恢复

**使用场景**:
- DataSegmentSet 路由查询时, 可跳过不在 [start_ts, end_ts] 范围内的段
- 避免不必要的段打开和遍历

**状态一致性**:
- 每次 `append_record` 后立即写入 mmap state 区域
- `idle_close()` 前 sync 确保落盘
- 崩溃恢复时从 file header 读取, 保证一致性

### 6.8 数据保留回收

**职责**: 删除超过数据有效期的数据段文件, 回收磁盘空间。

**回收规则 (DataSegmentSet)**:
```
reclaim_expired_segments(expiration_threshold: i64):
  前置条件: DataSet 已 close (), 所有 segment 均处于 closed 状态
  for seg in closed_segments:
    if seg.max_timestamp < expiration_threshold:
      fs::remove_file(seg.path)
      closed_segments.remove(seg)
```

**判断依据**:
- 使用段文件 header 中的 `max_timestamp` (已在 `DataSegmentMeta` 中缓存)
- 无需打开文件, closed_segments 在 idle_close_all (DataSet::close) 时已读取 header 中的 min/max_timestamp
- 过期判断: `max_timestamp < expiration_threshold` (整个段的最大时间戳早于过期阈值)
- 如果一个 data segment 同时包含过期和未过期 timestamp, 该混合分段必须保留, 不做部分裁剪
- 数据段回收不检查其数据是否仍被 index entry 引用; 可见性由 `DataSet` 的 retention 读写约束保证

**触发时机**:
- 由 `DataSet::reclaim_expired_segments()` 调用 (见 [数据集操作 §6.8](dataset-operations.md))
- 后台线程按 `retention_check_hour` 指定的 UTC hour 每日执行一次

`invalid_record_count` 只记录乱序写入、纠正写入回退和 delete 造成的无效 record 数量。当前版本不基于该字段执行 compaction, 也不回收段内局部空间。

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [懒分配与扩容](lazy-allocation.md)
