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
    compress_type: u8,
    segments: BTreeMap<u64, DataSegmentEntry>, // key = data segment file_offset
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
                         compress_level: u8, compress_type: u8) -> Result<Self>;
}
```

`compress_type` 是算法选择真源。append/seal/create single-record 路径使用当前 segment header / `DataSegmentSet.compress_type` 选择算法, `compress_level` 只表示该算法的级别。

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
    data_wrote_position: u64,    // 运行时数据区内已用字节数; header state wrote_position 保存文件内绝对偏移
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    mmap: Option<MmapMut>,       // None = closed/unmapped
    lifecycle: SegmentLifecycle,
    last_accessed_at: Instant,
    is_flushed: bool,            // 内存态: 当前 mmap 内容是否已 MS_SYNC
    queued_for_flush: bool,      // 内存态: dirty 后是否已进入 runtime flush 队列
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
const DATA_HEADER_SIZE: u64 = 124;  // v1 默认数据段 header_len
```

`DataSegmentSet` 接收索引中的 `block_offset`, 通过 `segment_size` 计算所属数据段:

```text
segment.file_offset = (block_offset / segment_size) * segment_size
block_segment_offset = block_offset - segment.file_offset
physical_file_offset = segment.header_len + block_segment_offset
```

`DataSegment` 内部读写使用数据区相对坐标: `block_segment_offset` 与运行时 `data_wrote_position` 均不包含 header。文件 header state 中的 `wrote_position` 持久化为文件内绝对偏移, 即 `header_len + data_wrote_position`; 打开文件时必须减去动态 `header_len` 恢复运行时相对坐标。任何 mmap/seek 位置都必须再加 `header_len`。

### 6.2 文件布局

```
┌──────────────────────────────────────────────────┐
│ DataFileHeader (variable, v1 default 124 bytes)  │
│ - 固定前缀: magic(4)+version(2)+fileType(1)+     │
│   meta_length(2)                                 │
│ - Meta(TLV, 41B): created_at, file_offset,       │
│   file_size, compress_level, compress_type       │
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

    /// 密封 pending block: 使用 selected algorithm 压缩+写回
    fn seal_pending_block(&mut self, block_segment_offset: u64, compress_level: u8) -> io::Result<()>;
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;
    fn create_pending_and_append(&mut self, timestamp: i64, data: &[u8]) -> io::Result<(u64, u16)>;
    fn create_single_record_block(&mut self, timestamp: i64, data: &[u8], compress_level: u8) -> io::Result<(u64, u16)>;

    /// 纠正写入: 在该段最后一个 pending raw block 的最末 record 位置原地覆盖 data 字节, 支持 tail-only 变 size
    /// 若最后一个 block 含 SEALED 或 COMPRESSED flag → 返回错误 (不可原地修改)
    /// 若该 record 不是 block 最末 record → 返回错误
    /// 修改后需更新: block.payload_size/uncompressed_size + 段 wrote_position/total_uncompressed_size/pending_wrote_position(仅 pending)
    fn overwrite_in_last_block(
        &mut self,
        block_segment_offset: u64,
        in_block_offset: u16,
        new_data: &[u8],
    ) -> io::Result<()>;

    /// append 写入: 只允许向本段最后一个 pending raw block 的最末 record 追加 bytes
    /// 返回 append 前的 old_data_len, 供 journal 记录 data_offset
    fn append_to_last_record(
        &mut self,
        block_segment_offset: u64,
        in_block_offset: u16,
        append_data: &[u8],
    ) -> io::Result<u32>;
}
```

**append 发布顺序约束**:

`DataSegment::append_record` 返回的 `(block_segment_offset, in_block_offset)` 会被 `DataSet` 与当前 `segment.file_offset` 合成为 `block_offset`, 再写入 `TimeIndex`。因此 data segment 侧必须先完成 record payload 和 block/header state 更新, 再让上层发布 index entry。对于新 pending block、追加 pending block、exclusive/single-record block 三类路径, 设计上的可见性顺序都是:

1. 写入 record payload。
2. 写入或更新 `BlockHeader` 与 data segment state。
3. 返回 offset 给 `DataSet`, 由 `DataSet` 最后写 index。

如果 crash 发生在第 3 步之前, 已写入 data segment 但未被 index 引用的数据视为丢失; 查询不会扫描 data segment 来发现它。如果 crash 发生在 index 发布之后但 data/index 落盘顺序不完整, 读取路径必须通过 block 边界和 record timestamp 校验避免返回错位数据。

#### overwrite_in_last_block: 纠正写入 (In-Place Overwrite, 支持变 size)

纠正写入场景下 (`timestamp == latest_written_timestamp`), 该最大已写 timestamp 对应的记录只有在仍位于 **本数据段最后一个 pending raw block (`flags=0`)** 的 **最末位置** 时, 才可通过 mmap 直接修改该 record 的 data 字节, 支持 data 长度变化。该能力是 tail-only resize: 仅改写当前 record header/data 和尾部计数, 不移动任何后续 block/record 字节; 如果校验发现 record 后仍有字节, 直接返回错误并由 `DataSet::correct_write` 回退为乱序追加并更新索引。只要 block 已经 sealed/compressed, 也不能再原地修改:

```rust
fn overwrite_in_last_block(
    &mut self,
    block_segment_offset: u64,
    in_block_offset: u16,
    new_data: &[u8],
) -> io::Result<()> {
    // 1. 验证这是段内最后一个 block:
    //    block_abs_end = block_segment_offset + BLOCK_HEADER_SIZE + block.payload_size
    //    若 block_abs_end < self.data_wrote_position → 不是最后 block, 返回错误
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
    //    - self.data_wrote_position         += delta
    //    - self.total_uncompressed_size     += delta
    //    - if block is pending (pending_block_offset.matches):
    //        self.pending_wrote_position += delta
    //        更新 file header pending_wrote_position
    //    - 更新 file header wrote_position = header_len + data_wrote_position
}
```

> **前置条件**:
> - 调用方已通过 `time_index.find_entry(timestamp)` 获取 `(block_offset, in_block_offset)`, 并在 `DataSegmentSet` 层转换为当前段的 `block_segment_offset`
> - 调用方已验证该 record 位于最新数据段
> - block.flags = 0 (pending raw)
> - record 是 block 内最末 record
>
> **不支持 byte-shift**: correction 变长覆盖只允许 tail-only resize。新 data 可以变大或变小, 但 record 必须已经是最新 pending raw block 的最后一条记录, 且 record 末尾必须等于当前 block/data 尾部。实现不得移动任何后续 block/record 字节; 只更新该 record 的 `data_len`、payload bytes、block 计数和 segment 计数。若校验发现 record 后仍有字节、block 已 sealed/compressed 或目标不是最新尾部记录, 返回错误并由 `DataSet::correct_write` fallback 为 append 新 record + 更新 index。

#### append_to_last_record: 追加写入 (Tail Append)

append 追加场景下 (`timestamp == latest_written_timestamp`), 目标 record 必须位于 **本数据段最后一个 pending raw block (`flags=0`)** 的 **最末位置**, 且 record 末尾必须等于数据段当前运行时 `data_wrote_position`。该方法只负责原地增长; `DataSet` 层必须在调用前完成 4MiB 上限判断。append 不再因为比例阈值迁移为 exclusive/single-record block, 若增长后普通 pending block 无法承载则直接返回错误。

```rust
fn append_to_last_record(
    &mut self,
    block_segment_offset: u64,
    in_block_offset: u16,
    append_data: &[u8],
) -> io::Result<u32> {
    // 1. 验证 block 是段内最后一个 block:
    //    block_abs_end = block_segment_offset + BLOCK_HEADER_SIZE + block.payload_size
    //    block_abs_end 必须等于 self.data_wrote_position
    //
    // 2. 读取 block header:
    //    - flags 必须等于 0; compressed/sealed 均返回错误
    //    - payload_size 必须等于 uncompressed_size
    //
    // 3. 验证 record 是块内最末 record:
    //    - old_data_len = read_u32(record_pos)
    //    - in_block_offset + 12 + old_data_len 必须等于 payload_size
    //
    // 4. final_data_len = old_data_len + append_data.len()
    //    - final_data_len 必须 <= 4MiB
    //    - 12 + final_data_len 必须 <= BLOCK_MAX_SIZE
    //
    // 5. 在 mmap 中更新 record.data_len 并把 append_data 复制到 old data 后方
    //
    // 6. 更新 block header:
    //    - payload_size      += append_data.len()
    //    - uncompressed_size += append_data.len()
    //
    // 7. 更新段 state:
    //    - self.data_wrote_position     += append_data.len()
    //    - self.total_uncompressed_size += append_data.len()
    //    - self.pending_wrote_position  += append_data.len()
    //    - record_count/min_timestamp/max_timestamp 不变
    //
    // 8. 返回 old_data_len, 作为 journal 0x13 append_info.data_offset
}
```

与 `overwrite_in_last_block` 的差异:

1. append 只增长 record data, 不支持缩小或替换已有 data。
2. append 失败不回退为乱序写入; compressed block、非末尾 record、历史段都直接返回错误。
3. append 修改已存在 latest record 时只允许原地追加; 不触发独占 block 迁移。
4. 原地 append 不改变索引, 因为 record 起始位置不变。


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
> **Header 起点约束**: 进入 `DataSegment` 后的 `block_segment_offset` 表示相对数据区起点的偏移, 即 `block_offset - segment.file_offset`; 运行时 `data_wrote_position` 也是相对数据区起点的已用字节数。物理文件位置必须通过 `segment.header_len + block_segment_offset` 计算。新建 v1 文件的 `header_len` 为 124, 但打开文件时必须使用 header 中的 `meta_length/state_length` 动态计算。索引中的 `block_offset` 是数据区逻辑全局偏移, 不可直接作为文件内 seek 位置。文件 header state 的 `wrote_position` 唯一保存形式是文件内绝对偏移: `header_len + data_wrote_position`。

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

### 6.5.1 DataSegmentSet 分段注册表与定位

`DataSegmentSet` 使用单个 `BTreeMap<u64, DataSegmentEntry>` 保存所有数据分段, key 为 `file_offset`:

- `DataSegmentEntry::Open(DataSegment)`: 当前 mmap/open 的分段。
- `DataSegmentEntry::Closed(DataSegmentMeta)`: 已 idle-close 或 load-existing 后尚未打开的分段元数据。

该注册表天然按 `file_offset` 升序维护。任何生命周期迁移都只改变同一 key 下的 entry 状态, 不在 open/closed 两个列表之间搬移:

- load-existing 扫描磁盘后插入 `Closed` entry。
- lazy-open: 通过 `file_offset` 在 `BTreeMap` 中 O(log n) 命中, 若为 `Closed` 则打开文件并替换为 `Open`。
- idle-close: 遍历 registry, 将 `Open` sync+unmap 后替换为 `Closed`。
- 新建 segment: 以新 `file_offset` 插入 `Open` entry。

最高 `file_offset` 的 data segment 是 inspect 统计中的 active tail segment。active tail 的判定只看 `file_offset`，不看 `Open` / `Closed` 生命周期状态；即使它已被 idle-close, 仍然是当前可能继续追加写入的尾段。创建下一 data segment 时, 旧 active tail 才进入 dataset state 文件的归档统计。

`DataSegmentMeta` 只承担 lazy-open、路由和段级过滤所需的元数据缓存，不扩展为保存整个 dataset inspect 汇总。归档分段的 `record_count`、`data_wrote_position`、`uncompressed_size`、`invalid_record_count` 汇总由 `{dataset_dir}/state` 文件保存；普通 inspect 不需要打开所有历史 data segment。

定位规则:

```text
segment_file_offset = (block_offset / data_segment_size) * data_segment_size
```

读路径、cache invalidation、`invalid_record_count` 更新、flush target 定位都应使用该精确 key 直接从 `BTreeMap` 定位。不得在大分段数量下线性扫描全部 segment。

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
- 每次 mmap 写入后仅更新内存 dirty 状态: `is_flushed=false`
- 当 dirty 状态首次从 flushed 变为 unflushed 时, 通过 `DataSetRuntimeContext` 引用的 Store 级共享 `flush_queue` 记录 `{ dataset_key, Data { file_offset } }`
- 后台 flush drain 队列后只同步 dirty segment, 成功后 `is_flushed=true`, `queued_for_flush=false`
- `idle_close()` 前 sync 确保落盘, 并清除该 segment 的 queued 标记; flush 队列中后续遇到该 stale target 时跳过
- 创建新 data segment 前, 对前一个已经完结的 data segment 直接执行 sync, 不等待后台 flush 间隔
- 崩溃恢复时从 file header 读取, 保证一致性

### 6.8 数据保留回收

**职责**: 删除超过数据有效期的数据段文件, 回收磁盘空间。

**回收规则 (DataSegmentSet)**:
```
reclaim_expired_segments(expiration_threshold: i64):
  前置条件: DataSet 已 close (), 所有 segment 均处于 closed 状态
  for (file_offset, entry) in segments:
    if entry is Closed(meta) && meta.max_timestamp < expiration_threshold:
      fs::remove_file(meta.path)
      segments.remove(file_offset)
```

**判断依据**:
- 使用段文件 header 中的 `max_timestamp` (已在 `DataSegmentMeta` 中缓存)
- 无需打开文件, `segments` 中的 `Closed(DataSegmentMeta)` entry 在 idle-close/load-existing 时已读取 header 中的 min/max_timestamp
- 过期判断: `max_timestamp < expiration_threshold` (整个段的最大时间戳早于过期阈值)
- 如果一个 data segment 同时包含过期和未过期 timestamp, 该混合分段必须保留, 不做部分裁剪
- 数据段回收不检查其数据是否仍被 index entry 引用; 可见性由 `DataSet` 的 retention 读写约束保证

**触发时机**:
- 由 `DataSet::reclaim_expired_segments()` 调用 (见 [数据集操作 §6.8](dataset-operations.md))
- 后台线程按 `retention_check_hour` 指定的 UTC hour 每日执行一次

`invalid_record_count` 只记录乱序写入、纠正写入回退和 delete 造成的无效 record 数量。当前版本不基于该字段执行 compaction, 也不回收段内局部空间。

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [懒分配与扩容](lazy-allocation.md)

