# 数据段管理 - DataSegmentSet + DataSegment

## 五、DataSegmentSet: 数据段集合

### 5.1 职责

- 管理同一数据集下的多个 DataSegment 文件
- 按 offset 路由到正确的数据段
- 自动创建新文件 (当前文件满或 sealed 时)
- 数据读取时跨段迭代

### 5.2 结构

```rust
struct DataSegmentSet {
    base_dir: PathBuf,
    segment_size: u64,
    initial_segment_size: u64,    // 初始分配大小
    block_max_size: u32,
    compress_level: u8,
    segments: Vec<DataSegment>,           // 打开中的 data segment
    closed_segments: Vec<DataSegmentMeta>, // 已关闭的 data segment
    next_offset: u64,
    last_used_at: Instant,
}

struct DataSegmentMeta {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
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
                         block_max_size: u32, compress_level: u8) -> Result<Self>;
}
```

> `DataSet::sync_all()` 需要同时调用 `segments.sync_all()` + `time_index.sync_all()`。
> `DataSet::idle_close_all()` 同理。

## 六、DataSegment: 单个数据段 (Block 管理核心)

### 6.1 结构

```rust
struct DataSegment {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,              // 运行时当前文件大小 (随扩容增长)
    max_file_size: u64,          // 扩容上限 (segment_size, 不可变)
    wrote_position: u64,         // 从 data_start(100) 起算
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    mmap: Option<MmapMut>,       // None = closed/unmapped
    lifecycle: SegmentLifecycle,
    last_accessed_at: Instant,
    // Pending Block 状态
    pending_block_offset: Option<u64>,
    pending_wrote_position: u64,
    pending_record_count: u64,
}

enum SegmentLifecycle {
    Closed,          // 文件未打开, mmap=None
    OpenReady,       // 打开中, mmap 有效, 可读写
}

const BLOCK_HEADER_SIZE: u64 = 16;
```

### 6.2 文件布局

```
┌──────────────────────────────────────────────────┐
│ FileHeader (100 bytes)                           │
│ - 固定前缀: magic(4)+version(2)+fileType(1)+     │
│   meta_length(2)                                 │
│ - Meta(TLV, 33B): created_at, file_offset,       │
│   file_size, compress_level                      │
│ - state_length: 2                                │
│ - State(56B): 7×8B wrote_position..pending_count │
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
        block_max_size: u32,
        compress_level: u8,
    ) -> io::Result<(u64, u16)> {
        let record_size = 2 + 8 + data.len();

        // 情况1: 单条 record 超过 block_max_size → 独占 Block
        if record_size > block_max_size as usize {
            if let Some(off) = self.pending_block_offset {
                self.seal_pending_block(off, compress_level)?;
                self.clear_pending();
            }
            return self.create_single_record_block(timestamp, data, compress_level);
        }

        // 情况2: 有 pending block
        if let Some(pending_off) = self.pending_block_offset {
            let new_total = self.pending_block_uncomp_size + record_size as u32;

            if new_total > block_max_size {
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
    fn seal_pending_block(&mut self, block_rel_offset: u64, compress_level: u8) -> io::Result<()>;
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;
    fn create_pending_and_append(&mut self, timestamp: i64, data: &[u8]) -> io::Result<(u64, u16)>;
    fn create_single_record_block(&mut self, timestamp: i64, data: &[u8], compress_level: u8) -> io::Result<(u64, u16)>;
}
```

### 6.4 读取: 通过索引定位 Block 内 record (含缓存)

```rust
impl DataSegment {
    fn read_at_index(
        &self,
        entry: &IndexEntry,
        cache: Option<&BlockCache>,
    ) -> io::Result<(i64, Vec<u8>)> {
        let m = self.mmap.as_ref().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "segment closed"))?;
        let hdr_pos = HEADER_SIZE as usize + entry.block_offset as usize;
        let block_offset = entry.block_offset;

        // 读取 block header, 检查 compressed flag
        // 缓存检查 → 命中则跳过读取+解压
        // 未命中 → 从 mmap 读取 + 解压 → 存入缓存
        // 定位 record → 返回 (timestamp, data)
    }
}
```

> **安全性保证**: 只有已 seal 的 block 才能进入缓存。pending block 数据仍在写入中, 不会被缓存。

### 6.5 生命周期方法

```rust
impl DataSegment {
    /// 确保 mmap 有效 (closed → open + mmap + pending恢复)
    pub fn ensure_open(&mut self, compress_level: u8) -> Result<()>;

    /// sync → unmmap → close
    pub fn idle_close(&mut self, compress_level: u8) -> Result<()>;

    /// 仅 msync (不 seal/不压缩)
    pub fn sync(&mut self) -> Result<()>;

    /// 密封 pending 但不压缩 (用于 idle-close 和 reopen recovery)
    fn seal_pending_block_no_compress(&mut self, _compress_level: u8) -> Result<()>;

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

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [懒分配与扩容](lazy-allocation.md)
