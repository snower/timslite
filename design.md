# timslite - Rust 时序数据存储库详细设计

> 参考: monitorcare-orbit TimeStore (Java)
> 目标: Rust 动态库(`cdylib`), 提供 FFI 可调用 C ABI
> 核心: 按数据集名称+类型分段 + 内存映射(mmap) + 时间索引 + Block 延迟压缩 + 懒加载生命周期

---

## 一、整体架构

```
libtimslite (CDylib)
│
├── Store              (门面 - data_dir 级别)
│   │
│   └── DataSet        (数据集 - (name, type) 级别)
│       │
│       ├───DataSegment       (单个数据文件, Mmap-backed, 含多个 Block)
│       ├───DataSegmentSet    (同类型数据文件集合)
│       │
│       └───TimeIndex         (当前数据集的专属时间索引)
│           │
│           └───IndexSegment  (单个索引文件, Mmap-backed)
└── FFI                (extern "C" API)
```

**核心设计思想**: 多条 record 聚合成 Block → Block 级压缩 → 时间索引指向 (block_offset, in_block_offset)

---

## 二、目录结构

```
{data_dir}/
├── {dataset_name_1}/
│   ├── {dataset_type_A}/
│   │   ├── .index/
│   │   │   ├── 00000000000000000000              # 起始秒级时间戳 (20位,0填充)
│   │   │   └── 0000000000001700000000
│   │   ├── 00000000000000000000                  # data segment, 起始offset (20位,0填充)
│   │   ├── 00000000000067108864                  # offset = 64MB
│   │   └── 000000000000134217728
│   │
│   └── {dataset_type_B}/
│       ├── .index/
│       │   └── 0000000000001700000000
│       └── 00000000000000000000
│
└── {dataset_name_2}/
    └── {dataset_type_C}/
        ├── .index/
        └── 00000000000000000000
```

### 2.1 命名规则

| 文件类型 | 目录 | 命名格式 | 示例 |
|---------|------|---------|------|
| 数据段(DataSegment) | `{name}/{type}/` | 20位十进制, 起始字节offset, 零填充 | `00000000000000000000` |
| 索引段(IndexSegment) | `{name}/{type}/.index/` | 20位十进制, 起始秒级timestamp, 零填充 | `0000000000001700000000` |

### 2.2 隔离保证

- 每个 `(dataset_name, dataset_type)` 拥有完全独立的 `.index/` 目录
- 索引文件只包含对应 `(name, type)` 的时间戳→偏移量映射
- 不同数据集名称之间文件物理隔离
- 同一名称不同类型之间文件物理隔离

---

## 三、核心数据模型

### 3.1 Record (逻辑数据记录)

每条数据由调用方提供:

```
┌─────────────────┬──────────────────────────────┐
│ timestamp       │ data                         │
│ i64 (8 bytes)   │ bytes (可变长度)              │
└─────────────────┴──────────────────────────────┘
```

### 3.2 Block (物理存储单元)

多条 record 聚合为一个 Block, 压缩仅针对单个 Block。

**Block 大小限制**: 最大 64KB (65536 字节)。如果单条 record 的原始数据就超过 64KB, 则该 record **独占一个 Block**, Block 实际大小可超过 64KB。

### 3.3 Block Layout (磁盘上的 Block 结构)

```
┌──────────────────────────────────────────────────────────┐
│ BlockHeader (16 bytes)                                   │
├──────────────────────────────────────────────────────────┤
│ Block Payload (compressed 或 uncompressed)               │
│ ┌──────────────────────────┬───────────────────────────┐  │
│ │ data_len:2 + ts:8 + data │ data_len:2 + ts:8 + data  │  │
│ │ (record 1)               │ (record 2)                │  │
│ └──────────────────────────┴───────────────────────────┘  │
│ ...                                                       │
└──────────────────────────────────────────────────────────┘
```

#### BlockHeader (16 bytes)

```
Offset  Size  Field                    Description
0-3     u32   block_payload_size       Block payload 总字节数 (不含 header 自身的 16 字节)
4-5     u16   flags
                  bit 0: 1=compressed, 0=uncompressed
                  bit 1: 1=sealed (不再写入)
                  bit 2: 1=single_record (独占 record 的超大 block)
6-7     u16   record_count             Block 内 record 数量
8-11    u32   uncompressed_size        Block 内所有 record 的原始数据总大小 (含 data_len+timestamp)
12-15   u32   reserved                 保留
```

#### Block Payload 内部结构 (Record 编码)

每条 record 在 Block Payload 中的存储:

```
┌──────────┬─────────────────┬──────────────────────────────┐
│ data_len │ timestamp       │ data                         │
│ u16      │ i64 (8 bytes)   │ bytes (data_len 字节)        │
│ 2 bytes  │                 │                              │
└──────────┴─────────────────┴──────────────────────────────┘
```

- `data_len`: 纯数据长度 (不含 data_len 的 2 字节和 timestamp 的 8 字节)
- 记录之间紧密排列, 无额外分隔符
- 遍历方式: offset += 2 + 8 + data_len

### 3.4 IndexEntry (索引条目)

每个索引条目固定 **18字节**:

```
┌──────────────────────┬──────────────────────┬──────────────┐
│ timestamp (i64)      │ block_offset (u64)   │ in_block     │
│ 8 bytes              │ 8 bytes              │ offset (u16) │
└──────────────────────┴──────────────────────┴──────────────┘
```

- `timestamp`: 秒级时间戳
- `block_offset`: 对应 Block 在数据段中的**绝对字节偏移** (指向 BlockHeader 起始)
- `in_block_offset`: record 在 Block Payload 中的**相对偏移** (从 payload 起始算, 指向该 record 的 data_len 字段)

### 3.5 FileMetadata (文件头, 128 字节)

每个数据段和索引段的头部元数据。

#### 设计原则: 固定核心 + 可扩展元数据区

```
┌─────────────────────────────────────────────┐
│ 固定核心 (10 bytes)                          │  ← 所有版本都能解析
│  magic:4 + version:2 + file_flags:2          │
├─────────────────────────────────────────────┤
│ 元数据扩展长度 (2 bytes)                     │  ← 告知后续元数据总长度
├─────────────────────────────────────────────┤
│ 扩展元数据区 (当前 52 bytes)                  │  ← 可随意增删字段
│  file_type, file_offset, file_size, ...      │
├─────────────────────────────────────────────┤
│ 预留区 (64 bytes)                            │  ← 未来扩展用
└─────────────────────────────────────────────┘
```

#### 详细偏移

```
Offset  Size  Field                    Description
────────────────────────────────────────────────── 固定核心 (v1+)
0-3     4     magic = b"TMSL"
4-5     u16   version                  = 1
6-7     u16   file_flags
                bit 0: 文件已 sealed
                bit 1: 有 pending block
                bit 2-15: 保留
────────────────────────────────────────────────── 元数据扩展长度
8-9     u16   meta_data_len            其后扩展元数据字节数 (当前=52)
                                     HEADER_SIZE = 10 + 2 + meta_data_len + reserved
                                     读者通过此值跳过未知元数据字段
────────────────────────────────────────────────── 扩展元数据区 (v1: 52 bytes)
10-17   i64   file_type
                >0 = data segment, <0 = index segment
18-25   i64   file_offset
                data segment: 起始字节offset
                index segment: 起始秒级timestamp
26-33   i64   file_size                文件总大小(字节)
34-41   i64   created_at               创建时间(unix ms)
42-49   i64   wrote_position           已写入位置(从数据区起始) ← moved after created_at
50-57   i64   record_count             文件内总记录条数
58-63   u64   total_uncompressed_size  文件内所有 record 原始数据总大小
────────────────────────────────────────────────── 预留区 (64 bytes, 未来扩展)
64-71   i64   pending_block_offset     当前未完成 Block 相对偏移 (-1表示无)
72-75   u32   pending_uncomp_size      pending block 内原始数据累计大小
76-77   u16   pending_record_count     pending block 内 record 数量
78-127  50    reserved                 保留 (50 bytes)

HEADER_SIZE = 128 bytes
```

#### 兼容性设计

| 场景 | 行为 |
|------|------|
| v1 reader 读 v1 文件 | 正常读取, `meta_data_len=52` |
| v2 reader 读 v1 文件 | 读 `meta_data_len=52`, 只解析已知字段, 跳过未知 |
| v1 reader 读 v2 文件 | 读固定核心 (10B) + `meta_data_len` 值, 跳过扩展元数据到预留区解析 |
| 未来添加新字段 | 增加 `meta_data_len` 值, 在预留区写入, 旧版本安全跳过 |

> **数据区起始位置 = `HEADER_SIZE = 128` 字节**

---

## 四、核心类型定义

```rust
/// 存储实例句柄 (线程安全)
pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,
    flush_handle: Option<JoinHandle<()>>,
    idle_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

/// 数据集唯一标识
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct DataSetKey {
    name: String,
    dataset_type: String,
}

/// FFI 数据集句柄 (不透明指针, 内部为 Arc ID)
pub struct DataSetHandle(pub(crate) u64);

/// 数据集句柄
struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,
    config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
}

/// 存储全局配置 (Store 级别, 所有 DataSet 共享)
pub struct StoreConfig {
    pub flush_interval: Duration,    // 默认 10 分钟 (mmap sync, 不密封/不压缩)
    pub idle_timeout: Duration,      // 默认 30 分钟 (sync + 密封 pending + unmmap + close)
    pub data_segment_size: u64,      // 默认 64MB
    pub index_segment_size: u64,     // 默认 4MB
    pub block_max_size: u32,         // 默认 65536 (64KB)
    pub compress_level: u8,          // 默认 6
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(600),    // 10 分钟
            idle_timeout: Duration::from_secs(1800),     // 30 分钟
            data_segment_size: 64 * 1024 * 1024,         // 64MB
            index_segment_size: 4 * 1024 * 1024,         // 4MB
            block_max_size: 65536,                       // 64KB
            compress_level: 6,
        }
    }
}

impl StoreConfig {
    /// Builder 模式
    pub fn builder() -> StoreConfigBuilder { StoreConfigBuilder::default() }
}

/// 数据集内部配置 (从 StoreConfig 派生)
struct DataSetConfig {
    data_segment_size: u64,
    index_segment_size: u64,
    block_max_size: u32,
    compress_level: u8,
}

impl From<&StoreConfig> for DataSetConfig {
    fn from(config: &StoreConfig) -> Self {
        Self {
            data_segment_size: config.data_segment_size,
            index_segment_size: config.index_segment_size,
            block_max_size: config.block_max_size,
            compress_level: config.compress_level,
        }
    }
}

/// Block 头
struct BlockHeader {
    block_payload_size: u32,     // payload 字节数 (不含 16 字节 header)
    flags: u16,
    record_count: u16,
    uncompressed_size: u32,      // 所有 record 原始数据总大小
    _reserved: u32,
}

const BLOCK_FLAG_COMPRESSED: u16     = 0x0001;
const BLOCK_FLAG_SEALED: u16         = 0x0002;
const BLOCK_FLAG_SINGLE_RECORD: u16  = 0x0004;

const FILE_FLAG_SEALED: u16          = 0x0001;
const FILE_FLAG_HAS_PENDING: u16     = 0x0002;

/// 文件元数据头 (Header)
///
/// 布局: 固定核心(10B) + meta_data_len(2B) + 扩展元数据(52B) + 预留(64B) = 128B
struct FileMetadata {
    // === 固定核心 (所有版本必须可读, 10 bytes) ===
    magic: [u8; 4],                  // b"TMSL"
    version: u16,                    // = 1
    file_flags: u16,
    // === 扩展信息 ===
    meta_data_len: u16,              // = 52 (v1)
                                     // 其后扩展元数据的总字节数
    file_type: i64,                  // >0=data, <0=index
    file_offset: i64,                // 数据段:字节offset / 索引段:秒级timestamp
    file_size: i64,                  // 文件总大小
    created_at: i64,                 // 创建时间(unix ms)
    wrote_position: i64,             // 已写入位置(从 HEADER_SIZE 起算) ← after created_at
    record_count: i64,               // 总记录数
    total_uncompressed_size: u64,    // 所有 record 原始数据总大小
    pending_block_offset: i64,       // 未完成 Block 相对偏移 (-1=无)
    pending_uncomp_size: u32,        // pending block 未压缩大小
    pending_record_count: u16,       // pending block record 数量
    _reserved: [u8; 50],            // 预留 (50 bytes)
}

const HEADER_SIZE: u64 = 128;

/// 索引条目
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    timestamp: i64,           // 秒级时间戳
    block_offset: u64,        // Block 在数据段中的绝对偏移 (相对 HEADER_SIZE)
    in_block_offset: u16,     // record 在 Block Payload 中的相对偏移
}
```

---

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
    block_max_size: u32,
    compress_level: u8,
    segments: Vec<DataSegment>,           // 打开中的 data segment
    closed_segments: Vec<DataSegmentMeta>, // 已关闭的 data segment (path, offset, size)
    next_offset: u64,
    last_used_at: Instant,                // 最近操作时间
}

struct DataSegmentMeta {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
}
```

### 5.3 生命周期管理 (仅 Data Segment)

```rust
impl DataSegmentSet {
    /// sync 所有打开的 data segment
    pub fn sync_all(&mut self) -> Result<()> {
        for seg in &mut self.segments {
            seg.sync()?;
        }
        Ok(())
    }

    /// idle-close 所有 data segment
    pub fn idle_close_all(&mut self) -> Result<()> {
        let mut closed = Vec::new();
        for seg in self.segments.drain(..) {
            let path = seg.path.clone();
            let file_offset = seg.file_offset;
            let file_size = seg.file_size;
            seg.idle_close()?;
            closed.push(DataSegmentMeta { path, file_offset, file_size });
        }
        self.closed_segments.extend(closed);
        Ok(())
    }

    /// 按需打开已关闭的 segment
    pub fn lazy_open(&mut self, file_offset: u64) -> Result<&mut DataSegment> {
        // 1. 先在打开中的 segments 查找
        if let Some(idx) = self.segments.iter().position(|s| s.file_offset == file_offset) {
            return Ok(&mut self.segments[idx]);
        }
        // 2. 在 closed_segments 查找
        let meta = self.closed_segments.iter()
            .find(|m| m.file_offset == file_offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no segment at offset"))?;
        // 3. 打开 + mmap + pending 恢复
        let seg = DataSegment::open(&meta.path, meta.file_offset, meta.file_size)?;
        self.segments.push(seg);
        let idx = self.segments.len() - 1;
        Ok(&mut self.segments[idx])
    }

    /// 加载已有的 data segment 元数据 (Store open 时)
    pub fn load_existing(base_dir: &Path, segment_size: u64,
                         block_max_size: u32, compress_level: u8) -> Result<Self> {
        let mut metas: Vec<DataSegmentMeta> = Vec::new();
        for entry in std::fs::read_dir(base_dir)? {
            let p = entry?.path();
            if p.is_dir() || p.file_name().map(|n| n == ".index").unwrap_or(false) {
                continue;
            }
            let offset = u64::from_str_radix(p.file_stem().and_then(|n| n.to_str()).unwrap_or("0"), 10)?;
            let file_size = std::fs::metadata(&p)?.len();
            metas.push(DataSegmentMeta { path: p, file_offset: offset, file_size });
        }
        metas.sort_by_key(|m| m.file_offset);
        let next_offset = metas.last().map(|m| m.file_offset + segment_size).unwrap_or(0);
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size, block_max_size, compress_level,
            segments: Vec::new(),
            closed_segments: metas,
            next_offset,
            last_used_at: Instant::now(),
        })
    }
}
```

> **注意**: DataSegmentSet 只管理 **data segment**。Index segment 由 TimeIndex 管理（见第七节）。
> `DataSet::sync_all()` 需要同时调用 `segments.sync_all()` + `time_index.sync_all()`。
> `DataSet::idle_close_all()` 同理。

---

## 六、DataSegment: 单个数据段 (Block 管理核心)

### 6.1 结构

```rust
struct DataSegment {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
    wrote_position: u64,            // 从 data_start(128) 起算
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    mmap: Option<MmapMut>,           // None = closed/unmapped
    sealed: bool,
    lifecycle: SegmentLifecycle,     // Closed / OpenReady
    last_accessed_at: Instant,       // 最近读写时间
    // Pending Block 状态
    pending_block_offset: Option<u64>,
    pending_block_uncomp_size: u32,
    pending_block_record_count: u16,
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
│ FileHeader (128 bytes)                           │
│ - "TMSL", version, flags, meta_data_len          │
│ - file_type, file_offset, file_size, created_at  │
│ - wrote_position, record_count, uncompressed,    │
│   pending_block_offset, pending_uncomp/counts,   │
│   reserved (50 bytes)                            │
├──────────────────────────────────────────────────┤
│ Block 1 (sealed, compressed)                     │
│   BlockHeader (16 bytes)                         │
│   Payload (compressed records)                   │
├──────────────────────────────────────────────────┤
│ Block 2 (sealed, compressed)                     │
│   BlockHeader (16 bytes)                         │
│   Payload (compressed records)                   │
├──────────────────────────────────────────────────┤
│ Current Pending Block (未完成, 未压缩)             │
│   BlockHeader (16 bytes, flags=0)               │
│   Payload (raw records)                          │
└──────────────────────────────────────────────────┘
```

### 6.3 写入核心逻辑

```rust
impl DataSegment {
    /// 写入一条记录
    fn append_record(
        &mut self,
        timestamp: i64,
        data: &[u8],
        block_max_size: u32,
        compress_level: u8,
    ) -> io::Result<(u64, u16)> {
        let record_size = 2 + 8 + data.len();  // data_len(2) + timestamp(8) + data

        // --- 情况1: 单条 record 超过 block_max_size → 独占 Block ---
        if record_size > block_max_size as usize {
            // 先密封当前 pending block
            if let Some(off) = self.pending_block_offset {
                self.seal_pending_block(off, compress_level)?;
                self.clear_pending();
            }
            return self.create_single_record_block(timestamp, data, compress_level);
        }

        // --- 情况2: 有 pending block ---
        if let Some(pending_off) = self.pending_block_offset {
            let new_total = self.pending_block_uncomp_size + record_size as u32;

            if new_total > block_max_size {
                // pending block 满了 → 密封+压缩
                self.seal_pending_block(pending_off, compress_level)?;
                self.clear_pending();
                // 创建新 pending block
                return self.create_pending_and_append(timestamp, data);
            }

            // 追加到 pending block (raw, 不压缩)
            let in_block_offset = self.pending_block_uncomp_size;
            self.write_raw_record_to_pending(timestamp, data)?;
            self.pending_block_uncomp_size = new_total;
            self.pending_block_record_count += 1;
            return Ok((pending_off, in_block_offset));
        }

        // --- 情况3: 创建新 pending block ---
        self.create_pending_and_append(timestamp, data)
    }

    /// 密封 pending block: 压缩+写回
    fn seal_pending_block(
        &mut self,
        block_rel_offset: u64,
        compress_level: u8,
    ) -> io::Result<()> {
        let header_pos = HEADER_SIZE as usize + block_rel_offset as usize;
        let payload_start = header_pos + BLOCK_HEADER_SIZE as usize;
        let payload_len = self.pending_block_uncomp_size as usize;

        // 读取 raw payload
        let raw = self.mmap[payload_start..payload_start + payload_len].to_vec();

        // 压缩
        let compressed = miniz_oxide::deflate::compress_to_vec(&raw, compress_level);

        if compressed.len() < payload_len {
            // 压缩有效: 写 header + 压缩数据
            write_block_header(&mut self.mmap, header_pos,
                compressed.len() as u32,
                BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED,
                self.pending_block_record_count,
                self.pending_block_uncomp_size);
            self.mmap[payload_start..payload_start + compressed.len()]
                .copy_from_slice(&compressed);
        } else {
            // 压缩无效: 保留 raw, 只设 sealed
            write_block_header(&mut self.mmap, header_pos,
                payload_len as u32,
                BLOCK_FLAG_SEALED,
                self.pending_block_record_count,
                self.pending_block_uncomp_size);
        }

        self.flush_file_header();
        Ok(())
    }

    /// 写入 raw record 到 pending block
    fn write_raw_record_to_pending(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()> {
        let base = HEADER_SIZE as usize
            + self.pending_block_offset.unwrap() as usize
            + BLOCK_HEADER_SIZE as usize
            + self.pending_block_uncomp_size as usize;

        // [data_len: u16][timestamp: i64][data]
        let data_len = data.len() as u16;
        self.mmap[base..base+2].copy_from_slice(&data_len.to_le_bytes());
        self.mmap[base+2..base+10].copy_from_slice(&timestamp.to_le_bytes());
        self.mmap[base+10..base+10+data.len()].copy_from_slice(data);

        // 更新 block header 的 payload_size 和 record_count
        let hdr = HEADER_SIZE as usize + self.pending_block_offset.unwrap() as usize;
        let new_size = self.pending_block_uncomp_size as u32 + 2 + 8 + data.len() as u32;
        self.mmap[hdr..hdr+4].copy_from_slice(&new_size.to_le_bytes());
        self.mmap[hdr+4..hdr+6].copy_from_slice(&self.pending_block_record_count.to_le_bytes());

        self.wrote_position += 2 + 8 + data.len() as u64;
        Ok(())
    }

    /// 创建新 pending block 并写入第一条 record
    fn create_pending_and_append(
        &mut self,
        timestamp: i64,
        data: &[u8],
    ) -> io::Result<(u64, u16)> {
        let block_pos = HEADER_SIZE + self.wrote_position;
        let rec_size = 2 + 8 + data.len() as u64;

        // 写入 BlockHeader (flags=0, not sealed)
        write_block_header(&mut self.mmap, block_pos as usize,
            rec_size as u32, 0, 1, rec_size as u32);

        // 写入 record
        let data_pos = (block_pos + BLOCK_HEADER_SIZE) as usize;
        self.mmap[data_pos..data_pos+2].copy_from_slice(&(data.len() as u16).to_le_bytes());
        self.mmap[data_pos+2..data_pos+10].copy_from_slice(&timestamp.to_le_bytes());
        self.mmap[data_pos+10..data_pos+10+data.len()].copy_from_slice(data);

        self.pending_block_offset = Some(block_pos - HEADER_SIZE);
        self.pending_block_uncomp_size = rec_size as u32;
        self.pending_block_record_count = 1;
        self.wrote_position += BLOCK_HEADER_SIZE + rec_size;
        self.record_count += 1;
        self.total_uncompressed_size += rec_size;
        self.flush_file_header();

        Ok((block_pos - HEADER_SIZE, 0))
    }

    /// 独占 block (record > 64KB)
    fn create_single_record_block(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress_level: u8,
    ) -> io::Result<(u64, u16)> {
        let rec_size = 2 + 8 + data.len();
        let block_pos = HEADER_SIZE + self.wrote_position;

        // 构建 record payload: [data_len:2][ts:8][data:N]
        let mut raw = Vec::with_capacity(rec_size);
        raw.extend_from_slice(&(data.len() as u16).to_le_bytes());
        raw.extend_from_slice(&timestamp.to_le_bytes());
        raw.extend_from_slice(data);

        let compressed = miniz_oxide::deflate::compress_to_vec(&raw, compress_level);
        let (payload, flags) = if compressed.len() < rec_size {
            (compressed, BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD)
        } else {
            (raw, BLOCK_FLAG_SEALED | BLOCK_FLAG_SINGLE_RECORD)
        };

        let hdr_pos = block_pos as usize;
        write_block_header(&mut self.mmap, hdr_pos,
            payload.len() as u32, flags, 1, rec_size as u32);

        let data_pos = hdr_pos + BLOCK_HEADER_SIZE as usize;
        self.mmap[data_pos..data_pos + payload.len()].copy_from_slice(&payload);

        self.wrote_position += BLOCK_HEADER_SIZE + payload.len() as u64;
        self.record_count += 1;
        self.total_uncompressed_size += rec_size as u64;
        self.flush_file_header();

        Ok((block_pos - HEADER_SIZE, 0))
    }

    fn clear_pending(&mut self) {
        self.pending_block_offset = None;
        self.pending_block_uncomp_size = 0;
        self.pending_block_record_count = 0;
    }
}
```


> **注意**: Section 6.3 写入核心逻辑中的 `self.mmap[...]` 访问是伪代码。
> 实际实现须使用 `self.mmap.as_mut().unwrap()[...]` 或 `self.mmap.as_ref().unwrap()[...]`。
> 所有写入方法须先调用 `ensure_open()` 确保 mmap 有效。
> 读取方法须先确保 segment 已打开 (e.g., DataSet::query 中 lazy_open)。

### 6.4 读取: 通过索引定位 Block 内 record

```rust
impl DataSegment {
    fn read_at_index(&self, entry: &IndexEntry) -> io::Result<(i64, Vec<u8>)> {
        // 调用者须确保 mmap 有效 (e.g., 先 ensure_open)
        let m = self.mmap.as_ref().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "segment closed"))?;
        let hdr_pos = HEADER_SIZE as usize + entry.block_offset as usize;

        // 读取 block header
        let payload_size = u32::from_le_bytes(
            m[hdr_pos..hdr_pos+4].try_into().unwrap()
        ) as usize;
        let flags = u16::from_le_bytes(
            m[hdr_pos+4..hdr_pos+6].try_into().unwrap()
        );
        let is_compressed = flags & BLOCK_FLAG_COMPRESSED != 0;

        // 读取 payload
        let pay_start = hdr_pos + BLOCK_HEADER_SIZE as usize;
        let payload = &m[pay_start..pay_start + payload_size];

        // 解压
        let block_data: Vec<u8>;
        let actual = if is_compressed {
            block_data = miniz_oxide::inflate::decompress_to_vec(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            &block_data[..]
        } else {
            payload
        };

        // 定位 record: entry.in_block_offset 指向 [data_len:2]
        let pos = entry.in_block_offset as usize;
        let data_len = u16::from_le_bytes(
            actual[pos..pos+2].try_into().unwrap()
        ) as usize;
        let timestamp = i64::from_le_bytes(
            actual[pos+2..pos+10].try_into().unwrap()
        );
        let data = actual[pos+10..pos+10+data_len].to_vec();

        Ok((timestamp, data))
    }
}
```

### 6.5 DataSegment 生命周期方法

```rust
impl DataSegment {
    /// 确保 mmap 有效 (closed → open + mmap + pending恢复)
    pub fn ensure_open(&mut self, compress_level: u8) -> Result<()> {
        if self.mmap.is_some() { return Ok(()); }
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // 读取 header, 恢复状态
        let metadata = FileMetadata::read_from(&mmap)?;
        self.wrote_position = metadata.wrote_position as u64;
        self.record_count = metadata.record_count as u64;
        self.total_uncompressed_size = metadata.total_uncompressed_size;
        
        // Pending 恢复: 检测 FILE_FLAG_HAS_PENDING
        if metadata.file_flags & FILE_FLAG_HAS_PENDING != 0 {
            // pending 存在 → 密封 (不压缩) 确保 reopen 后一致性
            self.pending_block_offset = Some(metadata.pending_block_offset as u64);
            self.pending_block_uncomp_size = metadata.pending_uncomp_size;
            self.pending_block_record_count = metadata.pending_record_count;
            self.seal_pending_block_no_compress(compress_level)?;
            self.clear_pending();
            // 清除 header 中的 pending flag
            FileMetadata::clear_pending(&mut mmap, self.wrote_position as i64);
        }
        
        self.mmap = Some(mmap);
        self.lifecycle = SegmentLifecycle::OpenReady;
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    /// sync → unmmap → close
    pub fn idle_close(&mut self, compress_level: u8) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush(MmapSync::Sync, None, 0)?;
        }
        // 如有 pending → 密封 (不压缩)
        if self.pending_block_offset.is_some() {
            self.seal_pending_block_no_compress(compress_level)?;
            self.clear_pending();
            FileMetadata::clear_pending(self.mmap.as_mut().unwrap(), self.wrote_position as i64);
        }
        self.mmap = None;
        self.lifecycle = SegmentLifecycle::Closed;
        Ok(())
    }

    /// 仅 msync (不 seal/不压缩)
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush(MmapSync::Sync, None, 0)?;
        }
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    /// 密封 pending 但不压缩 (用于 idle-close 和 reopen recovery)
    fn seal_pending_block_no_compress(&mut self, _compress_level: u8) -> Result<()> {
        let block_rel_offset = self.pending_block_offset.unwrap();
        let hdr_pos = HEADER_SIZE as usize + block_rel_offset as usize;
        let header_off = hdr_pos;
        
        // 读取当前 payload_size
        let payload_size = u32::from_le_bytes(
            self.mmap.as_mut().unwrap()[hdr_pos..hdr_pos+4].try_into()?
        );
        let record_count = self.pending_block_record_count;
        let uncomp_size = self.pending_block_uncomp_size;
        
        // 更新 flags: SEALED (no COMPRESSED)
        write_block_header(&mut self.mmap.as_mut().unwrap(), header_off,
            payload_size, BLOCK_FLAG_SEALED, record_count, uncomp_size);
        Ok(())
    }
}
```

#### 6.6 DataSegment 创建/打开

```rust
impl DataSegment {
    /// 创建新 segment
    pub fn create(path: &Path, file_offset: u64, file_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).open(path)?;
        file.set_len(file_size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = FileMetadata::create_default(1, file_offset as i64, file_size as i64);
        metadata.write_to(&mut mmap);
        Ok(Self {
            path: path.to_path_buf(),
            file_offset, file_size,
            wrote_position: 0, record_count: 0, total_uncompressed_size: 0,
            created_at: unix_ms_now(),
            mmap: Some(mmap),
            sealed: false, lifecycle: SegmentLifecycle::OpenReady,
            last_accessed_at: Instant::now(),
            pending_block_offset: None, pending_block_uncomp_size: 0,
            pending_block_record_count: 0,
        })
    }

    /// 打开已有 segment
    pub fn open(path: &Path, file_offset: u64, file_size: u64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let metadata = FileMetadata::read_from(&mmap)?;
        // 校验 magic/version
        if metadata.magic != MAGIC { return Err(TmslError::InvalidMagic); }
        if metadata.version != VERSION { return Err(TmslError::InvalidVersion(metadata.version)); }
        Ok(Self {
            path: path.to_path_buf(), file_offset, file_size,
            wrote_position: metadata.wrote_position as u64,
            record_count: metadata.record_count as u64,
            total_uncompressed_size: metadata.total_uncompressed_size,
            created_at: metadata.created_at,
            mmap: Some(mmap),
            sealed: metadata.file_flags & FILE_FLAG_SEALED != 0,
            lifecycle: SegmentLifecycle::OpenReady,
            last_accessed_at: Instant::now(),
            pending_block_offset: if metadata.file_flags & FILE_FLAG_HAS_PENDING != 0 {
                Some(metadata.pending_block_offset as u64)
            } else { None },
            pending_block_uncomp_size: metadata.pending_uncomp_size,
            pending_block_record_count: metadata.pending_record_count,
        })
    }
}
```

---

## 七、TimeIndex: 时间索引

### 7.1 结构

```rust
struct TimeIndex {
    base_dir: PathBuf,
    segment_size: u64,
    index_segments: Vec<IndexSegment>,              // 打开中的 index segment
    closed_index_segments: Vec<IndexSegmentMeta>,   // 已关闭的 index segment
    in_memory_buffer: Vec<IndexEntry>,
    in_memory_flush_threshold: usize,               // 默认 1024
}

struct IndexSegmentMeta {
    path: PathBuf,
    start_timestamp: i64,
    entries_capacity: usize,
}
```

### 7.1.1 TimeIndex 生命周期管理

```rust
impl TimeIndex {
    /// sync 所有打开的 index segment
    pub fn sync_all(&mut self) -> io::Result<()> {
        for seg in &mut self.index_segments {
            seg.sync()?;
        }
        Ok(())
    }

    /// idle-close 所有 index segment
    pub fn idle_close_all(&mut self) -> Result<()> {
        let mut closed = Vec::new();
        for seg in self.index_segments.drain(..) {
            closed.push(IndexSegmentMeta {
                path: seg.path.clone(),
                start_timestamp: seg.start_timestamp,
                entries_capacity: seg.entries_capacity,
            });
            seg.idle_close()?;
        }
        self.closed_index_segments.extend(closed);
        Ok(())
    }

    /// 按需打开已关闭的 index segment
    fn ensure_segment_open(&mut self, start_ts: i64) -> Result<&mut IndexSegment> {
        // 先在 segments 中查找
        if let Some(idx) = self.index_segments.iter().position(|s| s.start_timestamp == start_ts) {
            return Ok(&mut self.index_segments[idx]);
        }
        // 在 closed 中查找 meta
        let meta = self.closed_index_segments.iter()
            .find(|m| m.start_timestamp == start_ts)?;
        // 打开
        let seg = IndexSegment::open(&meta.path, meta.start_timestamp, meta.entries_capacity)?;
        self.index_segments.push(seg);
        Ok(self.index_segments.last_mut().unwrap())
    }
}
```

### 7.1.2 TimeIndex 查询与加载

```rust
impl TimeIndex {
    /// 查询时间范围 [start_ts, end_ts] 内的所有 entries
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        
        // 1. 内存缓冲中的 entries
        for entry in &self.in_memory_buffer {
            if entry.timestamp >= start_ts && entry.timestamp <= end_ts {
                results.push(*entry);
            }
        }
        
        // 2. 所有段 (打开 + 关闭)
        // 打开中的 segments
        for seg in &mut self.index_segments {
            seg.ensure_open()?;
            results.extend(seg.query_range(start_ts, end_ts));
        }
        // 已关闭的 segments (需要临时打开)
        for meta in &self.closed_index_segments {
            // 优化: 如果 meta 的时间范围不在 [start_ts, end_ts] 内, skip
            let seg = IndexSegment::open(&meta.path, meta.start_timestamp, meta.entries_capacity)?;
            // 需要临时确保 mmap 有效
            results.extend(seg.query_range(start_ts, end_ts));
            // 不保持打开, 查询后立即关闭
        }
        
        // 3. 去重 + 排序
        results.sort_by_key(|e| e.timestamp);
        results.dedup_by_key(|e| e.timestamp);
        Ok(results)
    }

    /// 从磁盘加载已有 index segments
    pub fn load_existing(base_dir: &Path, segment_size: u64) -> io::Result<Self> {
        let mut metas: Vec<IndexSegmentMeta> = Vec::new();
        if base_dir.exists() {
            for entry in std::fs::read_dir(base_dir)? {
                let p = entry?.path();
                if !p.is_file() { continue; }
                let stem = p.file_stem().and_then(|n| n.to_str()).unwrap_or("0");
                let start_ts = i64::from_str_radix(stem, 10)?;
                let file_size = std::fs::metadata(&p)?.len();
                let entries_capacity = ((file_size - HEADER_SIZE) / INDEX_ENTRY_SIZE as u64) as usize;
                metas.push(IndexSegmentMeta { path: p, start_timestamp: start_ts, entries_capacity });
            }
        }
        metas.sort_by_key(|m| m.start_timestamp);
        // 初始所有 segment 进入 closed_segments, 按需打开
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            segment_size,
            index_segments: Vec::new(),
            closed_index_segments: metas,
            in_memory_buffer: Vec::new(),
            in_memory_flush_threshold: 1024,
        })
    }
}
```

### 7.2 IndexEntry 序列化 (18 字节)

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

### 7.3 IndexSegment

```rust
struct IndexSegment {
    path: PathBuf,
    start_timestamp: i64,
    entries_capacity: usize,
    wrote_count: usize,
    mmap: Option<MmapMut>,           // None = closed/unmapped
    sealed: bool,
    last_accessed_at: Instant,       // 最近读写时间
}

impl IndexSegment {
    fn append_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        // 确保 mmap 有效 (closed → open on-demand)
        self.ensure_open()?;
        if self.wrote_count >= self.entries_capacity {
            self.seal()?;
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "index segment full"));
        }
        let pos = HEADER_SIZE as usize + self.wrote_count * INDEX_ENTRY_SIZE;
        let m = self.mmap.as_mut().unwrap();
        m[pos..pos + INDEX_ENTRY_SIZE].copy_from_slice(&entry.to_bytes());
        self.wrote_count += 1;
        write_u64_le(m, 32, self.wrote_count as u64);
        write_i64_le(m, 48, self.wrote_count as i64);
        Ok(())
    }

    /// lower_bound: 查找 >= target_ts 的第一个位置
    fn lower_bound(&self, target_ts: i64) -> usize {
        let m = self.mmap.as_ref().unwrap();  // 调用者须确保 mmap 有效
        let (mut lo, mut hi) = (0usize, self.wrote_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE as usize + mid * INDEX_ENTRY_SIZE;
            let ts = i64::from_le_bytes(m[pos..pos+8].try_into().unwrap());
            if ts < target_ts { lo = mid + 1; } else { hi = mid; }
        }
        lo
    }

    /// 精确查找
    fn find_exact(&self, target_ts: i64) -> Option<IndexEntry> {
        let m = self.mmap.as_ref().unwrap();  // 调用者须确保 mmap 有效
        let (mut lo, mut hi) = (0usize, self.wrote_count.saturating_sub(1));
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE as usize + mid * INDEX_ENTRY_SIZE;
            let ts = i64::from_le_bytes(m[pos..pos+8].try_into().unwrap());
            match ts.cmp(&target_ts) {
                Ordering::Equal => {
                    let buf: [u8; 18] = m[pos..pos+18].try_into().unwrap();
                    return Some(IndexEntry::from_bytes(&buf));
                }
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => { if mid == 0 { break; } hi = mid - 1; }
            }
        }
        None
    }

    /// 范围查询
    fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry> {
        let m = self.mmap.as_ref().unwrap();  // 调用者须确保 mmap 有效
        let mut results = Vec::new();
        let start_idx = self.lower_bound(start_ts);
        for i in start_idx..self.wrote_count {
            let pos = HEADER_SIZE as usize + i * INDEX_ENTRY_SIZE;
            let ts = i64::from_le_bytes(m[pos..pos+8].try_into().unwrap());
            if ts > end_ts { break; }
            let buf: [u8; 18] = m[pos..pos+18].try_into().unwrap();
            results.push(IndexEntry::from_bytes(&buf));
        }
        results
    }
}
```

### 7.3.1 IndexSegment 生命周期方法

```rust
impl IndexSegment {
    /// 确保 mmap 有效 (closed → open)
    pub fn ensure_open(&mut self) -> Result<()> {
        if self.mmap.is_some() {
            return Ok(());
        }
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        self.mmap = Some(mmap);
        self.last_accessed_at = Instant::now();
        // 注意: index segment 无 pending recovery
        Ok(())
    }

    /// sync → unmmap → close
    pub fn idle_close(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush(MmapSync::Sync, None, 0)?;
        }
        self.mmap = None;
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    /// 仅 msync
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.mmap {
            m.flush(MmapSync::Sync, None, 0)?;
        }
        self.last_accessed_at = Instant::now();
        Ok(())
    }
}
```

> **注意**: 上述 IndexSegment 的 `lower_bound`, `find_exact`, `query_range` 方法中
> `self.mmap[...]` 访问需在实现时改为 `self.mmap.as_ref().unwrap()[...]`,
> 或在方法内部先 `ensure_open()`。

### 7.4 索引文件布局

```
┌──────────────────────────────────────────────┐
│ FileHeader (128 bytes)                       │
│ - magic "TMSL", version, ...                 │
│ - file_offset = start_timestamp              │
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

---

## 八、DataSet: 数据集

```rust
struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,
    config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
}

impl DataSet {
    fn write(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()> {
        let (seg_offset, block_rel_offset, in_block_offset) =
            self.segments.append(timestamp, data)?;

        self.time_index.add_entry(
            timestamp,
            seg_offset + block_rel_offset,  // 绝对 block offset
            in_block_offset,
        )?;

        self.last_used_at = Instant::now();
        Ok(())
    }

    fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<(i64, Vec<u8>)>> {
        let entries = self.time_index.query(start_ts, end_ts)?;
        let mut records = Vec::with_capacity(entries.len());
        for entry in &entries {
            let segment = self.segments.find_segment(entry.block_offset)?;
            let (ts, data) = segment.read_at_index(entry)?;
            records.push((ts, data));
        }
        records.sort_by_key(|(ts, _)| *ts);
        Ok(records)
    }

    fn flush(&mut self) -> io::Result<()> {
        // flush 仅执行 mmap.sync(), 不密封/不压缩 pending block
        self.segments.sync_all()?;
        self.time_index.sync_all()?;
        self.last_used_at = Instant::now();
        Ok(())
    }

    fn idle_close_all(&mut self) -> io::Result<()> {
        // 关闭所有 data segment + index segment
        self.segments.idle_close_all()?;
        self.time_index.idle_close_all()?;
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// 从磁盘加载已有数据集
    fn load(
        id: DataSetKey, base_dir: PathBuf, config: &StoreConfig,
    ) -> io::Result<Self> {
        let segments = DataSegmentSet::load_existing(
            &base_dir, config.data_segment_size,
            config.block_max_size, config.compress_level,
        )?;
        let index_dir = base_dir.join(".index");
        let time_index = TimeIndex::load_existing(
            &index_dir, config.index_segment_size,
        )?;
        Ok(Self {
            id, base_dir,
            config: DataSetConfig::from(config),
            segments, time_index,
            last_used_at: Instant::now(),
        })
    }
}
```

---

## 九、写入流程详解 (Block 聚合 + 延迟压缩)

```
写入 record(timestamp, data)
    │
    ├─ record_size = 2 + 8 + data.len()
    │
    ├─ record_size > 64KB? ──Yes──→ 独占 Block
    │    │                            1. 密封当前 pending (如果有)
    │    │                            2. 压缩 record payload
    │    │                            3. 写入 BlockHeader(flags=SEALED|COMPRESSED|SINGLE_RECORD)
    │    │                            4. 返回
    │
    No
    │
    ├─ 有 pending Block? ──No───→ 创建新 pending Block
    │    │                           flags=0, 不压缩
    │    │                           写入 record (raw)
    │    │                           设置 pending 状态
    │    │                           返回
    │
    Yes
    │
    ├─ pending_size + record_size > 64KB? ──Yes──→ 密封 pending Block
    │    │                                             1. 读取 raw payload
    │    │                                             2. 压缩 → 比较大小
    │    │                                             3. 写回: compressed 或 raw
    │    │                                             4. flags = SEALED[|COMPRESSED]
    │    │                                             5. 清除 pending
    │    │                                             6. 创建新 pending, 追加 record
    │    │                                             7. 返回
    │
    No
    │
    └─ 追加 record 到 pending (raw, 不压缩)
       更新 BlockHeader
       返回
```

### 9.1 Flush 行为 (mmap sync only)

```
flush (配置化，默认10分钟):
  for each dataset:
    for each open segment (data + index):
      1. mmap.flush() (msync / MS_SYNC)
      2. 不密封 pending block
      3. 不压缩任何数据
  注: flush 仅确保数据持久化到磁盘，不改变 block 状态
      pending block 继续保持 raw 状态留在 mmap 中
```

> **关键区别**: flush ≠ seal。flush 只 msync，密封/压缩只发生在 block 溢出或 idle-close 时。

---

## 十、读取流程详解

```
查询 [start_ts, end_ts]
    │
    ├─ 1. TimeIndex.query()
    │      → Vec<IndexEntry(ts, block_offset, in_block_offset)>
    │
    ├─ 2. 对每个 entry:
    │      ├─ 通过 block_offset 定位 Block
    │      ├─ 读 BlockHeader, 检查 compressed flag
    │      ├─ compressed → 解压 entire block payload
    │      ├─ in_block_offset → 定位到 [data_len:2]
    │      ├─ 读 data_len, timestamp, data
    │      └─ 返回
    │
    └─ 3. 按 timestamp 排序返回
```

---

## 十一、Store: 存储门面

```rust
/// FFI 数据集句柄 (不透明指针)
pub struct DataSetHandle(pub u64);  // 内部为 Arc<Mutex<DataSet>> 的 ID

pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,                     // 统一配置, 替代扁平字段
    flush_handle: Option<JoinHandle<()>>,
    idle_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,   // 优雅关闭信号
}

impl Store {
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>;
    pub fn open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>;
    pub fn close_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn close(self) -> Result<()>;
}
```

---

## 十二、FFI API

```rust
// Store 管理
#[no_mangle] pub extern "C" fn tmsl_store_open(data_dir: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_open_with_config(data_dir: *const c_char, config_ptr: *const StoreConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_close(store: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集管理
#[no_mangle] pub extern "C" fn tmsl_dataset_open(store: *mut c_void, name: *const c_char, dataset_type: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_close(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_flush(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据写入
#[no_mangle] pub extern "C" fn tmsl_dataset_write(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 查询迭代器
#[no_mangle] pub extern "C" fn tmsl_dataset_query(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_iter_next(iter: *mut c_void, out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar);
#[no_mangle] pub extern "C" fn tmsl_iter_close(iter: *mut c_void);
```

> **内存所有权**:
> - `tmsl_iter_next` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_iter_free_data` 释放
> - `tmsl_iter_close` 释放迭代器本身 (Rust `Box::from_raw` + drop)
> - Store/dataset 指针为 `Box::into_raw` → 对应 `tmsl_store_close`/`tmsl_dataset_close` 调用 `Box::from_raw` + drop
> - 所有 FFI 函数用 `catch_unwind` 包裹, panic 时返回 -1/null + err_buf 写错误信息

---

## 十三、C 侧调用示例

```c
char err_buf[512];

// 1. 打开存储
void* store = tmsl_store_open("/data/timslite", err_buf, sizeof(err_buf));

// 2. 打开数据集 (任意 name, 任意 type)
void* ds = tmsl_dataset_open(store, "patient_001", "waveform", err_buf, sizeof(err_buf));

// 3. 写入
unsigned char d[] = {1,2,3,4};
tmsl_dataset_write(ds, 1700000000, d, 4, err_buf, sizeof(err_buf));
tmsl_dataset_write(ds, 1700000001, d, 4, err_buf, sizeof(err_buf));

// 4. 查询
void* iter = tmsl_dataset_query(ds, 1700000000, 1700000060, err_buf, sizeof(err_buf));
long ts; unsigned char* buf; size_t len;
while (tmsl_iter_next(iter, &ts, &buf, &len, err_buf, sizeof(err_buf)) == 0) {
    // 处理 buf[0..len]
    tmsl_iter_free_data(buf);
}
tmsl_iter_close(iter);

tmsl_dataset_close(ds, err_buf, sizeof(err_buf));
tmsl_store_close(store, err_buf, sizeof(err_buf));
```

---

## 十四、内存管理

- `memmap2`: MmapMut (写入), Mmap (只读)
- `madvise`: SEQUENTIAL (写), WILLNEED (读)
- `flush`: mmap.flush() (MS_SYNC) — 仅同步到磁盘, **不改变任何 block 状态**
- 数据/索引 segment 均使用 mmap, 生命周期相同
- 空闲 30min → msync → 密封 pending (不压缩) → munmap → close file
- 下次访问 → on-demand open + mmap → 检测/恢复 pending block
- 任意时刻只有活跃 segment 持有 mmap 文件句柄

---

## 十五、并发控制

```
Store: RwLock<HashMap>              (多读少写)
DataSet: Arc<Mutex<DataSet>>        (读写互斥)
不同 DataSet: 完全并行
```

---

## 十六、压缩

- `miniz_oxide`: 纯 Rust deflate
- Block 级压缩, 不是 record 级
- 延迟压缩: pending 时 raw, 溢出时 seal+压缩
- 如果压缩后不缩小, 保留 raw (不设 COMPRESSED flag)
- **idle-close 仅密封 pending, 不压缩** — 压缩延迟至 next write overflow
- 超大 record (独占 block) → 立即 seal+压缩 (因为不存在 pending)

---

## 十七、后台任务

| 任务 | 间隔 | 行为 |
|------|------|------|
| Flush | 可配置, 默认 10min | 遍历所有打开的 segment, mmap.flush() (MS_SYNC) |
| Idle Check | 60s | 扫描 dataset last_used_at, ≥30min → sync + 密封 pending + unmmap + close |

### 17.1 Flush 行为详解

```
flush (每 10 分钟):
  for each dataset:
    for each open segment (data + index):
      mmap.flush() — MS_SYNC
  注: flush 不密封 pending block, 不压缩
```

### 17.2 Idle-Close 行为详解

```
idle-check (每 60s):
  1. 读锁遍历 datasets
     收集 last_used_at.elapsed() >= idle_timeout 的 dataset keys
  2. 对每个 idle dataset key:
     写锁获取 → 获取 dataset 引用
     ⚠️ **二次检查 (race condition 防护)**:
       获取写锁后再次检查 last_used_at.elapsed() >= idle_timeout
       如果是 → 执行 idle-close (可能有 concurrent write 刚刚更新了 last_used_at)
       如果否 → 跳过 (并发写操作已经"唤醒"了这个 dataset)
  3. 对每个打开的 segment:
     a. mmap.flush() (MS_SYNC)
     b. 如果 data segment 有 pending block:
        密封 (不压缩), block.flags |= BLOCK_FLAG_SEALED
     c. 清除 header FILE_FLAG_HAS_PENDING
     d. munmap + close file
  4. dataset 进入 idle 状态 (last_used_at 不变, segments 清空)

on-demand reopen:
  当读取/写入操作命中已关闭的 segment:
    - data segment: open + mmap, 检测 FILE_FLAG_HAS_PENDING → 密封恢复
    - index segment: open + mmap, 直接恢复 (无 pending)
```

> **Race Condition 详述**:
> 后台线程读锁收集 idle datasets → 在获取写锁前, 前台写操作可能命中该 dataset
> → 更新 `last_used_at` → 写锁获取后必须重新检查, 否则会把刚写入的 dataset
> 错误地 idle-close。解决方案: **double-check last_used_at after write lock acquired**。

### 17.3 mmap 生命周期

```
┌─────────┐  write/read    ┌────────┐   idle 30min   ┌────────┐
│ closed  │ ─────────────→ │  open  │ ──────────────→ │ closed │
│         │ ←─ on-demand ──│(mmap) │                 │(unmap) │
└─────────┘                └────────┘                 └────────┘
    ↑                          │
    │      flush (10min)       │ msync only
    └──────────────────────────┘
```

### 17.4 Pending Block 恢复详情

```
reopen 时 pending block 恢复流程:
  1. 读取 FileMetadata, 校验 magic/version
     - magic != "TMSL" → 返回 InvalidMagic (文件损坏/非本库文件)
     - version 不兼容 → 返回 InvalidVersion
  2. 检查 FILE_FLAG_HAS_PENDING flag
     - 无 → 直接 OpenReady, 无 pending
     - 有 → 进入恢复流程
  3. 恢复流程:
     a. 从 header 恢复 pending_block_offset, pending_uncomp_size, pending_record_count
     b. 验证: pending_block_offset + HEADER_SIZE + pending_uncomp_size <= file_size
        - 不满足 → header 损坏, 回退到 wrote_position (丢弃 pending 数据)
     c. 密封 pending block (FLAGS=SEALED, 不压缩)
        - 读取当前 payload_size (可能已被部分写入)
        - 用 pending_record_count + payload_size 更新 block header
        - 设置 flags = BLOCK_FLAG_SEALED
     d. 清除 header: pending_block_offset=-1, 清 FILE_FLAG_HAS_PENDING
     e. wrote_position = sealed block 末尾
     f. 返回 OpenReady (pending 已清除)
```

> **Crash 安全分析**:
> idle-close 时 msync 已确保 header 和 block payload 同步到磁盘。
> Reopen 时如果 pending 数据已写入但 header 未 seal → 恢复流程可以安全密封。
> 如果 crash 发生在 msync 前 → 部分数据丢失 (但 header 记录的是 msync 前的状态)。
> 这 10min flush 间隔内的 crash 损失可接受 (mmap 本身已有 OS page cache 保护)。

---

## 十八、Cargo.toml

```toml
[package]
name = "timslite"
version = "0.1.0"
edition = "2021"

[lib]
name = "timslite"
crate-type = ["cdylib", "rlib"]

[dependencies]
memmap2 = "0.9"
miniz_oxide = "0.8"
log = "0.4"
libc = "0.2"

[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "timslite_benchmarks"
harness = false
```

> **注意**: miniz_oxide 0.8 支持更新的 API (e.g., `compress_with_level`)
> criterion 替换 nightly-only `#[bench]`, 运行 `cargo bench`

---

## 十九、与 TimeStore 的差异

| 对比项 | TimeStore (Java) | timslite (Rust) |
|--------|------------------|-----------------|
| 存储单元 | 单条 record | Block (多条聚合, ≤64KB) |
| 压缩粒度 | record | Block |
| 压缩时机 | 立即 | 延迟 (pending→sealed, 溢出或 idle-close 时) |
| 内存映射 | MappedByteBuffer | memmap2::MmapMut, 懒加载/超时关闭(30min) |
| 元数据 | Protobuf | 128字节 header (固定10B+扩展长度52B+预留64B) |
| 索引目录 | 同级子目录 | `.index/` 独立子目录 |
| 索引条目 | 16B (ts+offset) | 18B (ts+block+in_block) |
| 文件头 | 64B | 128B (含meta_data_len+pending扩展+预留) |
| Record编码 | size+ts+data | data_len+ts+data |
| FFI | 无 | `extern "C"` |

---

## 二十、模块结构

```
src/
├── lib.rs              # 入口, re-exports: Store, StoreConfig, TmslError, Result
├── store.rs            # Store (门面, 数据集管理, 后台任务启动)
├── dataset.rs          # DataSet (name+type 级别, sync_all/idle_close_all)
├── segment/
│   ├── mod.rs          # DataSegmentSet (~data segment, lazy open/close)
│   └── data.rs         # DataSegment (Block 管理, lifecycle, pending recovery)
├── block.rs            # BlockHeader (16B, read/write/flags)
├── index/
│   ├── mod.rs          # TimeIndex (~index segment, lazy open/close, query)
│   └── segment.rs      # IndexSegment (18B entries, lifecycle, binary search)
├── header.rs           # FileMetadata (128B, 固定10B+扩展52B+预留64B)
├── ffi.rs              # extern "C" (catch_unwind, opaque handles, memory mgmt)
├── error.rs            # TmslError enum + From impls
├── compress.rs         # deflate_compress/decompress + size comparison
├── config.rs           # StoreConfig + StoreConfigBuilder + DataSetConfig (internal)
├── util.rs             # endian helpers, mmap read/write macros
└── bg/
    ├── mod.rs          # BackgroundTasks (start/stop)
    ├── flush.rs        # flush loop (msync only, configurable interval)
    └── idle.rs         # idle-check loop (30min timeout, double-check race guard)
```

---

## 二十一、关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储单元 | Block 聚合 | 提高压缩率, 减少 overhead |
| Block 上限 | 64KB | 适配 L1/L2 缓存 |
| 压缩时机 | 延迟 (pending→sealed) | 写入时零 CPU, 避免重复压缩 |
| 超大 record | 独占 block | 不截断数据 |
| Record 编码 | data_len(2)+ts(8)+data | 支持 block 内随机定位 |
| 索引条目 | 18 字节 | 精确定位到 block 内 record |
| 文件头 | 128 字节 | 固定10B核心+扩展长度2B+扩展52B+预留64B, 向后兼容 |
| 元数据扩展 | meta_data_len (u16) | 告知后续字节数, 未知字段安全跳过 |
| 索引目录 | `.index/` 独立 | 与数据隔离 |
| wrote_position 位置 | created_at 之后 | 时间字段集中存放, 便于版本迁移 |
| 并发 | DataSet 级 Mutex | 不同数据集独立 |
| flush 行为 | 仅 msync (不 seal/不压缩) | 降低 flush CPU 开销, 压缩延迟至 block 溢出 |
| flush 间隔 | 可配置, 默认 10min | 平衡数据持久化与性能, mmap 本身已有 OS page cache 保护 |
| segment 生命周期 | 懒打开/超时关闭 (30min) | 控制内存占用, 避免大量数据集同时持有 mmap |
| idle-close pending | 密封 (不压缩) | 保证 reopen 后 block flag 一致, 延迟压缩至下次 overflow |
