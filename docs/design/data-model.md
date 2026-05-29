# 核心数据模型

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

**纠正写入 (Correction Write)**: 当 `timestamp == latest_written_timestamp` 时触发纠正写入。最新记录必然位于 **最新数据段的最后一个未压缩 block** (可以是 pending block 或 SEALED 但未压缩的 block) 的最末位置, 因此可通过 mmap 直接修改该 record 的 data 字节, **支持改变 data 长度** (增长或缩小)。索引条目保持不变 (block_offset/in_block_offset 不变)。修改时 delta = new_data.len() - old_data_len, 需同步更新 5 个字段: block 头的 payload_size/uncompressed_size + 段的 pending_wrote_position (仅 pending) / total_uncompressed_size / wrote_position。**回退行为**: 若目标 block 已密封或已压缩 (无法原地修改), 则自动回退为更新写入: 数据追加到最新数据段、更新索引值, 同时旧数据所在段的 `invalid_record_count` 加一。

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

### 3.5 FileMetadata (文件头, meta + state)

每个数据段和索引段的头部元数据。**数据段和索引段的 state 字段已分化**, 各自维护不同的可变状态。

#### 设计原则: 可变(state) 与 不可变(meta) 分离

```
┌──────────────────────────────────────────────────────────┐
│ 固定前缀 (9 bytes)                                        │
│  magic:4 + version:2 + fileType:1 + meta_length:2         │
├──────────────────────────────────────────────────────────┤
│ Meta 不可变 TLV 区 (variable, 当前 33 bytes)              │  ← 创建时写入一次, 永不修改
│  {type:1}{len:2}{value}, 可多个, 可跳过未知 type          │
├──────────────────────────────────────────────────────────┤
│ state_length: u16 (2 bytes)                               │  ← 告知后续 state 总字节数
├──────────────────────────────────────────────────────────┤
│ State 可变区 (按文件类型分化)                              │  ← 每次写入时动态更新
│  数据段: 72 bytes (9×8B)                                 │
│  索引段: 8 bytes (1×8B)                                  │
└──────────────────────────────────────────────────────────┘
```

#### 固定前缀 (数据段和索引段共享, 9 bytes)

```
Offset  Size  Field                    Description
0-3     4     magic = b"TMSL"
4-5     u16   version                  = 1
6       u8    fileType                 1 = index segment, 2 = data segment
7-8     u16   meta_length              Meta TLV 区总字节数
```

#### Meta 不可变 TLV 区 (创建时写入, 永不修改, 数据段和索引段共享)

| Meta Type (hex) | 名称 | 长度 | 数据类型 | 说明 |
|-----------------|------|------|---------|------|
| 0x01 | created_at | 8 | i64 LE | 创建时间(unix ms) |
| 0x02 | file_offset | 8 | i64 LE | data segment: 起始字节offset; index segment: 起始秒级timestamp |
| 0x03 | file_size | 4 | u32 LE | 文件总大小(字节) — 始终记录 max segment_size |
| 0x04 | compress_level | 1 | u8 | 压缩级别 |

> Meta TLV 可向前扩展: 未知 type 通过 length 字段跳过, 不影响解析。

#### Data Segment State 可变区 (每值固定 8 字节, 顺序存储)

```
Offset  (相对 state 起始)    Size  Field                       Description
0       i64(8)  min_timestamp             段内所有 record 的最小时间戳 (i64::MAX=空段)
8       i64(8)  max_timestamp             段内所有 record 的最大时间戳 (i64::MIN=空段)
16      u64(8)  wrote_position            当前写入位置(绝对偏移, 含 DATA_HEADER_SIZE)
24      u64(8)  record_count              已写入记录总数
32      u64(8)  total_uncompressed_size   文件内所有 record 原始数据总大小
40      u64(8)  pending_block_offset      当前未完成 block 相对偏移 (u64::MAX=无)
48      u64(8)  pending_wrote_position    pending block 内已写入位置(从 payload 起始)
56      u64(8)  pending_record_count      pending block 内 record 数量
64      u64(8)  invalid_record_count      段内无效记录数 (索引已不指向该 record, 但物理数据仍存在)
```

> `min_timestamp` / `max_timestamp`: 每次 `append_record` 更新, 用于 DataSegmentSet 的时间范围段级过滤优化。空段时 `min_timestamp = i64::MAX`, `max_timestamp = i64::MIN`。

#### Index Segment State 可变区 (仅 1 个字段)

```
Offset  (相对 state 起始)    Size  Field                    Description
0       u64(8)  wrote_position           当前写入位置(绝对偏移, 含 INDEX_HEADER_SIZE)
```

> `wrote_count` 可从 `wrote_position` 计算: `wrote_count = (wrote_position - INDEX_HEADER_SIZE) / INDEX_ENTRY_SIZE`。不再单独持久化 `record_count`、`total_uncompressed_size` 等数据段相关字段。

#### Header 大小计算

**数据段 (DataSegment):**
```
固定前缀:     4 + 2 + 1 + 2     = 9 bytes
Meta TLV:     11 + 11 + 7 + 4  = 33 bytes  (4 个 TLV 条目)
state_length: 2                 = 2 bytes
State 值:     9 × 8            = 72 bytes   (9 个字段)
────────────────────────────────────────────
DATA_HEADER_SIZE = 116 bytes
```

**索引段 (IndexSegment):**
```
固定前缀:     4 + 2 + 1 + 2     = 9 bytes
Meta TLV:     11 + 11 + 7 + 4  = 33 bytes  (4 个 TLV 条目)
state_length: 2                 = 2 bytes
State 值:     1 × 8            = 8 bytes    (1 个字段)
────────────────────────────────────────────
INDEX_HEADER_SIZE = 52 bytes
```

#### 常量定义

```rust
pub const DATA_HEADER_SIZE: u64 = 116;
pub const INDEX_HEADER_SIZE: u64 = 52;
```

> **数据段数据区起始位置 = `DATA_HEADER_SIZE = 116` 字节**
> **索引段数据区起始位置 = `INDEX_HEADER_SIZE = 52` 字节**

#### 兼容性设计

| 场景 | 行为 |
|------|------|
| v1 reader 读 v1 文件 | 正常读取, 解析已知 meta type, 跳过未知 |
| v2 reader 读 v1 文件 | 读 `meta_length` 跳过未知 meta; 读 `state_length` 对齐 state |
| v1 reader 读 v2 文件 | 读固定前缀 (9B) + `meta_length` 跳过 meta + `state_length` 跳过 state, 解析已知 state 位置 |
| 未来添加新 meta 字段 | 增加新 TLV type, `meta_length` 增加, 旧版本通过 length 跳过 |
| 未来添加新 state 字段 | 增加 state 条目, `state_length` 增加, 旧版本只读前 N 个 8B |
| 数据段 vs 索引段区分 | 通过 `fileType` 字段 (byte 6) 确定使用哪个 HEADER_SIZE 常量 |

## 四、核心类型定义

```rust
/// 存储实例句柄 (线程安全)
pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,
    block_cache: Arc<BlockCache>,      // 全局读缓存池 (0=禁用)
    bg_handle: Option<JoinHandle<()>>,
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
    pub initial_data_segment_size: u64,  // 默认 256KB
    pub initial_index_segment_size: u64, // 默认 4KB
    pub block_max_size: u32,         // 默认 65536 (64KB)
    pub compress_level: u8,          // 默认 6
    pub cache_max_memory: usize,     // 读缓存池上限 (字节, 0=禁用, 默认 256MB)
    pub cache_idle_timeout: Duration, // 缓存块空闲超时 (默认 30 分钟)
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(600),    // 10 分钟
            idle_timeout: Duration::from_secs(1800),     // 30 分钟
            data_segment_size: 64 * 1024 * 1024,         // 64MB
            index_segment_size: 4 * 1024 * 1024,         // 4MB
            initial_data_segment_size: 256 * 1024,       // 256KB
            initial_index_segment_size: 4 * 1024,        // 4KB
            block_max_size: 65536,                       // 64KB
            compress_level: 6,
            cache_max_memory: 256 * 1024 * 1024,         // 256MB
            cache_idle_timeout: Duration::from_secs(1800), // 30 分钟
        }
    }
}

/// 数据集内部配置 (从 StoreConfig 派生)
struct DataSetConfig {
    data_segment_size: u64,
    index_segment_size: u64,
    initial_data_segment_size: u64,
    initial_index_segment_size: u64,
    block_max_size: u32,
    compress_level: u8,
}

/// Block 头
struct BlockHeader {
    block_payload_size: u32,
    flags: u16,
    record_count: u16,
    uncompressed_size: u32,
    _reserved: u32,
}

const BLOCK_FLAG_COMPRESSED: u16     = 0x0001;
const BLOCK_FLAG_SEALED: u16         = 0x0002;
const BLOCK_FLAG_SINGLE_RECORD: u16  = 0x0004;

/// File type constants
const FILE_TYPE_INDEX: u8  = 1;
const FILE_TYPE_DATA: u8   = 2;

/// Meta TLV types (immutable, written once at creation)
const META_TYPE_CREATED_AT:     u8 = 0x01;  // i64 LE, unix ms
const META_TYPE_FILE_OFFSET:    u8 = 0x02;  // i64 LE
const META_TYPE_FILE_SIZE:      u8 = 0x03;  // u32 LE
const META_TYPE_COMPRESS_LEVEL: u8 = 0x04;  // u8

/// 数据段文件元数据头 (DataFileHeader, 116 bytes)
struct DataFileMetadata {
    // === 固定前缀 (所有版本必须可读, 9 bytes) ===
    magic: [u8; 4],
    version: u16,
    file_type: u8,  // FILE_TYPE_DATA = 2
    // === Meta 不可变 (TLV, 创建时写入) ===
    created_at: i64,
    file_offset: i64,
    file_size: u32,
    compress_level: u8,
    // === State 可变 (每值固定 8 字节, 顺序存储, 9 个字段) ===
    min_timestamp: i64,         // 段内最小时间戳 (i64::MAX 表示空段)
    max_timestamp: i64,         // 段内最大时间戳 (i64::MIN 表示空段)
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    pending_block_offset: u64,
    pending_wrote_position: u64,
    pending_record_count: u64,
    invalid_record_count: u64,  // 段内无效记录数 (乱序写入/delete 导致)
}

/// 索引段文件元数据头 (IndexFileHeader, 52 bytes)
struct IndexFileMetadata {
    // === 固定前缀 (所有版本必须可读, 9 bytes) ===
    magic: [u8; 4],
    version: u16,
    file_type: u8,  // FILE_TYPE_INDEX = 1
    // === Meta 不可变 (TLV, 创建时写入) ===
    created_at: i64,
    file_offset: i64,
    file_size: u32,
    compress_level: u8,
    // === State 可变 (仅 1 个字段) ===
    wrote_position: u64,
}

const DATA_HEADER_SIZE: u64 = 116;   // 数据段文件头大小
const INDEX_HEADER_SIZE: u64 = 52;   // 索引段文件头大小

/// 索引条目
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    timestamp: i64,
    block_offset: u64,
    in_block_offset: u16,
}
```

---

**相关**: [架构概览](architecture.md) | [元数据格式](meta-format.md) | [数据段管理](data-segment.md)
