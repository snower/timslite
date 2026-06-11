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

**Block 大小限制**: 普通聚合 Block 的 payload 最大 64KB (65536 字节)。如果单条 record 的编码后大小超过 64KB, 则该 record **独占一个 Block** (`SINGLE_RECORD`, exclusive/single-record block)。因此 `SINGLE_RECORD` 表示该 block 只服务一条 record, 不再等同于 record 一定超过 64KB。

**单条 Record 上限**: `write` 与 `append` 都必须拒绝纯数据长度超过 4MiB 的单条 record。普通写入校验 `data.len() <= 4MiB`; append 校验追加后的逻辑 record 数据长度 `old_data_len + append_len <= 4MiB`。该限制独立于 `data_len:u32` 的磁盘编码上限, 是当前实现的资源保护边界。

**Append 增长边界**: `DataSet::append` 修改已存在的最新 record 时只允许原地增长最后一个 pending raw block 的最末 record。追加后的逻辑 record 仍受 4MiB 资源上限约束, 且编码后大小必须能继续落在当前普通聚合 block 内; 不再因为比例阈值迁移为独占 block。若目标 block 已压缩、目标 record 不是分段最末尾, 或增长后超过普通 block 可承载范围, append 返回错误。

**纠正写入 (Correction Write)**: 当 `timestamp == latest_written_timestamp` 时触发纠正写入。该最大已写 timestamp 对应的记录只有在仍位于 **最新数据段最后一个 pending raw block (`flags=0`)** 的最末位置时, 才可通过 mmap 直接修改该 record 的 data 字节, **支持改变 data 长度** (增长或缩小)。索引条目保持不变 (`block_offset`/`in_block_offset` 不变, 其中 `block_offset` 是数据区逻辑全局偏移, 不含 header)。修改时 delta = new_data.len() - old_data_len, 需同步更新 5 个字段: block 头的 payload_size/uncompressed_size + 段的 pending_wrote_position / total_uncompressed_size / wrote_position。**回退行为**: 若目标 block 已经 `SEALED|COMPRESSED` 或不是可原地修改位置, 则自动回退为更新写入: 数据追加到最新数据段、更新索引值, 同时旧数据所在段的 `invalid_record_count` 加一, 并 invalidate 旧索引对应的全局缓存 key。compressed block 一旦写入后不允许再被修改, 这是全局 BlockCache 只缓存 compressed block 解压结果的前提。

### 3.3 Block Layout (磁盘上的 Block 结构)

```
┌──────────────────────────────────────────────────────────┐
│ BlockHeader (16 bytes)                                   │
├──────────────────────────────────────────────────────────┤
│ Block Payload (compressed 或 uncompressed)               │
│ ┌──────────────────────────┬───────────────────────────┐  │
│ │ data_len:4 + ts:8 + data │ data_len:4 + ts:8 + data  │  │
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
                  bit 2: 1=single_record (exclusive/single-record block)
6-7     u16   record_count             Block 内 record 数量
8-11    u32   uncompressed_size        Block 内所有 record 的原始数据总大小 (含 data_len+timestamp)
12-15   u32   reserved                 保留
```

合法状态只有两类:
- `flags = 0`: pending raw block, 可继续追加、可纠正写、可读取, 不进入全局缓存。
- `flags` 同时包含 `SEALED | COMPRESSED`: sealed compressed block, 不再修改, 可进入全局缓存。

`SEALED && !COMPRESSED` 与 `COMPRESSED && !SEALED` 均为非法状态。

#### Block Payload 内部结构 (Record 编码)

每条 record 在 Block Payload 中的存储:

```
┌──────────┬─────────────────┬──────────────────────────────┐
│ data_len │ timestamp       │ data                         │
│ u32      │ i64 (8 bytes)   │ bytes (data_len 字节)        │
│ 4 bytes  │                 │                              │
└──────────┴─────────────────┴──────────────────────────────┘
```

- `data_len`: 纯数据长度 (不含 data_len 的 4 字节和 timestamp 的 8 字节), little-endian `u32`
- Record header 固定 12 字节 (`data_len:4 + timestamp:8`)
- 记录之间紧密排列, 无额外分隔符
- 遍历方式: offset += 4 + 8 + data_len
- 写入路径必须保证单条 record 的纯数据长度不超过 4MiB; append 路径增长 `data_len` 时同样受该限制约束

### 3.4 IndexEntry (索引条目)

每个索引条目固定 **18字节**:

```
┌──────────────────────┬──────────────────────┬──────────────┐
│ timestamp (i64)      │ block_offset (u64)   │ in_block     │
│ 8 bytes              │ 8 bytes              │ offset (u16) │
└──────────────────────┴──────────────────────┴──────────────┘
```

- `timestamp`: 秒级时间戳
- `block_offset`: 对应 Block 在数据流中的**逻辑全局偏移** (相对各数据段数据区起点, 指向 BlockHeader 起始), 不包含任何数据段文件 header 长度。落到具体段后, 物理文件偏移 = `segment.header_len + (block_offset - segment.file_offset)`。
- `in_block_offset`: record 在 Block Payload 中的**相对偏移** (从 payload 起始算, 指向该 record 的 data_len 字段)。普通聚合 Block 的 payload 受 64KB 上限约束, 因此真实 record 起始偏移不会达到 `0xFFFF` 哨兵值; exclusive/single-record block 只包含一条 record, `in_block_offset` 固定为 0。

#### Block Offset 坐标系

为避免和物理文件偏移混淆, 文档统一使用 `block_offset` 表达索引中保存的值。它不是文件内物理 offset, 而是所有数据段的数据区串联后的逻辑 offset。

| 名称 | 含义 | 是否包含 header |
|------|------|----------------|
| `segment.file_offset` | 数据段在同一数据流坐标系中的起始 offset, 也是数据段文件名和 `DataSegment.file_offset` | 否 |
| `block_offset` | `IndexEntry.block_offset` 存储的值, 指向 BlockHeader 起始位置 | 否 |
| `block_offset - segment.file_offset` | BlockHeader 在所属数据段**数据区内**的相对 offset | 否 |
| `segment.header_len + (block_offset - segment.file_offset)` | 实际 mmap/seek 读取 BlockHeader 的文件内物理字节位置 | 是 |

转换公式:

```text
segment.file_offset   = (block_offset / data_segment_size) * data_segment_size
block_segment_offset  = block_offset - segment.file_offset
                      = block_offset % data_segment_size
physical_file_offset  = segment.header_len + block_segment_offset
```

因此 `block_offset / data_segment_size` 可定位第几个数据段, `block_offset % data_segment_size` 可定位该段数据区内的 block 起点; 真正读写文件时必须再加运行时解析出的 `segment.header_len`。可变 header 设计要求 index 中的 `block_offset` 永远不包含 header, 否则文件格式扩展会改变历史索引含义。

#### On-Disk Integer 编码与校验约束

所有 on-disk 多字节整数均使用 **Little Endian (LE)**, 不使用 native endian。signed/unsigned 按字段语义区分:

| 字段类别 | 磁盘类型 | 说明 |
|----------|----------|------|
| 时间戳、时间范围、创建时间 | `i64 LE` | `timestamp`, `min_timestamp`, `max_timestamp`, `created_at/create_time`。允许业务使用负 timestamp; 空数据段使用 `i64::MAX` / `i64::MIN` 作为 sentinel。 |
| segment header 的 `file_offset` | `i64 LE` | 复用字段: data segment 中必须为非负数据区逻辑起点; index segment 中表示 `start_timestamp`, 因此保持 signed。 |
| 数据长度、payload 长度、压缩前长度 | `u32 LE` | `data_len`, `block_payload_size`, `uncompressed_size`, `file_size`。写入时必须拒绝超过 `u32::MAX` 的值; 当前 API 还必须拒绝纯数据长度超过 4MiB 的单条 record; 读取时必须校验不会越过 block/file 边界。 |
| 逻辑 offset、写入位置、计数、retention、segment size | `u64 LE` | `block_offset`, `wrote_position`, `record_count`, `pending_*`, `invalid_record_count`, `*_segment_size`, `retention_window`。`retention_window` 使用 timestamp unit。所有加法/乘法必须使用 checked/saturating 语义并校验上界。 |
| block 内 offset、flags、version、length | `u16 LE` | `in_block_offset`, `flags`, `version`, `meta_length`, `state_length`, TLV length。`0xFFFF` 是 `in_block_offset` filler sentinel, 真实 record offset 不得使用该值。 |
| type、fileType、compress_level、boolean flag | `u8` | 单字节字段不涉及端序。 |

读取已有文件时必须执行以下校验:

- `meta_length + state_length` 计算 `header_len` 时不得溢出, 且 `header_len <= file_len`; v1 已知 state 长度不得短于对应最小值。
- data segment 的 `wrote_position` 必须满足 `header_len <= wrote_position <= file_len`。index segment 的 entry area 固定从文件内绝对偏移 128 开始, 因此 index `wrote_position` 必须满足 `128 <= wrote_position <= file_len`, 且 `wrote_position - 128` 必须是 `INDEX_ENTRY_SIZE` 的整数倍。
- `block_payload_size` 必须完全落在所属 segment 文件内; compressed block 解压后的长度必须等于 `uncompressed_size`。
- 遍历 record 时, 每个 `data_len` 必须满足 `record_pos + 12 + data_len <= block_payload_len`; `timestamp` 直接按 `i64 LE` 读取。
- real `IndexEntry` 不得使用 filler sentinel: `block_offset != u64::MAX` 且 `in_block_offset != 0xFFFF`; filler 必须同时使用 `block_offset = u64::MAX` 与 `in_block_offset = 0xFFFF`。
- data segment header 中的 `file_offset` 必须可转换为非负 `u64`; index segment header 中的 `file_offset` 作为 signed `start_timestamp` 使用。
- 写入路径中所有 size/offset/count 累加必须使用 `checked_add`/`try_from` 或等价校验, 溢出时返回 `InvalidData` 或对应参数错误。

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
| 0x02 | file_offset | 8 | i64 LE | data segment: `segment.file_offset` (数据区逻辑全局起点, 不含 header); index segment: 起始 timestamp |
| 0x03 | file_size | 4 | u32 LE | 文件总大小(字节) — 始终记录 max segment_size |
| 0x04 | compress_level | 1 | u8 | 压缩级别 |

> Meta TLV 可向前扩展: 未知 type 通过 length 字段跳过, 不影响解析。

#### Data Segment State 可变区 (每值固定 8 字节, 顺序存储)

```
Offset  (相对 state 起始)    Size  Field                       Description
0       i64(8)  min_timestamp             段内所有 record 的最小时间戳 (i64::MAX=空段)
8       i64(8)  max_timestamp             段内所有 record 的最大时间戳 (i64::MIN=空段)
16      u64(8)  wrote_position            当前写入位置(文件内绝对偏移, 含动态 header_len)
24      u64(8)  record_count              已写入记录总数
32      u64(8)  total_uncompressed_size   文件内所有 record 原始数据总大小
40      u64(8)  pending_block_offset      当前未完成 block 相对偏移 (u64::MAX=无)
48      u64(8)  pending_wrote_position    pending block 内已写入位置(从 payload 起始)
56      u64(8)  pending_record_count      pending block 内 record 数量
64      u64(8)  invalid_record_count      段内无效记录数 (仅统计, 不触发 compaction)
```

> `min_timestamp` / `max_timestamp`: 每次 `append_record` 更新, 用于 DataSegmentSet 的时间范围段级过滤优化。空段时 `min_timestamp = i64::MAX`, `max_timestamp = i64::MIN`。

#### Index Segment State 可变区 (仅 1 个字段)

```
Offset  (相对 state 起始)    Size  Field                    Description
0       u64(8)  wrote_position           当前写入位置(文件内绝对偏移, index entry area 固定从 128 开始)
```

> `wrote_count` 可从 `wrote_position` 计算: `wrote_count = (wrote_position - 128) / INDEX_ENTRY_SIZE`。不再单独持久化 `record_count`、`total_uncompressed_size` 等数据段相关字段。

#### Header 长度与数据区起点

Header 采用可变长度设计。Data segment 的 Block 区从动态 `header_len` 开始。Index segment 为保持连续模式 timestamp 路由稳定, 文件前 128 字节固定保留给 fixed prefix、Meta TLV、state 和未来扩展, 所有 index entry 无论连续/非连续模式都从文件内绝对偏移 128 开始。

运行时必须从文件自身读取长度并计算:

```text
meta_start          = 9
state_length_offset = meta_start + meta_length
state_start         = state_length_offset + 2
header_len          = state_start + state_length
```

所有 state 字段偏移均以 `state_start` 为基准。数据段 Block 区从动态 `header_len` 开始; 索引段 Entry 区固定从 128 开始。未来新增 index meta TLV 或 state 字段必须仍放入 128 字节保留区, 不能改变 entry area 起点或连续索引 `segment_capacity`。

#### v1 默认 Header 长度计算

**数据段 (DataSegment):**
```
固定前缀:     4 + 2 + 1 + 2     = 9 bytes
Meta TLV:     11 + 11 + 11 + 4 + 4 = 41 bytes  (5 个 TLV 条目)
state_length: 2                 = 2 bytes
State 值:     9 × 8            = 72 bytes   (9 个字段)
────────────────────────────────────────────
DATA_HEADER_SIZE = 124 bytes
```

**索引段 (IndexSegment):**
```
固定前缀:     4 + 2 + 1 + 2     = 9 bytes
Meta TLV:     11 + 11 + 11 + 4 + 4 = 41 bytes  (5 个 TLV 条目)
state_length: 2                 = 2 bytes
State 值:     1 × 8            = 8 bytes    (1 个字段)
Reserved:      68               = 68 bytes   (padding to fixed entry area)
────────────────────────────────────────────
INDEX_HEADER_SIZE = 128 bytes
```

#### 常量定义

```rust
pub const DATA_HEADER_SIZE: u64 = 124;   // v1 默认 data header_len
pub const INDEX_HEADER_SIZE: u64 = 128;   // index entry area fixed start
```

> 新建 v1 文件时 data Block 区起点为 124, index Entry 区起点为 128。打开已有 data 文件时, Block 区起点必须使用文件 header 中计算出的 `header_len`; 打开 index 文件时仍解析可变 header 内容, 但 Entry 区起点固定为 128。
> `wrote_position` 是文件内绝对偏移。data 运行时可派生 `data_wrote_position = wrote_position - header_len` 作为数据区相对已用字节数; index 运行时可派生 `wrote_count = (wrote_position - 128) / INDEX_ENTRY_SIZE`。

#### 兼容性设计

| 场景 | 行为 |
|------|------|
| v1 reader 读 v1 文件 | 正常读取, 解析已知 meta type, 跳过未知 |
| v2 reader 读 v1 文件 | 读 `meta_length` 跳过未知 meta; 读 `state_length` 对齐 state |
| v1 reader 读 v2 文件 | 读固定前缀 (9B) + `meta_length` 定位 state + `state_length` 计算 `header_len`, 解析已知 state 字段 |
| 未来添加新 meta 字段 | 增加新 TLV type, `meta_length` 增加, 旧版本通过 length 跳过 |
| 未来添加新 state 字段 | 增加 state 条目, `state_length` 增加, 旧版本只读前 N 个 8B |
| 数据段 vs 索引段区分 | 通过 `fileType` 字段 (byte 6) 选择已知 state 字段集合, 不用固定 HEADER_SIZE 推导数据区 |

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

// Store 层只接受非空且匹配 ^[0-9A-Za-z_-]+$ 的 name/type。
// name/type 直接作为目录组件, 不做 escaping。

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

/// Store 配置: 后台/缓存等运行时设置 + 新建 DataSet 默认值
pub struct StoreConfig {
    pub flush_interval: Duration,    // 默认 15 秒 (dirty segment mmap sync, 不密封/不压缩)
    pub idle_timeout: Duration,      // 默认 30 分钟 (sync + unmmap + close, 不改变 pending)
    pub data_segment_size: u64,      // 新建 DataSet 默认 64MB
    pub index_segment_size: u64,     // 新建 DataSet 默认 4MB
    pub initial_data_segment_size: u64,  // 新建 DataSet 默认 256KB
    pub initial_index_segment_size: u64, // 新建 DataSet 默认 4KB
    pub compress_level: u8,          // 新建 DataSet 默认 6
    pub cache_max_memory: usize,     // 读缓存池上限 (字节, 0=禁用, 默认 256MB)
    pub cache_idle_timeout: Duration, // 缓存块空闲超时 (默认 30 分钟)
    pub enable_journal: bool,        // 是否启用内置 .journal/logs (默认 true)
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(1800),     // 30 分钟
            data_segment_size: 64 * 1024 * 1024,         // 64MB
            index_segment_size: 4 * 1024 * 1024,         // 4MB
            initial_data_segment_size: 256 * 1024,       // 256KB
            initial_index_segment_size: 4 * 1024,        // 4KB
            compress_level: 6,
            cache_max_memory: 256 * 1024 * 1024,         // 256MB
            cache_idle_timeout: Duration::from_secs(1800), // 30 分钟
            enable_journal: true,
        }
    }
}

/// 数据集内部配置 (创建时来自 StoreConfig/DataSetConfigBuilder; 打开时来自 meta)
struct DataSetConfig {
    data_segment_size: u64,
    index_segment_size: u64,
    initial_data_segment_size: u64,
    initial_index_segment_size: u64,
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
const BLOCK_MAX_SIZE: u32            = 65536;  // 普通聚合 Block payload 固定上限

/// File type constants
const FILE_TYPE_INDEX: u8  = 1;
const FILE_TYPE_DATA: u8   = 2;

/// Meta TLV types (immutable, written once at creation)
const META_TYPE_CREATED_AT:     u8 = 0x01;  // i64 LE, unix ms
const META_TYPE_FILE_OFFSET:    u8 = 0x02;  // i64 LE
const META_TYPE_FILE_SIZE:      u8 = 0x03;  // u32 LE
const META_TYPE_COMPRESS_LEVEL: u8 = 0x04;  // u8

/// 数据段文件元数据头 (DataFileHeader, v1 默认 124 bytes)
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
    meta_length: u16,
    state_length: u16,
    header_len: u64,
    // === State 可变 (每值固定 8 字节, 顺序存储, 9 个字段) ===
    min_timestamp: i64,         // 段内最小时间戳 (i64::MAX 表示空段)
    max_timestamp: i64,         // 段内最大时间戳 (i64::MIN 表示空段)
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    pending_block_offset: u64,
    pending_wrote_position: u64,
    pending_record_count: u64,
    invalid_record_count: u64,  // 段内无效记录数 (仅统计, 不触发 compaction)
}

/// 索引段文件元数据头 (IndexFileHeader, entry area 固定从 128 bytes 开始)
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
    meta_length: u16,
    state_length: u16,
    header_len: u64,
    // === State 可变 (仅 1 个字段) ===
    wrote_position: u64,
}

const DATA_HEADER_SIZE: u64 = 124;   // v1 默认数据段 header_len
const INDEX_HEADER_SIZE: u64 = 128;   // index entry area 固定起点

/// 索引条目
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    timestamp: i64,
    block_offset: u64,    // 数据区逻辑全局 offset
    in_block_offset: u16,
}
```

---

**相关**: [架构概览](architecture.md) | [元数据格式](meta-format.md) | [数据段管理](data-segment.md)

## P1-2 Active Contract: Segment Size And Compression Type

- All on-disk multi-byte integers use little-endian encoding.
- Dataset meta `data_segment_size` / `index_segment_size` are `u64 LE`.
- Data/index segment header immutable meta `file_size` is `u64 LE`; it records the configured max segment size, not the current lazy-allocated physical file length.
- The active format no longer has segment header `file_size:u32`. The project is still in initial development, so no old-layout compatibility shim is required.
- Dataset meta and data/index segment header immutable meta both store `compress_type:u8`.
- `compress_type = 0` means zstd and is the default for new stores, datasets, and segments. `compress_type = 1` means deflate.
- Unknown `compress_type` values must be rejected on open and before compress/decompress.
- `compress_level` remains `u8` and is interpreted by the selected compression algorithm.

Active segment header immutable TLV set:

| Meta Type (hex) | Name | Length | Type | Description |
|-----------------|------|--------|------|-------------|
| 0x01 | created_at | 8 | i64 LE | Creation time in unix milliseconds |
| 0x02 | file_offset | 8 | i64 LE | Data segment logical base offset or index segment base timestamp |
| 0x03 | file_size | 8 | u64 LE | Configured max segment size |
| 0x04 | compress_level | 1 | u8 | Compression level |
| 0x05 | compress_type | 1 | u8 | Compression algorithm: 0=zstd, 1=deflate |
