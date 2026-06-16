# DataSetMeta - 数据集元数据格式

## 二点五、DataSetMeta: 数据集元数据文件

每个数据集目录 (`{name}/{type}/meta`) 存在唯一的 `meta` 文件, 记录数据集的**不可变**创建参数。
该文件在数据集**首次创建时写入一次**, 之后**永不更新**。

`DataSetMeta` 是打开已有数据集时的唯一持久化真源。`StoreConfig` 和 FFI config 只提供创建新数据集时的默认值以及后台任务、缓存等运行时配置; `DataSet::open()` 不拿当前 `StoreConfig` 默认值与 meta 做“不一致”比较。

`block_max_size` 不属于 `DataSetMeta`: 普通聚合 Block 的 payload 上限固定为代码/文件格式常量 `BLOCK_MAX_SIZE = 65536`。该值与 `IndexEntry.in_block_offset: u16` 的哨兵空间绑定, 不可按数据集配置修改; 未来若要改变必须升级文件格式版本。

### 文件格式

```
┌─────────────────────────────────────────────────────┐
│ 固定头 (8 bytes)                                     │
├─────────────────────────────────────────────────────┤
│  magic: [u8; 4] = *b"TMSM"                          │
│  version: u16 = 1                                   │
│  meta_data_length: u16                              │
│    后续 TLV meta_values 的总字节数                   │
├─────────────────────────────────────────────────────┤
│ TLV meta_values (meta_data_length bytes)             │
│  每 一 个: {type: u8}{length: u16}{value: bytes}    │
│  解析: 读取 type, 读取 length → 跳过未知 type        │
└─────────────────────────────────────────────────────┘
```

`meta_data_length` 是 `u16 LE`, 因此 meta_values 最大 65535 字节。当前 v1 固定 meta_values 长度为 82 字节。journal enabled 且该 dataset 自身允许 journal 时, create/drop 日志会把 meta 文件固定 8 字节头之后的 meta_values 作为 metadata 字段 value; Store 必须在主 create/drop 操作前校验该 snapshot 可被 journal record 编码。

### TLV (Type-Length-Value) 编码

```rust
/// TLV meta value type
const META_TYPE_DATA_SEGMENT_SIZE: u8  = 0x01;  // u64 LE
const META_TYPE_INDEX_SEGMENT_SIZE: u8 = 0x02;  // u64 LE
const META_TYPE_COMPRESS_LEVEL: u8     = 0x03;  // u8
const META_TYPE_CREATE_TIME: u8        = 0x04;  // i64 LE (unix ms)
const META_TYPE_INDEX_CONTINUOUS: u8   = 0x05;  // u8 (0=非连续, 1=连续)
const META_TYPE_INITIAL_DATA_SEGMENT_SIZE: u8 = 0x06;  // u64 LE
const META_TYPE_INITIAL_INDEX_SEGMENT_SIZE: u8 = 0x07; // u64 LE
const META_TYPE_RETENTION_WINDOW: u8   = 0x08;  // u64 LE (timestamp unit)
const META_TYPE_COMPRESS_TYPE: u8      = 0x09;  // u8 (0=zstd, 1=deflate)
const META_TYPE_ENABLE_JOURNAL: u8     = 0x0A;  // u8 (0=false, 1=true)
```

### TLV 类型定义

| Type (hex) | 名称 | 长度 | 数据类型 | 说明 |
|------------|------|------|---------|------|
| 0x01 | data_segment_size | 8 | u64 LE | 数据段文件大小(字节) |
| 0x02 | index_segment_size | 8 | u64 LE | 索引段文件大小(字节) |
| 0x03 | compress_level | 1 | u8 | 压缩级别 |
| 0x04 | create_time | 8 | i64 LE | 数据集创建时间(unix ms) |
| 0x05 | index_continuous | 1 | u8 | 0=非连续, 1=连续存储 |
| 0x06 | initial_data_segment_size | 8 | u64 LE | 数据分段初始大小 |
| 0x07 | initial_index_segment_size | 8 | u64 LE | 索引分段初始大小 |
| 0x08 | retention_window | 8 | u64 LE | 数据保留窗口 (timestamp unit, 0=不限) |
| 0x09 | compress_type | 1 | u8 | Compression algorithm: 0=zstd, 1=deflate |
| 0x0A | enable_journal | 1 | u8 | 是否记录本 dataset 的 journal, 0=false, 1=true |

> `block_max_size` 无 TLV type。普通聚合 Block 上限由 `BLOCK_MAX_SIZE=65536` 固定定义, 不是 dataset 创建参数。
> `retention_window` 磁盘编码为 `u64 LE`, 但有效范围是 `0..=i64::MAX`。builder、FFI config decode、dataset create 和 `DataSetMeta::from_bytes` 均必须拒绝超过 `i64::MAX` 的值, 避免与 signed timestamp 阈值计算发生 wrap 或错误过期。
> `enable_journal` 是 dataset 级不可变创建参数, 默认 `true`。新 meta 必须写入 canonical 值 `0` 或 `1`; 解析到其它值必须返回 `InvalidData`。缺失该 TLV 的旧 meta 按 `true` 处理。
>
> 所有多字节 TLV length/value 均为 Little Endian。时间类字段使用 signed `i64 LE` (`create_time`), size/count/duration 类字段使用 unsigned LE。解析时必须校验 TLV length 与字段类型长度一致; 未知 type 仅按 length 跳过, 但 length 不得越过 `meta_data_length` 边界。

### Rust 类型

```rust
pub struct DataSetMeta {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,       // 0=zstd, 1=deflate
    pub create_time: i64,        // unix ms
    pub index_continuous: u8,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub retention_window: u64,   // 数据保留窗口 (timestamp unit, 0=不限)
    pub enable_journal: bool,    // 是否记录本 dataset 的 journal, 默认 true
}

impl DataSetMeta {
    /// 创建新的 meta (用于新数据集, 不可变, 写入后不再修改)
    pub fn new(data_segment_size: u64, index_segment_size: u64,
               compress_level: u8, compress_type: u8, index_continuous: u8,
               initial_data_segment_size: u64, initial_index_segment_size: u64,
               retention_window: u64, enable_journal: bool) -> Self;

    /// 序列化: magic + version + meta_data_length + TLV values
    pub fn to_bytes(&self) -> Vec<u8>;

    /// 反序列化: 校验 magic → 读 version → 读 meta_data_length → 解析 TLV
    /// 未知 type 自动跳过 (向前兼容)
    pub fn from_bytes(buf: &[u8]) -> Result<Self>;

    /// 写入文件 (创建时调用一次, 之后不再调用)
    pub fn write_to_file(&self, path: &Path) -> io::Result<()>;

    /// 从文件读取 (打开数据集时调用)
    pub fn read_from_file(path: &Path) -> Result<Self>;
}
```

### 写入时机

| 时机 | 操作 |
|------|------|
| 首次创建数据集 | `DataSetMeta::new()` + `write_to_file()` — **仅此一次** |
| 数据集已存在 | **不写入, 不更新** |

### 打开时读取与校验

`DataSet::open()` 流程:
1. 检查 `{base_dir}/meta` 是否存在
2. 不存在 → **返回错误 `DatasetNotFound`**
3. 存在 → `DataSetMeta::read_from_file()` → 解析并校验 meta 自身
   - `magic` 不匹配、TLV 越界或文件截断 → 返回错误
   - 缺失或为 0 的 `data_segment_size` / `index_segment_size` → 返回错误
   - `compress_level > 9` → 返回错误
   - `index_continuous` 不为 0/1 → 返回错误
   - `initial_data_segment_size == 0` 的旧 meta → 兼容为 `data_segment_size`
   - `initial_index_segment_size == 0` 的旧 meta → 兼容为 `index_segment_size`
   - `initial_* > segment_size` → 返回错误
   - `enable_journal` 缺失 → 兼容为 `true`
   - `enable_journal` 值不为 0/1 → 返回错误

读取成功后, `DataSet` 的 layout/压缩/索引/retention/journal 配置全部来自 meta。当前 `StoreConfig` 中的 data/index segment 默认值、压缩默认值、retention 默认值或 dataset journal 默认值不会覆盖已存在数据集, 也不会触发“不一致”错误。

### 向前兼容

- 旧版本库读取新 meta 文件时, 通过 TLV length 字段跳过未知 type
- 新版本库读取旧 meta 文件时, 缺失的字段使用默认值

---

**返回**: [架构概览](architecture.md) | [数据模型](data-model.md)

## P1-2 Active Contract: `compress_type` And u64 Segment Size

The active dataset meta format is not backward-compatible with earlier draft files because the project is still in initial development.

- `data_segment_size` and `index_segment_size` are `u64 LE` and remain required.
- `compress_type` is a required immutable TLV for new dataset meta files.
- `compress_type = 0` means zstd and is the default.
- `compress_type = 1` means deflate.
- Unknown `compress_type` values are invalid.
- Segment file headers must copy the dataset `compress_type` into their immutable meta TLV so a segment can be decoded according to its own header.
- `compress_level` remains a per-dataset immutable level and is interpreted by the selected algorithm. The current persisted range is `0..=9`; values greater than `9` are invalid on open.

Active dataset meta TLV set:

| Type (hex) | Name | Length | Type | Description |
|------------|------|--------|------|-------------|
| 0x01 | data_segment_size | 8 | u64 LE | Data segment max size |
| 0x02 | index_segment_size | 8 | u64 LE | Index segment max size |
| 0x03 | compress_level | 1 | u8 | Compression level |
| 0x04 | create_time | 8 | i64 LE | Dataset creation time in unix milliseconds |
| 0x05 | index_continuous | 1 | u8 | 0=non-continuous, 1=continuous |
| 0x06 | initial_data_segment_size | 8 | u64 LE | Initial lazy allocation size for data segments |
| 0x07 | initial_index_segment_size | 8 | u64 LE | Initial lazy allocation size for index segments |
| 0x08 | retention_window | 8 | u64 LE | Retention window in timestamp units, 0=no limit |
| 0x09 | compress_type | 1 | u8 | Compression algorithm: 0=zstd, 1=deflate |
| 0x0A | enable_journal | 1 | u8 | Dataset journal recording flag: 0=false, 1=true |

The active `META_VALUES_LEN_V1` is 82 bytes: six `u64/i64` entries at 11 bytes each and four `u8` entries at 4 bytes each.
