# DataSetMeta - 数据集元数据格式

## 二点五、DataSetMeta: 数据集元数据文件

每个数据集目录 (`{name}/{type}/meta`) 存在唯一的 `meta` 文件, 记录数据集的**不可变**配置参数。
该文件在数据集**首次创建时写入一次**, 之后**永不更新**。

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

### Rust 类型

```rust
pub struct DataSetMeta {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub create_time: i64,        // unix ms
    pub index_continuous: bool,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
}

impl DataSetMeta {
    /// 创建新的 meta (用于新数据集, 不可变, 写入后不再修改)
    pub fn new(data_segment_size: u64, index_segment_size: u64,
               compress_level: u8) -> Self;

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

### 打开时校验

`DataSet::open()` 流程:
1. 检查 `{base_dir}/meta` 是否存在
2. 不存在 → **返回错误 `DatasetNotFound`**
3. 存在 → `DataSetMeta::read_from_file()` → 校验不可变参数
   - `data_segment_size` 不一致 → **返回错误** (影响文件布局, 不可兼容)
   - `index_segment_size` 不一致 → **返回错误** (影响索引文件布局, 不可兼容)
   - `compress_level` 不一致 → **仅读取使用** (meta 值为准, 不可修改)
   - `index_continuous` 不一致 → **仅日志警告** (不影响已有数据)
   - `initial_*` 不一致 → **仅日志警告** (仅影响新分段创建, 不破坏已有数据)

### 向前兼容

- 旧版本库读取新 meta 文件时, 通过 TLV length 字段跳过未知 type
- 新版本库读取旧 meta 文件时, 缺失的字段使用默认值

---

**返回**: [架构概览](architecture.md) | [数据模型](data-model.md)
