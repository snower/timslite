# Phase 1: 项目骨架 + 基础工具

**目标**: 搭建项目结构, 编译通过, 基础工具函数就绪

---

## 1.1 初始化 Rust 项目

- 创建 `cargo init --lib timslite`
- 配置 `Cargo.toml`:
  - `[lib] crate-type = ["cdylib", "rlib"]`
  - 添加依赖: `memmap2 = "0.9"`, `miniz_oxide = "0.8"`, `log = "0.4"`, `libc = "0.2"`
  - `[dev-dependencies]`: `criterion = "0.5"`
  - `edition = "2021"`

## 1.2 创建模块目录结构

```
src/
├── lib.rs
├── store.rs
├── dataset.rs
├── cache.rs              # 新增: 全局读缓存池 (BlockCache)
├── meta.rs               # 新增: TLV meta file
├── segment/
│   ├── mod.rs
│   └── data.rs
├── block.rs
├── index/
│   ├── mod.rs
│   └── segment.rs
├── header.rs
├── ffi.rs
├── error.rs
├── compress.rs
├── config.rs             # StoreConfig + builder + DataSetConfig
├── util.rs
└── bg/
    ├── mod.rs
    ├── flush.rs
    └── idle.rs
```

## 1.3 util.rs — 字节工具

- `read_u16_le(&[u8; 2]) -> u16` / `write_u16_le(buf: &mut [u8], v: u16)`
- `read_u32_le(&[u8; 4]) -> u32` / `write_u32_le(buf: &mut [u8], v: u32)`
- `read_i64_le(&[u8; 8]) -> i64` / `write_i64_le(buf: &mut [u8], v: i64)`
- `read_u64_le(&[u8; 8]) -> u64` / `write_u64_le(buf: &mut [u8], v: u64)`
- 便捷宏: `read_u16_from_mmap(&mmap, pos)`, `write_u32_to_mmap(&mut mmap, pos, v)`

## 1.4 error.rs — 错误类型

```rust
#[derive(Debug)]
pub enum TmslError {
    Io(io::Error),
    InvalidMagic,
    InvalidVersion(u16),
    MmapError(String),
    CompressionError(String),
    DecompressionError(String),
    InvalidData(String),
    NotFound(String),
    AlreadyExists(String),
}
```
- `impl From<io::Error> for TmslError`
- `impl Display for TmslError`
- `pub type Result<T> = std::result::Result<T, TmslError>`

## 1.5 lib.rs — 入口

- re-export: `pub use error::{TmslError, Result}`
- `pub use store::Store`
- 模块声明
- 基础常量导出: `HEADER_SIZE`, `BLOCK_HEADER_SIZE`, `INDEX_ENTRY_SIZE`

## 1.6 StoreConfig — 可配置参数

```rust
pub struct StoreConfig {
    pub flush_interval: Duration,    // 默认 10 分钟 (600s)
    pub idle_timeout: Duration,      // 默认 30 分钟 (1800s)
    pub data_segment_size: u64,      // 默认 64MB
    pub index_segment_size: u64,     // 默认 4MB
    pub block_max_size: u32,         // 默认 64KB
    pub compress_level: u8,          // 默认 6
}
impl Default for StoreConfig { ... }
```
- Builder 模式: `StoreConfig::builder().flush_interval(...).build()`

## 1.7 meta.rs — 数据集不可变配置 (TLV)

- 常量定义:
  ```rust
  pub const META_MAGIC: [u8; 4] = *b"TMSM";
  pub const META_VERSION: u16 = 1;
  pub const META_TYPE_DATA_SEGMENT_SIZE:  u8 = 0x01;
  pub const META_TYPE_INDEX_SEGMENT_SIZE: u8 = 0x02;
  pub const META_TYPE_COMPRESS_LEVEL:     u8 = 0x03;
  pub const META_TYPE_CREATE_TIME:        u8 = 0x04;
  ```
- struct `DataSetMeta` (仅4个字段):
  - `data_segment_size: u64`, `index_segment_size: u64`
  - `compress_level: u8`, `create_time: i64`
- `fn new(data_seg_size, idx_seg_size, compress_level) -> Self`
- `fn to_bytes(&self) -> Vec<u8>` — 序列化: magic(4)+version(2)+meta_data_length(2)+TLV
- `fn from_bytes(buf: &[u8]) -> Result<Self>` — 反序列化, 未知 type 跳过
- `fn write_to_file(&self, path: &Path) -> io::Result<()>`
- `fn read_from_file(path: &Path) -> Result<Self>`
- **无 `update_and_click`** — meta 创建后永不修改

## 验收标准

- [x] `cargo build` 通过, 生成 .dll/.so
- [x] `cargo test` 至少 1 个 test pass
- [x] util.rs 所有 endian 函数单元测试通过
- [x] error.rs 所有 From impl 覆盖
- [x] meta.rs TLV roundtrip 测试通过 (4 个测试全部通过)

---

**导航**: [← 概览](overview.md) | [→ Phase 2](phase-02-header-block.md)
