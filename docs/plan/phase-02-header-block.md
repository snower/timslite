# Phase 2: 文件头 + Block 核心

**目标**: FileMetadata 序列化/反序列化完成, BlockHeader 读写正确

---

## 2.1 header.rs — FileMetadata (100 字节, meta/state 分离)

- **常量定义**:
  ```rust
  pub const HEADER_SIZE: u64 = 100;
  pub const MAGIC: [u8; 4] = *b"TMSL";
  pub const VERSION: u16 = 1;
  pub const FILE_TYPE_DATA: u8 = 2;
  pub const FILE_TYPE_INDEX: u8 = 1;
  // Meta TLV types (immutable)
  pub const META_TYPE_CREATED_AT:     u8 = 0x01;
  pub const META_TYPE_FILE_OFFSET:    u8 = 0x02;
  pub const META_TYPE_FILE_SIZE:      u8 = 0x03;
  pub const META_TYPE_COMPRESS_LEVEL: u8 = 0x04;
  ```
- **文件头布局**:
  - 固定前缀(9B): magic(4) + version(2) + fileType(1) + meta_length(2)
  - Meta 不可变 TLV(33B): created_at + file_offset + file_size + compress_level
  - state_length(2B): 后续 state 总字节数
  - State 可变 7×8B(56B): wrote_position, record_count, total_uncompressed_size,
    invalid_record_count, pending_block_offset(u64::MAX=无),
    pending_wrote_position, pending_record_count
- struct `FileMetadata` — meta/state 分离 Rust 结构
- `fn write_to(&self, mmap: &mut MmapMut)` — 序列化: 前缀 + TLV + state
- `fn read_from(&mmap: &Mmap) -> Result<Self>` — 反序列化
  - 校验 magic/version, 读取 meta_length, 解析已知 TLV 类型, 跳过未知
  - 读取 state_length, 解析 7×8B state 值
  - 未来版本通过 meta_length/state_length 安全跳过未知
- `fn create_default(file_type: u8, file_offset: i64, file_size: u32)` — 创建新 header
- `fn update_state(&mut self, mmap: &mut MmapMut, pos: u64, count: u64, uncomp: u64)`
- `fn update_pending(&mut self, mmap: &mut MmapMut, offset: u64, wrote: u64, count: u64)`
- `fn clear_pending(&mut self, mmap: &mut MmapMut)` — pending_block_offset=u64::MAX

## 2.2 block.rs — BlockHeader (16字节)

```rust
pub const BLOCK_HEADER_SIZE: u64 = 16;
pub const BLOCK_FLAG_COMPRESSED: u16 = 0x0001;
pub const BLOCK_FLAG_SEALED: u16 = 0x0002;
pub const BLOCK_FLAG_SINGLE_RECORD: u16 = 0x0004;
```
- struct `BlockHeader`
- `fn write_to(&self, mmap: &mut [u8], pos: usize)`
- `fn read_from(mmap: &[u8], pos: usize) -> BlockHeader`
- `fn is_compressed(&self) -> bool` / `fn is_sealed(&self) -> bool` / `fn is_single_record(&self) -> bool`

## 2.3 compress.rs — 压缩封装

- `fn deflate_compress(data: &[u8], level: u8) -> Vec<u8>` — 使用 `miniz_oxide::deflate::compress_to_vec`
- `fn deflate_decompress(data: &[u8]) -> Result<Vec<u8>>` — 使用 `miniz_oxide::inflate::decompress_to_vec`
- `fn should_use_compressed(compressed: &[u8], original: &[u8]) -> bool` — `compressed.len() < original.len()`

## 验收标准

- [x] header.rs: 创建→写入→读取, 所有 meta TLV 和 state roundtrip 一致 (7 个测试全部通过)
- [x] header.rs: 未来版本兼容性 — 未知 TLV type 被正确跳过
- [x] header.rs: HEADER_SIZE = 100
- [x] block.rs: 写入→读取, flags 测试 (compress, sealed, single_record)
- [x] compress.rs: deflate roundtrip 测试, 压缩率测试
- [x] `cargo test --lib` all pass
- [x] `cargo clippy` clean

---

**导航**: [← Phase 1](phase-01-skeleton.md) | [→ Phase 3](phase-03-datasegment.md)
