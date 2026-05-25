# timslite 开发计划

> 基于 design.md 详细设计  
> 目标: 完成 Rust cdylib 时序数据存储库, 提供 C ABI FFI

---

## 总体里程碑

```
Phase 1: 项目骨架 + 基础工具     ✅ (含 meta.rs)
Phase 2: 文件头 + Block 核心     ✅ (100B meta/state 分离)
Phase 3: DataSegment 写入/读取   ✅ (u64::MAX pending, data/ 子目录)
Phase 4: 时间索引系统            ✅ (index/ 子目录)
Phase 5: DataSegmentSet + DataSet✅ (路径 + meta 文件)
Phase 6: Store 门面 + 后台任务   ✅ (适配路径变更)
Phase 7: FFI 接口                ✅
Phase 8: 集成测试 + 性能调优     ✅ (57 单元 + 5 集成测试全部通过)
```

**全部 8 个 Phase 完成! HEADER_SIZE = 100B, meta/state 分离, data/ + index/ 子目录, meta TLV 文件**

**全部 8 个 Phase 完成, 但需根据新设计调整: data/ 子目录, index/ 子目录, meta 文件。**

---

## 目录结构变更 (核心)

```
旧: {data_dir}/{name}/{type}/
    ├── {segment_files}     ← 数据段直接在 type/ 下
    ├── .index/             ← 索引目录带前导点
    └── ...

新: {data_dir}/{name}/{type}/
    ├── meta                ← 新增: TLV 元数据文件
    ├── data/               ← 新增: 数据段子目录
    │   └── {segment_files}
    └── index/              ← 重命名: 无前导点
        └── {segment_files}
```

---

## Phase 1: 项目骨架 + 基础工具

**目标**: 搭建项目结构, 编译通过, 基础工具函数就绪

### ✅ 1.1 初始化 Rust 项目
- 创建 `cargo init --lib timslite`
- 配置 `Cargo.toml`:
  - `[lib] crate-type = ["cdylib", "rlib"]`
  - 添加依赖: `memmap2 = "0.9"`, `miniz_oxide = "0.8"`, `log = "0.4"`, `libc = "0.2"`
  - `[dev-dependencies]`: `criterion = "0.5"`
  - `edition = "2021"`

### ✅ 1.2 创建模块目录结构
```
src/
├── lib.rs
├── store.rs
├── dataset.rs
├── meta.rs               # 新增: TLV meta file (magic+version+meta_data_length+TLV)
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
├── config.rs             # StoreConfig + builder + DataSetConfig (internal)
├── util.rs
└── bg/
    ├── mod.rs
    ├── flush.rs
    └── idle.rs
```

### ✅ 1.3 util.rs - 字节工具
- `read_u16_le(&[u8; 2]) -> u16`
- `write_u16_le(buf: &mut [u8], v: u16)`
- `read_u32_le(&[u8; 4]) -> u32`
- `write_u32_le(buf: &mut [u8], v: u32)`
- `read_i64_le(&[u8; 8]) -> i64`
- `write_i64_le(buf: &mut [u8], v: i64)`
- `read_u64_le(&[u8; 8]) -> u64`
- `write_u64_le(buf: &mut [u8], v: u64)`
- 便捷宏: `read_u16_from_mmap(&mmap, pos)`, `write_u32_to_mmap(&mut mmap, pos, v)`

### ✅ 1.4 error.rs - 错误类型
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
}
```
- `impl From<io::Error> for TmslError`
- `impl Display for TmslError`
- `pub type Result<T> = std::result::Result<T, TmslError>`

### ✅ 1.5 lib.rs - 入口
- re-export: `pub use error::{TmslError, Result}`
- `pub use store::Store`
- 模块声明
- 基础常量导出: `HEADER_SIZE`, `BLOCK_HEADER_SIZE`, `INDEX_ENTRY_SIZE`

### ✅ 1.6 StoreConfig - 可配置参数
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
- 提供 builder 模式: `StoreConfig::builder().flush_interval(...).build()`

### ✅ 1.7 meta.rs - 数据集不可变配置 (TLV)
- 常量定义:
  ```rust
  pub const META_MAGIC: [u8; 4] = *b"TMSM";
  pub const META_VERSION: u16 = 1;
  pub const META_TYPE_DATA_SEGMENT_SIZE:  u8 = 0x01; // u64 LE
  pub const META_TYPE_INDEX_SEGMENT_SIZE: u8 = 0x02; // u64 LE
  pub const META_TYPE_COMPRESS_LEVEL:     u8 = 0x03; // u8
  pub const META_TYPE_CREATE_TIME:        u8 = 0x04; // i64 LE (unix ms)
  ```
- struct `DataSetMeta` (仅4个不可变字段):
  - `data_segment_size: u64`, `index_segment_size: u64`
  - `compress_level: u8`, `create_time: i64`
- `fn new(data_seg_size, idx_seg_size, compress_level) -> Self` — 创建时写入, 之后永不更新
- `fn to_bytes(&self) -> Vec<u8>` — 序列化: magic(4)+version(2)+meta_data_length(2)+TLV
- `fn from_bytes(buf: &[u8]) -> Result<Self>` — 反序列化, 校验 magic, 解析 TLV, 未知 type 跳过
- `fn write_to_file(&self, path: &Path) -> io::Result<()>`
- `fn read_from_file(path: &Path) -> Result<Self>`
- **无 `update_and_write`** — meta 创建后永不修改

### ✅ Phase 1 验收标准
- `cargo build` 通过, 生成 .dll/.so
- `cargo test` 至少 1 个 test pass
- util.rs 所有 endian 函数单元测试通过
- error.rs 所有 From impl 覆盖
- meta.rs TLV roundtrip 测试通过 **(4 个测试全部通过)**

---

## Phase 2: 文件头 + Block 核心

**目标**: FileMetadata 序列化/反序列化完成, BlockHeader 读写正确

### ✅ 2.1 header.rs - FileMetadata (100 字节, meta/state 分离)
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
  - 校验 magic/version
  - 读取 `meta_length`, 解析已知 TLV 类型, 跳过未知
  - 读取 `state_length`, 解析 7×8B state 值
  - 未来版本通过 meta_length/state_length 安全跳过未知
- `fn create_default(file_type: u8, file_offset: i64, file_size: u32)` — 创建新 header
- `fn update_state(&mut self, mmap: &mut MmapMut, pos: u64, count: u64, uncomp: u64)` — 更新 wrote_position/record_count/total_uncompressed_size
- `fn update_pending(&mut self, mmap: &mut MmapMut, offset: u64, wrote: u64, count: u64)` — 更新 pending state
- `fn clear_pending(&mut self, mmap: &mut MmapMut)` — pending_block_offset=u64::MAX

### ✅ 2.2 block.rs - BlockHeader (16字节)
- 常量定义:
  ```rust
  pub const BLOCK_HEADER_SIZE: u64 = 16;
  pub const BLOCK_FLAG_COMPRESSED: u16 = 0x0001;
  pub const BLOCK_FLAG_SEALED: u16 = 0x0002;
  pub const BLOCK_FLAG_SINGLE_RECORD: u16 = 0x0004;
  ```
- struct `BlockHeader`
- `fn write_to(&self, mmap: &mut [u8], pos: usize)`
- `fn read_from(mmap: &[u8], pos: usize) -> BlockHeader`
- `fn is_compressed(&self) -> bool`
- `fn is_sealed(&self) -> bool`
- `fn is_single_record(&self) -> bool`

### ✅ 2.3 compress.rs - 压缩封装
- `fn deflate_compress(data: &[u8], level: u8) -> Vec<u8>`
  - 使用 `miniz_oxide::deflate::compress_to_vec`
  - level 映射到 miniz_oxide 级别
- `fn deflate_decompress(data: &[u8]) -> Result<Vec<u8>>`
  - 使用 `miniz_oxide::inflate::decompress_to_vec`
- `fn should_use_compressed(compressed: &[u8], original: &[u8]) -> bool`
  - `compressed.len() < original.len()`

### ✅ Phase 2 验收标准
- header.rs: 创建→写入→读取, 所有 meta TLV 和 state roundtrip 一致 **(7 个测试全部通过)**
- header.rs: 未来版本兼容性 — 未知 TLV type 被正确跳过
- header.rs: HEADER_SIZE = 100
- block.rs: 写入→读取, flags 测试 (compress, sealed, single_record)
- compress.rs: deflate roundtrip 测试, 压缩率测试
- `cargo test --lib` all pass
- `cargo clippy` clean

---

## Phase 3: DataSegment 写入/读取 (核心)

**目标**: DataSegment 完整的 Block 聚合写入、延迟压缩、懒加载生命周期、恢复逻辑

### ✅ 3.1 DataSegment 结构定义 (segment/data.rs)
```rust
pub struct DataSegment {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    pub mmap: Option<MmapMut>,      // None = closed
    lifecycle: SegmentLifecycle,     // Closed / OpenReady
    last_accessed_at: Instant,       // 最近读写时间
    // Pending Block 状态 (从 header state 读取)
    pending_block_offset: Option<u64>,  // u64::MAX = no pending
    pending_wrote_position: u64,
    pending_record_count: u64,
}
```

pub enum SegmentLifecycle {
    Closed,          // 文件未打开
    OpenReady,       // 打开中, 可读写
    OpenIdle,        // 即将关闭 (idle timeout 触发)
}

const BLOCK_HEADER_SIZE: u64 = 16;
```

### ✅ 3.2 DataSegment 创建与打开
- `fn create(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 创建/截断文件到 file_size
  - mmap (MmapMut)
  - 写入 FileMetadata (HEADER + data_start=100)
  - 初始化所有计数为 0, lifecycle = OpenReady
  - pending_block_offset = u64::MAX (无 pending)
- `fn open(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 打开文件 (不截断)
  - mmap
  - 读取 FileMetadata, 校验 magic/version
  - 恢复 wrote_position, record_count, total_uncompressed_size
  - 恢复 pending_block 状态
  - **pending 恢复**: 如果 `pending_block_offset != u64::MAX`:
      1. 在 pending_block_offset 处密封 block (flags = SEALED, 不压缩)
      2. 清除 header pending state: pending_block_offset=u64::MAX
      3. flush file header 到 mmap
      4. wrote_position 指向 sealed block 之后

### ✅ 3.3 DataSegment 生命周期管理
- `fn ensure_open(&mut self) -> Result<()>` — lazily open if closed
- `fn idle_close(&mut self) -> Result<()>` — idle timeout 触发:
  1. `mmap.flush()` (MS_SYNC)
  2. 如果 pending_block_offset.is_some():
     - 密封 pending block (flags = SEALED, 不压缩)
  3. 更新 header: clear pending fields
  4. `munmap` + close file
  5. Set lifecycle = Closed, mmap = None
- `fn sync(&mut self) -> Result<()>` — flush loop 调用:
  - 如果 mmap.is_some(): `mmap.flush()` (MS_SYNC)
  - **不密封 pending, 不压缩**

### ✅ 3.4 核心写入逻辑
### ✅ 3.5 方法: write_raw_record_to_pending
### ✅ 3.6 方法: create_pending_and_append
### ✅ 3.7 方法: seal_pending_block
### ✅ 3.8 方法: create_single_record_block
### ✅ 3.9 读取逻辑
### ✅ 3.10 sync 方法 (flush loop 调用)
- `fn sync(&mut self) -> Result<()>`
  - `self.mmap.flush()` (MS_SYNC)
  - **不密封 pending block, 不压缩**
  - 更新 last_accessed_at

### ✅ Phase 3 验收标准
- 集成测试: 创建 DataSegment → 写入 1000 条 record → 全部逐条读取, 数据一致
- 集成测试: 写入 record 触发 block 切换 (>64KB) → 验证多 block 写入读取
- 集成测试: 写入 record > 64KB → 独占 block → 读取验证
- 集成测试: block 溢出 → 密封+压缩 → 验证 compression flag 正确
- 集成测试: idle_close → 验证 pending block 密封 (不压缩) → munmap → reopen → pending 已密封, 数据可读
- 集成测试: sync → 验证 mmap 内容同步到磁盘, pending block 不变
- 集成测试: crash 模拟 (不 sync) → reopen → pending block 恢复+密封, 数据部分可恢复
- 集成测试: create → 写入部分 → close → reopen → 验证 wrote_position 恢复, pending 状态恢复
- 目录验证: 数据文件保存在 `data/` 子目录下, 而非直接在 base_dir 下
- `cargo test --lib` all pass

---

## Phase 4: 时间索引系统

**目标**: TimeIndex + IndexSegment 完整实现, 支持按时间范围查询

### ✅ 4.1 IndexEntry 定义 (index/mod.rs)
### ✅ 4.2 IndexSegment 结构 (index/segment.rs)
### ✅ 4.3 IndexSegment 创建/打开/生命周期
### ✅ 4.4 IndexSegment 写入
### ✅ 4.5 IndexSegment 查询 (二分查找)
### ✅ 4.6 TimeIndex 结构 (index/mod.rs)
### ✅ 4.7 TimeIndex 写入
### ✅ 4.8 TimeIndex 查询
### ✅ 4.9 TimeIndex 加载
- `fn load_existing(base_dir: &Path, segment_size: u64) -> Result<Self>`
  - 扫描 `{base_dir}/index/*` 文件 (即 {data_dir}/{name}/{type}/index/)
  - 按 start_timestamp 排序加载

### ✅ Phase 4 验收标准
- 单元测试: IndexEntry bytes roundtrip
- 单元测试: IndexSegment append + 读 back 一致
- 单元测试: IndexSegment lower_bound / query_range 正确 (含边界: 空段, 全段, 超出范围)
- 集成测试: TimeIndex 写入 10000 entries → flush → reopen → query_range 验证
- 集成测试: in_memory_buffer threshold 触发 flush 测试
- `cargo test --lib` all pass

---

## Phase 5: DataSegmentSet + DataSet

**目标**: 多文件管理、懒打开/超时关闭、数据集完整 CRUD 流程

### ✅ 5.1 DataSegmentSet (segment/mod.rs)
### ✅ 5.2 DataSegmentSet 生命周期管理
### ✅ 5.3 DataSegmentSet 写入
### ✅ 5.4 DataSegmentSet 读取/查询
### ✅ 5.5 DataSegmentSet flush/load
### ✅ 5.6 DataSet 整合
### ✅ 5.7 DataSet 写入
### ✅ 5.8 DataSet 读取
### ✅ 5.9 DataSet flush/close
### ✅ 5.10 DataSet 加载
- `fn load(id: DataSetKey, base_dir: PathBuf, config: &StoreConfig) -> Result<Self>`
  - 确保 `{base_dir}/data/` 子目录存在
  - 确保 `{base_dir}/index/` 子目录存在
  - 读取/创建 `{base_dir}/meta` TLV 文件 (仅4个不可变字段, 创建一次永不更新)
  - 校验 meta 中的 `data_segment_size`, `index_segment_size` 与 config 一致性
  - 加载 DataSegmentSet (从 `data/` 子目录, 初始所有 segment closed)
  - 加载 TimeIndex (从 `index/` 子目录)
  - 恢复 last_used_at

### ✅ Phase 5 验收标准
- 集成测试: 创建 DataSet → 写入 5000 条(覆盖多个 segments/blocks) → query 全部
- 集成测试: 时间范围查询 (部分数据) → 验证数量和顺序
- 集成测试: close → reopen → 写入更多 → 验证所有数据可读
- 集成测试: 多数据集并行 (不同 name/type) → 数据完全隔离
- 集成测试: meta 文件创建 → roundtrip → data_segment_size/index_segment_size 不一致时拒绝打开
- 目录验证: 数据文件在 `data/` 下, 索引文件在 `index/` 下, meta 在 type/ 根下
- 不可变验证: 创建 meta 后再次打开, meta 文件内容未变

---

## Phase 6: Store 门面 + 后台任务

**目标**: Store 生命周期管理、数据集管理、后台 flush/idle (mmap 生命周期管理)

### ✅ 6.1 Store 结构
### ✅ 6.2 Store::open
### ✅ 6.3 Store::open_dataset
### ✅ 6.4 Store::close_dataset
### ✅ 6.5 Store::close (self)
### ✅ 6.6 bg/flush.rs - Flush 线程
### ✅ 6.7 bg/idle.rs - Idle Check 线程
### ✅ 6.8 bg/mod.rs - 后台管理
- `pub struct BackgroundTasks { flush_handle, idle_handle, shutdown_tx }`
- `fn start(datasets, flush_interval, idle_timeout) -> Self`
- `fn stop(self)` - 发送信号, join, 返回

### ✅ Phase 6 验收标准
- 集成测试: Store::open → open_dataset × 2 → write data → flush 10min 触发 sync → close → reopen → 数据仍在
- 集成测试: Store flush 循环只执行 msync, pending block 保持 raw
- 集成测试: Store idle check 在 30min 后关闭所有 segment, 释放 mmap
- 集成测试: idle-close 后 → write/read 操作 → on-demand reopen → pending 已密封, 数据一致
- 集成测试: Store::close 完整关闭所有资源, 无泄漏
- 目录验证: 所有数据集的 `data/` 和 `index/` 子目录正确创建
- `cargo test` all pass

---

## Phase 7: FFI 接口

**目标**: C ABI 接口完整实现, errno-safe, panic-safe

### ✅ 7.1 ffi.rs - 核心工具
### ✅ 7.2 FFI: Store 管理
### ✅ 7.3 FFI: 数据集管理
### ✅ 7.4 FFI: 数据写入
### ✅ 7.5 FFI: 查询迭代器
### ✅ 7.6 头文件生成 (.h)
- 创建 `include/timslite.h` C 头文件, 包含所有 FFI 声明

### ✅ Phase 7 验收标准
- 编译: `cargo build --release` → 生成 `libtimslite.dll`/`.so`
- C 程序链接测试: 编译 C 测试程序 → 链接 libtimslite → 运行
- FFI 测试: open → write × 100 → query → verify → close (全部 FFI 调用)
- 边界测试: 空 data_dir, 长 name, nullptr 参数 → 返回 -1, err_buf 有错误信息
- panic 测试: 触发 panic → FFI 返回 -1, 不崩溃

---

## Phase 8: 集成测试 + 性能调优

**目标**: 完整集成测试套件, 性能达标, 内存安全验证

### ✅ 8.1 端到端集成测试
### ✅ 8.2 单元测试补全
### ✅ 8.3 性能基准测试 (benches/)
### ✅ 8.4 内存安全验证
### ✅ 8.5 文档
- crate 级文档 (`//!`)
- 所有 public API 的 doc comments (`///`)
- README.md: 快速开始, FFI 示例, 目录结构说明

### ✅ Phase 8 验收标准
- `cargo test` 覆盖率 ≥ 80%
- 所有集成测试 pass
- 无内存泄漏 (valgrind clean 或等效)
- `cargo clippy -- -D warnings` clean
- `cargo doc` 无 warning
- README.md 完整

---

## 依赖关系图

```
Phase 1 (骨架+工具+StoreConfig+meta.rs)
    │
    ├─────────────────────────────┐
    ▼                             ▼
Phase 2 (文件头+Block)       Phase 1 (util.rs)
    │                             │
    ▼                             ▼
Phase 3 (DataSegment + 生命周期) ◄──── Phase 2 (BlockHeader + compress)
    │  - open/create               │
    │  - idle_close (sync + seal pending, no compress)     │
    │  - sync (msync only)         │
    │  - pending recovery on reopen │
    │  - 数据保存在 data/ 子目录      │
    │                             │
    ├──────┐                      │
    ▼      ▼                      ▼
Phase 4 (索引 + 生命周期)  Phase 3
    │  - sync, idle_close, ensure_open
    │  - 索引保存在 index/ 子目录
    └──┬───┘
       ▼
Phase 5 (DataSet + DataSegmentSet + lazy open/close + meta file)
       │
       ▼
Phase 6 (Store + 后台任务: flush 10min / idle 30min)
       │
       ▼
Phase 7 (FFI 接口)
       │
       ▼
Phase 8 (集成测试 + 性能 + idle-close 恢复测试 + 目录结构验证)
```

---

## 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| memmap2 在 Windows 上行为差异 | Phase 3 延迟 | 提前在 Windows 上做 mmap 原型验证 |
| miniz_oxide 压缩率不足 | Phase 3 压缩效果差 | 预留切换 zstd 的能力 |
| FFI panic 跨语言 | 崩溃调用方 | 所有 FFI 函数必须 `catch_unwind` |
| 大量数据集同时打开 | Phase 6 OOM | Store open 时初始所有 segment → closed, 30min idle-close 释放 mmap (即使未访问), 按需 lazy open |
| 索引 binary search 溢出 | 查询错误 | 边界条件充分测试 (0, 1, n entries) |
| pending block crash 恢复失败 | 数据丢失 | reopen 时完整校验 header 一致性, 密封 pending 但不压缩 |
| idle-close 后 reopen 性能 | 延迟增加 | mmap open 开销小 (<1ms), 可接受 |
| idle-check 竞态 (write between read-lock and write-lock) | 错误关闭活跃 dataset | double-check last_used_at after write-lock acquired |
| index segment 查询时需遍历所有段 (含 closed) | 查询延迟 | 时间范围过滤: skip 段时间范围不在查询区间内的段 |
| 10min flush 间隔过长 | crash 损失数据 | mmap 写入已有 OS page cache 保护, 实际风险可控 |
| meta 文件与 config 不一致 | 数据损坏风险 | data_segment_size / index_segment_size 不一致时直接拒绝打开; compress_level 不一致仅警告 |
| `.index` 迁移到 `index/` | 旧数据不可读 | 打开时检测 `.index/` 目录, 自动重命名为 `index/` (向后兼容迁移) |
| 数据文件迁移到 `data/` | 旧数据不可读 | 打开时检测 base_dir 下的 segment 文件, 自动移动到 `data/` 子目录 (向后兼容迁移) |

---

## 开发规范

1. **原子提交**: 每个 Phase 内的小任务独立提交
2. **TDD**: 先写测试, 再实现 (Phase 2+)
3. **clippy**: `cargo clippy -- -D warnings` 作为 pre-commit check
4. **doc**: 所有 public API 必须有 doc comment
5. **log**: 关键操作 (open/close/flush/error/idle-close/reopen) 必须有日志
6. **no unsafe (except FFI)**: 除 ffi.rs 外, 其他模块禁止 unsafe
7. **error handling**: 不 unwrap, 不 expect, 返回 Error 或 Result
8. **mmap safety**: 所有 mmap 操作必须确保文件句柄生命周期 ≥ mmap 生命周期, idle-close 必须先 munmap 再 close
9. **last_used_at**: 每次 write/query 操作必须更新 last_used_at, idle check 据此判断是否关闭 segment