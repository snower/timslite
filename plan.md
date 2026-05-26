# timslite 开发计划

> 基于 design.md 详细设计  
> 目标: 完成 Rust cdylib 时序数据存储库, 提供 C ABI FFI

---

## 总体里程碑

```
Phase 1: 项目骨架 + 基础工具     ✅ (含 meta.rs, AlreadyExists 错误)
Phase 2: 文件头 + Block 核心     ✅ (100B meta/state 分离)
Phase 3: DataSegment 写入/读取   ✅ (u64::MAX pending, data/ 子目录)
Phase 4: 时间索引系统            ✅ (index/ 子目录)
Phase 5: DataSegmentSet + DataSet✅ (create/open/drop 分离, 替代 new)
Phase 6: Store 门面 + 后台任务   ✅ (create_dataset/drop_dataset 分离)
Phase 7: FFI 接口                ☐ (待添加 create/drop)
Phase 8: 集成测试 + 性能调优     ☐ (待更新 create/open/drop 生命周期测试)
Phase 9: 读缓存池 (BlockCache)   ✅ (LRU + idle 回收, 解压 block 缓存)
Phase 10: 索引连续存储           ✅ (filler 条目, sentinel 值, mmap 覆盖写, meta TLV 扩展)
Phase 11: 连续模式 O(1) 查询优化 ✅ (直接计算索引位置, 消除二分查找)
Phase 12: 分段懒分配 + 倍率扩容 ✅ (初始大小创建, 2x 扩容, max=segment_size)
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
├── cache.rs              # 新增: 全局读缓存池 (BlockCache)
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
    AlreadyExists(String),   // 数据集已存在 (create 时返回)
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

**目标**: 多文件管理、懒打开/超时关闭、数据集完整 CRUD 流程 (create/open/close/drop)

### ✅ 5.1 DataSegmentSet (segment/mod.rs)
### ✅ 5.2 DataSegmentSet 生命周期管理
### ✅ 5.3 DataSegmentSet 写入
### ✅ 5.4 DataSegmentSet 读取/查询
### ✅ 5.5 DataSegmentSet flush/load
### ✅ 5.6 DataSet 整合

### ☐ 5.7 DataSet::create — 显式创建 (替代 new)
- `fn create(id, base_dir, data_segment_size, index_segment_size, compress_level, block_max_size) -> Result<Self>`
  - 检测 base_dir 是否已有 meta 文件 → 存在则返回 `AlreadyExists` 错误
  - 创建 `data/` 和 `index/` 子目录
  - 写入 meta 文件 (仅一次, 之后不可修改)
  - 初始化 DataSegmentSet (空, 首个 segment 未创建)
  - 初始化 TimeIndex (空, 首个 segment 未创建)
  - 记录 last_used_at
- 参数验证: data_segment_size > 0, index_segment_size > 0, compress_level 1-9, block_max_size <= 64KB

### ☐ 5.8 DataSet::open — 仅打开已有 (替代 new)
- `fn open(id, base_dir, block_max_size) -> Result<Self>`
  - 读取 meta 文件 → 不存在返回 `NotFound` 错误
  - 从 meta 读取 data_segment_size, index_segment_size, compress_level (不可设置)
  - 加载 DataSegmentSet (从 `data/` 子目录, 初始所有 segment closed)
  - 加载 TimeIndex (从 `index/` 子目录)
  - 恢复 last_used_at

### ☐ 5.9 DataSet::close — 关闭数据集
- `fn close(&mut self) -> Result<()>`
  - flush 所有 segments + index
  - idle_close_all segments + index
  - 更新 last_used_at

### ☐ 5.10 DataSet::drop_dataset — 删除数据集
- `fn drop_dataset(base_dir: &Path) -> Result<()>`
  - 删除整个 base_dir 目录 (含 data/ + index/ + meta)
  - 使用 `std::fs::remove_dir_all`

### ☐ 5.11 DataSet::write
### ☐ 5.12 DataSet::query
### ☐ 5.13 DataSet flush/close
### ☐ 5.14 DataSet 加载 (open_dataset 调用)
- `fn open(id: DataSetKey, base_dir: PathBuf, block_max_size: u32) -> Result<Self>`
  - 读取 `{base_dir}/meta` TLV 文件 (必须存在, 否则返回错误)
  - 从 meta 读取不可变参数, 构建 DataSetConfig
  - 加载 DataSegmentSet (从 `data/` 子目录, 初始所有 segment closed)
  - 加载 TimeIndex (从 `index/` 子目录)
  - 恢复 last_used_at

### ✅ Phase 5 验收标准
- 集成测试: `DataSet::create` → 检查目录和 meta 文件创建 → 写入 5000 条 → query 全部
- 集成测试: `DataSet::create` 对已存在数据集 → 返回 `AlreadyExists` 错误
- 集成测试: `DataSet::open` 对不存在数据集 → 返回 `NotFound` 错误
- 集成测试: `DataSet::open` 后写入更多数据 → close → reopen → 验证所有数据可读
- 集成测试: 时间范围查询 (部分数据) → 验证数量和顺序
- 集成测试: 多数据集并行 (不同 name/type) → 数据完全隔离
- 集成测试: meta 文件创建 → roundtrip → data_segment_size/index_segment_size 固定不可变
- 集成测试: `DataSet::drop_dataset` 删除后, 目录和所有文件不可访问
- 目录验证: 数据文件在 `data/` 下, 索引文件在 `index/` 下, meta 在 type/ 根下
- 不可变验证: 创建 meta 后再次 open, meta 文件内容未变, 参数从 meta 读取

---

## Phase 6: Store 门面 + 后台任务

**目标**: Store 生命周期管理、数据集管理(create/open/drop)、后台 flush/idle (mmap 生命周期管理)

### ✅ 6.1 Store 结构
### ✅ 6.2 Store::open
### ☐ 6.3 Store::create_dataset — 显式创建
- `fn create_dataset(&mut self, name, dataset_type, data_segment_size, index_segment_size, compress_level) -> Result<DataSetHandle>`
  - 检测 `{data_dir}/{name}/{type}/meta` 是否存在 → 存在返回 `AlreadyExists`
  - 调用 `DataSet::create(...)` 
  - 注册到 datasets HashMap, 返回 handle

### ☐ 6.4 Store::open_dataset — 仅打开已有
- `fn open_dataset(&mut self, name, dataset_type) -> Result<DataSetHandle>`
  - 检查是否已在内存中
  - 调用 `DataSet::open(...)` 从 meta 读取参数
  - 注册到 datasets HashMap, 返回 handle

### ☐ 6.5 Store::close_dataset — 关闭
### ☐ 6.6 Store::drop_dataset — 删除整个数据集
- `fn drop_dataset(&mut self, handle) -> Result<()>`
  - 先从 datasets HashMap 移除
  - 调用 `DataSet::drop_dataset(...)` 删除目录树
  - 清理 handle

### ☐ 6.7 Store::close (self)
### ☐ 6.8 bg/mod.rs - 单线程后台循环 (合并 flush + idle)
- 重构: 将 `bg/flush.rs` 和 `bg/idle.rs` 合并到 `bg/mod.rs` 中的单一循环
- `pub struct BackgroundTasks { handle: Option<JoinHandle<()>>, shutdown_tx: Option<mpsc::Sender<()>> }`
- `fn start(datasets, flush_interval, idle_timeout) -> Self`:
  - 创建单一 mpsc channel
  - 启动单一线程, 内部循环:
    - 计算 `next_flush = last_flush + flush_interval`
    - 计算 `next_idle = last_idle_check + idle_check_interval` (默认 60s)
    - `recv_timeout(min(next_flush, next_idle) - now)` 等待
    - 超时后检查: 如果到了 flush 时间 → 执行 flush; 如果到了 idle 时间 → 执行 idle check
- `fn stop(self)` - 发送信号, join, 返回
- **删除 `bg/flush.rs` 和 `bg/idle.rs`** (逻辑合并到 mod.rs)

### ✅ Phase 6 验收标准
- 集成测试: `Store::create_dataset` → 创建成功, `create_dataset` 再次调用 → `AlreadyExists`
- 集成测试: `Store::open_dataset` → 打开成功, `open_dataset` 对不存在数据集 → `NotFound`
- 集成测试: `Store::create_dataset` × 2 → write data → flush 10min 触发 sync → close → reopen → 数据仍在
- 集成测试: `Store::drop_dataset` → 删除后目录不可访问 → 重新 `create_dataset` 成功
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
### ☐ 7.2 FFI: Store 管理
### ☐ 7.3 FFI: 数据集管理 — create/open/close/drop
- `tmsl_dataset_create` — 创建数据集, 传入 data_segment_size/index_segment_size/compress_level
- `tmsl_dataset_open` — 打开已有数据集 (不传参数)
- `tmsl_dataset_close` — 关闭数据集
- `tmsl_dataset_drop` — 删除整个数据集目录
- `tmsl_dataset_flush` — flush 数据集
### ✅ 7.4 FFI: 数据写入
### ✅ 7.5 FFI: 查询迭代器
### ✅ 7.6 头文件生成 (.h)
- 创建 `include/timslite.h` C 头文件, 包含所有 FFI 声明
- 新增 `tmsl_dataset_create`, `tmsl_dataset_drop` 声明

### ✅ Phase 7 验收标准
- 编译: `cargo build --release` → 生成 `libtimslite.dll`/`.so`
- C 程序链接测试: 编译 C 测试程序 → 链接 libtimslite → 运行
- FFI 测试: `create` → write × 100 → query → verify → `close` → `open` → query → verify (全部 FFI 调用)
- FFI 测试: `create` 已存在 → 返回 -1, err_buf 有错误信息
- FFI 测试: `open` 不存在 → 返回 -1, err_buf 有错误信息
- FFI 测试: `drop` 后重新 `create` → write → query → verify
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
- 所有集成测试 pass (含 create/open/drop 生命周期测试)
- 无内存泄漏 (valgrind clean 或等效)
- `cargo clippy -- -D warnings` clean
- `cargo doc` 无 warning
- README.md 完整

---

## Phase 9: 读缓存池 (BlockCache)

**目标**: 全局读缓存池, LRU + idle 回收, 解压后 block payload 缓存

### ✅ 9.1 src/cache.rs — BlockCache 结构定义
### ✅ 9.2 BlockCache::get — 缓存查询
### ✅ 9.3 BlockCache::put — 缓存写入 + LRU 淘汰
### ✅ 9.4 BlockCache::evict_idle — 后台回收
### ✅ 9.5 读取流程集成
### ✅ 9.6 后台线程集成
### ✅ 9.7 StoreConfig 扩展
### ✅ Phase 9 验收标准
- 单元测试: put/get roundtrip, 命中/未命中计数 ✅
- 单元测试: LRU 淘汰 → used_memory ≤ max_memory × 85% ✅
- 单元测试: idle 回收 → 30min 后条目被移除 ✅
- 单元测试: `cache_max_memory=0` → put/get 无效果 ✅
- 集成测试: 所有 74 tests pass ✅

---

## Phase 10: 索引连续存储 (Index Continuous Storage)

**目标**: 索引条目按连续序号增长, 缺失时间戳填充哨兵值条目, 逆序写入统一拒绝

### ✅ 10.1 meta.rs 扩展 — 新增 TLV type 0x05
- 常量: `META_INDEX_CONTINUOUS: u8 = 0x05` (u8: 0=非连续, 1=连续)
- `DataSetMeta` 新增字段: `index_continuous: u8` (default=0)
- `DataSetMeta::new()` 新增参数 `index_continuous`, 写入 TLV
- `DataSetMeta::from_bytes()` 解析 type 0x05, 未知旧版本跳过

### ✅ 10.2 DataSetMeta/DataSetConfig 更新
- `DataSetMeta` struct: 新增 `index_continuous: u8` 字段
- `DataSetConfig` struct: 新增 `index_continuous: u8` 字段
- `DataSet::create()`: 新增 `index_continuous` 参数, 传入 meta + config
- `DataSet::open()`: 从 meta 读取 `index_continuous`
- `Store::create_dataset()`: 新增 `index_continuous` FFI/Rust API 参数
- `DataSetConfigBuilder` 新增 `index_continuous()` builder method

### ✅ 10.3 DataSet 写入逻辑更新
- `DataSet::write()`: 
  - 新增状态跟踪: `latest_written_timestamp: i64` (初始从 index segment 恢复)
  - 检查逆序: 
    - 非连续模式: `timestamp <= latest_written_timestamp` → `Error("out-of-order")`
    - 连续模式: 
      - `timestamp > latest_written_timestamp`: 填充缺失 + 正常写入
      - `timestamp < latest_written_timestamp`: 数据追加到最新段 + 替换匹配的 filler
      - `timestamp == latest_written_timestamp`: `Error("duplicate timestamp")`
  - 如果 `index_continuous == 1` 且 `timestamp > latest`:
    - 填充缺失: for ts in `(latest+1)..(timestamp-1)` → `time_index.add_filler_entry(ts)`
    - 然后写入真实 entry
  - 否则 (连续模式补数据):
    - 写入数据到 DataSegmentSet
    - `replace_filler_with_real(ts)` → mmap 覆盖写 18 字节 → 替换为真实 entry

### ✅ 10.3.1 IndexSegment: find_entry_index + overwrite_entry
- `IndexSegment::find_exact(timestamp)` 已存在, 返回 `IndexEntry` (副本)
- 新增: `IndexSegment::find_entry_index(timestamp) -> Option<usize>` — 返回 entry 在 segment 中的索引位置
- 新增: `IndexSegment::overwrite_entry(entry_index: usize, new_entry: &IndexEntry)`
  - 确保 mmap 有效
  - 计算 mmap 偏移: `HEADER_SIZE + entry_index * INDEX_ENTRY_SIZE`
  - 覆盖写 18 字节

### ✅ 10.4 TimeIndex 填充逻辑
- `TimeIndex::add_entry()` 保持不变 (仅追加, 不感知 filler)
- 新增: `TimeIndex::add_filler_entry(timestamp)` — 添加哨兵条目
- 填充循环在 `DataSet::write()` 层完成, 每次调用 `add_filler_entry()`
- 利用现有的 in_memory_buffer + flush 机制

### ✅ 10.5 Index Segment 跳过规则
- `TimeIndex::flush_to_disk()`:
  - flush 完成后调用 `remove_pure_filler_segments()`
  - 仅含 filler 的 segment: close + delete 文件 + 从 vec 移除
- Filler 识别: `block_offset == BLOCK_OFFSET_FILLER (0xFFFFFFFFFFFFFFFF)`

### ✅ 10.6 读取时 Filler 过滤
- `DataSet::query()`: 
  - 查询时跳过: `if entry.block_offset == BLOCK_OFFSET_FILLER { continue; }`

### ✅ 10.7 Timestamp = 0 保护
- `DataSet::write()`: 检查 `timestamp > 0`, 否则返回 `Error("timestamp must be > 0")`

### ✅ 10.8 重启恢复 latest_written_timestamp
- `DataSet::open()`:
  - `recover_latest_timestamp(&time_index)`: 扫描所有 index segments + buffer, 取最大 timestamp

### ✅ 10.9 FFI API 更新
- `tmsl_dataset_create`: ✅ 新增 `index_continuous: c_uchar` 参数
- `include/timslite.h`: ✅ 更新函数声明: 新增 `tmsl_dataset_create` (含 index_continuous), `tmsl_dataset_drop`
- 错误处理: 逆序写入返回 -1, err_buf 写错误信息

### ✅ Phase 10 验收标准
- 单元测试: meta TLV 0x05 roundtrip (创建→写入→读取) ✅
- 单元测试: 连续模式正序写入 ts=100 → ts=150 → filler 49 条, index 共 51 entries ✅ test_continuous_mode_filler_filling
- 单元测试: 连续模式补数据 ts=120 → filler 被替换 → 查询返回 3 条真实数据 ✅ test_continuous_mode_backfill_replaces_filler
- 单元测试: 连续模式补数据 ts=100 (对应真实 entry) → Error("already has real data") ✅ test_continuous_backfill_non_filler_rejected
- 单元测试: 连续模式补数据 ts=150 (等于 latest) → Error("duplicate timestamp") ✅ test_continuous_mode_duplicate_timestamp_rejected
- 单元测试: 非连续模式逆序写入 ts=100 → ts=50 → Error("out-of-order") ✅ test_noncontinuous_mode_out_of_order_rejected
- 单元测试: Filler 识别: `block_offset == 0xFFFFFFFFFFFFFFFF` → query 时正确跳过 ✅ (已验证于 test_continuous_mode_filler_filling 和 test_continuous_mode_backfill_replaces_filler)
- 单元测试: 大量填充 (跨 segment) → 仅含真实 entry 的 segment 被创建 ✅ (test_time_index_pure_filler_segments_removed)
- 单元测试: IndexSegment find_entry_index ✅, overwrite_entry ✅
- 集成测试: 连续模式创建→写入→close→reopen→补数据→写入→数据一致 ✅ test_continuous_open_recovery_latest_timestamp
- 集成测试: 非连续模式写入 ts=100 → ts=150 → 仅 2 entries (无 filler) ✅ (现有集成测试验证)
- 集成测试: timestamp≤0 写入 → Error ✅ test_timestamp_zero_rejected
- 集成测试: 所有 86 tests pass (77 unit + 9 integration) ✅

---

## Phase 11: 连续模式 O(1) 查询优化

**目标**: 连续模式下索引位置直接计算 (entry_index = target_ts - start_timestamp), 消除二分查找

### ✅ 11.1 IndexSegment: direct_lookup 方法
- 新增 `IndexSegment::direct_lookup(target_ts: i64) -> Option<IndexEntry>`
  - 检查范围: `target_ts < start_timestamp || target_ts >= start_timestamp + wrote_count` → None
  - 计算: `entry_index = (target_ts - start_timestamp) as usize`
  - 从 mmap 读取 8 字节 timestamp → 校验是否等于 `target_ts`
  - 匹配 → 读取完整 18 字节 entry → return Some(entry)

### ✅ 11.2 IndexSegment: 添加 *_cs 连续模式变体方法

**不修改现有二分查找方法** — 添加新 `*_cs` 方法, 内部根据 `index_continuous` 参数分支:

- ✅ `lower_bound_cs(target_ts, index_continuous: bool) -> usize` — 连续模式 O(1), 非连续二分查找
- ✅ `upper_bound_cs(target_ts, index_continuous: bool) -> usize` — 同上
- ✅ `find_exact_cs(target_ts, index_continuous: bool) -> Option<IndexEntry>` — 连续模式 direct_lookup, 非连续二分查找
- ✅ `find_entry_index_cs(target_ts, index_continuous: bool, wrote_count: Option<usize>) -> Option<usize>` — 连续模式 O(1), 支持外部 wrote_count (closed segments 范围检查)

### ✅ 11.3 `IndexSegmentMeta` 新增 `wrote_count` 字段
- ✅ `IndexSegmentMeta` 新增 `wrote_count: usize` 字段
- ✅ `TimeIndex::load_existing` 中从文件 header 读取 `record_count` (mmap 读取偏移 52)
- ✅ `idle_close_all()` 从 open segment 复制 wrote_count

### ✅ 11.4 `TimeIndex` 新增 `index_continuous: bool` 字段
- ✅ `TimeIndex::new()` 新增 `index_continuous` 参数 (默认 false)
- ✅ `TimeIndex::load_existing()` 新增 `index_continuous` 参数
- ✅ `TimeIndex` struct 新增 `index_continuous: bool` 字段

### ✅ 11.5 `TimeIndex::query` 更新为使用 `query_range_cs`
- ✅ `TimeIndex::query` 调用 `seg.query_range_cs(start_ts, end_ts, self.index_continuous)`

### ✅ 11.6 `DataSet::query` 传递 `index_continuous` 标志
- ✅ 自动传递 (TimeIndex::query 使用 self.index_continuous)

### ✅ 11.7 `replace_filler_with_real` 连续模式优化
- ✅ 对 open segments: 使用 `find_entry_index_cs` 直接计算 O(1)
- ✅ 对 closed segments: `meta.wrote_count` 范围检查 → 直接计算 entry_index (避免不必要的文件打开)

### ✅ 11.8 `DataSet::open` 传递 `index_continuous` 到 `TimeIndex`
- ✅ `DataSet::create()` → `TimeIndex::new(..., index_continuous != 0)`
- ✅ `DataSet::open()` → `TimeIndex::load_existing(..., config.index_continuous != 0)`

### ✅ Phase 11 验收标准
- ✅ 单元测试: `direct_lookup` — 范围内 O(1) 命中 (`test_index_segment_direct_lookup`)
- ✅ 单元测试: `direct_lookup` — 范围外正确返回 None
- ✅ 单元测试: `lower_bound_cs` 连续模式 vs 非连续模式结果一致性 (`test_index_segment_lower_bound`)
- ✅ 单元测试: `find_entry_index_cs` 与 `find_entry_index` 结果相同 (`test_index_segment_find_entry_index_cs`)
- ✅ 单元测试: non-continuous 模式使用 `*_cs` = 原有二分查找行为
- ✅ 单元测试: closed segment `find_entry_index_cs` 使用 `wrote_count` 范围检查
- ✅ 集成测试: 连续模式 query 正确性不变 (所有已有集成测试继续通过)
- ✅ 集成测试: 总 89 tests pass (80 unit + 9 integration)

---

## Phase 12: 分段文件懒分配与倍率扩容 (Lazy File Allocation + Doubling Expansion)

**目标**: 分段文件以初始大小创建, 写入过程中按 2 倍扩容, 上限为 segment_size。达到 segment_size 后密封 + 创建新段。优化小数据量场景磁盘空间使用。

### ✅ 12.1 meta.rs — 新增 TLV type 0x06, 0x07
- 常量:
  ```rust
  const META_INITIAL_DATA_SEGMENT_SIZE: u8 = 0x06; // u64 LE
  const META_INITIAL_INDEX_SEGMENT_SIZE: u8 = 0x07; // u64 LE
  ```
- `DataSetMeta` 新增字段:
  - `initial_data_segment_size: u64`
  - `initial_index_segment_size: u64`
- `DataSetMeta::new()` 新增参数: `initial_data_segment_size`, `initial_index_segment_size`
- `DataSetMeta::to_bytes()`: 写入 TLV type 0x06, 0x07
- `DataSetMeta::from_bytes()`: 解析 type 0x06, 0x07 (旧版本跳过)
- 单元测试: TLV roundtrip 含新字段
- 单元测试: 无 0x06/0x07 的旧 meta 数据, from_bytes 正常解析 (default=0 或合理默认)

### ✅ 12.2 config.rs — StoreConfig / DataSetConfig 扩展
- `StoreConfig` 新增字段:
  ```rust
  pub initial_data_segment_size: u64,    // 默认 256KB
  pub initial_index_segment_size: u64,    // 默认 4KB
  ```
- `StoreConfig::default()`: 设置默认值
- `StoreConfigBuilder`: 新增 `.initial_data_segment_size()`, `.initial_index_segment_size()`
- `DataSetConfig` 新增字段:
  ```rust
  pub initial_data_segment_size: u64,
  pub initial_index_segment_size: u64,
  ```
- `DataSetConfig::from_store()`: 从 StoreConfig 映射
- `DataSetConfigBuilder`: 新增对应 builder methods
- 单元测试: builder roundtrip, default 值验证

### ✅ 12.3 header.rs — FileMetadata 语义调整
- **无需新增方法** — 扩容时不更新 header
- `FileMetadata::create_default()` 的 `file_size` 参数传入 `max_file_size` (segment_size)
  - header 中 file_size 始终记录标准分段大小, 写入后不再变化
  - 打开时忽略 header 中的 file_size, 以磁盘实际大小为准
- 无需新增 `update_file_size_in_header` 方法

### ✅ 12.4 segment/data.rs — DataSegment 扩容支持
- `DataSegment` struct 新增字段:
  ```rust
  pub max_file_size: u64,  // 扩容上限 (segment_size)
  ```
- `DataSegment::create()` 签名变更:
  ```rust
  pub fn create(path: &Path, file_offset: u64, initial_size: u64, max_size: u64) -> Result<Self>
  ```
  - `file.set_len(initial_size)` (不再用 file_size=segment_size)
  - header file_size 写入 max_size (创建时一次性写入, 之后不变)
  - `self.file_size = initial_size` (内存中跟踪实际大小)
  - `self.max_file_size = max_size`
- `DataSegment::open()` 签名变更 (移除 file_size 外部传入参数):
  ```rust
  pub fn open(path: &Path, file_offset: u64, max_size: u64) -> Result<Self>
  ```
  - 内部通过 `file.metadata()?.len()` 获取 actual_file_size
  - 忽略 header 中的 file_size (始终为 max_size)
  - `self.file_size = actual_file_size`
  - `self.max_file_size = max_size`
- 新增 `DataSegment::expand(&mut self) -> Result<()>`:
  - 计算 target = min(file_size * 2, max_file_size)
  - 如果 target == file_size → 已达上限, 返回错误
  - unmap → file.set_len(target) → remap
  - 更新内存字段 `self.file_size = target` (不更新 header)
  - `self.mmap.flush()` (仅确保 header 和数据持久化)
- 修改 `append_record()`: 写入前检查空间
  ```rust
  // 检查是否有足够空间: file_size 是否容纳 wrote_position + record 需要的空间
  // 如果不足: 尝试 expand()
  // expand 失败 (已达上限): 由调用方处理 (密封当前段, 创建新段)
  ```
- 修改 `expand()` 中 mmap 重映射后的状态恢复 (wrote_position, pending 等)

### ✅ 12.5 segment/mod.rs — DataSegmentSet 扩容 + 新建段逻辑
- `DataSegmentSet` 新增字段:
  ```rust
  pub initial_segment_size: u64,  // initial_data_segment_size
  ```
- `DataSegmentSet::new()` 新增 `initial_segment_size` 参数
- `DataSegmentSet::load_existing()` 新增 `initial_segment_size` 参数 (向后兼容: 已有文件不受影响)
- 修改 `DataSegmentSet::append()`:
  - 如果当前段空间不足:
    1. 调用 `seg.expand()` → 成功 → 重试 append
    2. expand 返回已达上限 → 密封当前段 (seal pending) → 创建新段 (initial_size)
  - `next_offset` 仍按 `segment_size` (max) 计算

### ✅ 12.6 index/segment.rs — IndexSegment 扩容支持
- `IndexSegment` struct 新增字段:
  ```rust
  pub current_file_size: u64,   // 当前实际文件大小
  pub max_file_size: u64,        // 扩容上限 (segment_size)
  ```
- `IndexSegment::create()` 签名变更:
  ```rust
  pub fn create(base_dir: &Path, start_timestamp: i64, initial_size: u64, max_size: u64) -> Result<Self>
  ```
  - `file.set_len(initial_size)`
  - header file_size 写入 max_size (创建时一次性写入, 之后不变)
  - `entries_capacity = (initial_size - HEADER_SIZE) / INDEX_ENTRY_SIZE`
  - `self.current_file_size = initial_size`
  - `self.max_file_size = max_size`
- `IndexSegment::open()` 签名变更:
  ```rust
  pub fn open(path: &Path, start_timestamp: i64, max_size: u64) -> Result<Self>
  ```
  - 内部 `file.metadata()?.len()` → actual_size
  - 忽略 header 中的 file_size (始终为 max_size)
  - `entries_capacity = (actual_size - HEADER_SIZE) / INDEX_ENTRY_SIZE`
  - `self.current_file_size = actual_size`
  - `self.max_file_size = max_size`
- 新增 `IndexSegment::expand(&mut self) -> Result<()>`:
  - 同 DataSegment 逻辑: target = min(current * 2, max), unmap → set_len → remap
  - 重新计算 `self.entries_capacity = (new_size - HEADER_SIZE) / INDEX_ENTRY_SIZE`
  - 更新内存字段 `self.current_file_size` (不更新 header)
- 修改 `append_entry()`:
  - 如果 `wrote_count >= entries_capacity`:
    - 如果 `current_file_size < max_file_size`: 调用 `expand()` → 重新计算 capacity → 追加
    - 否则: seal → 返回错误 (由调用方创建新段)

### ✅ 12.7 index/mod.rs — TimeIndex 扩容 + 新建段逻辑
- `TimeIndex` 新增字段:
  ```rust
  pub initial_segment_size: u64,  // initial_index_segment_size
  ```
- `TimeIndex::new()` 新增 `initial_segment_size` 参数
- `TimeIndex::load_existing()` 新增 `initial_segment_size` 参数
- 修改 `TimeIndex::get_or_create_segment_for_ts()`:
  - 创建新段时: `IndexSegment::create(base_dir, start_ts, initial_segment_size, segment_size)`
  - 修改已有 `IndexSegment::create(..., self.segment_size)` 调用
- `IndexSegmentMeta` 可能需要新增 `current_file_size` 字段 (用于打开时恢复)

### ✅ 12.8 dataset.rs — DataSet 传递新增参数
- `DataSet::create()` 新增 `initial_data_segment_size`, `initial_index_segment_size` 参数
  - 写入 `DataSetMeta` (含 TLV 0x06, 0x07)
  - `DataSegmentSet::new(..., initial_data_segment_size)`
  - `TimeIndex::new(..., initial_index_segment_size)`
- `DataSet::open()`:
  - 从 meta 读取 `initial_data_segment_size`, `initial_index_segment_size`
  - 传入 `DataSegmentSet::load_existing(..., initial_data_segment_size)`
  - 传入 `TimeIndex::load_existing(..., initial_index_segment_size)`
  - **注意**: `initial_*` 不硬性校验, 仅用于新段创建
  - `DataSetConfig` 存储初始大小

### ✅ 12.9 config.rs / store.rs — StoreConfig 集成
- `Store::create_dataset()` FFI/Rust API 新增 `initial_data_segment_size`, `initial_index_segment_size` 参数
- Builder 模式支持
- 默认值: 256KB data, 4KB index

### ☐ 12.10 ffi.rs — FFI API 更新
- `tmsl_dataset_create` 新增 2 个 u64 参数
- `include/timslite.h` 更新函数声明

### ✅ 12.11 单元测试
- test_meta_tlv_06_07_roundtrip: meta 0x06/0x07 序列化/反序列化
- test_data_segment_create_initial_size: 创建时文件为 initial_size, header file_size = max
- test_data_segment_expand: 256KB → 512KB → 1MB → ... → 64MB (header file_size 始终不变)
- test_data_segment_expand_max_reached: 达到 64MB 后 expand 返回错误
- test_index_segment_create_initial_size: 创建时文件为 initial_size, header file_size = max
- test_index_segment_expand: 4KB → 8KB → 16KB → ... → 4MB
- test_index_segment_expand_recalculate_capacity: 扩容后 entries_capacity 正确增长
- test_data_segment_append_triggers_expand: 写入数据 → 自动扩容
- test_index_segment_append_triggers_expand: 写入条目 → 自动扩容
- test_data_segment_load_existing_compat: 打开全量预分配的旧文件
- test_index_segment_load_existing_compat: 打开全量预分配的旧文件
- test_dataset_create_with_initial_sizes: 创建时所有参数正确传递
- test_header_file_size_always_max: 验证 header 中 file_size 字段始终为 segment_size

### ☐ 12.12 集成测试
- test_lazy_create_write_query_small_data: 写入少量数据, 验证文件大小 < segment_size
- test_lazy_write_until_max_then_new_segment: 持续写入, 验证达到 max 后创建新段
- test_open_legacy_full_allocated_dataset: 打开旧数据集 (全量预分配), 正常读写
- test_disk_space_efficiency: 写入 100 条记录, 验证磁盘占用 < 1MB (对比 68MB 旧方案)
- test_expansion_data_integrity: 扩容前后数据完整性 (写入 → 扩容 → 验证所有数据可读, header file_size 始终不变)
- test_expansion_consecutive_open_write: 持续写入触发多次自动扩容, close → open → query

### ✅ Phase 12 验收标准
- ✅ 单元测试: DataSegment expand roundtrip (256KB → 64MB)
- ✅ 单元测试: IndexSegment expand + entries_capacity 重算
- ✅ 单元测试: meta TLV 0x06/0x07 roundtrip
- ✅ 单元测试: config builder 新字段
- ✅ 集成测试: 写入少量数据 → 文件大小 = initial_size (不是 segment_size)
- ✅ 集成测试: 写入触发扩容 → 新文件大小 = 2x → 验证数据完整
- ✅ 集成测试: 达到 max → 密封 → 创建新段 (initial_size)
- ✅ 集成测试: 打开全量预分配旧文件 → 正常读写
- ✅ 总 90/90 tests pass (81 unit + 9 integration)
- ✅ `cargo build --release` clean
- ✅ `cargo clippy` clean (仅 pre-existing warnings)

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
Phase 6 (Store + 单线程后台任务: flush 10min / idle 60s 统一循环)
       │
       ▼
Phase 7 (FFI 接口)
       │
       ▼
Phase 8 (集成测试 + 性能 + idle-close 恢复测试 + 目录结构验证)
       │
       ▼
Phase 9 (读缓存池: BlockCache LRU + idle 回收 + 读取集成)
        │
        ▼
Phase 10 (索引连续存储: filler 条目 + sentinel 值 + mmap 覆盖写 + meta TLV 扩展) ✅
        │
        ▼
Phase 11 (连续模式 O(1) 查询优化: 直接计算索引位置 + 消除二分查找)
        │
        ▼
Phase 12 (分段懒分配 + 倍率扩容: 初始大小创建, 2x 增长, max=segment_size)
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
| index 迁移到 index/ | 旧数据不可读 | 打开时检测 `.index/` 目录, 自动重命名为 `index/` (向后兼容迁移) |
| 数据文件迁移到 data/ | 旧数据不可读 | 打开时检测 base_dir 下的 segment 文件, 自动移动到 `data/` 子目录 (向后兼容迁移) |
| create vs open 混淆 | 误创建已存在数据集 | create 检查 meta 文件已存在则返回明确错误, 文档明确区分两种操作 |
| 误删数据集 (drop) | 数据丢失不可恢复 | drop_dataset 使用 remove_dir_all 删除整个目录, 不可恢复; FFI 层添加确认参数 |
| 单线程后台任务阻塞 | flush/idle/cache eviction 互相延迟 | flush、idle、cache eviction 在同一线程顺序执行; cache eviction 是内存操作 (毫秒级), 影响极小 |
| 缓存内存超限 | OOM | LRU 淘汰降至 85%, 留有安全余量; idle 回收每 60s 清理冷数据 |
| 缓存数据一致性 | 返回过期数据 | 只缓存已 seal 的 block, seal 后数据永不修改, 无一致性风险 |
| 缓存内存碎片 | 内存利用率低 | `Vec<u8>` 直接存储解压数据, 无额外包装; 回收时 `drop` 释放完整 Vec |
| Filler 条目爆炸 | Index 体积暴增 | 连续模式下大时间间隔产生大量 filler; 用户需控制写入时间间隔 |
| Index segment 仅含 filler | 无效磁盘写入 | flush 时检测并删除仅含 filler 的 segment 文件 |
| 连续模式逆序写入 | Filler 替换 | 数据段追加 + mmap 覆盖写 18 字节替换 filler |
| 连续/非连续切换 | 已有数据不兼容 | `index_continuous` 创建后不可变, meta 文件锁定配置 |
| 扩容 crash | 无 header 损坏风险 | header file_size 不更新, 打开时以磁盘实际大小为准 |
| initial_size 过小 | 频繁扩容降低性能 | 默认 256KB/4KB, 64MB 仅需 9 次扩容; 用户可调大 |
| timestamp=0 冲突 | index segment 命名歧义 | timestamp=0 保留为空位标记, 写入时拒绝 |

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