# timslite 开发计划

> 基于 design.md 详细设计  
> 目标: 完成 Rust cdylib 时序数据存储库, 提供 C ABI FFI

---

## 总体里程碑

```
Phase 1: 项目骨架 + 基础工具     ▓▓▓
Phase 2: 文件头 + Block 核心     ▓▓▓
Phase 3: DataSegment 写入/读取   ▓▓▓
Phase 4: 时间索引系统            ▓▓▓
Phase 5: DataSegmentSet + DataSet▓▓▓
Phase 6: Store 门面 + 后台任务   ▓▓▓
Phase 7: FFF 接口                ▓▓▓
Phase 8: 集成测试 + 性能调优     ▓▓▓
```

---

## Phase 1: 项目骨架 + 基础工具

**目标**: 搭建项目结构, 编译通过, 基础工具函数就绪

### 1.1 初始化 Rust 项目
- 创建 `cargo init --lib timslite`
- 配置 `Cargo.toml`:
  - `[lib] crate-type = ["cdylib", "rlib"]`
  - 添加依赖: `memmap2 = "0.9"`, `miniz_oxide = "0.8"`, `log = "0.4"`, `libc = "0.2"`
  - `[dev-dependencies]`: `criterion = "0.5"`
  - `edition = "2021"`

### 1.2 创建模块目录结构
```
src/
├── lib.rs
├── store.rs
├── dataset.rs
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

### 1.3 util.rs - 字节工具
- `read_u16_le(&[u8; 2]) -> u16`
- `write_u16_le(buf: &mut [u8], v: u16)`
- `read_u32_le(&[u8; 4]) -> u32`
- `write_u32_le(buf: &mut [u8], v: u32)`
- `read_i64_le(&[u8; 8]) -> i64`
- `write_i64_le(buf: &mut [u8], v: i64)`
- `read_u64_le(&[u8; 8]) -> u64`
- `write_u64_le(buf: &mut [u8], v: u64)`
- 便捷宏: `read_u16_from_mmap(&mmap, pos)`, `write_u32_to_mmap(&mut mmap, pos, v)`

### 1.4 error.rs - 错误类型
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

### 1.5 lib.rs - 入口
- re-export: `pub use error::{TmslError, Result}`
- `pub use store::Store`
- 模块声明
- 基础常量导出: `HEADER_SIZE`, `BLOCK_HEADER_SIZE`, `INDEX_ENTRY_SIZE`

### 1.6 StoreConfig - 可配置参数
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

### ✅ Phase 1 验收标准
- `cargo build` 通过, 生成 .dll/.so
- `cargo test` 至少 1 个 test pass
- util.rs 所有 endian 函数单元测试通过
- error.rs 所有 From impl 覆盖

---

## Phase 2: 文件头 + Block 核心

**目标**: FileMetadata 序列化/反序列化完成, BlockHeader 读写正确

### 2.1 header.rs - FileMetadata (128字节)
- 常量定义:
  ```rust
  pub const HEADER_SIZE: u64 = 128;
  pub const META_DATA_LEN: u16 = 52;
  pub const MAGIC: [u8; 4] = *b"TMSL";
  pub const VERSION: u16 = 1;
  ```
- Flags:
  ```rust
  pub const FILE_FLAG_SEALED: u16 = 0x0001;
  pub const FILE_FLAG_HAS_PENDING: u16 = 0x0002;
  ```
- struct `FileMetadata` (128B)
- `fn write_to(&self, mmap: &mut MmapMut)` - 写入 128 字节
- `fn read_from(&mmap: &Mmap) -> Result<Self>` - 从 mmap 解析
  - 校验 magic
  - 读取 version, 兼容未来版本
  - 读取 `meta_data_len`, 跳过未知扩展字段
  - 读取扩展元数据 (52B)
  - 预留区 50B 解析
- `fn create_default(file_type: i64, file_offset: i64, file_size: i64) -> Self`
- `fn update_wrote_position(&mut self, pos: i64)` 原地更新 mmap
- `fn update_pending_state(&mut self, offset: i64, uncomp_size: u32, count: u16)`
- `fn flush(&mut self, mmap: &mut MmapMut)` - 将内存结构写回 mmap

### 2.2 block.rs - BlockHeader (16字节)
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

### 2.3 compress.rs - 压缩封装
- `fn deflate_compress(data: &[u8], level: u8) -> Vec<u8>`
  - 使用 `miniz_oxide::deflate::compress_to_vec`
  - level 映射到 miniz_oxide 级别
- `fn deflate_decompress(data: &[u8]) -> Result<Vec<u8>>`
  - 使用 `miniz_oxide::inflate::decompress_to_vec`
- `fn should_use_compressed(compressed: &[u8], original: &[u8]) -> bool`
  - `compressed.len() < original.len()`

### ✅ Phase 2 验收标准
- header.rs: 创建→写入→读取, 所有字段 roundtrip 一致 (单元测试)
- header.rs: `meta_data_len` 正确写入, 未来版本兼容逻辑测试
- block.rs: 写入→读取, flags 测试 (compress, sealed, single_record)
- compress.rs: deflate roundtrip 测试, 压缩率测试
- `cargo test --lib` all pass
- `cargo clippy` clean

---

## Phase 3: DataSegment 写入/读取 (核心)

**目标**: DataSegment 完整的 Block 聚合写入、延迟压缩、懒加载生命周期、恢复逻辑

### 3.1 DataSegment 结构定义 (segment/data.rs)
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
    sealed: bool,
    pending_block_offset: Option<u64>,
    pending_block_uncomp_size: u32,
    pending_block_record_count: u16,
    last_accessed_at: Instant,       // 最近读写时间
    lifecycle: SegmentLifecycle,     // Closed / OpenReady / OpenIdle
}

pub enum SegmentLifecycle {
    Closed,          // 文件未打开
    OpenReady,       // 打开中, 可读写
    OpenIdle,        // 即将关闭 (idle timeout 触发)
}

const BLOCK_HEADER_SIZE: u64 = 16;
```

### 3.2 DataSegment 创建与打开
- `fn create(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 创建/截断文件到 file_size
  - mmap (MmapMut)
  - 写入 FileMetadata (HEADER + data_start=128)
  - 初始化所有计数为 0, lifecycle = OpenReady
- `fn open(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 打开文件 (不截断)
  - mmap
  - 读取 FileMetadata, 校验 magic/version
  - 恢复 wrote_position, record_count, total_uncompressed_size
  - 恢复 pending_block 状态
  - **pending 恢复**: 如果 header 有 pending_block_offset → 密封 (不压缩) → clear pending
    - 读取 header flags, 如果 `FILE_FLAG_HAS_PENDING`:
      1. 在 pending_block_offset 处密封 block (flags = SEALED, 不压缩)
      2. 清除 header: `pending_block_offset = -1`, 清 `FILE_FLAG_HAS_PENDING`
      3. flush file header 到 mmap
      4. wrote_position 指向 sealed block 之后

### 3.3 DataSegment 生命周期管理
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

### 3.4 核心写入逻辑
- `pub fn append_record(&mut self, timestamp: i64, data: &[u8], block_max_size: u32, compress_level: u8) -> Result<(u64, u16)>`
  - 计算 `record_size = 2 + 8 + data.len()`
  - **情况 1**: `record_size > block_max_size` → 独占 block
  - **情况 2**: 有 pending block, 检查是否溢出 → seal 或追加
  - **情况 3**: 无 pending → 创建新 pending
  - 返回 `(block_relative_offset, in_block_offset)`

### 3.5 方法: write_raw_record_to_pending
- 写入 `[data_len:2][timestamp:8][data:N]` 到 pending block payload
- 更新 block header 的 `payload_size` 和 `record_count`
- 更新 `self.wrote_position`
- 更新 file header 的 `wrote_position`

### 3.6 方法: create_pending_and_append
- 在当前 wrote_position 创建新 BlockHeader (flags=0)
- 写入第一条 record (raw)
- 设置 `pending_block_offset`, `pending_block_uncomp_size`, `pending_block_record_count`
- 更新 file header 的 `pending_block_offset`, `FILE_FLAG_HAS_PENDING`

### 3.7 方法: seal_pending_block
- 读取 pending block payload (raw data)
- 压缩 (deflate)
- 比较压缩后与原始大小:
  - 压缩有效 → 写回压缩数据, flags = SEALED|COMPRESSED
  - 压缩无效 → 保留 raw, flags = SEALED
- 更新 block header (payload_size, flags)
- 清除 pending 状态
- 更新 file header (`pending_block_offset = -1`, 清除 `FILE_FLAG_HAS_PENDING`)

### 3.8 方法: create_single_record_block
- record_size > 64KB 的场景
- 构建 record payload `[data_len:2][ts:8][data:N]`
- 压缩
- 比较大小决定是否使用 compressed
- 写入 BlockHeader (flags=SEALED[|COMPRESSED]|SINGLE_RECORD)
- 写入 payload 到 mmap
- 更新计数器

### 3.9 读取逻辑
- `pub fn read_at_index(&self, entry: &IndexEntry) -> Result<(i64, Vec<u8>)>`
  1. 通过 `entry.block_offset` 定位 BlockHeader
  2. 读取 BlockHeader, 检查 compressed flag
  3. 如果需要, 解压 block payload
  4. 通过 `entry.in_block_offset` 定位到 `[data_len:2]`
  5. 读取 `data_len`, `timestamp`, `data`
  6. 返回

### 3.10 sync 方法 (flush loop 调用)
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
- `cargo test --lib` all pass

---

## Phase 4: 时间索引系统

**目标**: TimeIndex + IndexSegment 完整实现, 支持按时间范围查询

### 4.1 IndexEntry 定义 (index/mod.rs)
- 常量: `pub const INDEX_ENTRY_SIZE: usize = 18`
- struct `IndexEntry { timestamp: i64, block_offset: u64, in_block_offset: u16 }`
- `fn to_bytes(&self) -> [u8; 18]`
- `fn from_bytes(buf: [u8; 18]) -> Self`

### 4.2 IndexSegment 结构 (index/segment.rs)
```rust
pub struct IndexSegment {
    path: PathBuf,
    start_timestamp: i64,
    entries_capacity: usize,
    wrote_count: usize,
    mmap: Option<MmapMut>,          // None = closed
    sealed: bool,
    last_accessed_at: Instant,
}
```

### 4.3 IndexSegment 创建/打开/生命周期
- `fn create(base_dir: &Path, start_timestamp: i64, segment_size: u64) -> Result<Self>`
  - 计算 `entries_capacity = (segment_size - HEADER_SIZE) / 18`
  - 创建文件, mmap(128 + entries_capacity * 18 字节)
  - 写入 FileHeader (file_type < 0, file_offset = start_timestamp)
- `fn open(path: &Path) -> Result<Self>`
  - 打开文件, mmap
  - 读取 FileHeader 恢复 wrote_count
- `fn sync(&mut self) -> Result<()>` — flush loop 调用:
  - `mmap.flush()` (MS_SYNC)
- `fn idle_close(&mut self) -> Result<()>` — idle timeout 触发:
  1. `mmap.flush()` (MS_SYNC)
  2. `munmap` + close file
  3. mmap = None
- `fn ensure_open(&mut self) -> Result<()>` — lazily open if closed

### 4.4 IndexSegment 写入
- `fn append_entry(&mut self, entry: &IndexEntry) -> Result<()>`
  - 检查 `wrote_count < entries_capacity`
  - 写入 `[timestamp:8][block_offset:8][in_block_offset:2]`
  - 更新 header wrote_position/record_count
- `fn is_full(&self) -> bool`

### 4.5 IndexSegment 查询 (二分查找)
- `fn lower_bound(&self, target_ts: i64) -> usize` - 查找 >= target 的第一个位置
- `fn upper_bound(&self, target_ts: i64) -> usize` - 查找 > target 的第一个位置
- `fn find_exact(&self, target_ts: i64) -> Option<IndexEntry>` - 精确匹配
- `fn query_range(&self, start_ts: i64, end_ts: i64) -> Vec<IndexEntry>` - 范围查询

### 4.6 TimeIndex 结构 (index/mod.rs)
```rust
pub struct TimeIndex {
    base_dir: PathBuf,
    segment_size: u64,
    index_segments: Vec<IndexSegment>,
    in_memory_buffer: Vec<IndexEntry>,
    in_memory_flush_threshold: usize,   // default 1024
}
```

### 4.7 TimeIndex 写入
- `fn add_entry(&mut self, timestamp: i64, block_offset: u64, in_block_offset: u16) -> Result<()>`
  - 写入内存缓冲
  - 检查 buffer len >= threshold → flush_to_disk
- `fn flush_to_disk(&mut self) -> Result<()>`
  - 对 in_memory_buffer 按 timestamp 排序
  - 获取或创建当前写入段:
    - 检查最后一段是否已满 (`wrote_count >= entries_capacity`)
    - 已满 → 密封 (更新 header), 创建新段 (文件名 = buffer[0].timestamp)
    - 未满 → 使用最后一段
  - 批量 append_entry 到当前段

> **索引段路由**: 索引段按写入顺序填充, 不按 timestamp 哈希。
> 每段文件名 = 该段第一条 entry 的 timestamp (20位零填充)。
> 段满后创建新段, 新段文件名 = 当前第一条待写入 entry 的 timestamp。
> 查询时: 遍历所有段, 用 lower_bound 在每个段中二分查找。

### 4.8 TimeIndex 查询
- `fn get_or_create_segment(&mut self, timestamp: i64) -> Result<&mut IndexSegment>`
- `fn query(&self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>>`
  - 合并内存缓冲中的 entries (过滤 [start_ts, end_ts])
  - 合并各 index_segment 中的 entries (query_range)
  - 去重 + 按 timestamp 排序

### 4.9 TimeIndex 加载
- `fn load_existing(base_dir: &Path, segment_size: u64) -> Result<Self>`
  - 扫描 `{base_dir}/.index/*` 文件
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

### 5.1 DataSegmentSet (segment/mod.rs)
```rust
pub struct DataSegmentSet {
    base_dir: PathBuf,
    segment_size: u64,
    block_max_size: u32,
    compress_level: u8,
    segments: Vec<DataSegment>,           // 打开中的 segment
    closed_segments: Vec<DataSegmentMeta>, // 已关闭的 segment (path, offset, size)
    next_offset: u64,
    last_used_at: Instant,
}

struct DataSegmentMeta {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
}
```

### 5.2 DataSegmentSet 生命周期管理
- `fn sync_all(&mut self) -> Result<()>` — flush loop 调用:
  - 遍历所有打开的 segment → `seg.sync()`
  - 遍历索引 segment → `idx_seg.sync()`
- `fn idle_close_all(&mut self) -> Result<()>` — idle timeout 调用:
  - 遍历所有打开的 segment (data + index) → `seg.idle_close()`
  - data segment 移入 `closed_segments`, index segment 单独管理
- `fn lazy_open(&mut self, offset: u64) -> Result<&mut DataSegment>`
  - 先在 `segments` 中查找
  - 未找到 → 在 `closed_segments` 中查找 meta
  - 找到 meta → `DataSegment::open(path, ...)` → 移入 `segments`

### 5.3 DataSegmentSet 写入
- `fn append(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u64, u16)>`
  - `segment = lazy_open(next_offset)` (if current closed, reopen)
  - `segment.append_record(...)`
  - `last_used_at = Instant::now()`
  - 如果 segment 已满/密封 → `next_offset += segment_size`, 创建新 segment
  - 返回 `(segment_file_offset, block_relative_offset, in_block_offset)`
- `fn get_or_create_segment(&mut self, offset: u64) -> Result<&mut DataSegment>`
  - 查找最后一个非密封 segment
  - 如满/密封 → 创建新文件

### 5.4 DataSegmentSet 读取/查询
- `fn find_segment(&self, block_absolute_offset: u64) -> Result<&DataSegment>`
  - `relative = block_absolute_offset - absolute_base`
  - 找到包含 relative 偏移的 segment
  - 如果 segment 已关闭 → lazy_open → 返回
- `fn read_at_index(&self, entry: &IndexEntry) -> Result<(i64, Vec<u8>)>`
  - 定位 segment → 调用 segment.read_at_index

### 5.5 DataSegmentSet flush/load
- `fn flush_all(&mut self) -> Result<()>` - flush 所有 segments
- `fn load_existing(base_dir: &Path, segment_size: u64, block_max_size: u32, compress_level: u8) -> Result<Self>`
  - 扫描 `{base_dir}/*`, 排除 `.index/`
  - 按 file_offset 排序加载

### 5.6 DataSet 整合
```rust
pub struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,
    config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
}
```

### 5.7 DataSet 写入
- `fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()>`
  - write to DataSegmentSet
  - add to TimeIndex (block_offset = seg_file_offset + block_rel_offset)
  - `last_used_at = Instant::now()`

### 5.8 DataSet 读取
- `fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>`
  - TimeIndex.query → Vec<IndexEntry>
  - 逐 entry 读取 data
  - 按 timestamp 排序

### 5.9 DataSet flush/close
- `fn flush(&mut self) -> Result<()>`
- `fn close(&mut self) -> Result<()>` = flush + 释放 mmap

### 5.10 DataSet 加载
- `fn load(id: DataSetKey, base_dir: PathBuf, config: &StoreConfig) -> Result<Self>`
  - 加载 DataSegmentSet (初始所有 segment closed)
  - 加载 TimeIndex
  - 恢复 last_used_at

### ✅ Phase 5 验收标准
- 集成测试: 创建 DataSet → 写入 5000 条(覆盖多个 segments/blocks) → query 全部
- 集成测试: 时间范围查询 (部分数据) → 验证数量和顺序
- 集成测试: close → reopen → 写入更多 → 验证所有数据可读
- 集成测试: 多数据集并行 (不同 name/type) → 数据完全隔离

---

## Phase 6: Store 门面 + 后台任务

**目标**: Store 生命周期管理、数据集管理、后台 flush/idle (mmap 生命周期管理)

### 6.1 Store 结构
```rust
pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,
    flush_handle: Option<JoinHandle<()>>,
    idle_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}
```

### 6.2 Store::open
- `pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>`
  - 创建 data_dir
  - 扫描已有数据集目录 → load
  - 启动后台线程 (使用 config 中 flush_interval, idle_timeout)
  - 返回 Store

### 6.3 Store::open_dataset
- `pub fn open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>`
  - 读锁查找 → 存在则返回 Arc clone
  - 写锁创建 → 创建目录 → DataSet::load/new → 插入
  - 返回 DataSetHandle(Arc<Mutex<DataSet>>)

### 6.4 Store::close_dataset
- `pub fn close_dataset(&self, handle: DataSetHandle) -> Result<()>`
  - flush dataset (mmap sync)
  - idle_close_all (关闭所有打开的 segment)
  - 从 HashMap 移除 key

### 6.5 Store::close (self)

```
关闭流程:
1. 发送 shutdown 信号 (mpsc::channel send)
   → 后台线程在下一次 loop 检查时退出
2. join flush_handle (等待当前 flush 循环完成, timeout 30s)
3. join idle_handle (等待当前 idle check 完成, timeout 30s)
4. 此时所有后台线程已退出, 安全执行 final flush:
   for each dataset: flush() + idle_close_all()
5. clear datasets HashMap
6. 释放 Store (self dropped)
```

> **关键**: join 必须等待后台线程退出, 否则 final flush 可能与后台 flush 并发
> timeout 保护: 如果线程卡住, 30s 后 force 继续 (log warning)

### 6.6 bg/flush.rs - Flush 线程
```rust
pub fn spawn_flush_loop(
    datasets: Weak<RwLock<HashMap<...>>>,
    interval: Duration,   // 默认 10 分钟
) -> JoinHandle<()>
```
- 每 10 分钟 (可配置):
  - 获取数据集读锁
  - 对每个 dataset: lock → sync_all (仅 mmap.flush())
  - **不密封 pending block, 不压缩**
  - 收到 shutdown 信号退出

### 6.7 bg/idle.rs - Idle Check 线程
```rust
pub fn spawn_idle_loop(
    datasets: Weak<RwLock<HashMap<...>>>,
    timeout: Duration,    // 默认 30 分钟
) -> JoinHandle<()>
```
- 每 60 秒:
  - 读锁遍历 → 找 last_used_at.elapsed() >= 30min 的 dataset
  - 写锁 → sync_all + idle_close_all (所有 segment)
  - 更新 dataset 状态为 idle
  - 收到 shutdown 信号退出

### 6.8 bg/mod.rs - 后台管理
- `pub struct BackgroundTasks { flush_handle, idle_handle, shutdown_tx }`
- `fn start(datasets, flush_interval, idle_timeout) -> Self`
- `fn stop(self)` - 发送信号, join, 返回

### ✅ Phase 6 验收标准
- 集成测试: Store::open → open_dataset × 2 → write data → flush 10min 触发 sync → close → reopen → 数据仍在
- 集成测试: Store flush 循环只执行 msync, pending block 保持 raw
- 集成测试: Store idle check 在 30min 后关闭所有 segment, 释放 mmap
- 集成测试: idle-close 后 → write/read 操作 → on-demand reopen → pending 已密封, 数据一致
- 集成测试: Store::close 完整关闭所有资源, 无泄漏
- `cargo test` all pass

---

## Phase 7: FFI 接口

**目标**: C ABI 接口完整实现, errno-safe, panic-safe

### 7.1 ffi.rs - 核心工具
- `fn write_error(buf: *mut c_char, len: usize, msg: &str)`
- `fn catch_ffi_result<F, T>(f: F, err_buf: *mut c_char, err_buf_len: usize) -> T`
  - `catch_unwind`
  - 错误写入 err_buf
  - 返回 -1 或 null
- DataSetHandle FFI wrapper (opaque pointer)
- QueryIterator FFI wrapper

### 7.2 FFI: Store 管理
- `tmsl_store_open(data_dir: *const c_char, err_buf, err_buf_len) -> *mut c_void`
- `tmsl_store_close(store: *mut c_void, err_buf, err_buf_len) -> c_int`

### 7.3 FFI: 数据集管理
- `tmsl_dataset_open(store, name, dataset_type, err_buf, err_buf_len) -> *mut c_void`
- `tmsl_dataset_close(dataset, err_buf, err_buf_len) -> c_int`
- `tmsl_dataset_flush(dataset, err_buf, err_buf_len) -> c_int`

### 7.4 FFI: 数据写入
- `tmsl_dataset_write(dataset, timestamp: c_long, data: *const c_uchar, data_len: usize, err_buf, err_buf_len) -> c_int`

### 7.5 FFI: 查询迭代器
- `tmsl_dataset_query(dataset, start_ts: c_long, end_ts: c_long, err_buf, err_buf_len) -> *mut c_void`
- `tmsl_iter_next(iter, out_ts: *mut c_long, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf, err_buf_len) -> c_int`
  - 0 = success (有数据)
  - 1 = 无更多数据 (iterator exhausted)
  - -1 = error
  - **内存分配**: `out_data` 指向的内存由 Rust `libc::malloc` 分配, C 侧必须调用 `tmsl_iter_free_data` 释放
- `tmsl_iter_free_data(data: *mut c_uchar)` - 释放 `tmsl_iter_next` 分配的内存 (对应 `libc::free`)
- `tmsl_iter_close(iter: *mut c_void)` - 关闭迭代器, 释放迭代器本身 (对应 `Box::into_raw`/`Box::from_raw`)

### 7.6 头文件生成 (.h)
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

### 8.1 端到端集成测试 (tests/integration_test.rs)
- **T8.1.1** 基本生命周期: open_store → open_dataset → write 1000 records → query all → close
- **T8.1.2** 多数据集隔离: open 2 datasets (不同 name/type) → 分别写入 → 交叉查询 → 数据不混合
- **T8.1.3** Block 聚合: 写入小数据 (<1KB) × 100 → 验证压缩后 block 数量合理
- **T8.1.4** 大 record: 写入 >64KB 的 record → 验证独占 block
- **T8.1.5** 时间范围查询: 写入 ts=[0,9999] → query [1000,2000] → 验证返回 1001 条
- **T8.1.6** 持久化: write → close → reopen → query → 数据一致
- **T8.1.7** 异常恢复: 写入中途 crash (模拟) → reopen → 验证 pending block 正确恢复/密封
- **T8.1.8** 并发: 多线程 open_dataset (不同 dataset) → 并发写入 → 数据完整
- **T8.1.9** idle-close 生命周期: write → wait 30min (simulate idle) → verify all segments closed → write → verify on-demand reopen + pending sealed → query → 数据一致
- **T8.1.10** flush 不密封: write (pending block) → flush → verify pending NOT sealed → write more → block overflows → seal+compress → verify
- **T8.1.11** mmap 懒加载: open dataset → write (creates segment) → idle-close → verify munmap + file closed → read → verify reopen + mmap → data correct

### 8.2 单元测试补全
- util.rs 全部 endian 函数
- header.rs metadata roundtrip
- block.rs flags 解析
- compress.rs deflate roundtrip + 大小比较
- DataSegment 各种写入场景
- DataSegment idle_close → reopen recovery
- IndexSegment 二分查找边界
- TimeIndex 内存缓冲 flush
- StoreConfig builder pattern 测试
- StoreConfig → DataSetConfig conversion (From impl)
- DataSegment lifecycle: create → idle_close → ensure_open → write → verify pending sealed
- IndexSegment lifecycle: create → idle_close → ensure_open → append → verify
- Idle-check double-check race condition: simulate write between read-lock and write-lock

### 8.3 性能基准测试 (benches/)

使用 [criterion](https://github.com/bheisler/criterion.rs) (stable Rust, 不是 nightly-only `#[bench]`):

`Cargo.toml` 添加:
```toml
[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "timslite_benchmarks"
harness = false
```

基准测试覆盖:
- `bench_write_small_100b` — 写入 100B 小数据
- `bench_write_large_10kb` — 写入 10KB 数据
- `bench_write_mixed_sizes` — 混合大小写入
- `bench_query_1000_records` — 查询 1000 条记录
- `bench_query_time_range` — 时间范围查询

运行: `cargo bench`

### 8.4 内存安全验证
- `cargo test` under `valgrind` (如果环境支持)
- 检查 mmap 泄漏 (打开/关闭后文件句柄释放)
- 检查 FFI 内存泄漏 (iter_free_data 后的内存)
- 压测: 连续写入 1GB → 验证内存不增长

### 8.5 文档
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
Phase 1 (骨架+工具+StoreConfig)
    │
    ├─────────────────────────────┐
    ▼                             ▼
Phase 2 (文件头+Block)       Phase 1 (util.rs)
    │                             │
    ▼                             ▼
Phase 3 (DataSegment + 生命周期) ◄──── Phase 2 (BlockHeader + compress)
    │  - open/create
    │  - idle_close (sync + seal pending, no compress)
    │  - sync (msync only)
    │  - pending recovery on reopen
    │
    ├──────┐
    ▼      ▼
Phase 4 (索引 + 生命周期)  Phase 3
    │  - sync, idle_close, ensure_open
    └──┬───┘
       ▼
Phase 5 (DataSet + DataSegmentSet + lazy open/close)
       │
       ▼
Phase 6 (Store + 后台任务: flush 10min / idle 30min)
       │
       ▼
Phase 7 (FFI 接口)
       │
       ▼
Phase 8 (集成测试 + 性能 + idle-close 恢复测试)
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