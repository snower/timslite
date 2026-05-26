# Phase 12: 分段文件懒分配与倍率扩容

**目标**: 分段文件以初始大小创建, 写入过程中按 2 倍扩容, 上限为 segment_size。达到 segment_size 后密封 + 创建新段。优化小数据量场景磁盘空间使用。

---

## 12.1 meta.rs — 新增 TLV type 0x06, 0x07

```rust
const META_INITIAL_DATA_SEGMENT_SIZE: u8 = 0x06; // u64 LE
const META_INITIAL_INDEX_SEGMENT_SIZE: u8 = 0x07; // u64 LE
```
- `DataSetMeta` 新增字段: `initial_data_segment_size: u64`, `initial_index_segment_size: u64`
- `DataSetMeta::new()` 新增参数
- `to_bytes()`: 写入 TLV type 0x06, 0x07
- `from_bytes()`: 解析 type 0x06, 0x07 (旧版本跳过)
- 单元测试: TLV roundtrip 含新字段

## 12.2 config.rs — StoreConfig / DataSetConfig 扩展

- `StoreConfig` 新增:
  ```rust
  pub initial_data_segment_size: u64,    // 默认 256KB
  pub initial_index_segment_size: u64,    // 默认 4KB
  ```
- `StoreConfigBuilder`: 新增 `.initial_data_segment_size()`, `.initial_index_segment_size()`
- `DataSetConfig` 新增对应字段 + builder methods
- `DataSetConfig::from_store()`: 从 StoreConfig 映射

## 12.3 header.rs — FileMetadata 语义调整

- **无需新增方法** — 扩容时不更新 header
- `FileMetadata::create_default()` 的 `file_size` 参数传入 `max_file_size` (segment_size)
  - header 中 file_size 始终记录标准分段大小, 写入后不再变化
  - 打开时忽略 header 中的 file_size, 以磁盘实际大小为准

## 12.4 segment/data.rs — DataSegment 扩容支持

- `DataSegment` struct 新增: `pub max_file_size: u64`
- `DataSegment::create()` 签名变更:
  ```rust
  pub fn create(path: &Path, file_offset: u64, initial_size: u64, max_size: u64) -> Result<Self>
  ```
  - `file.set_len(initial_size)`, header file_size 写入 max_size
  - `self.file_size = initial_size`, `self.max_file_size = max_size`
- `DataSegment::open()` 签名变更:
  ```rust
  pub fn open(path: &Path, file_offset: u64, max_size: u64) -> Result<Self>
  ```
  - 内部通过 `file.metadata()?.len()` 获取 actual_file_size
- 新增 `DataSegment::expand(&mut self) -> Result<()>`:
  - target = min(file_size * 2, max_file_size)
  - 如果 target == file_size → 已达上限, 返回错误
  - unmap → file.set_len(target) → remap → 更新 `self.file_size = target`
- 修改 `append_record()`: 写入前检查空间, 不足时尝试 expand

## 12.5 segment/mod.rs — DataSegmentSet 扩容 + 新建段逻辑

- `DataSegmentSet` 新增: `pub initial_segment_size: u64`
- 修改 `DataSegmentSet::append()`:
  - 如果当前段空间不足:
    1. 调用 `seg.expand()` → 成功 → 重试 append
    2. expand 返回已达上限 → 密封当前段 → 创建新段 (initial_size)
  - `next_offset` 仍按 `segment_size` (max) 计算

## 12.6 index/segment.rs — IndexSegment 扩容支持

- `IndexSegment` struct 新增: `pub current_file_size: u64`, `pub max_file_size: u64`
- `IndexSegment::create()` 签名变更:
  ```rust
  pub fn create(base_dir: &Path, start_timestamp: i64, initial_size: u64, max_size: u64) -> Result<Self>
  ```
- `IndexSegment::open()` 签名变更:
  ```rust
  pub fn open(path: &Path, start_timestamp: i64, max_size: u64) -> Result<Self>
  ```
- 新增 `IndexSegment::expand(&mut self) -> Result<()>`:
  - target = min(current * 2, max), unmap → set_len → remap
  - 重新计算 `self.entries_capacity`
- 修改 `append_entry()`: 如果 `wrote_count >= entries_capacity` → expand 或新建段

## 12.7 index/mod.rs — TimeIndex 扩容 + 新建段逻辑

- `TimeIndex` 新增: `pub initial_segment_size: u64`
- `TimeIndex::new()` / `load_existing()` 新增 `initial_segment_size` 参数
- 修改 `TimeIndex::get_or_create_segment_for_ts()`:
  - 创建新段时: `IndexSegment::create(base_dir, start_ts, initial_segment_size, segment_size)`

## 12.8 dataset.rs — DataSet 传递新增参数

- `DataSet::create()` 新增 `initial_data_segment_size`, `initial_index_segment_size` 参数
  - 写入 `DataSetMeta` (含 TLV 0x06, 0x07)
  - 传入 `DataSegmentSet::new(..., initial_data_segment_size)`
  - 传入 `TimeIndex::new(..., initial_index_segment_size)`
- `DataSet::open()`: 从 meta 读取初始大小, 传入加载函数

## 12.9 config.rs / store.rs — StoreConfig 集成

- `Store::create_dataset()` FFI/Rust API 新增 `initial_data_segment_size`, `initial_index_segment_size` 参数
- Builder 模式支持, 默认值: 256KB data, 4KB index

## 12.10 ffi.rs — FFI API 更新

- `tmsl_dataset_create` 新增 2 个 u64 参数
- `include/timslite.h` 更新函数声明

## 12.11 单元测试

- [x] test_meta_tlv_06_07_roundtrip: meta 0x06/0x07 序列化/反序列化
- [x] test_data_segment_create_initial_size: 创建时文件为 initial_size, header file_size = max
- [x] test_data_segment_expand: 256KB → 512KB → 1MB → ... → 64MB (header file_size 始终不变)
- [x] test_data_segment_expand_max_reached: 达到 64MB 后 expand 返回错误
- [x] test_index_segment_create_initial_size: 创建时文件为 initial_size, header file_size = max
- [x] test_index_segment_expand: 4KB → 8KB → 16KB → ... → 4MB
- [x] test_index_segment_expand_recalculate_capacity: 扩容后 entries_capacity 正确增长
- [x] test_data_segment_append_triggers_expand: 写入数据 → 自动扩容
- [x] test_index_segment_append_triggers_expand: 写入条目 → 自动扩容
- [x] test_data_segment_load_existing_compat: 打开全量预分配的旧文件
- [x] test_index_segment_load_existing_compat: 打开全量预分配的旧文件
- [x] test_dataset_create_with_initial_sizes: 创建时所有参数正确传递
- [x] test_header_file_size_always_max: 验证 header 中 file_size 字段始终为 segment_size

## 12.12 集成测试

- [x] test_lazy_create_write_query_small_data: 写入少量数据, 验证文件大小 < segment_size
- [x] test_lazy_write_until_max_then_new_segment: 持续写入, 验证达到 max 后创建新段
- [x] test_open_legacy_full_allocated_dataset: 打开旧数据集 (全量预分配), 正常读写
- [x] test_disk_space_efficiency: 写入 100 条记录, 验证磁盘占用 < 1MB (对比 68MB 旧方案)
- [x] test_expansion_data_integrity: 扩容前后数据完整性
- [x] test_expansion_consecutive_open_write: 持续写入触发多次自动扩容, close → open → query

## 验收标准

- [x] 单元测试: DataSegment expand roundtrip (256KB → 64MB)
- [x] 单元测试: IndexSegment expand + entries_capacity 重算
- [x] 单元测试: meta TLV 0x06/0x07 roundtrip
- [x] 单元测试: config builder 新字段
- [x] 集成测试: 写入少量数据 → 文件大小 = initial_size (不是 segment_size)
- [x] 集成测试: 写入触发扩容 → 新文件大小 = 2x → 验证数据完整
- [x] 集成测试: 达到 max → 密封 → 创建新段 (initial_size)
- [x] 集成测试: 打开全量预分配旧文件 → 正常读写
- [x] 总 94/94 tests pass (81 unit + 13 integration)
- [x] `cargo build --release` clean
- [x] `cargo clippy` clean (仅 pre-existing warnings)

---

**导航**: [← Phase 11](phase-11-o1-optimization.md) | [← 概览](overview.md)
