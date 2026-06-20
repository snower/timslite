# Phase 4: 时间索引系统

**目标**: TimeIndex + IndexSegment 完整实现, 支持按时间范围查询

---

## 4.1 IndexEntry 定义 (index/mod.rs)

- 14 字节: timestamp_delta(u32, 4B) + block_offset(u64, 8B) + in_block_offset(u16, 2B)

## 4.2 IndexSegment 结构 (index/segment.rs)

- 包含: path, start_timestamp, mmap, wrote_count, entries_capacity, lifecycle

## 4.3 IndexSegment 创建/打开/生命周期

- `fn create(base_dir, start_timestamp, segment_size) -> Result<Self>`
- `fn open(path, start_timestamp, segment_size) -> Result<Self>`
- 生命周期管理同 DataSegment (ensure_open, idle_close, sync)

## 4.4 IndexSegment 写入

- `fn append_entry(timestamp, block_offset, in_block_offset) -> Result<()>`
- 检查容量, 写入 index entry (14 字节) 到 mmap
- 更新 wrote_count 和 wrote_position

## 4.5 IndexSegment 查询 (二分查找)

- `fn lower_bound(target_ts: i64) -> usize` — 二分查找第一个 >= target_ts 的 entry
- `fn upper_bound(target_ts: i64) -> usize` — 二分查找第一个 > target_ts 的 entry
- `fn find_exact(target_ts: i64) -> Option<IndexEntry>` — 精确匹配
- `fn query_range(start_ts: i64, end_ts: i64) -> Vec<IndexEntry>` — 返回时间范围内所有 entry

## 4.6 TimeIndex 结构 (index/mod.rs)

- `struct TimeIndex { base_dir: PathBuf, segments: Vec<IndexSegmentMeta>, in_memory_buffer: Vec<IndexEntry>, segment_size: u64 }`

## 4.7–4.8 TimeIndex 写入 / 查询

- `fn add_entry(timestamp, block_offset, in_block_offset) -> Result<()>` — 追加到 in_memory_buffer, 超过阈值时 flush
- `fn query(start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>>` — 遍历所有 segments, 调用各自的 query_range

## 4.9 TimeIndex 加载

- `fn load_existing(base_dir: &Path, segment_size: u64) -> Result<Self>`
  - 扫描 `{base_dir}/index/*` 文件
  - 按 start_timestamp 排序加载

## 验收标准

- [x] 单元测试: IndexEntry bytes roundtrip
- [x] 单元测试: IndexSegment append + 读 back 一致
- [x] 单元测试: IndexSegment lower_bound / query_range 正确 (含边界: 空段, 全段, 超出范围)
- [x] 集成测试: TimeIndex 写入 10000 entries → flush → reopen → query_range 验证
- [x] 集成测试: in_memory_buffer threshold 触发 flush 测试
- [x] `cargo test --lib` all pass

---

**导航**: [← Phase 3](phase-03-datasegment.md) | [→ Phase 5](phase-05-dataset.md)
