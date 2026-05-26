# Phase 5: DataSegmentSet + DataSet

**目标**: 多文件管理、懒打开/超时关闭、数据集完整 CRUD 流程 (create/open/close/drop)

---

## 5.1 DataSegmentSet (segment/mod.rs)

- 管理多个 DataSegment 的集合
- 负责 segment 的创建、加载、懒打开、idle 关闭

## 5.2–5.5 DataSegmentSet 生命周期 / 写入 / 读取 / flush / load

- `fn new(base_dir, segment_size) -> Self`
- `fn load_existing(base_dir, segment_size) -> Result<Self>`
- `fn append(timestamp, data) -> Result<()>` — 写入当前段, 满则创建新段
- `fn query(start_ts, end_ts, time_index) -> Result<Vec<(i64, Vec<u8>)>>`
- `fn flush() -> Result<()>` — 同步所有打开的 segment
- `fn idle_close_all() -> Result<()>` — 关闭所有空闲 segment

## 5.6 DataSet 整合

- 整合 DataSegmentSet + TimeIndex + last_used_at 为统一数据集接口

## 5.7 DataSet::create

- `fn create(id, base_dir, data_segment_size, index_segment_size, compress_level, block_max_size) -> Result<Self>`
  - 检测 base_dir 是否已有 meta 文件 → 存在则返回 `AlreadyExists` 错误
  - 创建 `data/` 和 `index/` 子目录
  - 写入 meta 文件 (仅一次, 之后不可修改)
  - 初始化 DataSegmentSet (空, 首个 segment 未创建)
  - 初始化 TimeIndex (空, 首个 segment 未创建)
  - 记录 last_used_at
- 参数验证: data_segment_size > 0, index_segment_size > 0, compress_level 1-9, block_max_size <= 64KB

## 5.8 DataSet::open

- `fn open(id, base_dir, block_max_size) -> Result<Self>`
  - 读取 meta 文件 → 不存在返回 `NotFound` 错误
  - 从 meta 读取 data_segment_size, index_segment_size, compress_level (不可设置)
  - 加载 DataSegmentSet (从 `data/` 子目录, 初始所有 segment closed)
  - 加载 TimeIndex (从 `index/` 子目录)
  - 恢复 last_used_at

## 5.9 DataSet::close

- `fn close(&mut self) -> Result<()>`
  - flush 所有 segments + index, idle_close_all segments + index
  - 更新 last_used_at

## 5.10 DataSet::drop_dataset

- `fn drop_dataset(base_dir: &Path) -> Result<()>`
  - 删除整个 base_dir 目录 (含 data/ + index/ + meta)
  - 使用 `std::fs::remove_dir_all`

## 5.11–5.13 DataSet::write / query / flush / close

- `fn write(timestamp: i64, data: &[u8]) -> Result<()>`
- `fn query(start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>`
- `fn flush() -> Result<()>` / `fn close() -> Result<()>`

## 5.14 DataSet 加载 (open_dataset 调用)

- `fn open(id: DataSetKey, base_dir: PathBuf, block_max_size: u32) -> Result<Self>`
  - 读取 `{base_dir}/meta` TLV 文件 (必须存在, 否则返回错误)
  - 从 meta 读取不可变参数, 构建 DataSetConfig
  - 加载 DataSegmentSet (从 `data/` 子目录, 初始所有 segment closed)
  - 加载 TimeIndex (从 `index/` 子目录)
  - 恢复 last_used_at

## 验收标准

- [x] 集成测试: `DataSet::create` → 检查目录和 meta 文件创建 → 写入 5000 条 → query 全部
- [x] 集成测试: `DataSet::create` 对已存在数据集 → 返回 `AlreadyExists` 错误
- [x] 集成测试: `DataSet::open` 对不存在数据集 → 返回 `NotFound` 错误
- [x] 集成测试: `DataSet::open` 后写入更多数据 → close → reopen → 验证所有数据可读
- [x] 集成测试: 时间范围查询 (部分数据) → 验证数量和顺序
- [x] 集成测试: 多数据集并行 (不同 name/type) → 数据完全隔离 (t8_1_2_multi_dataset_isolation)
- [x] 整合测试: meta 文件创建 → roundtrip → data_segment_size/index_segment_size 固定不可变
- [x] 整合测试: `DataSet::drop_dataset` 删除后, 目录和所有文件不可访问
- [x] 目录验证: 数据文件在 `data/` 下, 索引文件在 `index/` 下, meta 在 type/ 根下
- [x] 不可变验证: 创建 meta 后再次 open, meta 文件内容未变, 参数从 meta 读取

---

**导航**: [← Phase 4](phase-04-time-index.md) | [→ Phase 6](phase-06-store-bg.md)
