# Phase 11: 连续模式 O(1) 查询优化

**目标**: 连续模式下索引位置直接计算 (entry_index = target_ts - start_timestamp), 消除二分查找

---

## 11.1 IndexSegment: direct_lookup 方法

- 新增 `IndexSegment::direct_lookup(target_ts: i64) -> Option<IndexEntry>`
  - 检查范围: `target_ts < start_timestamp || target_ts >= start_timestamp + wrote_count` → None
  - 计算: `entry_index = (target_ts - start_timestamp) as usize`
  - 从 mmap 读取 8 字节 timestamp → 校验是否等于 `target_ts`
  - 匹配 → 读取完整 18 字节 entry → return Some(entry)

## 11.2 IndexSegment: 添加 *_cs 连续模式变体方法

**不修改现有二分查找方法** — 添加新 `*_cs` 方法, 内部根据 `index_continuous` 参数分支:

- `lower_bound_cs(target_ts, index_continuous: bool) -> usize` — 连续模式 O(1), 非连续二分查找
- `upper_bound_cs(target_ts, index_continuous: bool) -> usize` — 同上
- `find_exact_cs(target_ts, index_continuous: bool) -> Option<IndexEntry>` — 连续模式 direct_lookup, 非连续二分查找
- `find_entry_index_cs(target_ts, index_continuous: bool, wrote_count: Option<usize>) -> Option<usize>` — 连续模式 O(1), 支持外部 wrote_count

## 11.3 `IndexSegmentMeta` 新增 `wrote_count` 字段

- `IndexSegmentMeta` 新增 `wrote_count: usize` 字段
- `TimeIndex::load_existing` 中从文件 header 读取 `record_count` (mmap 读取偏移 52)
- `idle_close_all()` 从 open segment 复制 wrote_count

## 11.4 `TimeIndex` 新增 `index_continuous: bool` 字段

- `TimeIndex::new()` 新增 `index_continuous` 参数 (默认 false)
- `TimeIndex::load_existing()` 新增 `index_continuous` 参数
- `TimeIndex` struct 新增 `index_continuous: bool` 字段

## 11.5 `TimeIndex::query` 更新为使用 `query_range_cs`

- `TimeIndex::query` 调用 `seg.query_range_cs(start_ts, end_ts, self.index_continuous)`

## 11.6 `DataSet::query` 传递 `index_continuous` 标志

- 自动传递 (TimeIndex::query 使用 self.index_continuous)

## 11.7 `replace_filler_with_real` 连续模式优化

- 对 open segments: 使用 `find_entry_index_cs` 直接计算 O(1)
- 对 closed segments: `meta.wrote_count` 范围检查 → 直接计算 entry_index

## 11.8 `DataSet::open` 传递 `index_continuous` 到 `TimeIndex`

- `DataSet::create()` → `TimeIndex::new(..., index_continuous != 0)`
- `DataSet::open()` → `TimeIndex::load_existing(..., config.index_continuous != 0)`

## 验收标准

- [x] 单元测试: `direct_lookup` — 范围内 O(1) 命中
- [x] 单元测试: `direct_lookup` — 范围外正确返回 None
- [x] 单元测试: `lower_bound_cs` 连续模式 vs 非连续模式结果一致性
- [x] 单元测试: `find_entry_index_cs` 与 `find_entry_index` 结果相同
- [x] 单元测试: non-continuous 模式使用 `*_cs` = 原有二分查找行为
- [x] 单元测试: closed segment `find_entry_index_cs` 使用 `wrote_count` 范围检查
- [x] 集成测试: 连续模式 query 正确性不变 (所有已有集成测试继续通过)
- [x] 集成测试: 总 89 tests pass (80 unit + 9 integration)

---

**导航**: [← Phase 10](phase-10-continuous-storage.md) | [→ Phase 12](phase-12-lazy-allocation.md)
