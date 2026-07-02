# Phase 10: 索引连续存储 (Index Continuous Storage)

**目标**: 索引条目按连续序号增长, 缺失时间戳填充哨兵值条目, 逆序写入统一拒绝

> 历史说明: 本 Phase 记录最初的连续存储实现。设计审查 P0-2 已在 [Phase 24](phase-24-sparse-continuous-index.md) 中重设计大 gap 行为: 连续模式改为稀疏 filler + 逻辑空洞, 不再按真实 timestamp 跨度全量物化 filler。以下涉及全量 filler 的内容仅代表旧实现背景。

---

## 10.1 meta.rs 扩展 — 新增 TLV type 0x05

```rust
const META_INDEX_CONTINUOUS: u8 = 0x05; // u8: 0=非连续, 1=连续
```
- `DataSetMeta` 新增字段: `index_continuous: u8` (default=0)
- `DataSetMeta::new()` 新增参数 `index_continuous`
- `from_bytes()` 解析 type 0x05, 未知旧版本跳过

## 10.2 DataSetMeta/DataSetConfig 更新

- `DataSetMeta` struct: 新增 `index_continuous: u8`
- `DataSetConfig` struct: 新增 `index_continuous: u8`
- `DataSet::create()`: 新增 `index_continuous` 参数, 传入 meta + config
- `DataSet::open()`: 从 meta 读取 `index_continuous`
- `Store::create_dataset()`: 新增 `index_continuous` FFI/Rust API 参数
- `DataSetConfigBuilder` 新增 `.index_continuous()` builder method

## 10.3 DataSet 写入逻辑更新

- `DataSet::write()`:
  - 新增状态跟踪: `latest_written_timestamp: Option<i64>` (从 index segment 恢复; 空 dataset 为 `None`)
  - 检查逆序:
    - **非连续模式**: `timestamp <= latest_written_timestamp` → `Error("out-of-order")`
    - **连续模式**:
      - `timestamp > latest_written_timestamp`: 填充缺失 + 正常写入
      - `timestamp < latest_written_timestamp`: 数据追加 + 替换 filler
      - `timestamp == latest_written_timestamp`: `Error("duplicate timestamp")`
  - 如果连续模式且 `timestamp > latest`:
    - 填充缺失: for ts in `(latest+1)..(timestamp-1)` → `time_index.add_filler_entry(ts)`
    - 然后写入真实 entry
  - 连续模式补数据:
    - 写入数据到 DataSegmentSet
    - `replace_filler_with_real(ts)` → mmap 覆盖写 14 字节 index entry

## 10.4 IndexSegment: find_entry_index + overwrite_entry

- `IndexSegment::find_entry_index(timestamp) -> Option<usize>` — 返回 entry 索引位置
- `IndexSegment::overwrite_entry(entry_index: usize, new_entry: &IndexEntry)`
  - 确保 mmap 有效
  - 计算 mmap 偏移: `HEADER_SIZE + entry_index * INDEX_ENTRY_SIZE`
  - 覆盖写 14 字节 index entry

## 10.5 TimeIndex 填充逻辑

- 新增: `TimeIndex::add_filler_entry(timestamp)` — 添加哨兵条目
- 填充循环在 `DataSet::write()` 层完成

## 10.6 Index Segment 跳过规则

- flush 路径不执行 pure-filler cleanup
- 仅含 filler 的 segment: 现阶段保留, 后续如需清理再单独设计
- Filler 识别: `block_offset == BLOCK_OFFSET_FILLER (0xFFFFFFFFFFFFFFFF)`

## 10.7 读取时 Filler 过滤

- `DataSet::query()`: `if entry.block_offset == BLOCK_OFFSET_FILLER { continue; }`

## 10.8 Timestamp = 0 保护

- `DataSet::write()`: `timestamp` 为非负 `i64` 业务时间戳；负值保留给读/查 public 入口表示相对最新 timestamp 的偏移

## 10.9 重启恢复 latest_written_timestamp

- `DataSet::open()`: `recover_latest_timestamp(&time_index)` — 扫描所有 index segments + buffer, 取最大 timestamp

## 10.10 FFI API 更新

- `tmsl_dataset_create`: 新增 `index_continuous: c_uchar` 参数
- `wrapper/cffi/include/timslite.h`: 更新函数声明
- 错误处理: 逆序写入返回 -1, err_buf 写错误信息

## 验收标准

- [x] 单元测试: meta TLV 0x05 roundtrip (创建→写入→读取)
- [x] 单元测试: 连续模式正序写入 ts=100 → ts=150 → filler 49 条
- [x] 单元测试: 连续模式补数据 ts=120 → filler 被替换 → 查询返回 3 条真实数据
- [x] 单元测试: 连续模式补数据 ts=100 (对应真实 entry) → Error
- [x] 单元测试: 连续模式补数据 ts=150 (等于 latest) → Error("duplicate timestamp")
- [x] 单元测试: 非连续模式逆序写入 ts=100 → ts=50 → Error("out-of-order")
- [x] 单元测试: Filler 识别: `block_offset == 0xFFFFFFFFFFFFFFFF` → query 跳过
- [x] 单元测试: 大量填充 (跨 segment) → 仅含真实 entry 的 segment 被创建
- [x] 单元测试: IndexSegment find_entry_index, overwrite_entry
- [x] 集成测试: 连续模式创建→写入→close→reopen→补数据→写入→数据一致
- [x] 集成测试: 非连续模式写入 ts=100 → ts=150 → 仅 2 entries (无 filler)
- [x] 集成测试: timestamp≤0 写入 → Error
- [x] 集成测试: 所有 86 tests pass (77 unit + 9 integration)

---

**导航**: [← Phase 9](phase-09-blockcache.md) | [→ Phase 11](phase-11-o1-optimization.md)
