# Phase 18: 乱序写入与删除 (Out-of-Order Write & Delete)

## 1. 目标

扩展写入流程支持乱序写入 (`timestamp < latest_written_timestamp`), 并新增 `DataSet::delete(timestamp)` 操作。两项特性共享:
- **invalid_record_count**: 数据段文件 state 中的新计数器 (原 `reserved` 字段重命名)
- **索引条目原地更新**: 通过 mmap 直接修改索引段的 18 字节 entry
- **哨兵值复用**: `block_offset = 0xFFFFFFFFFFFFFFFF, in_block_offset = 0xFFFF` 既用于连续模式 filler 也用于 delete 标记

## 2. 设计概要

### 2.1 invalid_record_count 字段

| 位置 | 原字段 | 新字段 | 说明 |
|------|--------|--------|------|
| Data Segment State (offset 64, u64) | `reserved` (初始化为 0, 从未更新/读取) | `invalid_record_count` | 段内无效记录数 |

**递增时机**:
- `write(ts, data)` 乱序写入且旧索引条目引用真实数据时 (旧数据成为孤儿记录)
- `delete(ts)` 将索引条目标记为哨兵时

**不递增时机**:
- 连续模式 filler 替换: 无实际数据被替代
- 纠正写入 (ts == latest): 索引不变, 不产生孤儿记录

### 2.2 乱序写入 (timestamp < latest_written_timestamp)

**统一流程 (连续模式与非连续模式)**:

```
1. 数据 append 到最新数据段 (正常写入到 pending block 或新 block)
   → (seg_offset, new_block_offset, new_in_block_offset)

2. time_index.update_entry(ts, new_block_offset, new_in_block_offset)
   → 三级搜索: in_memory_buffer → open segments → closed segments
   → 若条目存在: 原地覆盖 18 字节, 返回 old_entry
   → 若条目不存在: Error("out-of-order write requires existing index entry")

3. 根据 old_entry 状态:
   - old_entry.block_offset ≠ FILLER (引用真实数据):
     old_segment = segments.locate_segment(old_entry.block_offset)
     old_segment.invalid_record_count += 1
   - old_entry.block_offset == FILLER (仅连续模式):
     // 无实际数据被替代, invalid_record_count 不变
```

**约束**:
- **要求索引中存在该时间戳条目**: 连续模式总是存在 (filler 或真实数据), 非连续模式仅在曾写入过该时间戳时存在
- `latest_written_timestamp` 不因乱序写入而改变
- 新数据始终追加到最新数据段, 永不写入旧段 (避免破坏段内排序)

**非连续模式行为变化**:
- **旧行为**: `ts < latest` → Error("out-of-order")
- **新行为**: `ts < latest` 且索引存在条目 → 乱序写入; 索引无条目 → Error

### 2.3 删除操作 (DataSet::delete)

```
DataSet::delete(timestamp):
  1. if timestamp <= 0 || latest_written_timestamp == 0 → Error
  2. time_index.find_and_delete_entry(timestamp):
     - 查找索引条目 (三级搜索)
     - 条目存在且引用真实数据:
       覆盖为哨兵 (block_offset = FILLER, in_block_offset = FILLER)
       返回 old_entry (含旧 block_offset)
     - 条目不存在 / 条目为 filler:
       Error("not found")
  3. old_segment = segments.locate_segment(old_entry.block_offset)
     old_segment.invalid_record_count += 1
  4. return Ok(())
```

**查询影响**: 查询路径已跳过 `block_offset == FILLER` 的条目, delete 后自动不可见, 无需修改查询逻辑。

### 2.4 FFI 接口

```c
// 删除指定时间戳的记录
// 返回: 0=成功, -1=失败 (err_buf 记录原因)
int tmsl_dataset_delete(void* ds, int64_t timestamp, char* err_buf, size_t err_buf_len);
```

**C 头文件**: `include/timslite.h` 新增声明。

**错误场景**:
- `ds == NULL` → Error(null pointer)
- `timestamp <= 0` → Error("timestamp must be > 0")
- 条目不存在 / filler → Error("not found")
- 其他 → Error(exception message)

## 3. 关键实现细节

### 3.1 TimeIndex::update_entry

```rust
/// 原地更新索引条目, 返回旧条目
/// 用于乱序写入: 将现有条目替换为新的数据位置
pub fn update_entry(
    &mut self,
    timestamp: i64,
    new_block_offset: u64,
    new_in_block_offset: u16,
) -> Result<IndexEntry> {
    // 三级搜索 (与 find_entry 同):
    // 1. in_memory_buffer: 线性搜索 (buffer 未排序时) / binary_search (已排序时)
    // 2. open index segments: 连续模式 O(1), 非连续模式二分查找
    // 3. closed segments: lazy_open → 查找 → overwrite_entry → idle_close
    //
    // 找到后: 构造 new_entry (timestamp 不变), 调用 segment.overwrite_entry(idx, &new_entry)
    // 返回 old_entry (覆盖前的值, 供调用方判断 invalid_record_count)
    //
    // 未找到: Error("out-of-order write requires existing index entry")
}
```

### 3.2 TimeIndex::find_and_delete_entry

```rust
/// 查找并将索引条目标记为哨兵, 返回旧条目
pub fn find_and_delete_entry(&mut self, timestamp: i64) -> Result<IndexEntry> {
    // 三级搜索定位条目
    // 若找到且引用真实数据:
    //   sentinel_entry = IndexEntry { timestamp, block_offset: FILLER, in_block_offset: FILLER }
    //   segment.overwrite_entry(idx, &sentinel_entry)
    //   返回 old_entry
    // 若 filler 或不存在: Error("not found")
}
```

### 3.3 Segment invalid_record_count 递增

```rust
/// DataSegmentSet: 路由 block_offset 到对应数据段, 递增其 invalid_record_count
pub fn increment_invalid_record_count(&mut self, block_offset: u64) -> Result<()> {
    // 1. 段路由: 通过 block_offset 与 segments[].file_offset 范围匹配
    //    block_offset 是相对段起始的偏移 (从段内视角),
    //    但段路由需全局视角: 段 file_offset ≤ block_offset < file_offset + wrote_position
    // 2. 若段在 segments (open): seg.invalid_record_count += 1, 更新 mmap state
    // 3. 若段在 closed_segments: lazy_open → 递增 → update_file_header → idle_close
}
```

### 3.4 连续模式 vs 非连续模式对比

| 方面 | 连续模式 | 非连续模式 |
|------|---------|-----------|
| Filler 条目 | 始终存在于 1..latest 范围 | 不存在 |
| ts < latest 时查找结果 | 必定找到 (filler 或真实数据) | 可能找不到 → Error |
| 条目定位 | O(1): `pos = HEADER + (ts - seg_start) × 18` | 二分查找 / in_memory_buffer |
| delete 定位 | 同上 | 同上 |
| delete 时 filler 处理 | Error("not found") — filler 不应被 delete | N/A (不存在 filler) |

## 4. 文件改动清单

| 文件 | 改动类型 | 改动内容 |
|------|---------|---------|
| `src/header.rs` | 修改 | Data Segment State: `reserved` → `invalid_record_count` (常量名、结构体字段、默认值、序列化、反序列化 — 共 6 处) |
| `src/segment/data.rs` | 新增 | DataSegment: `invalid_record_count` 字段 + `increment_invalid_record_count()` + file header state 持久化 |
| `src/segment/mod.rs` | 新增 | DataSegmentSet: `increment_invalid_record_count(block_offset)` 段路由方法 |
| `src/index/mod.rs` | 新增 | TimeIndex: `update_entry()` + `find_and_delete_entry()` |
| `src/dataset.rs` | 修改 | `write()` 乱序分支重写 (统一两种模式); 新增 `delete(ts)` 方法 |
| `src/ffi.rs` | 新增 | `tmsl_dataset_delete` extern "C" 函数 |
| `include/timslite.h` | 新增 | `tmsl_dataset_delete` C 声明 |
| `tests/integration_test.rs` | 新增 | 乱序写入 + delete 集成测试 (3 tests) |
| `README.md` | 修改 | header state 表 `reserved` → `invalid_record_count` |

## 5. 测试计划

### 5.1 单元测试 (src/dataset.rs `#[cfg(test)]`)

| 测试名 | 覆盖场景 |
|--------|---------|
| `test_out_of_order_write_continuous_fill_to_real` | 连续模式: write(1) → write(3) → write(2) (filler[2] → 真实, invalid_record_count 不变) |
| `test_out_of_order_write_continuous_real_to_real` | 连续模式: write(1) → write(2) → write(3) → write(2) (真实→真实, old seg invalid_record_count++) |
| `test_out_of_order_write_non_continuous` | 非连续模式: write(1) → write(2) → write(3) → write(2) (真实→真实, invalid_record_count++) |
| `test_out_of_order_write_no_entry_error` | 非连续模式: write(1) → write(3) → write(2) — ts=2 从未写入 → Error |
| `test_delete_existing_entry` | write(1) → write(2) → delete(1) → query(1,1) 空; invalid_record_count = 1 |
| `test_delete_filler_entry_error` | 连续模式: write(1) → write(5) → delete(3) (filler) → Error("not found") |
| `test_delete_nonexistent_error` | write(1) → write(2) → delete(99) → Error("not found") |
| `test_delete_idempotent_error` | write(1) → delete(1) → delete(1) → Error("not found") |
| `test_invalid_record_count_reopen` | write(1) → write(2) → write(1) (乱序) → close → reopen → invalid_record_count 持久化 |
| `test_delete_then_write_same_ts` | write(1) → delete(1) → write(1) (乱序, ts=1 旧条目为 filler → 替换为真实, invalid_record_count 不变) |

### 5.2 集成测试 (tests/integration_test.rs)

| 测试名 | 覆盖场景 |
|--------|---------|
| `t18_1_out_of_order_write` | 连续 + 非连续: 正序写入 → 乱序写入 → 查询验证最新数据 → 检查 old segment invalid_record_count |
| `t18_2_delete_lifecycle` | 写入 10 条 → delete(5) → query 验证 9 条 → close → reopen → 验证持久化 |
| `t18_3_mixed_operations` | write → delete → rewrite same ts → correction write → 验证数据正确性 |

## 6. 验收标准

- [ ] `reserved` 字段在所有设计文档和代码中均替换为 `invalid_record_count`
- [ ] `write(ts < latest)` 在连续模式和非连续模式均支持 (索引存在条目时)
- [ ] 乱序写入时旧数据段 `invalid_record_count` 正确递增 (真实数据被替代时)
- [ ] 连续模式 filler 替换时 `invalid_record_count` 不变
- [ ] `delete(ts)` 成功将真实条目覆盖为哨兵 + old segment `invalid_record_count++`
- [ ] `delete(ts)` 对 filler / 不存在条目 / 重复删除返回 `TmslError::NotFound`
- [ ] 查询路径无需修改, 被 delete 的记录自动不可见
- [ ] FFI `tmsl_dataset_delete` 函数实现 + C 头文件声明
- [ ] 单元测试 + 集成测试全部通过
- [ ] `cargo clippy -- -D warnings` clean
- [ ] `cargo fmt -- --check` clean

## 7. 依赖

- **Phase 17** (✅ 已完成): `find_entry` 三级搜索机制可直接复用于 `update_entry` 和 `find_and_delete_entry`
- **Phase 16** (✅ 已完成): `reclaim_expired_segments` 可参考 closed segment 的 lazy_open pattern
- **Phase 10** (✅ 已完成): filler 哨兵值机制 + 连续模式 O(1) 定位

## 8. 未来扩展 (不在本 Phase 范围)

- **Compaction**: 当 `invalid_record_count / record_count > 阈值` 时触发段压缩, 物理回收孤儿记录空间
- **invalid_record_count 监控 API**: 暴露段级统计信息, 辅助运维决策
- **Batch delete**: 范围删除多个时间戳 (`delete_range(start_ts, end_ts)`)