# 测试审查待办事项

> **来源**: [docs/review/test-review.md](test-review.md)
> **创建日期**: 2026-06-05
> **最后更新**: 2026-06-05

状态标记：
- `[ ]` 待完成
- `[x]` 已完成
- `[~]` 进行中

---

## 严重（设计缺口）

| # | 问题 | 位置 | 状态 | 完成日期 |
|---|------|------|------|---------|
| S-1 | 新增 retention 回收集成测试：跨多个段写入过期数据，验证 `reclaim_expired_segments()` 实际删除文件 | `tests/` 新增或 `src/dataset.rs` 修改 | [ ] | |
| S-2 | 新增 Journal 集成测试：`enable_journal=true` 时自动创建 `.journal/logs` | `tests/` | [ ] | |
| S-3 | 新增 Journal 集成测试：DataSet create/write/delete 时自动写入 Journal 记录 | `tests/` | [ ] | |
| S-4 | 新增 Journal 集成测试：`.journal/logs` 只读强制，公开 write 应被拒绝 | `tests/` | [ ] | |
| S-5 | 新增 Journal 集成测试：Journal open_queue 实时消费 | `tests/` | [ ] | |
| S-6 | 新增 Consumer 组名校验测试：空字符串、路径分隔符、超长（>255字节）、特殊字符 | `src/queue/mod.rs` 或 `tests/queue_test.rs` | [ ] | |
| S-7 | 扩展 FFI 测试覆盖：query iterator、delete、flush、drop、error 路径、Queue FFI | `src/ffi.rs` | [ ] | |
| S-8 | 新增端到端崩溃恢复测试：partial block 写入 + 未密封 + reopen，验证 pending block 被安全密封 | `tests/` 或 `src/segment/data.rs` | [ ] | |

---

## 高（覆盖缺失）

| # | 问题 | 位置 | 状态 | 完成日期 |
|---|------|------|------|---------|
| H-1 | 新增跨段查询测试：数据跨越 2+ 个数据段，通过 QueryIterator 完整迭代 | `tests/query_test.rs` 或 `src/query/iter.rs` | [ ] | |
| H-2 | 新增数据段 2 倍扩容测试：写入超出初始分配触发扩容，验证新文件大小为 2x，header 不变 | `tests/lazy_allocation_test.rs` | [ ] | |
| H-3 | 新增 Store 级 idle-close double-check 竞态测试：后台判定 idle → 前台写入 → 后台跳过 close | `tests/background_test.rs` | [ ] | |
| H-4 | 新增后台缓存驱逐集成测试：BlockCache 空闲条目被后台任务实际驱逐 | `tests/background_test.rs` | [ ] | |
| H-5 | 新增 Journal Queue 外部 push 拒绝测试：打开 `.journal/logs` queue 调用 `push()` 应返回错误 | `tests/` | [ ] | |
| H-6 | 新增 Python 封装测试：`ds.delete(timestamp)` | `wrapper/python/tests/` | [ ] | |
| H-7 | 新增 Python 封装测试：非连续模式纠正写（同时间戳覆盖） | `wrapper/python/tests/` | [ ] | |
| H-8 | 新增 Python 封装测试：`read(timestamp=-1)` 读取最新记录 | `wrapper/python/tests/` | [ ] | |
| H-9 | 新增 Python 封装测试：对旧时间戳 append 应失败 | `wrapper/python/tests/` | [ ] | |
| H-10 | 新增端到端 append 通知流程测试：Condvar 信号 → consumer poll 返回数据 | `src/dataset.rs` 或 `tests/queue_test.rs` | [ ] | |
| H-11 | 新增 `read_entry_at_index` Python 测试 | `wrapper/python/tests/` | [ ] | |

---

## 中（测试质量）

| # | 问题 | 位置 | 状态 | 完成日期 |
|---|------|------|------|---------|
| M-1 | 修复 `test_next_delay_during_tick` 无意义断言（`delay >= 0.0` 恒为 true），替换为有意义的验证 | `src/bg/mod.rs` L676 | [ ] | |
| M-2 | 修复 `test_block_offset_routes_to_next_data_segment_after_rollover` 脆弱尺寸（180字节），加大余量或添加计算注释 | `src/dataset.rs` L1203 | [ ] | |
| M-3 | 重命名 `test_clone_queue_for_threads`（实际未使用线程），或添加真正的多线程 push/poll | `wrapper/python/tests/test_queue.py` L245 | [ ] | |
| M-4 | 收紧 `test_continuous_large_gap_filler_is_bounded_by_edge_segments` 断言边界至精确预期值 | `src/dataset.rs` L1267 | [ ] | |
| M-5 | 修复 FFI 测试临时目录泄漏：每个测试末尾清理或改用共享目录 + drop 清理 | `src/ffi.rs` L1047 | [ ] | |
| M-6 | 修复 `test_tick_bg_all_tasks_due_after_expiry` 时序脆弱：将 retention 与其他 3 个任务分开测试 | `src/bg/mod.rs` L581 | [ ] | |
| M-7 | 修复 `test_continuous_large_gap_filler_is_bounded_by_edge_segments` filler 上界过于宽松 | `src/dataset.rs` L1267 | [ ] | |
| M-8 | `test_retention_window_stored_and_roundtrip` 补充实际回收行为验证 | `src/dataset.rs` L1997 | [ ] | |
| M-9 | `tests/background_test.rs` 补充至少一个启用后台线程的测试（写入→等待→验证自动 flush） | `tests/background_test.rs` | [ ] | |
| M-10 | Python `test_store_config_custom` 补充 `retention_check_hour` 和 `enable_background_thread` 字段测试 | `wrapper/python/tests/test_config.py` | [ ] | |

---

## 低（优化建议）

| # | 问题 | 位置 | 状态 | 完成日期 |
|---|------|------|------|---------|
| L-1 | `dataset.rs` 单元测试：引入 `TestDataSetBuilder` 辅助，减少 9 参数 `DataSet::create` 重复调用 | `src/dataset.rs` | [ ] | |
| L-2 | 统一临时目录管理：创建 `#[cfg(test)] mod test_helpers`，提供 `temp_dir(name)` + 自动清理 | 新增 `src/test_helpers.rs` | [ ] | |
| L-3 | 序列化模块添加属性测试（proptest / quickcheck）：meta.rs、header.rs、block.rs、index/segment.rs | 各模块 `#[cfg(test)]` | [ ] | |
| L-4 | 新增完整查询迭代器生命周期集成测试：覆盖内存索引→磁盘索引→惰性打开段→HotBlockCache→全局 BlockCache 全 5 条路径 | `tests/query_test.rs` | [ ] | |
| L-5 | Python `test_store_manual_bg.py` 改用 conftest `tmpdir` fixture 替代手动 `mkdtemp` | `wrapper/python/tests/test_store_manual_bg.py` | [ ] | |
| L-6 | 新增 Store 操作错误路径测试：损坏目录 open、损坏 meta 文件 open、segment full 时 queue push | `tests/` | [ ] | |

---

## 进度统计

| 严重程度 | 总数 | 已完成 | 进行中 | 待完成 |
|---------|------|--------|--------|--------|
| 严重 | 8 | 0 | 0 | 8 |
| 高 | 11 | 0 | 0 | 11 |
| 中 | 10 | 0 | 0 | 10 |
| 低 | 6 | 0 | 0 | 6 |
| **合计** | **35** | **0** | **0** | **35** |
