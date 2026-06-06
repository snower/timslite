# 测试审查 — 第 3 轮（基于设计文档深度审查）

> **日期**: 2026-06-05
> **审查范围**: 所有单元测试 (src/\*)、集成测试 (tests/\*)、Python 封装测试 (wrapper/python/tests/\*)
> **审查基准**: design.md + docs/design/\*（共 18 份设计文档）
> **状态**: 仅审查，未做任何代码修改

---

## 1. 测试清单

| 层级 | 文件数 | 测试数 | 备注 |
|------|--------|--------|------|
| 单元测试 (src/\*) | 18 个模块 | ~170 | 各核心模块的 #[cfg(test)] |
| 集成测试 (tests/) | 10 个文件 | 66 | cargo test --test-threads=1 |
| Python 封装测试 | 11 个文件 | ~56 | pytest |
| **合计** | **39** | **~292** | |

### 模块级明细

| 模块 | 单元测试数 | 主要覆盖内容 |
|------|-----------|------------|
| lib.rs | 1 | 常量健全性检查 |
| config.rs | 11 | StoreConfig 构建器、DataSetConfigBuilder |
| error.rs | 6 | 错误转换、Display、source |
| util.rs | 6 | 字节序列化往返、字节序 |
| meta.rs | 8 | TLV 往返、旧格式兼容、参数校验 |
| header.rs | 9 | 数据/索引文件头往返、扩展 meta、pending 状态 |
| block.rs | 6 | BlockHeader 往返、flags、大小 |
| compress.rs | 7 | Deflate 往返、压缩级别、非法数据 |
| cache.rs | 13 | LRU、空闲驱逐、invalidate、HotBlockCache |
| segment/data.rs | 19 | 追加、密封、溢出、idle-close、重开、缓存 |
| index/segment.rs | 3 | find_entry_index、扩展文件头、覆盖写入 |
| index/mod.rs | 14 | IndexEntry 往返、范围查询、TimeIndex flush/重开 |
| dataset.rs | ~55 | append、纠正写、连续模式、retention、delete、read(-1) |
| bg/mod.rs | 12 | 执行器状态、tick、线程模式、并发 |
| queue/mod.rs | 24 | PendingEntry、ConsumerStateFile 增删改查、QueueInner |
| journal/mod.rs | 5 | 编解码、截断、文本长度限制、序列号 |
| query/iter.rs | 2 | 空 entries、filler 跳过 |
| ffi.rs | 2 | Store 生命周期、数据集创建+读取 |

---

## 2. 严重缺陷 / 逻辑错误

### 2.1 FFI 测试：临时目录无清理（src/ffi.rs）

**文件**: `src/ffi.rs` L1047-1052
**问题**: `temp_store_dir()` 在 `std::env::temp_dir()` 下创建目录但从不删除，每次测试运行都会泄漏目录。其他单元测试模块（如 `segment/data.rs`、`bg/mod.rs`）也未清理，但它们复用相同目录名，积累有限。FFI 测试为每个测试创建唯一名称，导致目录无限增长。

**建议**: 在每个测试末尾添加清理，或使用共享临时目录 + 测试唯一子目录，在 `drop` 时自动删除。

### 2.2 Retention 回收测试未真正验证段回收行为（tests/ + src/dataset.rs）

**文件**: `src/dataset.rs` L2024-2100，`tests/`（无专门的 retention 测试）
**问题**: `test_retention_reclaim_basic` 写入的数据全部在 retention 窗口内，`reclaim_expired_segments()` 始终返回 0，测试从未验证段是否能被真正回收。类似地，`test_retention_reclaim_removes_all_when_expired` 测试 retention=0（无限制），同样返回 0。

设计文档（`dataset-operations.md` §11）规定回收应删除 `max_timestamp < threshold` 的段文件。当前没有任何测试创建 max_timestamp 超出 retention 窗口的段并验证文件被物理删除。

**建议**: 新增测试：
1. 使用小 `data_segment_size` 写入多个段
2. 设置使最早段过期的 retention 窗口
3. 调用 `reclaim_expired_segments()`
4. 断言 reclaimed > 0 并验证文件已被物理删除

### 2.3 Journal 集成功能完全未测试

**设计参考**: `docs/design/journal.md` — 规定内置 `.journal/logs` 数据集、create/write/delete 时自动记录日志、只读强制、`enable_journal` 配置标志。

**问题**: 仅 `src/journal/mod.rs` 有 5 个 JournalRecord 编解码单元测试，以下场景零覆盖：
- `enable_journal=true`（StoreConfig 默认值）时自动创建 `.journal/logs`
- DataSet create/write/append/delete 时自动写入 Journal 记录
- `.journal/logs` 只读强制（公开 write 应被拒绝）
- Journal open_queue 实时消费
- Journal 数据集生命周期隔离

**建议**: 新增集成测试覆盖 Journal 自动记录、只读强制和通过 Queue 实时消费。

### 2.4 Consumer 组名校验未测试

**设计参考**: `docs/design/queue-overview.md` §28.2 — 组名必须匹配 `^[0-9A-Za-z_-]+$`，最长 255 字节。

**问题**: Rust 测试和 Python 测试均未验证 `open_consumer` / `drop_consumer` 拒绝非法组名（如包含 `/`、`\`、空格或超过 255 字节）。这是防止目录遍历的路径安全校验。

**建议**: 新增测试覆盖非法组名：空字符串、路径分隔符、超过 255 字节的名称、含特殊字符的名称。

### 2.5 `test_next_delay_during_tick` 断言无意义（src/bg/mod.rs L676）

```rust
assert!(delay.as_secs_f64() > 0.0 || delay.as_secs_f64() == 0.0);
```

等价于 `delay >= 0.0`，对任何非负 Duration 恒为 true。该测试提供零验证价值。

**建议**: 替换为有意义的断言，例如验证 `delay <= flush_interval`，或验证 `next_delay()` 在 `tick()` 持有状态锁期间能快速返回。

---

## 3. 缺失测试（差距分析）

### 3.1 FFI API 覆盖率极低

**设计参考**: `docs/design/store-and-ffi.md` — 规定约 20 个 FFI 函数。
**当前覆盖**: 仅 2 个测试，覆盖 `tmsl_store_open_with_config`、`tmsl_dataset_create`、`tmsl_dataset_write`、`tmsl_dataset_append`、`tmsl_dataset_read`、`tmsl_dataset_close`、`tmsl_store_close`、`tmsl_data_free`。

**未测试的 FFI 函数**:
- `tmsl_store_open`（无 config 版本）
- `tmsl_dataset_create_with_config`
- `tmsl_dataset_open`
- `tmsl_dataset_query` / `tmsl_iter_next` / `tmsl_iter_free_data`
- `tmsl_dataset_delete`
- `tmsl_dataset_flush`
- `tmsl_dataset_drop`
- `tmsl_store_config_default` 错误路径
- `tmsl_dataset_read_entry_at_index`
- `tmsl_store_tick_background_tasks` / `tmsl_store_next_background_delay`
- 所有 Queue 相关 FFI 函数
- 失败路径下的 `err_buf` 输出验证

### 3.2 无端到端崩溃恢复测试

**设计参考**: `docs/design/memory-and-concurrency.md` — 规定崩溃后 pending block 恢复。
**当前覆盖**: `segment/data.rs` 测试了 `idle_close` + `reopen` 的 pending 状态保持，但没有测试模拟实际崩溃场景（如写入部分 block、未密封、然后重新打开）。

**缺失场景**:
- 进程崩溃后重新打开（含部分数据的 pending block）
- 重新打开时密封 pending block（FLAGS=SEALED，按设计不压缩）
- 跨多个段有多个 pending block 时的恢复

### 3.3 无 Store 级 idle-close + double-check 竞态测试

**设计参考**: `docs/design/background-and-cache.md` §17.2 — 规定获取写锁后 double-check `last_used_at`。
**问题**: 没有测试验证以下竞态保护：
1. 后台线程读取 `last_used_at`（判定为 idle）
2. 前台写入数据集（更新 `last_used_at`）
3. 后台获取写锁后重新检查，跳过 idle-close

### 3.4 无数据段 2 倍扩容测试

**设计参考**: `docs/design/lazy-allocation.md` — 规定初始分配 + 段满时 2 倍扩容。
**当前覆盖**: `tests/lazy_allocation_test.rs` 测试了初始分配大小，但未测试扩容行为。
**缺失**: 写入超出初始分配时触发扩容的测试，验证新文件大小为 2 倍，且文件头不变。

### 3.5 无跨段查询测试

**设计参考**: `docs/design/query-iterator.md` — 规定跨多个数据段的惰性迭代。
**问题**: `query/iter.rs` 仅有 2 个单元测试，均使用单个段。没有测试查询跨越 2+ 个数据段的数据，而这是核心设计特性。

### 3.6 无 retention + 扩容交互测试

**设计参考**: `docs/design/lazy-allocation.md` + `docs/design/dataset-operations.md` §11。
**问题**: 没有测试验证段扩容后的 retention 回收行为。如果段被惰性扩容到 2x/4x，retention 能否正确回收整个文件？

### 3.7 Python 封装：缺少纠正写 / 删除 / 乱序写入测试

**当前覆盖**: Python `test_write_query.py` 有 `test_write_out_of_order_succeeds` 但缺少：
- `ds.delete(timestamp)` — Python API
- 非连续模式下的纠正写（同时间戳覆盖）
- 对旧时间戳 append（应失败）
- timestamp=-1 读取（最新记录）
- 通过 Python 调用 `read_entry_at_index`

### 3.8 无 Journal Queue 外部 push 拒绝测试

**设计参考**: `docs/design/queue-overview.md` — `.journal/logs` 的 queue 必须拒绝外部 `queue.push()`。
**问题**: 没有测试验证打开 journal 数据集的 queue 后调用 `push()` 会返回错误。

### 3.9 无 BlockCache 与后台驱逐集成测试

**设计参考**: `docs/design/background-and-cache.md` §18 — BlockCache 空闲驱逐作为后台任务运行。
**问题**: 缓存测试（`src/cache.rs`）仅孤立测试驱逐。没有测试验证后台缓存驱逐任务实际从活跃 Store 的 BlockCache 中驱逐空闲条目。

---

## 4. 不完整 / 不合理的测试

### 4.1 `test_tick_bg_all_tasks_due_after_expiry` 断言 4 个任务但时序脆弱

**文件**: `src/bg/mod.rs` L581-606
**问题**: 测试将 4 个任务截止时间全部推到过去，然后断言 `executed_tasks == 4`。但 retention 任务使用系统时钟（`next_retention_time`），测试设置 `state.next_retention = Instant::now() - Duration::from_secs(1)`。如果系统时钟的下一个 retention 边界恰好在未来（如测试在午夜后立即运行），retention 任务可能在 `tick()` 期间被重新调度，导致断言偶发失败。

**建议**: 使用更确定性的调度方式，或将 retention 与其他 3 个任务分开测试。

### 4.2 `test_block_offset_routes_to_next_data_segment_after_rollover` 尺寸脆弱

**文件**: `src/dataset.rs` L1203-1236
**问题**: 使用 `data_segment_size = 180` 字节。第一条记录（32 字节数据 + ~28 字节开销）刚好放下。测试依赖精确的字节级计算来确定何时发生 rollover。文件头大小或 block 开销的微小变化都可能导致测试失败。

**建议**: 使用稍大的段大小并留有余量，或添加注释说明精确的尺寸计算。

### 4.3 Queue `test_clone_queue_for_threads` 名称误导

**文件**: `wrapper/python/tests/test_queue.py` L245-268
**问题**: 测试名和文档字符串说"多线程各自持有 queue clone 时可以 push"，但测试完全在主线程运行，没有使用任何线程。

**建议**: 重命名以反映实际行为（queue push+poll 基本流程），或添加实际的多线程 push/poll。

### 4.4 Python `test_store_config_custom` 未测试 `retention_check_hour` 和 `enable_background_thread`

**文件**: `wrapper/python/tests/test_config.py` L22-45
**问题**: 自定义配置测试设置了许多字段，但遗漏了 `retention_check_hour` 和 `enable_background_thread`，这两个字段在 StoreConfig 中存在。这些字段可能未暴露给 Python，这是一个缺口。

**建议**: 验证所有 config 字段在 Python 中可访问，为缺失的字段添加测试。

### 4.5 集成测试 `tests/background_test.rs` 仅测试手动模式

**文件**: `tests/background_test.rs`（3 个测试）
**问题**: 全部 3 个测试使用 `enable_background_thread=false` 并调用 `tick_background_tasks()`。没有集成测试验证实际后台线程行为（如定时自动 flush）。

**建议**: 至少添加一个启用后台线程的测试：写入数据、短暂等待、验证自动 flush 已执行。

### 4.6 `test_continuous_large_gap_filler_is_bounded_by_edge_segments` 断言过于宽松

**文件**: `src/dataset.rs` L1267-1306
**问题**: 断言 `filler_count < 2 * segment_capacity - 2`。这个上界非常宽泛，无法捕获 filler 生成远超预期的回归。

**建议**: 根据间隔和段容量收紧到预期的精确值。

### 4.7 `test_retention_window_stored_and_roundtrip` 未测试回收行为

**文件**: `src/dataset.rs` L1997-2021
**问题**: 仅验证 `retention_window()` 返回存储的值，未验证 retention 窗口在实际过期数据场景下是否影响 `reclaim_expired_segments()` 行为。

---

## 5. 设计与测试对齐问题

### 5.1 retention_window 单位模糊

**设计**: `dataset-operations.md` 说 "retention_window: u64 // 数据保留窗口 (timestamp unit, 0=不限)"
**测试**: 使用 50（dataset.rs）等值，按原始 timestamp 单位处理。
**StoreConfig**: 在 builder 中使用秒，但 meta 直接存储原始值。
**问题**: retention_window 的语义取决于 timestamp 粒度。如果 timestamp 是毫秒，`retention_window=50` 表示 50ms。测试未记录假设的单位，也没有测试使用真实世界的 timestamp 值（如 Unix 纪元秒）验证 retention。

### 5.2 append() 通知行为部分测试

**设计**: `docs/design/queue-overview.md` — append 创建新时间戳时应通知等待的消费者。
**测试**: `test_append_notifies_queue_only_when_creating_new_timestamp` 正确测试了通知标志。
**差距**: 没有测试验证实际的消费者唤醒（Condvar 信号 → poll 返回数据）。测试仅检查通知标志，而非端到端通知流程。

### 5.3 Journal `.journal/logs` 数据集创建未在 Store 级别测试

**设计**: 当 `enable_journal=true`（默认）时，Store 应自动创建 `.journal/logs` 数据集。
**测试差距**: 没有测试验证 `Store::open()` 后 `.journal/logs` 数据集存在且可打开。

### 5.4 DataSet name/type 校验未在 Store 级别全面测试

**设计**: `docs/design/architecture.md` — 数据集名必须匹配 `^[0-9A-Za-z_-]+$`。
**测试**: `tests/dataset_lifecycle_test.rs` 测试了部分名称校验，但未覆盖所有边界情况（如含点号、unicode 字符、或恰好 255 字节极限的名称）。

---

## 6. 优化建议

### 6.1 减少 dataset.rs 单元测试中的样板代码

`make_cache_dataset()` 辅助函数被复用，但约 30 个其他测试重复调用含 9 个参数的完整 `DataSet::create(...)`。构建器风格的测试辅助（如 `TestDataSetBuilder`）可减少重复并提高可读性。

### 6.2 统一临时目录管理

多个模块使用不同的临时目录模式：
- `src/segment/data.rs`: `temp_dir().join(name)` + `remove_file`
- `src/bg/mod.rs`: `temp_dir().join(base)` + `remove_dir_all`
- `src/queue/mod.rs`: 基于时间戳的唯一目录
- `src/ffi.rs`: 基于名称的目录，无清理

共享测试工具模块（如 `#[cfg(test)] mod test_helpers`），提供创建唯一目录并注册清理的 `temp_dir(name)` 函数，可提高一致性。

### 6.3 添加序列化的属性测试

`meta.rs`、`header.rs`、`block.rs`、`index/segment.rs` 均使用固定值测试序列化往返。使用 `proptest` 或 `quickcheck` 进行随机化往返测试可更有效地发现边界情况（如最大值、零值、负时间戳）。

### 6.4 添加完整查询迭代器生命周期集成测试

查询迭代器设计（`docs/design/query-iterator.md`）描述了一个复杂的惰性加载迭代器：
1. 从内存索引缓冲区读取
2. 从磁盘索引段读取
3. 按需惰性打开数据段
4. 使用 HotBlockCache 进行同块顺序读取
5. 使用全局 BlockCache 缓存压缩块

没有单一测试按序覆盖全部 5 条路径。编写足够数据跨越多个索引段和数据段，然后完整迭代的综合测试，可验证完整管线。

### 6.5 Python test_store_manual_bg 未使用 fixture tmpdir

**文件**: `wrapper/python/tests/test_store_manual_bg.py`
**问题**: 使用手动 `tempfile.mkdtemp()` + `shutil.rmtree()` 而非 conftest 的 `tmpdir` fixture。与其他 Python 测试不一致，且绕过了 Windows 安全清理逻辑。

**建议**: 重构为使用 `tmpdir` fixture 以保持一致性。

### 6.6 添加 Store 操作的错误路径测试

**缺失的错误路径测试**:
- `Store::close()` 时存在未关闭的数据集句柄（FFI 测试了此场景，但 Rust 集成测试未覆盖）
- `Store::open()` 在损坏的目录结构上
- `DataSet::open()` 在损坏的 meta 文件上（无效 TLV、截断数据）
- 数据集容量已满（segment full）时 Queue `push()`

---

## 7. 按严重程度汇总

| 严重程度 | 数量 | 典型示例 |
|---------|------|---------|
| **严重**（设计缺口） | 5 | Journal 集成未测试、retention 回收从未验证、consumer 名校验缺失、FFI 覆盖率极低、崩溃恢复未测试 |
| **高**（覆盖缺失） | 7 | 跨段查询、段扩容、Store idle-close 竞态、后台缓存驱逐、journal queue 拒绝、Python delete/correction、端到端通知 |
| **中**（测试质量） | 6 | 无意义断言、脆弱尺寸、误导命名、宽松边界、临时目录泄漏、时序脆弱 |
| **低**（优化建议） | 6 | 样板代码精简、属性测试、共享测试工具、一致 tmpdir、错误路径覆盖、迭代器生命周期测试 |

---

## 8. 推荐优先处理事项

1. **新增 retention 回收集成测试** — 跨多个段写入过期数据，验证文件被物理删除
2. **新增 journal 集成测试** — 自动创建、写入日志、只读强制、队列消费
3. **新增 consumer 组名校验测试** — 非法字符、空、超长、路径遍历
4. **扩展 FFI 测试覆盖** — 至少覆盖：查询迭代器、delete、flush、drop、错误路径
5. **新增数据段 2 倍扩容测试** — 验证惰性扩容行为
6. **新增跨段查询测试** — 数据跨越 2+ 个数据段，通过 QueryIterator 查询
7. **新增崩溃恢复模拟** — 部分 block 写入 + 重新打开
8. **修复 `test_next_delay_during_tick` 中的无意义断言**
9. **修复 FFI 测试中的临时目录清理**
10. **新增 Python 封装测试** — delete、纠正写、read(-1)、retention
