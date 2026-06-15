# Phase 27: Queue 模块

> 目标: 在 Dataset 之上实现队列语义, 支持多消费组、多 Consumer 实例、持久化消费进度、等待/通知机制。

> 更新: Phase 41 保持 QSTF version=1, 将 pending entry 从 17B 改为 18B 并新增 `retry_count`; 后台 timeout cleanup 已被 poll-time retry/丢弃逻辑取代。本文保留 Phase 27 建设背景, 当前契约以 Phase 41 和 `docs/design/queue-state-file.md` 为准。

## 27.0 设计文档

- [Queue 架构与 API](../design/queue-overview.md)
- [Queue 状态文件格式与同步](../design/queue-state-file.md)

## 27.1 核心数据结构与状态文件

**目标**: 实现 4KB mmap 状态文件 (`ConsumerStateFile`) 和 pending entry 管理。

### 实现任务

- [x] **创建 `src/queue/mod.rs`** — 模块入口, 导出核心类型
- [x] **实现 `ConsumerStateFile` 结构**
  - [x] 字段: `path: PathBuf`, `mmap: MmapMut`, `processed_ts: i64`, `pending_entries: Vec<PendingEntry>`
  - [x] 常量: `STATE_FILE_SIZE = 4096`, `PENDING_ENTRY_SIZE = 18`, `MAX_PENDING_ENTRIES = 226`
- [x] **实现 `PendingEntry` 结构**
  - [x] 字段: `timestamp: i64`, `start_time: i64`, `status: u8` (0=待ack, 1=已ack), `retry_count: u8`
- [x] **实现状态文件操作**
  - [x] `open_or_create(path, initial_processed_ts)` — 打开或创建 4KB mmap 文件
  - [x] `sync()` — 将内存状态写入 mmap (不立即 flush, 由后台任务统一同步)
  - [x] `add_pending(entry)` — 添加 pending entry (容量检查: max 226)
  - [x] `find_pending(timestamp)` / `find_pending_mut(timestamp)` — 查找待 ack entry
  - [x] `is_in_pending(timestamp)` — 检查 timestamp 是否在 pending 中
  - [x] `update_processed_ts()` — 扫描连续 ack, 更新 processed_ts
  - [x] `cleanup_acked()` — 清理已 ack entries
  - [x] `take_retryable_pending(config, now)` — poll 时处理过期 retry 和 retry 超限丢弃
- [x] **实现状态文件序列化**
  - [x] Header: magic "QSTF" (4B) + version (4B) + state_length (2B) + processed_ts (8B) + pending_length (2B) + pending_value_size (1B)
  - [x] Pending entry: timestamp (8B) + start_time (8B) + status (1B) + retry_count (1B)
  - [x] 读取/写入辅助函数: `read_i64()`, `write_i64()`, `read_u16()`, `write_u16()`, `read_u32()`, `write_u32()`

### 测试策略

- [x] **单元测试**: `test_state_file_create_and_open`
  - 创建新状态文件, 验证 magic/version/header 正确
  - 重新打开, 验证 processed_ts 和 pending_entries 恢复
- [x] **单元测试**: `test_state_file_pending_operations`
  - 添加/查找/清理 pending entries
  - 验证容量限制 (226 entries)
- [x] **单元测试**: `test_state_file_processed_ts_update`
  - 连续 ack 后 update_processed_ts()
  - 非连续 ack 不更新 processed_ts

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] `cargo fmt -- --check` 格式正确
- [x] 所有单元测试通过

---

## 27.2 QueueInner 与 DatasetQueue 基础设施

**目标**: 实现 QueueInner 共享状态和 DatasetQueue 核心结构, 支持 push 操作。

### 实现任务

- [x] **实现 `QueueInner` 结构**
  - [x] 字段: `consumers: HashMap<String, Vec<Arc<ConsumerStateFile>>>`, `closed: AtomicBool`
- [x] **实现 `DatasetQueue` 结构**
  - [x] 字段: `dataset: Arc<Mutex<DataSet>>`, `inner: Arc<Mutex<QueueInner>>`, `notify: Arc<(Mutex<bool>, Condvar)>`
  - [x] 实现 Clone trait (共享 Arc 内部状态)
- [x] **实现 `DatasetQueue::push()`**
  - [x] 检查 closed 标志
  - [x] 获取 dataset 锁
  - [x] 计算 timestamp = `latest_written_timestamp + 1`
  - [x] 调用 `dataset.write(timestamp, data)`
  - [x] 返回分配的 timestamp
  - [x] 注意: 不立即 sync, 由后台 flush 任务统一同步
- [x] **实现 `DatasetQueue::close()`**
  - [x] 设置 `closed.store(true, Ordering::SeqCst)`
  - [x] 获取 notify guard, 设置 flag=true, `notify_all()`, 设置 flag=false
  - [x] 标记 Dataset `queue_inner = None`, `queue_notify = None`
  - [x] 状态文件保持打开, 不删除

### 测试策略

- [x] **单元测试**: `test_queue_push_basic`
  - 创建 dataset, open_queue, push 数据
  - 验证返回的 timestamp = latest + 1
  - 验证 dataset.latest_written_timestamp 更新
- [x] **单元测试**: `test_queue_push_multiple`
  - 连续 push 多条数据
  - 验证 timestamp 单调递增
- [x] **单元测试**: `test_queue_close`
  - push 后 close
  - 验证 closed 标志设置
  - 验证后续 push 返回 QueueClosed 错误

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] push 操作正确分配 timestamp 并写入 dataset

---

## 27.3 Dataset 集成 — open_queue / close_queue / Write Hook

**目标**: 在 DataSet 上集成 queue 功能, 实现 open_queue/close_queue 和 write hook 通知机制。

### 实现任务

- [x] **DataSet 新增字段**
  - [x] `queue_inner: Option<Arc<Mutex<QueueInner>>>`
  - [x] `queue_notify: Option<Arc<(Mutex<bool>, Condvar)>>`
- [x] **实现 `DataSet::open_queue()`**
  - [x] 检查 `queue_inner.is_some()`, 若已打开返回已存在的 queue handle
  - [x] 创建 `QueueInner` 实例
  - [x] 创建 `Condvar` pair
  - [x] 设置 `self.queue_inner = Some(...)`, `self.queue_notify = Some(...)`
  - [x] 返回 `DatasetQueue` handle
- [x] **实现 `DataSet::close_queue()`**
  - [x] 检查 `queue_inner.is_some()`, 若未打开返回 QueueNotOpen 错误
  - [x] 调用 `DatasetQueue::close()` 逻辑
  - [x] 清理 `self.queue_inner = None`, `self.queue_notify = None`
- [x] **实现 Write Hook**
  - [x] 在 `DataSet::write_with_cache()` 中, 正常写入成功后 (timestamp > old_latest)
  - [x] 检查 `self.queue_notify.is_some()`
  - [x] 获取 notify guard, 设置 flag=true, `condvar.notify_all()`, 设置 flag=false
  - [x] 注意: correction/out-of-order 写入不触发通知
- [x] **实现 Idle-Close 阻塞**
  - [x] 在 `bg/mod.rs` 的 idle-check 逻辑中, 检查 `dataset.queue_inner.is_some()`
  - [x] 若 queue 打开, 跳过该 dataset 的 idle-close

### 测试策略

- [x] **单元测试**: `test_dataset_open_queue`
  - open_queue 返回 DatasetQueue
  - 重复 open_queue 返回相同 queue handle
- [x] **单元测试**: `test_dataset_close_queue`
  - close_queue 清理 queue_inner
  - 重复 close_queue 返回 QueueNotOpen 错误
- [x] **单元测试**: `test_write_hook_notification`
  - open_queue 后 push 数据
  - 验证 queue_notify 被触发 (可通过 consumer poll 验证)
- [x] **单元测试**: `test_idle_close_blocked_by_queue`
  - open_queue 后, dataset 不被 idle-close
  - close_queue 后, dataset 可被 idle-close

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] open_queue/close_queue 正确管理 queue 生命周期
- [x] write hook 正确触发通知 (仅正常写入)
- [x] queue 打开时 dataset 不被 idle-close

---

## 27.4 DatasetQueueConsumer 实现

**目标**: 实现消费者核心功能 — poll 和 ack。

### 实现任务

- [x] **实现 `DatasetQueueConsumer` 结构**
  - [x] 字段: `group_name: String`, `state_file: Arc<Mutex<ConsumerStateFile>>`, `notify: Arc<(Mutex<bool>, Condvar)>`, `dataset: Arc<Mutex<DataSet>>`, `closed: Arc<AtomicBool>`
  - [x] 实现 Clone trait (共享 Arc 内部状态)
- [x] **实现 `DatasetQueue::open_consumer()`**
  - [x] 检查 closed 标志
  - [x] 构建状态文件路径: `dataset.base_dir/queue/{group_name}`
  - [x] 获取 `initial_processed_ts = dataset.latest_written_timestamp()`
  - [x] 调用 `ConsumerStateFile::open_or_create()`
  - [x] 恢复: `cleanup_acked()` 清理已 ack 但未清理的 entries
  - [x] 注册到 `QueueInner.consumers`
  - [x] 返回 `DatasetQueueConsumer` handle
- [x] **实现 `DatasetQueue::drop_consumer()`**
  - [x] 检查 closed 标志
  - [x] 从 `QueueInner.consumers` 移除
  - [x] 删除状态文件 (Arc 引用计数管理)
- [x] **实现 `DatasetQueueConsumer::poll(timeout)`**
  - [x] 循环:
    1. 检查 closed 标志 → 返回 QueueClosed 错误
    2. 获取 dataset 锁
    3. 再次检查 closed (可能在等锁期间被关闭)
    4. 获取 state_file 锁
    5. 调用 `find_next_available_ts()` 查找可分配的 timestamp
       - 从 `processed_ts + 1` 开始扫描
       - 跳过已在 pending 中的 timestamp
       - 返回第一个可用 timestamp, 或 None
    6. 若找到:
       - 创建 `PendingEntry { timestamp, start_time: now_unix_epoch(), status: 0 }`
       - `add_pending(pending)`
       - 读取数据: `dataset.read(timestamp, None)`
       - 若数据为空 (filler entry), 自动 ack 并继续循环
       - 返回 `Some((timestamp, data))`
    7. 若未找到:
       - 释放 state_file 锁和 dataset 锁
       - 计算剩余 timeout
       - 获取 notify guard, `condvar.wait_timeout(guard, remaining)`
       - 若 timeout, 返回 `None`
       - 否则继续循环
  - [x] 注意: 不立即 sync, 由后台 flush 任务统一同步
- [x] **实现 `DatasetQueueConsumer::ack(timestamp)`**
  - [x] 检查 closed 标志
  - [x] 获取 state_file 锁
  - [x] `find_pending_mut(timestamp)` 查找待 ack entry
  - [x] 设置 `entry.status = 1`
  - [x] `update_processed_ts()` 扫描连续 ack
  - [x] `cleanup_acked()` 清理已 ack entries
  - [x] 注意: 不立即 sync, 由后台 flush 任务统一同步

### 测试策略

- [x] **单元测试**: `test_consumer_open_and_drop`
  - open_consumer 创建状态文件
  - drop_consumer 删除状态文件
  - 重复 open_consumer 相同 group 返回相同 consumer handle
- [x] **单元测试**: `test_consumer_poll_basic`
  - push 数据后 poll
  - 验证返回正确的 (timestamp, data)
  - 验证 pending entry 创建
- [x] **单元测试**: `test_consumer_poll_timeout`
  - 无数据时 poll(timeout)
  - 验证返回 None
- [x] **单元测试**: `test_consumer_poll_wait_for_push`
  - 线程 A: poll(5s) 等待
  - 线程 B: push 数据
  - 验证线程 A 被唤醒并返回数据
- [x] **单元测试**: `test_consumer_ack`
  - poll 后 ack
  - 验证 processed_ts 更新
  - 验证 pending entry 清理
- [x] **单元测试**: `test_consumer_ack_consecutive`
  - 连续 poll 多条数据
  - 按序 ack
  - 验证 processed_ts 连续更新
- [x] **单元测试**: `test_consumer_ack_non_consecutive`
  - poll ts=1, ts=2, ts=3
  - 先 ack ts=3, 再 ack ts=1
  - 验证 processed_ts 只在 ack ts=2 后更新到 3
- [x] **单元测试**: `test_multi_consumer_same_group`
  - 打开多个 consumer 实例 (相同 group)
  - 并发 poll
  - 验证每个 consumer 拿到不同数据
- [x] **单元测试**: `test_multi_consumer_different_groups`
  - 打开多个 consumer 实例 (不同 group)
  - 每个 consumer 独立 poll
  - 验证每个 group 都收到所有数据

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] poll 正确等待/超时/返回数据
- [x] ack 正确更新 processed_ts 并清理 pending
- [x] 多 consumer 并发安全

---

## 27.5 Crash 恢复与状态文件管理

**目标**: 实现 crash 后状态文件恢复, 保证 at-least-once 语义。

### 实现任务

- [x] **实现 `ConsumerStateFile` 恢复逻辑**
  - [x] `open_existing()` 时验证 magic/version
  - [x] 读取 processed_ts 和 pending_entries
  - [x] 恢复时调用 `cleanup_acked()` 清理已 ack 但未清理的 entries
  - [x] 所有 `status=0` 的 pending entries 保留并设置 `start_time=0`, 下次 poll 按 retry 规则处理
- [x] **实现 poll-time retry/丢弃**
  - [x] `running_expired_seconds` 控制运行中未 ack pending 的可重试时间
  - [x] `max_retry_count` 控制重试上限; 超限后标记完成并按连续完成前缀推进 `processed_ts`
  - [x] 后台任务不删除超时 pending

### 测试策略

- [x] **单元测试**: `test_state_file_crash_recovery`
  - 创建状态文件, 添加 pending entries
  - 模拟 crash (不 sync, 直接 drop)
  - 重新打开, 验证状态恢复
- [x] **单元测试**: `test_state_file_retry_scan`
  - 添加 pending entry, start_time 设为过去时间
  - 调用 retryable pending scan
  - 验证 retry_count 递增或超限 entry 被标记完成

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] crash 后状态文件正确恢复
- [x] 过期 pending entries 被正确重试或按上限丢弃

---

## 27.6 后台任务集成

**目标**: 将 queue 状态文件纳入后台 flush 任务, 统一同步策略。

### 实现任务

- [x] **修改 `bg/mod.rs` flush 逻辑**
  - [x] 遍历所有 dataset 时, 检查 `dataset.queue_inner`
  - [x] 若 queue 打开, 遍历所有 consumer state files
  - [x] 调用 `sync()` 写入 mmap
  - [x] 注意: 不立即 flush, 由 dataset.flush() 统一同步
- [x] **验证 idle-close 阻塞**
  - [x] 确保 idle-check 逻辑正确检查 `queue_inner.is_some()`

### 测试策略

- [x] **单元测试**: `test_flush_includes_queue_state`
  - open_queue, push, poll, ack
  - 触发 flush
  - 验证状态文件已同步到磁盘
- [x] **单元测试**: `test_flush_preserves_retry_state`
  - 添加 pending entry
  - 触发 flush
  - 验证 flush 仅同步状态, 不删除未 ack pending

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] flush 任务正确同步 queue 状态文件
- [x] flush 任务正确清理超时 pending entries

---

## 27.7 错误类型扩展

**目标**: 在 `TmslError` 中添加 queue 相关错误变体。

### 实现任务

- [x] **扩展 `src/error.rs`**
  - [x] `QueueAlreadyOpen` — 重复 open_queue
  - [x] `QueueNotOpen` — close_queue 或 consumer 操作时 queue 未打开
  - [x] `ConsumerGroupNotFound(String)` — drop_consumer 或 ack 时消费组不存在
  - [x] `ConsumerGroupExists(String)` — open_consumer 时消费组已存在 (若要求不存在)
  - [x] `QueueClosed` — poll/ack/push 操作时 queue 已关闭
  - [x] `PendingFull` — poll 时 pending 列表已满 (max 239)
- [x] **实现 `Display` 和 `Error` trait**
- [x] **更新 FFI 错误映射** (若有)

### 测试策略

- [x] **单元测试**: 验证所有新错误变体的 Display 输出

### 验收标准

- [x] `cargo build` 编译通过
- [x] `cargo clippy -- -D warnings` 无警告
- [x] 所有单元测试通过
- [x] 错误信息清晰准确

---

## 27.8 集成测试

**目标**: 端到端测试 queue 功能, 验证多场景下的正确性和性能。

### 测试策略

- [x] **集成测试**: `test_queue_end_to_end`
  - 创建 dataset, open_queue
  - 打开多个 consumer (不同 group)
  - 并发 push 100 条数据
  - 每个 consumer poll 并 ack
  - 验证每个 group 都收到所有 100 条数据
  - 验证 processed_ts 正确更新
- [x] **集成测试**: `test_queue_crash_recovery`
  - push 数据, poll 但不 ack
  - 模拟 crash (不 clean shutdown)
  - 重新打开 dataset, open_queue, open_consumer
  - 验证未 ack 的数据重新可 poll
- [x] **集成测试**: `test_queue_concurrent_push_poll`
  - 线程 A: 连续 push 1000 条数据
  - 线程 B: 连续 poll + ack
  - 验证所有数据被正确消费
- [x] **集成测试**: `test_queue_multi_consumer_same_group`
  - 打开 5 个 consumer (相同 group)
  - push 100 条数据
  - 并发 poll
  - 验证 100 条数据被 5 个 consumer 分配消费
  - 验证 processed_ts 最终更新到 100

### 验收标准

- [x] 所有集成测试通过
- [x] 无数据丢失或重复 (at-least-once 语义)
- [x] 并发场景下无死锁或 panic

---

## 27.9 设计文档更新

**目标**: 更新项目文档, 反映 queue 模块的添加。

### 实现任务

- [x] **更新 `design.md`**
  - [x] 在设计文档索引表中添加 Queue 相关条目
  - [x] 在快速导航中添加 queue 相关链接
- [x] **更新 `docs/plan/overview.md`**
  - [x] 在总体里程碑列表中添加 Phase 27
  - [x] 在依赖关系图中添加 Phase 27 依赖 (Phase 5, Phase 6)
- [x] **更新 `plan.md`**
  - [x] 在计划状态总览表中添加 Phase 27 行
  - [x] 在文档结构中添加 phase-27-queue-module.md

### 验收标准

- [x] 所有文档更新完成
- [x] 链接和引用正确

---

## 27.10 依赖关系

```
Phase 5 (DataSet + DataSegmentSet)
    │
    ▼
Phase 6 (Store + 后台任务)
    │
    ▼
Phase 27 (Queue 模块)
```

**前置依赖**:
- Phase 5: DataSet 结构和 write/read API
- Phase 6: 后台任务 (flush, idle-check)

**后续依赖**:
- 无 (queue 模块是独立功能)

---

## 27.11 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|----------|
| Condvar 竞态条件 | poll 可能丢失通知 | 使用循环 + flag 模式, 唤醒后重新检查 |
| 死锁 | 系统挂起 | 严格遵循锁层级: Store → Dataset → QueueInner → ConsumerStateFile |
| 状态文件损坏 | crash 后无法恢复 | 验证 magic/version, 损坏时重建 |
| Pending 容量不足 | poll 失败 | 返回 PendingFull 错误, 调用方处理 |
| 超时清理误删 | 数据重复消费 | at-least-once 语义保证, 调用方需幂等 |

---

## 27.12 文件清单

### 新增文件

- `src/queue/mod.rs` — 模块入口
- `src/queue/state.rs` — ConsumerStateFile 和 PendingEntry
- `src/queue/queue.rs` — DatasetQueue 和 QueueInner
- `src/queue/consumer.rs` — DatasetQueueConsumer

### 修改文件

- `src/dataset.rs` — 添加 queue_inner/queue_notify 字段, open_queue/close_queue, write hook
- `src/bg/mod.rs` — flush 逻辑添加 queue 状态文件同步, idle-check 添加 queue 阻塞
- `src/error.rs` — 添加 queue 相关错误变体
- `src/lib.rs` — 导出 queue 模块

---

## 27.13 验收总览

- [x] 所有子阶段任务完成
- [x] `cargo build` 编译通过
- [x] `cargo clippy --all-targets -- -D warnings` 无警告
- [x] `cargo fmt -- --check` 格式正确
- [x] 所有单元测试通过 (27 个 queue 单元测试)
- [x] 所有集成测试通过 (15 个 queue 集成测试)
- [x] Python 功能测试通过 (14 个 Python queue 测试)
- [x] 设计文档更新完成
- [x] plan.md 更新完成

### 测试统计

| 测试层 | 测试数量 | 状态 |
|--------|----------|------|
| Rust 单元测试 (src/queue/mod.rs #[cfg(test)]) | 27 | ✅ |
| Rust 集成测试 (tests/integration_test.rs t27_*) | 15 | ✅ |
| Python 测试 (wrapper/python/tests/test_queue.py) | 14 | ✅ |
| 主 crate 总测试数 | 244 (200 lib + 44 integration) | ✅ |
| Python 总测试数 | 56 (42 existing + 14 new) | ✅ |
