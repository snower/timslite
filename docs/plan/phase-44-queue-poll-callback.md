# Phase 44: Queue Poll Callback

> 目标: 为普通 `DatasetQueueConsumer` 与专用 `JournalQueueConsumer` 增加轻量 `poll_callback` 唤醒 API, 覆盖 Rust、C ABI 与 Python wrapper。

---

## 背景

外部消费线程可以先调用 `poll(0)` 做非阻塞检查; 如果暂无数据, 注册一个无参 wake callback 后释放线程资源。后续 queue 数据通知发生时, callback 只负责唤醒外部调度, 由调用方重新进入正常 `poll/ack` 流程处理数据。

该 callback 不是可靠事件订阅, 不参与 queue state 或一致性处理。

---

## 设计决策

- `poll_callback(Some(callback))` 注册无参轻量唤醒回调, `poll_callback(None)` 清除。
- callback 在数据通知完成 waiter `notify_all()` 后由触发通知的线程同步执行。
- callback 仅用于唤醒外部处理线程, 不得执行数据处理、耗时逻辑或依赖精确通知次数。
- callback 不记录 generation, 不补偿 lost wake, 不改变 pending、`processed_ts`、retry、ack 或 journal sequence 语义。
- `DatasetQueue::push()` 由 dataset normal write hook 触发 callback, 额外的 push waiter wake 不重复触发 callback。
- `JournalQueueConsumer` 与普通 `DatasetQueueConsumer` 保持同名 API。
- C ABI 使用 `TmslQueuePollCallback callback, void* userdata`; `callback == NULL` 清除。
- Python wrapper 使用 `poll_callback(callable_or_none)`, 回调异常写为 unraisable, 不影响写入路径。

---

## 任务清单

- [x] 设计文档 — `design.md`, `docs/design/queue-overview.md`, `docs/design/queue-state-file.md`, `docs/design/journal.md`, `docs/design/store-and-ffi.md`, `wrapper/python/design.md`
- [x] 计划文档 — `docs/plan/phase-44-queue-poll-callback.md`, `docs/plan/overview.md`, `plan.md`, `wrapper/python/plan.md`
- [x] Rust RED/GREEN — 普通 queue callback 注册、触发、清除; journal queue callback 注册、触发、清除
- [x] Rust 实现 — `QueueNotifier`, `QueuePollCallback`, `DatasetQueueConsumer::poll_callback`, `JournalQueueConsumer::poll_callback`
- [x] FFI/C header — `TmslQueuePollCallback`, 普通 queue 与 journal queue callback 注册/清除入口
- [x] Python wrapper — `DatasetQueueConsumer.poll_callback`, `JournalQueueConsumer.poll_callback`
- [x] 验证 — targeted Rust/FFI/Python callback tests

---

## 验证计划

1. `cargo test --test queue_test t44_1_poll_callback_runs_for_dataset_queue_write_and_can_be_cleared -- --test-threads=1`
2. `cargo test --test journal_test t44_2_poll_callback_runs_for_journal_queue_and_can_be_cleared -- --test-threads=1`
3. `cargo test ffi::tests::test_ffi_queue_poll_callback_register_and_clear -- --test-threads=1`
4. `cargo test ffi::tests::test_ffi_journal_queue_poll_callback_register_and_clear -- --test-threads=1`
5. `cargo check --manifest-path wrapper/python/Cargo.toml`
6. `maturin develop --manifest-path wrapper/python/Cargo.toml`
7. `python -m pytest wrapper/python/tests/test_queue.py::TestQueueBasicFlow::test_poll_callback_wakes_and_can_be_cleared wrapper/python/tests/test_journal.py::test_journal_queue_poll_callback_wakes_and_can_be_cleared -q`
8. `cargo fmt -- --check`
9. `cargo clippy --all-targets -- -D warnings`
10. `cargo test -- --test-threads=1`
11. `python -m pytest wrapper/python/tests -q`
12. `git diff --check`
