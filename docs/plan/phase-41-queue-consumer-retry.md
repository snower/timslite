# Phase 41: Queue Consumer Retry / Visibility Timeout

> 目标: 为普通 DatasetQueue 与 JournalQueue 增加消费组级 visibility timeout 和 retry limit, 并在 QSTF v1 中以 18B pending entry 持久化 `retry_count`。

---

## 背景

旧 queue state file 的 pending entry 只有 `timestamp/start_time/status` 17B, 且后台 timeout cleanup 会直接删除超时 pending。该策略存在两个问题:

- 未 ack pending 会被立即重投, 无法表达“处理中但尚未过期”。
- timeout 删除会丢失 retry 次数, 无法按最大重试次数丢弃并推进连续 processed 水位。

Phase 41 将 pending entry 调整为 18B, 新增 `retry_count`, 并把 timeout 逻辑从后台 cleanup 移到 poll 流程中。项目仍处于首次开发阶段, 不提升 `QUEUE_STATE_VERSION`, 当前 QSTF 版本保持为 `1`。

---

## 设计决策

- QSTF version 保持为 `1`, `pending_value_size=18`, 最大 pending entries 从 `239` 改为 `226`。
- `QueueConsumerConfig` 是消费组级配置:
  - `running_expired_seconds`: 默认 `900`, `0` 表示运行期间不过期, 最大 `u16::MAX`。
  - `max_retry_count`: 默认 `3`, `0` 表示不限重试, 最大 `u8::MAX`。
- 第一次正常投递不计入 retry; 每次实际重试投递前递增 `retry_count`。
- `max_retry_count > 0 && retry_count >= max_retry_count` 时, 下一次重试机会丢弃该 pending, 不再返回给 consumer。
- `processed_ts` 只能按连续已完成 pending 前缀推进。
- `ConsumerStateFile::open_existing` 从磁盘加载时将所有未 ack pending 标记为恢复过期, 覆盖程序重启/queue 重开场景。
- 后台 flush 不再删除 timeout pending。
- JournalQueue 使用同样配置和重试语义。

---

## 任务清单

- [x] 设计文档 — `docs/design/queue-state-file.md`, `docs/design/queue-overview.md`, `docs/design/journal.md`, `docs/design/store-and-ffi.md`, `design.md`
- [x] 计划文档 — `docs/plan/phase-41-queue-consumer-retry.md`, `docs/plan/overview.md`, `plan.md`
- [x] 测试 RED — QSTF v1 18B roundtrip、未过期不重投、过期重试递增、retry 超限丢弃、组级配置冲突、JournalQueue 同步语义
- [x] Rust 实现 — `QueueConsumerConfig` builder、state file v1 18B pending、retryable pending 扫描、processed 水位推进、后台 flush 去 timeout cleanup
- [x] FFI/C header — consumer config struct、普通 queue/journal queue `open_with_config` 入口、边界校验
- [x] Python wrapper — `open_consumer(..., running_expired_seconds=900, max_retry_count=3)` 普通 queue 与 journal queue kwargs
- [x] 验证 — targeted queue/journal tests、FFI tests、Python wrapper tests、fmt/check/clippy/full cargo test/diff hygiene

---

## 验证计划

1. `cargo test queue::tests -- --test-threads=1`
2. `cargo test --test queue_test -- --test-threads=1`
3. `cargo test journal::queue::tests -- --test-threads=1`
4. `cargo test --test journal_test -- --test-threads=1`
5. FFI targeted tests covering config validation and open_with_config.
6. `cargo fmt -- --check`
7. `cargo test -- --test-threads=1`
8. `cargo check`
9. `cargo clippy --all-targets -- -D warnings`
10. `cargo test --manifest-path wrapper/python/Cargo.toml`
11. `maturin develop --manifest-path wrapper/python/Cargo.toml`
12. `python -m pytest wrapper/python/tests -q`
13. `git diff --check`
