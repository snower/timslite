# Phase 33: Dirty Segment Flush Queue

> 目标: 用 per-segment dirty 状态和 Store 级共享 dirty flush queue 替代后台全量 dataset/segment flush, 将默认 flush 间隔从 10 分钟缩短到 15 秒, 同时控制 mmap.flush() 次数。

## 33.1 设计契约

- [x] 每个 data/index segment 保存内存态 `is_flushed`。
- [x] 每次 mmap 写入后置 `is_flushed=false`。
- [x] `is_flushed` 从 `true` 变为 `false` 时, 把 `{ dataset_key, segment_target }` 加入 Store 级共享 `flush_queue`。
- [x] 后台 flush 默认间隔调整为 15 秒。
- [x] 后台 flush drain 全局等待队列并清空, 按 dataset key 去重后只定位队列涉及的 dataset。
- [x] 后台 flush 不遍历未出现在 dirty queue 中的 dataset, 也不锁这些 dataset。
- [x] `DataSet::flush()` 只同步当前 dataset 的 dirty segment, 并清理全局队列中属于当前 dataset 的 stale target。
- [x] 创建新分段文件时, 直接 flush 前一个已经完结的分段文件。
- [x] idle-close 前仍执行 sync; 若队列中还有该 segment 的 stale target, 后续 flush 跳过即可。

## 33.2 开发任务

- [x] 更新 `StoreConfig` 默认 `flush_interval` 和相关测试。
- [x] 在 `DataSetRuntimeContext` 增加 Store 级共享 `flush_queue` 引用。
- [x] 在 `DataSegment` 增加 `is_flushed` / `queued_for_flush`, 并在所有 mmap 写入路径标记 dirty。
- [x] 在 `IndexSegment` 增加 `is_flushed` / `queued_for_flush`, 并在 append/overwrite 路径标记 dirty。
- [x] 在 `DataSet` 增加 dirty target 收集、入队、当前 dataset dirty flush 逻辑。
- [x] 在 `BackgroundTasks` 增加全局 dirty queue drain 和按 key 定位 flush 逻辑。
- [x] 在 `DataSegmentSet` rollover 和 `TimeIndex` 新 segment 创建前 flush 已完结 segment。
- [x] 保持无 runtime context 的低层 `DataSet::flush()` 退化为同步所有打开 segment。
- [x] 补充 dirty queue、默认间隔、idle-close stale target、rollover direct flush、后台不锁未入队 dataset 测试。

## 33.3 验证命令

- [x] `cargo test config::tests -- --test-threads=1`
- [x] `cargo test dataset::tests::test_dirty_flush_queue -- --test-threads=1`
- [x] `cargo test dataset::tests::test_data_rollover_flushes_completed_previous_segment -- --test-threads=1`
- [x] `cargo test bg::tests::test_flush_drains_queue_without_locking_unqueued_dataset -- --test-threads=1`
- [x] `cargo test bg::tests -- --test-threads=1`
- [x] `cargo test -- --test-threads=1`
- [x] `cargo check`
- [x] `cargo clippy -- -D warnings`
- [x] `cargo fmt -- --check`

## 33.4 完成状态

- 当前状态: 已完成, 等待审核。
- 提交策略: 本 phase 完成后先等待审核, 不自动 commit。
