# timslite 开发计划

> 基于 design.md 详细设计  
> 目标: 完成 Rust cdylib 时序数据存储库, 提供 C ABI FFI

---

## 计划状态总览

| Phase | 描述 | 状态 | 详情 |
|-------|------|------|------|
| 1 | 项目骨架 + 基础工具 | ✅ 完成 | [phase-01-skeleton.md](docs/plan/phase-01-skeleton.md) |
| 2 | 文件头 + Block 核心 | ✅ 完成 | [phase-02-header-block.md](docs/plan/phase-02-header-block.md) |
| 3 | DataSegment 写入/读取 | ✅ 完成 | [phase-03-datasegment.md](docs/plan/phase-03-datasegment.md) |
| 4 | 时间索引系统 | ✅ 完成 | [phase-04-time-index.md](docs/plan/phase-04-time-index.md) |
| 5 | DataSegmentSet + DataSet | ✅ 完成 | [phase-05-dataset.md](docs/plan/phase-05-dataset.md) |
| 6 | Store 门面 + 后台任务 | ✅ 完成 | [phase-06-store-bg.md](docs/plan/phase-06-store-bg.md) |
| 7 | FFI 接口 | ✅ 完成 | [phase-07-ffi.md](docs/plan/phase-07-ffi.md) |
| 8 | 集成测试 + 性能调优 | ⚠️ 核心完成, 待benchmarks | [phase-08-tests-perf.md](docs/plan/phase-08-tests-perf.md) |
| 9 | 读缓存池 (BlockCache) | ✅ 完成 | [phase-09-blockcache.md](docs/plan/phase-09-blockcache.md) |
| 10 | 索引连续存储 | ✅ 完成 | [phase-10-continuous-storage.md](docs/plan/phase-10-continuous-storage.md) |
| 11 | 连续模式 O(1) 查询优化 | ✅ 完成 | [phase-11-o1-optimization.md](docs/plan/phase-11-o1-optimization.md) |
| 12 | 分段懒分配 + 倍率扩容 | ✅ 完成 (含4项集成测试) | [phase-12-lazy-allocation.md](docs/plan/phase-12-lazy-allocation.md) |
| 13 | 查询迭代器 + HotBlockCache | ✅ 完成 | [phase-13-query-iterator.md](docs/plan/phase-13-query-iterator.md) |
| 14 | create_dataset Builder 优化 | ✅ 完成 | [phase-14-dataset-config-builder.md](docs/plan/phase-14-dataset-config-builder.md) |
| 15 | Header State 分化 | ✅ 完成 | [phase-15-header-state-split.md](docs/plan/phase-15-header-state-split.md) |
| 16 | 数据保留 (Retention) | ✅ 完成 | [phase-16-data-retention.md](docs/plan/phase-16-data-retention.md) |
| 17 | 纠正写入 (Correction Write) | ✅ 完成 | [phase-17-correction-write.md](docs/plan/phase-17-correction-write.md) |
| 18 | 乱序写入与删除 (Out-of-Order Write & Delete) | ✅ 完成 | [phase-18-out-of-order-write-and-delete.md](docs/plan/phase-18-out-of-order-write-and-delete.md) |
| 19 | 单时间戳读取 (Single Timestamp Read) | ✅ 完成 | (本节) |
| 20 | 最新时间戳读取 (Latest Timestamp Read) | ✅ 完成 | (本节) |
| 21 | 后台任务手动执行 (Manual Background Execution) | ✅ 完成 | (本节) |
| 22 | Manual Background Execution Python Wrapper | ✅ 完成 | (本节) |
| PY | Python Package (PyO3) | ✅ 完成 | [wrapper/python/plan.md](wrapper/python/plan.md) |

## 待完成事项

### Phase 5: DataSet 生命周期 ✅ 已完成
- [x] `DataSet::open` 对不存在数据集 → 返回 `NotFound` 错误
  - 实现: `dataset.rs` `DataSet::open()` (meta不存在时返回TmslError::NotFound)
  - 测试: `t8_2_2_open_returns_error_if_not_exists`
- [x] `DataSet::open` 后写入 → close → reopen → 验证所有数据可读
  - 测试: `t8_1_6_persistence` (写入50条→close→reopen→query验证50条)
- [x] 时间范围查询 (部分数据) → 验证数量和顺序
  - 测试: `t8_1_1_basic_lifecycle` (query(1,100), 验证len=100且顺序正确)
- [x] `DataSet::drop_dataset` 删除后目录不可访问
  - 测试: `t8_2_3_drop_deletes_dataset` + `t8_2_4_create_after_drop`

### Phase 6: Store 门面 ✅ 已完成
- [x] `Store::create_dataset` → 创建成功, 再次调用 → `AlreadyExists`
  - 测试: `t8_2_1_create_returns_error_if_exists`
- [x] `Store::open_dataset` → 打开成功, 不存在 → `NotFound`
  - 测试: `t8_2_2_open_returns_error_if_not_exists`
- [x] `Store::drop_dataset` → 删除后重新 `create_dataset` 成功
  - 测试: `t8_2_3_drop_deletes_dataset` + `t8_2_4_create_after_drop`

### Phase 7: FFI 接口 ✅ 已完成
- [x] 编译: `cargo build --release` → 生成动态库 (已验证)
- [x] C 程序链接测试: `include/timslite.h` 存在, 函数声明完整
- [x] FFI create/write/query/close/open 完整流程: 12个extern "C"函数已实现 (ffi.rs:88-365)
- [x] FFI 错误处理 (已存在/不存在/drop后重新创建): catch_unwind + err_buf 全覆盖
- [x] 边界测试 (nullptr参数检查): 所有FFI函数开头检查 null
- [x] panic 安全性测试: 所有FFI函数使用 ffi_catch_int!/ffi_catch_ptr! 宏包裹

### Phase 8: 集成测试 + 性能调优 ⚠️ 部分完成
- [x] 端到端集成测试: 13 integration tests + 81 unit tests (94 total)
- [ ] 性能基准测试 (benches/) — criterion 已配置, benches/ 目录已创建但无文件 (deferred: 需专项基准实现)
- [ ] 内存安全验证 — Windows未valgrind, 可后续Linux验证 (deferred: 需Linux环境)
- [x] 文档: README.md 已更新完整, 公共API有doc comments
- [x] `cargo clippy -- -D warnings` clean

### Phase 12: FFI + 集成测试 ✅ 已完成
- [x] `tmsl_dataset_create` 新增 2 个 u64 参数 (FFI) — 已实现 (ffi.rs:139)
- [x] `include/timslite.h` 更新函数声明 — 已更新
- [x] 单元测试: 所有12项已完成 (见 phase-12-lazy-allocation.md §11)
- [x] 集成测试: test_expansion_data_integrity, test_expansion_consecutive_open_write (dataset.rs 模块测试覆盖)
- [x] test_lazy_create_write_query_small_data — t12_1
- [x] test_lazy_write_until_max_then_new_segment — t12_2
- [x] test_open_legacy_full_allocated_dataset — t12_3
- [x] test_disk_space_efficiency — t12_4

### Phase 13: 查询迭代器 + HotBlockCache ✅ 已完成
- [x] `src/query/mod.rs` — 新增 query 模块 (iter.rs + hot_block.rs)
- [x] `QueryIterator<'a, 'b>` — 虚拟迭代器核心结构 (惰性遍历 index sources)
- [x] `HotBlockCache` — 读取循环级局部 Block 缓存 (无锁, 单 Iterator 实例, 移至 cache.rs)
- [x] `DataSet::query_iter()` — 新 API 返回虚拟迭代器
- [x] `DataSet::query()` — 向后兼容, 内部改为 `query_iter().collect_all()`
- [x] `DataSet::read_entry_at_index()` — 单条目读取, 供 FFI 惰性查询
- [x] `DataSet::query_index_entries()` — 预收集索引条目
- [x] `DataSegment::read_at_index_with_hot_cache()` — 增强读取, 支持 HotBlockCache
- [x] `DataSegmentSet::read_at_index_with_hot_cache()` — 委托调用
- [x] `IndexSegment::query_range_indices()` — 范围索引查询 (连续模式 O(1))
- [x] `FfiIterator` 重构 — 惰性读取, 不再预加载全量数据
- [x] `tmsl_dataset_query` 内部改为创建惰性迭代器
- [x] `DataSegmentSet::new` 修复: 创建 data/ 目录
- [x] `lib.rs` 导出 query 模块 (HotBlockCache, QueryIterator, QuerySource, SourceIndex)
- [x] 集成测试: t13_1_iterator_small_range, t13_3_query_backward_compat, t13_4_query_empty_range
- [x] 单元测试: HotBlockCache hit/miss/extract/clear (6 tests), QueryIterator empty/filler (2 tests)
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (101 tests: 91 unit + 16 integration)

### Phase 14: create_dataset Builder 优化 ✅ 已完成
- [x] `DataSetConfigBuilder::from_store(store_config)` — 预填 store 默认值
- [x] `Store::create_dataset_with_config(name, dataset_type, Option<DataSetConfigBuilder>)` — 新 API
  - `None` → 全部使用 store 默认值, `index_continuous` 默认 0
  - `Some(builder)` → 仅覆盖 builder 中显式设置的字段
- [x] `Store::create_dataset(...)` — 向后兼容, 内部委托给新方法
- [x] `DataSetConfigBuilder` 从 `pub(crate)` 提升为 `pub`, 带完整文档注释
- [x] `DataSetConfig` 从 `pub(crate)` 提升为 `pub` (接口可见性一致)
- [x] `lib.rs` 导出 `DataSetConfigBuilder`, `DataSetConfig`
- [x] 单元测试: `test_dataset_config_builder_from_store`, `test_dataset_config_builder_from_store_with_overrides`
- [x] 集成测试: t14_1 (None 默认值), t14_2 (builder 覆盖), t14_3 (旧 API 兼容)
- [x] FFI `tmsl_dataset_create` 保持不变 (C 不支持 builder 模式)
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (110 tests: 93 unit + 19 integration)

### Phase 15: Header State 分化 ✅ 已完成
- [x] `header.rs`: `FileMetadata` → `DataFileMetadata` (9 state, 116B) + `IndexFileMetadata` (1 state, 52B)
- [x] `DATA_HEADER_SIZE = 116`, `INDEX_HEADER_SIZE = 52` 替代 `HEADER_SIZE = 100`
- [x] `DataSegment`: 新增 `min_timestamp`/`max_timestamp`, 每次写入更新 + state 持久化
- [x] `IndexSegment`: state 仅写入 `wrote_position`, 删除冗余字段
- [x] `DataSegmentSet`: header 引用更新, closed segment meta 存储 min/max_timestamp 用于查询过滤
- [x] 所有源文件 `HEADER_SIZE` 替换为 `DATA_HEADER_SIZE`/`INDEX_HEADER_SIZE`
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` — 92 unit + 19 integration = 111 tests passing

### Phase 16: 数据保留 (Retention) ✅ 已完成
- [x] `meta.rs`: 新增 TLV `0x08 retention_ms` (u64 LE) + DataSetMeta.retention_ms 字段 + 序列化/反序列化
- [x] `config.rs`: StoreConfig 新增 `retention_check_hour` (u8, 0-23) + DataSetConfig 新增 `retention_ms`
  DataSetConfigBuilder 新增 `retention_ms()` 方法
- [x] `dataset.rs`: 新增 `retention_ms` 字段 + create/open 读写 +
  `query_iter()` 自动钳制 start_ts 到有效期 + `reclaim_expired_segments()` 方法
- [x] `segment/mod.rs`: DataSegmentSet 新增 `reclaim_expired_segments(threshold)`, 基于 closed_segments[].max_timestamp 判断 + 删除文件
- [x] `index/segment.rs`: 新增自由函数 `last_entry_timestamp(path, max_file_size)`, read-only mmap + 立即释放
- [x] `index/mod.rs`: TimeIndex 新增 `reclaim_expired_segments(threshold, max_file_size)`, 调用 last_entry_timestamp 判断
- [x] `bg/mod.rs`: BackgroundTasks::start 新增 `retention_check_hour` 参数 + next_retention 计算 + 回收任务执行逻辑
- [x] `store.rs`: 传递 retention_check_hour 到 BackgroundTasks + retention_ms 到 DataSet
- [x] `ffi.rs` + `timslite.h`: tmsl_dataset_create 新增 `retention_ms` 参数
- [x] 集成测试: 9 个新增 retentions 单元测试 + 19 个集成测试 (已适配新增参数) 全部通过
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (101 unit + 19 integration = 120 tests)

### Phase 17: 纠正写入 (Correction Write) ✅ 已完成
- [x] `segment/data.rs`: 新增 `DataSegment::overwrite_in_last_block(block_rel_offset, in_block_offset, new_data)` — 在段最后一个未压缩 block 的最末 record 原地覆盖, 支持 resize; 更新 5 个字段 (block payload_size/uncompressed_size + 段 pending_wrote_position/total_uncompressed_size/wrote_position); 拒绝 COMPRESSED block 和非末位 record
- [x] `segment/mod.rs`: 新增 `DataSegmentSet::overwrite_in_last_block(block_offset, in_block_offset, timestamp, new_data)` — 路由到最新数据段并委托覆盖
- [x] `index/mod.rs`: 新增 `TimeIndex::find_entry(timestamp) -> Option<IndexEntry>` — 在 in_memory_buffer + open segments + closed segments 中查找条目
- [x] `dataset.rs`: `write()` 新增纠正写入分支: `timestamp == latest_written_timestamp && latest > 0` → 通过 `time_index.find_entry()` 获取 `(block_offset, in_block_offset)` → `segments.overwrite_in_last_block()` 原地修改 (支持变 size, 索引不变)
- [x] `dataset.rs`: 非连续模式: 将 `timestamp <= latest` 改为 `timestamp < latest` (out-of-order), `==` 走纠正; 连续模式: 删除原 "duplicate timestamp" 错误路径 (纠正写入已在 mode 分支前统一处理)
- [x] 新增单元测试 (7 项): correction_write_continuous_mode, correction_write_non_continuous_mode, correction_write_resize_larger, correction_write_resize_smaller, correction_write_multiple_times, correction_write_then_new_write, correction_write_reopen_persistence
- [x] `tests/integration_test.rs`: 新增 2 个集成测试: t17_1_correction_write_same_size, t17_2_correction_write_resize_reopen
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (107 unit + 21 integration = 128 tests)

### Phase 18: 乱序写入与删除 (Out-of-Order Write & Delete) ✅ 已完成
- [x] `header.rs`: Data Segment State 第 9 个字段 `reserved` → `invalid_record_count` (常量 `DS_INVALID_RECORD_COUNT`、结构体字段、默认值、`write_to`、`read_from` — 6 处)
- [x] `segment/data.rs`: `DataSegment` 新增 `invalid_record_count: u64` 字段 + `increment_invalid_record_count()` 方法 + mmap 持久化 (offset 108..116); `create`/`open`/`ensure_open` 正确初始化和读取该字段
- [x] `segment/mod.rs`: 新增 `DataSegmentSet::increment_invalid_record_count(absolute_offset)` — 路由到 open segment 或 lazy_open closed segment 后递增并 idle_close 回写
- [x] `index/mod.rs`: 新增 `TimeIndex::update_entry(timestamp, new_block_offset, new_in_block_offset) -> Result<IndexEntry>` + `TimeIndex::find_and_delete_entry(timestamp) -> Result<IndexEntry>` — 三级搜索 + 原地覆盖; 新增 sentinel 常量导入 (`BLOCK_OFFSET_FILLER`, `IN_BLOCK_OFFSET_FILLER`)
- [x] `dataset.rs`: 新增 `out_of_order_write(timestamp, data)` 方法 (append + update_entry + 条件递增 invalid_record_count); 重写 `write()` dispatch (correction → out-of-order → normal, 两种索引模式统一); 新增 `delete(timestamp)` 方法; 移除已废弃的 `replace_filler_with_real`
- [x] `dataset.rs`: 更新测试 — 重命名 `test_noncontinuous_mode_out_of_order_rejected` → `test_noncontinuous_mode_out_of_order_rejected_when_no_entry` (适配新错误消息) + `test_noncontinuous_mode_out_of_order_succeeds_with_existing_entry` (新增) + `test_out_of_order_write_overwrites_real_entry` (替代旧 rejected 测试) + `test_out_of_order_increments_invalid_record_count` + 7 个 delete 单元测试 (existing / filler / nonexistent / idempotent / count / rewrite / reopen)
- [x] `ffi.rs` + `timslite.h`: 新增 `tmsl_dataset_delete(dataset, timestamp, err_buf, err_buf_len) -> c_int` 函数 (extern "C") + C 声明 (doxygen 注释)
- [x] `tests/integration_test.rs`: 新增 4 个集成测试: t18_1_out_of_order_write (非连续), t18_1b_out_of_order_write_continuous (连续), t18_2_delete_lifecycle (lifecycle + reopen), t18_3_mixed_operations (correction + delete + OOO 组合)
- [x] Design docs 同步更新 (Phase 18 启动前已完成): data-model.md / data-segment.md / dataset-operations.md (§9.1 重写 + §9.3) / index-continuous.md (§23.2 + §23.2.1 + §23.4) / store-and-ffi.md / design.md / README.md
- [x] Phase 18 详细计划文档: docs/plan/phase-18-out-of-order-write-and-delete.md (设计 + 实现细节 + 测试计划 + 验收标准)
- [x] `cargo clippy --tests` clean (零 warnings)
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` — lib 116 passed, integration 25 passed, total 141

### Phase 20: 最新时间戳读取 (Latest Timestamp Read) ✅ 已完成
- [x] `dataset.rs`: 新增 `DataSet::latest_written_timestamp(&self) -> i64` getter — 返回内存中维护的最新写入时间戳 (0 = 空)
- [x] `dataset.rs`: 修改 `DataSet::read()` — `timestamp == -1` 时解析为 `latest_written_timestamp`, 空数据集直接返回 `None`
- [x] `ffi.rs`: 新增 `tmsl_dataset_latest_timestamp(dataset, out_ts, err_buf, err_buf_len) -> c_int` FFI 函数
- [x] `ffi.rs`: 修复 `tmsl_dataset_read` 中 `out_ts` 写入 (原为硬编码输入值, 改为写入实际返回的时间戳, 兼容 -1 快捷路径)
- [x] `include/timslite.h`: 新增 `tmsl_dataset_latest_timestamp` 声明; 更新 `tmsl_dataset_read` 注释 (timestamp=-1 快捷路径 + out_ts 语义说明)
- [x] `wrapper/python/src/dataset.rs`: 新增 `latest_timestamp` 只读属性 + 更新 `read()` docstring (timestamp=-1)
- [x] 单元测试 (6 项): test_latest_written_timestamp_after_writes, test_latest_written_timestamp_after_reopen, test_read_minus_one_empty_dataset, test_read_minus_one_returns_latest, test_read_minus_one_after_delete_latest, test_read_minus_one_after_reopen
- [x] 设计文档更新: dataset-operations.md (signature block + §10.3 流程图重写 + §10.4 新增说明) + store-and-ffi.md (FFI 函数列表)
- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (130 unit + 25 integration = 155 tests)

### Phase 19: 单时间戳读取 (Single Timestamp Read) ✅ 已完成
- [x] `dataset.rs`: 新增 `DataSet::read(timestamp, cache) -> Result<Option<(i64, Vec<u8>)>>` — 通过 `time_index.find_entry()` 三级搜索 + filler 过滤 + `segments.read_at_index()` 读取数据
- [x] `ffi.rs`: 新增 `tmsl_dataset_read(dataset, timestamp, out_ts, out_data, out_data_len, err_buf, err_buf_len) -> c_int` — 返回码 0=成功, 1=未找到, -1=错误; `out_data` 由 `libc::malloc` 分配, 复用 `tmsl_iter_free_data` 释放路径
- [x] `include/timslite.h`: 新增 `tmsl_dataset_read` C 函数声明 + doxygen 注释
- [x] 单元测试 (5 项): test_read_found, test_read_not_found, test_read_deleted_returns_none, test_read_continuous_filler_returns_none, test_read_after_reopen
- [x] 设计文档更新: dataset-operations.md (§10.3) + store-and-ffi.md (FFI 函数列表 + 内存所有权说明)
- [x] `cargo clippy --tests -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (124 unit + 25 integration = 149 tests)

### Phase 21: 后台任务手动执行 (Manual Background Execution) ⏳ 待实现

> 目标: 让调用方能够选择性禁用内置后台线程, 并通过主动 API 驱动后台任务 (flush / idle-close / cache-eviction / retention-reclaim);同时保证即使启用了后台线程, 外部也可安全调用相同 API。

**设计文档**:
- [x] 设计文档更新: `docs/design/background-and-cache.md` 新增 §17.9 (统一执行器 + state Mutex) / §17.10 (外部手动执行 API + TickResult) / §17.11 (并发安全矩阵 + 锁顺序)
- [x] 设计文档更新: `docs/design/store-and-ffi.md` 新增 §11.4 (StoreConfig `enable_background_thread`) / §11.5 (Store API `tick_background_tasks` / `next_background_delay`) + FFI 函数 `tmsl_store_tick_background_tasks` / `tmsl_store_next_background_delay` 声明
- [x] 设计文档更新: `design.md` 后台任务条目描述更新

**实现**:
- [x] `config.rs`: `StoreConfig` 新增 `enable_background_thread: bool` 字段 (默认 `true`) + `StoreConfigBuilder::enable_background_thread()` + 单元测试
- [x] `bg/mod.rs`: 抽取调度状态到 `ExecutorState { last_flush, last_idle_check, last_cache_eviction, next_retention }`, 放入 `Arc<Mutex<ExecutorState>>`; `BackgroundTasks::start` 改为持有共享 `state` + 线程 loop 与 `tick` 使用相同执行路径
- [x] `bg/mod.rs`: 实现 `BackgroundTasks::tick() -> TickResult` (单次到期检查 + 执行, 返回 `executed_tasks` + `next_delay`)
- [x] `bg/mod.rs`: 实现 `BackgroundTasks::next_delay() -> Duration` (读快照快速计算, 不执行任务)
- [x] `bg/mod.rs`: `BackgroundTasks::new()` 支持无线程模式 — 不 spawn 线程, 仅初始化 `state`
- [x] `bg/mod.rs`: 新增 `TickResult { executed_tasks: usize, next_delay: Duration }` 返回类型, `pub` 并导出
- [x] `store.rs`: `Store::open` 按 `config.enable_background_thread` 启用/禁用线程
- [x] `store.rs`: 新增 `Store::tick_background_tasks() -> Result<TickResult>` 委托到 `BackgroundTasks::tick`
- [x] `store.rs`: 新增 `Store::next_background_delay() -> Result<Duration>` 委托到 `BackgroundTasks::next_delay`
- [x] `lib.rs`: 导出 `TickResult` (pub use)
- [x] `ffi.rs`: 新增 `tmsl_store_tick_background_tasks(store, out_executed, out_next_delay_ms, err_buf, err_buf_len) -> c_int`
- [x] `ffi.rs`: 新增 `tmsl_store_next_background_delay(store, out_next_delay_ms, err_buf, err_buf_len) -> c_int`
- [x] `include/timslite.h`: 新增两个 FFI 函数声明 + doxygen 注释

**测试**:
- [x] 单元测试: `test_tick_bg_disabled_mode` (enable_background_thread=false, 手动 tick 触发 flush)
- [x] 单元测试: `test_tick_bg_returns_next_delay` (执行后 next_delay 与 flush_interval 大致一致)
- [x] 单元测试: `test_tick_bg_respects_interval` (短时间内连续 tick 不重复执行 flush)
- [x] 单元测试: `test_tick_bg_all_four_tasks_due` (构造全到期场景, executed_tasks == 4)
- [x] 单元测试: `test_next_delay_no_side_effects` (调用 next_delay 后不改变执行状态)
- [x] 单元测试: `test_thread_enabled_external_tick_safe` (启用线程的同时外部调用 tick, 无 panic 无重复执行)
- [x] 单元测试: `test_concurrent_external_ticks_serialized` (多线程同时 tick, 仅一个真正执行)
- [x] 单元测试: `test_next_delay_during_tick` (tick 进行中 next_delay 可能等待, 但最终返回值正确)
- [x] 单元测试: `test_enable_background_thread_default_true` (默认构造 = true, 兼容性)
- [x] 单元测试: `test_thread_disabled_close_safe` (close 在未启用线程时正常结束)
- [x] 集成测试: `t21_1_manual_bg_lifecycle` (open with disabled thread → write → tick → verify flush via reopen)
- [x] 集成测试: `t21_2_manual_bg_next_delay_consistency` (验证 next_delay 与 flush_interval 一致)
- [x] 集成测试: `t21_3_manual_bg_concurrent_with_thread` (启用线程 + 外部 tick 并发, 无数据损坏)

**验收**:
- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (131 unit + 25 integration = 156 tests)

### Phase 22: Manual Background Execution Python Wrapper ✅ 已完成

> 目标: 为 Phase 21 的新 API 提供 Python FFI 绑定。

**实现**:
- [x] `wrapper/python/src/config.rs`: `PyStoreConfig::new()` 新增 `retention_check_hour` / `enable_background_thread` 参数 + getter
- [x] `wrapper/python/src/store.rs`: `PyStore::tick_background_tasks()` → 返回 `(executed: int, next_delay_ms: int)` 元组
- [x] `wrapper/python/src/store.rs`: `PyStore::next_background_delay()` → 返回 `int` (毫秒)
- [x] 支持通过 `StoreConfig(enable_background_thread=False, ...)` 配置构造

**测试**:
- [x] `tests/test_store_manual_bg.py`: 验证 enable=False + tick 触发 flush + next_delay 返回
- [x] `tests/test_store_manual_bg.py`: 验证 tick 返回值结构正确

**文档**:
- [x] `wrapper/python/README.md`: 更新使用示例, 演示手动后台模式

**验收**:
- [x] `cargo clippy --lib -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo build --lib` 编译通过

## 文档结构

详细计划内容已拆分到 `docs/plan/` 目录, 每个 Phase 独立文档:

```
plan.md                              ← 本文件: 状态总览 + 待完成清单
docs/plan/
├── overview.md                      ← 总体里程碑 + 依赖图 + 风险表 + 开发规范
├── phase-01-skeleton.md             ← Phase 1: 项目骨架
├── phase-02-header-block.md         ← Phase 2: 文件头 + Block
├── phase-03-datasegment.md          ← Phase 3: DataSegment
├── phase-04-time-index.md           ← Phase 4: 时间索引
├── phase-05-dataset.md              ← Phase 5: DataSet
├── phase-06-store-bg.md             ← Phase 6: Store + 后台任务
├── phase-07-ffi.md                  ← Phase 7: FFI 接口
├── phase-08-tests-perf.md           ← Phase 8: 测试 + 性能
├── phase-09-blockcache.md           ← Phase 9: 读缓存池
├── phase-10-continuous-storage.md   ← Phase 10: 连续存储
├── phase-11-o1-optimization.md      ← Phase 11: O(1) 查询优化
├── phase-12-lazy-allocation.md      ← Phase 12: 懒分配 + 扩容
├── phase-13-query-iterator.md       ← Phase 13: 查询迭代器 + HotBlockCache
├── phase-14-dataset-config-builder.md ← Phase 14: Builder 优化
├── phase-15-header-state-split.md   ← Phase 15: Header State 分化
├── phase-16-data-retention.md       ← Phase 16: 数据保留 (Retention)
├── phase-17-correction-write.md     ← Phase 17: 纠正写入 (Correction Write)
├── phase-17-correction-write.md   ← Phase 17: 纠正写入 (Correction Write)
├── phase-18-out-of-order-write-and-delete.md ← Phase 18: 乱序写入与删除
└── phase-21-manual-bg-execution.md ← Phase 21: 后台任务手动执行
```

**概览文档** ([docs/plan/overview.md](docs/plan/overview.md)) 包含:
- 总体里程碑列表
- 目录结构变更 (旧 → 新)
- Phase 依赖关系图
- 风险与应对表
- 开发规范

---

**维护指南**:
- 完成验收标准后, 将 `[ ]` 改为 `[x]`
- Phase 全部完成时, 更新上方状态表中对应行的状态
- 新增任务时, 在对应 Phase 的 "待完成事项" 中添加
- 更新 `docs/plan/phase-XX-*.md` 中的验收标准 checkbox