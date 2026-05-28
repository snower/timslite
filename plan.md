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
- [ ] 性能基准测试 (benches/) — criterion 已配置, benches/ 目录已创建但无文件
- [ ] 内存安全验证 — Windows未valgrind, 可后续Linux验证
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
└── phase-16-data-retention.md       ← Phase 16: 数据保留 (Retention) (待实现)
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