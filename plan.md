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
| 19 | 单时间戳读取 (Single Timestamp Read) | ✅ 完成 | [phase-19-single-timestamp-read.md](docs/plan/phase-19-single-timestamp-read.md) |
| 20 | 最新时间戳读取 (Latest Timestamp Read) | ✅ 完成 | [phase-20-latest-timestamp-read.md](docs/plan/phase-20-latest-timestamp-read.md) |
| 21 | 后台任务手动执行 (Manual Background Execution) | ✅ 完成 | [phase-21-manual-bg-execution.md](docs/plan/phase-21-manual-bg-execution.md) |
| 22 | Manual Background Python Wrapper | ✅ 完成 | [phase-22-manual-bg-python-wrapper.md](docs/plan/phase-22-manual-bg-python-wrapper.md) |
| 23 | Record 长度编码升级为 u32 | ✅ 完成 | [phase-23-record-length-u32.md](docs/plan/phase-23-record-length-u32.md) |
| 24 | 连续索引稀疏 filler 分段 | ✅ 完成 | [phase-24-sparse-continuous-index.md](docs/plan/phase-24-sparse-continuous-index.md) |
| 25 | Header 可变长度 (P0-3 修复) | ✅ 完成 | [phase-25-header-variable-length.md](docs/plan/phase-25-header-variable-length.md) |
| 26 | GitHub Actions CI/CD | ✅ 完成 | [phase-26-github-actions-ci.md](docs/plan/phase-26-github-actions-ci.md) |
| 27 | Queue 模块 (DatasetQueue + Consumer) | ✅ 完成 (含完整测试) | [phase-27-queue-module.md](docs/plan/phase-27-queue-module.md) |
| 28 | Journal 变更日志 (`.journal/logs`) | ✅ 完成 | [phase-28-journal.md](docs/plan/phase-28-journal.md) |
| 29 | Dataset Append API + Journal `0x13` | ✅ 完成 | [phase-29-dataset-append.md](docs/plan/phase-29-dataset-append.md) |
| 30 | Dataset 读操作优化 | ⏳ 待实现 | [phase-30-dataset-read-operations.md](docs/plan/phase-30-dataset-read-operations.md) |
| PY | Python Package (PyO3) | ✅ 完成 | [wrapper/python/plan.md](wrapper/python/plan.md) |

---

## 待完成事项

### Phase 7: FFI 接口
- [ ] C 链接测试 — 独立 C 程序链接 `libtimslite` 并调用 FFI 完整流程验证

### Phase 8: 集成测试 + 性能调优
- [ ] 性能基准测试 (`benches/`) — criterion 已配置, 目录已创建但无文件
- [ ] 内存安全验证 — Windows 未 valgrind, 需 Linux/Valgrind 环境

### Phase 28: Journal 变更日志
- [x] `src/journal/mod.rs` — JournalManager + record encoder/decoder
- [x] `StoreConfig.enable_journal` — Rust/FFI/header/wrapper 配置同步
- [x] `.journal/logs` — 默认启用的内置只读 journal dataset
- [x] 操作 hook — create/drop/write/delete 成功后追加 `0x01/0x02/0x11/0x12`
- [x] 查询与实时消费 — read/query/query_iter/latest/open_queue + queue poll/ack
- [x] 验证 — journal/queue/ffi 集成测试、fmt、clippy、全量 cargo test

### Phase 29: Dataset Append API + Journal `0x13`
- [x] 设计文档 — append 行为、4MiB 上限、70% 迁移阈值、journal `0x13`
- [x] 测试 — append 行为矩阵、迁移、错误路径、journal 编解码与 Store/FFI hook
- [x] 实现 — DataSegment tail append、DataSet append、Store/FFI API、journal `0x13`
- [x] 验证 — `cargo test -- --test-threads=1`, `cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings`

### Phase 30: Dataset 读操作优化
- [ ] 设计文档 — read_exist/query_exist/read_length/query_length/query_length_iter 接口规范
- [ ] DataSegmentSet::read_record_data_len() — 仅读取 record header 获取 data_len
- [ ] DataSet::read_exist() — 单时间戳索引存在检查
- [ ] DataSet::query_exist() — 范围索引存在性检查，返回位图
- [ ] DataSet::read_length() — 单时间戳数据长度读取
- [ ] DataSet::query_length() — 范围查询数据长度列表
- [ ] QueryLengthIterator + query_length_iter() — 惰性数据长度迭代器
- [ ] FFI 接口 — tmsl_dataset_read_exist/query_exist/read_length/query_length/query_length_iter
- [ ] Store 门面 API — dataset_read_exist/query_exist/read_length/query_length/query_length_iter
- [ ] C 头文件 — include/timslite.h 新增函数声明
- [ ] Python Wrapper — DataSet 类新增方法
- [ ] 集成测试 — 完整测试矩阵覆盖
- [ ] 验证 — `cargo test -- --test-threads=1`, `cargo fmt -- --check`, `cargo clippy -- -D warnings`

---

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
├── phase-17-correction-write.md     ← Phase 17: 纠正写入
├── phase-18-out-of-order-write-and-delete.md ← Phase 18: 乱序写入与删除
├── phase-19-single-timestamp-read.md ← Phase 19: 单时间戳读取
├── phase-20-latest-timestamp-read.md ← Phase 20: 最新时间戳读取
├── phase-21-manual-bg-execution.md  ← Phase 21: 后台任务手动执行
├── phase-22-manual-bg-python-wrapper.md ← Phase 22: Manual BG Python Wrapper
├── phase-23-record-length-u32.md    ← Phase 23: Record 长度编码升级为 u32
├── phase-24-sparse-continuous-index.md ← Phase 24: 连续索引稀疏 filler 分段
├── phase-25-header-variable-length.md ← Phase 25: Header 可变长度 (P0-3)
├── phase-26-github-actions-ci.md    ← Phase 26: GitHub Actions CI/CD
├── phase-27-queue-module.md         ← Phase 27: Queue 模块 (DatasetQueue + Consumer)
├── phase-28-journal.md              ← Phase 28: Journal 变更日志 (.journal/logs)
├── phase-29-dataset-append.md       ← Phase 29: Dataset Append API + Journal 0x13
└── phase-30-dataset-read-operations.md ← Phase 30: Dataset 读操作优化
```

**概览文档** ([docs/plan/overview.md](docs/plan/overview.md)) 包含:
- 总体里程碑列表
- 目录结构变更 (旧 → 新)
- Phase 依赖关系图
- 风险与应对表
- 开发规范

---

**维护指南**:
- 新增 Phase 时, 同步更新上方状态表和下方目录树
- Phase 全部完成后, 将实现细节写入 `docs/plan/phase-XX-*.md`
- 将 `⚠️` 状态项的待完成勾选后更新为 `✅ 完成`
