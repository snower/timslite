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
| 5 | DataSegmentSet + DataSet | ✅ 核心完成, 部分待完成 | [phase-05-dataset.md](docs/plan/phase-05-dataset.md) |
| 6 | Store 门面 + 后台任务 | ✅ 核心完成, 部分待完成 | [phase-06-store-bg.md](docs/plan/phase-06-store-bg.md) |
| 7 | FFI 接口 | ☐ 部分待完成 | [phase-07-ffi.md](docs/plan/phase-07-ffi.md) |
| 8 | 集成测试 + 性能调优 | ☐ 部分待完成 | [phase-08-tests-perf.md](docs/plan/phase-08-tests-perf.md) |
| 9 | 读缓存池 (BlockCache) | ✅ 完成 | [phase-09-blockcache.md](docs/plan/phase-09-blockcache.md) |
| 10 | 索引连续存储 | ✅ 完成 | [phase-10-continuous-storage.md](docs/plan/phase-10-continuous-storage.md) |
| 11 | 连续模式 O(1) 查询优化 | ✅ 完成 | [phase-11-o1-optimization.md](docs/plan/phase-11-o1-optimization.md) |
| 12 | 分段懒分配 + 倍率扩容 | ✅ 核心完成, 部分待完成 | [phase-12-lazy-allocation.md](docs/plan/phase-12-lazy-allocation.md) |

## 待完成事项

### Phase 5: DataSet 生命周期
- [ ] `DataSet::open` 对不存在数据集 → 返回 `NotFound` 错误
- [ ] `DataSet::open` 后写入 → close → reopen → 验证所有数据可读
- [ ] 时间范围查询 (部分数据) → 验证数量和顺序
- [ ] `DataSet::drop_dataset` 删除后目录不可访问

### Phase 6: Store 门面
- [ ] `Store::create_dataset` → 创建成功, 再次调用 → `AlreadyExists`
- [ ] `Store::open_dataset` → 打开成功, 不存在 → `NotFound`
- [ ] `Store::drop_dataset` → 删除后重新 `create_dataset` 成功

### Phase 7: FFI 接口 (全部待完成)
- [ ] 编译: `cargo build --release` → 生成动态库
- [ ] C 程序链接测试
- [ ] FFI create/write/query/close/open 完整流程
- [ ] FFI 错误处理 (已存在/不存在/drop 后重新创建)
- [ ] 边界测试 (空 data_dir, 长 name, nullptr)
- [ ] panic 安全性测试

### Phase 8: 集成测试 + 性能调优 (全部待完成)
- [ ] 端到端集成测试
- [ ] 性能基准测试 (benches/)
- [ ] 内存安全验证
- [ ] 文档 (README, doc comments)

### Phase 12: FFI + 集成测试
- [ ] `tmsl_dataset_create` 新增 2 个 u64 参数 (FFI)
- [ ] `include/timslite.h` 更新函数声明
- [ ] test_lazy_create_write_query_small_data
- [ ] test_lazy_write_until_max_then_new_segment
- [ ] test_open_legacy_full_allocated_dataset
- [ ] test_disk_space_efficiency

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
└── phase-12-lazy-allocation.md      ← Phase 12: 懒分配 + 扩容
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