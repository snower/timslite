# timslite 测试深度审查报告

> 基于 design.md 和 docs/design/ 详细设计文档  
> 审查范围: 单元测试 + 集成测试  
> 审查日期: 2026-06-11

---

## 一、测试覆盖总览

### 1.1 集成测试 (tests/ 目录)

| 文件 | 测试数 | 覆盖范围 |
|------|--------|----------|
| `queue_test.rs` | 10 | push/poll/ack、多消费组、多线程、持久化、filler gap |
| `journal_test.rs` | 22 | journal 创建/禁用、记录类型 (0x01/0x02/0x11/0x12)、queue 消费、边界条件 |
| `background_test.rs` | 6 | 手动 tick 生命周期、延迟一致性、并发、flush/idle/retention/cache |
| `crash_recovery_test.rs` | 6 | pending block 恢复、多段恢复、index 恢复、多 block 恢复 |
| `read_operations.rs` | 14 | read_exist/query_exist/read_length/query_length/query_length_iter |
| `dataset_basic_test.rs` | 3 | 基本生命周期、多数据集隔离、持久化 reopen |
| `dataset_lifecycle_test.rs` | 6 | create 重复/open 不存在/drop 删除/close-after-drop/reopen |
| `correction_write_test.rs` | 2 | 同大小纠正写、变长纠正写 + reopen |
| `out_of_order_delete_test.rs` | 5 | 乱序写 (稀疏+连续)、删除生命周期、delete+write 混合、大型乱序 |
| `query_test.rs` | 4 | 迭代器小范围、向后兼容、空范围、迭代器大范围 |
| `lazy_allocation_test.rs` | 4 | 小数据写入、写满扩容、多 segment 创建、索引扩容 |
| `config_test.rs` | 4 | 默认配置、builder 覆盖、向后兼容、retention 配置 |
| `store_listing_test.rs` | 6 | get_dataset_names、get_dataset_types、去重 |
| `dataset_inspect_test.rs` | 4 | inspect 基本字段、info 字段、写入后状态、只读 |

**集成测试总计: 100 个**

### 1.2 单元测试 (src/ 目录)

| 文件 | 测试数 | 覆盖范围 |
|------|--------|----------|
| `dataset.rs` | 31 | write/query/read、retention、correction、delete、append、out-of-order |
| `bg/mod.rs` | 15 | flush/idle/retention/cache、tick 并发、flush 队列 |
| `segment/data.rs` | 13 | create/open、write/read、block overflow、single record、pending 恢复 |
| `segment/mod.rs` | 14 | lazy_open、closed 段查询、段生命周期 |
| `index/segment.rs` | 13 | 基本读写、filler、连续模式、base timestamp |
| `queue/mod.rs` | 33 | push/poll/ack、多消费组、多 consumer、等待/通知、close、drop |
| `journal/mod.rs` | 21 | 记录编码/解码、Store/DataSet hook、queue open/poll |
| `store.rs` | 17 | create/open/drop dataset、config、listing |
| `cache.rs` | 5 | insert/get/eviction、LRU、空闲回收 |
| `config.rs` | 7 | builder、retention、compression |
| `query/mod.rs` | 3 | 迭代器基本功能 |
| `meta.rs` | 8 | TLV 编码/解码、边界条件 |
| `compress.rs` | 5 | deflate/inflate、级别、大数据 |
| `header.rs` | 3 | 可变长度 header 读写 |
| `block.rs` | 3 | BlockHeader 序列化 |
| `util.rs` | 4 | 路径校验、endian |
| `error.rs` | 4 | 错误类型、Display |

**单元测试总计: 203 个**

**测试总计: 303 个**

---

## 二、缺陷与逻辑错误

### 2.1 测试隔离问题 (严重)

**问题**: 所有集成测试共享 `temp_dir()` 函数，使用 `std::time::SystemTime::now().as_nanos()` 作为目录名。在高并发或快速连续运行时，可能产生目录名冲突。

**影响**: 测试可能相互干扰，导致随机失败。

**建议**: 使用 `tempfile::tempdir()` 或 UUID 生成唯一目录名。

### 2.2 Queue 测试: `t27_1_4_poll_skips_continuous_filler_gap` 逻辑问题

**问题**: 该测试写入 ts=10 和 ts=20，然后 poll 两次。但测试没有验证 filler gap 是否被正确跳过——它只是验证了正常顺序消费。

**设计文档描述**: `poll` 应跳过 filler/gap 记录，`processed_ts` 应推进到下一个真实记录。

**实际测试**: 没有创建 filler/gap 场景，只是正常顺序写入和消费。

**建议**: 应该先删除某个 timestamp (创建 filler)，然后验证 poll 跳过它。

### 2.3 Crash Recovery 测试: 缺少 index segment 恢复验证

**问题**: `crash_recovery_test.rs` 中的测试只验证了 data 恢复，没有验证 index segment 在 crash 后的完整性。

**设计文档**: `memory-and-concurrency.md` 描述了 crash 后 index 的恢复流程。

**建议**: 添加测试验证 crash 后 index segment 的 base timestamp、entry 数量、查询正确性。

### 2.4 Background 测试: `t21_3_manual_bg_concurrent_with_thread` 竞态条件

**问题**: 该测试启动后台线程后立即 `std::thread::sleep(500ms)`，然后验证数据。但 sleep 时间可能不足以让后台线程完成 flush。

**影响**: 在慢速系统上可能随机失败。

**建议**: 使用 condvar 或 channel 等待后台任务完成，而非固定 sleep。

### 2.5 Correction Write 测试: 缺少压缩 block 场景

**问题**: `correction_write_test.rs` 只测试了 pending raw block 的纠正写，没有测试 sealed+compressed block 的 fallback 路径。

**设计文档**: `compression.md` 描述了 "correction 写入不得原地修改 sealed+compressed block, 只能回退为乱序追加并更新索引"。

**建议**: 添加测试验证对 sealed+compressed block 的 correction write 走 fallback 路径。

---

## 三、不合理或不完整测试

### 3.1 Queue 测试: 缺少边界条件测试

**缺失场景**:

| 场景 | 描述 | 严重程度 |
|------|------|----------|
| push 到已关闭的 queue | 应返回错误 | 高 |
| poll 已关闭的 consumer | 应返回错误或 None | 高 |
| ack 已关闭的 consumer | 应返回错误 | 高 |
| drop consumer 两次 | 第二次应返回错误 | 中 |
| poll 超时精度 | 验证超时时间误差在合理范围内 | 中 |
| push 空数据 | 应成功或返回特定错误 | 中 |
| consumer group 名称边界 | 长名称、特殊字符、空名称 | 低 |

### 3.2 Journal 测试: 缺少 0x13 append 记录测试

**问题**: `journal_test.rs` 覆盖了 0x01 (create)、0x02 (drop)、0x11 (write)、0x12 (delete)，但没有 0x13 (append) 的测试。

**设计文档**: `journal.md` 明确列出了 0x13 作为 append 操作的日志类型。

**建议**: 添加测试验证 `DataSet::append()` 写入正确的 journal 记录。

### 3.3 Background 测试: 缺少 retention reclaim 详细验证

**缺失场景**:

| 场景 | 描述 |
|------|------|
| retention_window=0 | 验证不限制时 reclaim 不执行 |
| retention 边界时间 | 验证恰好在窗口边界的数据是否被正确回收 |
| reclaim 后查询 | 验证过期数据查询返回 None |
| reclaim 后写入 | 验证过期 timestamp 不允许写入 |
| reclaim 与 cache invalidation | 验证 reclaim 清除相关缓存 |

### 3.4 Read Operations 测试: 缺少 deleted/filler 记录测试

**问题**: `read_operations.rs` 测试了 read_exist、query_exist、read_length、query_length，但没有测试 deleted (filler) 记录的行为。

**设计文档**: `dataset-read-operations.md` 描述了 "跳过 filler 和过期记录"。

**建议**: 添加测试验证:
- `read_exist(deleted_ts)` 返回 false
- `read_length(deleted_ts)` 返回 None
- `query_exist` 对 deleted timestamp 返回 false
- `query_length` 跳过 deleted 记录

### 3.5 Lazy Allocation 测试: 缺少 index segment 扩容测试

**问题**: `lazy_allocation_test.rs` 测试了 data segment 扩容，但没有测试 index segment 扩容。

**设计文档**: `lazy-allocation.md` 描述了 index segment 也需要懒分配和扩容。

**建议**: 添加测试验证 index segment 从 initial 扩容到 max 的过程。

### 3.6 Config 测试: 缺少边界值测试

**缺失场景**:

| 参数 | 缺失测试 |
|------|----------|
| `data_segment_size` | 最小值、最大值、0 |
| `index_segment_size` | 最小值、最大值、0 |
| `compress_level` | 0、10、超出范围 |
| `retention_window` | 负值、极大值 |
| `flush_interval` | 0、极大值 |
| `idle_timeout` | 0、极大值 |

---

## 四、测试缺失分析

### 4.1 压缩相关测试严重不足

**设计文档**: `compression.md` 描述了详细的压缩状态机和 invariant。

**现有测试**: 仅 `compress.rs` 有 5 个单元测试，验证基本 deflate/inflate。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| pending overflow seal | 写入导致 pending block 溢出时的压缩流程 | P0 |
| single-record block | 超大 record 创建 exclusive block | P0 |
| SEALED+COMPRESSED flags | 验证 flags 组合的正确性 | P0 |
| 压缩后更大的情况 | deflate 后 payload 变大时的处理 | P1 |
| compress_level 效果 | 不同压缩级别的压缩率差异 | P2 |

### 4.2 Cache 测试严重不足

**设计文档**: `background-and-cache.md` 描述了 BlockCache LRU + 空闲回收。

**现有测试**: 仅 `cache.rs` 有 5 个单元测试。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| cache 与 correction write | 纠正写后缓存失效 | P0 |
| cache 与 delete | 删除后缓存失效 | P0 |
| cache 与 retention reclaim | 过期回收后缓存失效 | P0 |
| cache 与 out-of-order write | 乱序写后缓存失效 | P0 |
| cache LRU 淘汰 | 缓存满时的淘汰行为 | P1 |
| cache 空闲回收 | 30 分钟未访问的回收 | P1 |
| cache 并发访问 | 多线程同时读写缓存 | P1 |

### 4.3 Append 测试不足

**设计文档**: `dataset-operations.md` 描述了 append 的详细语义。

**现有测试**: `dataset.rs` 有少量 append 测试。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| append 到未压缩 tail record | 原地追加 | P0 |
| append 迁移阈值 | 超过 70% 阈值时迁移到 single-record block | P0 |
| append 到 sealed block | 应返回错误或创建新 record | P0 |
| append 空数据 | 应为 no-op | P1 |
| append timestamp 顺序 | `timestamp < latest_written_timestamp` 应返回错误 | P0 |
| append 与 queue 通知 | 创建新 timestamp 时通知 queue | P1 |

### 4.4 Iterator 测试不足

**设计文档**: `query-iterator.md` 描述了 Virtual Iterator 和 HotBlockCache。

**现有测试**: `query_test.rs` 有 4 个测试，`query/mod.rs` 有 3 个单元测试。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| iterator 跨 segment | 查询范围跨越多个 data segment | P0 |
| iterator 跨 block | 查询范围跨越多个 block | P0 |
| iterator 与 cache | HotBlockCache 命中/未命中 | P1 |
| iterator 大范围 | 百万级记录的迭代 | P2 |
| iterator 中断恢复 | 部分消费后 drop iterator | P2 |

### 4.5 FFI 测试完全缺失

**设计文档**: `store-and-ffi.md` 描述了完整的 C ABI 函数列表。

**现有测试**: 无 FFI 层测试。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| C 调用约定 | 验证所有 FFI 函数可从 C 调用 | P1 |
| 错误码传递 | 验证错误码正确传递到 C 侧 | P1 |
| 内存安全 | 验证无内存泄漏、无悬垂指针 | P1 |
| 并发安全 | 验证多线程 FFI 调用安全 | P2 |

### 4.6 Python Wrapper 测试覆盖不完整

**现有测试**: `wrapper/python/tests/` 有基本测试。

**缺失测试**:

| 场景 | 描述 | 优先级 |
|------|------|--------|
| 所有 FFI 函数 | 验证每个 FFI 函数都有 Python 绑定 | P1 |
| 异常处理 | 验证 Rust 错误正确转换为 Python 异常 | P1 |
| 类型安全 | 验证类型不匹配时的错误处理 | P1 |
| 大数据 | 验证大数据传输的正确性 | P2 |

---

## 五、测试质量问题

### 5.1 硬编码 magic number

**问题**: 多个测试使用硬编码的 segment size、compress level 等参数，没有解释为什么选择这些值。

**示例**:
```rust
store.create_dataset("test", "data", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
```

**建议**: 使用命名常量或注释解释参数选择。

### 5.2 测试命名不一致

**问题**: 部分测试使用 `t{phase}_{number}` 格式 (如 `t8_1_1_basic_lifecycle`)，部分使用描述性名称 (如 `test_read_exist_existing_timestamp`)。

**建议**: 统一命名规范，推荐使用描述性名称。

### 5.3 缺少 negative test

**问题**: 大部分测试只验证正常路径，缺少错误路径测试。

**建议**: 为每个 API 添加 negative test:
- 无效参数
- 边界条件
- 资源不足
- 并发冲突

### 5.4 测试可重复性问题

**问题**: 部分测试依赖时间 (如 `std::thread::sleep`)，可能导致在不同系统上行为不一致。

**建议**: 使用 mock 或可控的时间源。

---

## 六、测试架构建议

### 6.1 测试分层

**当前**: 单元测试和集成测试混合，没有明确分层。

**建议**:
- **单元测试** (src/ 内): 测试单个函数/方法，不依赖外部资源
- **集成测试** (tests/): 测试模块间交互，可能依赖文件系统
- **端到端测试**: 测试完整工作流，模拟真实使用场景

### 6.2 测试辅助工具

**建议创建**:
- `TestDataset`: 封装 dataset 创建和清理
- `TestStore`: 封装 store 创建和配置
- `TestData`: 生成测试数据的工具函数
- `TestAssertions`: 自定义断言宏

### 6.3 测试覆盖率度量

**建议**: 引入 `cargo-tarpaulin` 或 `llvm-cov` 度量代码覆盖率，识别未覆盖的代码路径。

### 6.4 模糊测试

**建议**: 引入 `cargo-fuzz` 对关键路径进行模糊测试:
- 序列化/反序列化 (meta、block、index)
- 查询边界条件
- 并发操作

---

## 七、优先级排序

### P0 (必须修复)

1. **压缩相关测试**: pending overflow seal、single-record block、flags 验证
2. **Cache 失效测试**: correction write、delete、retention、out-of-order write 后的缓存失效
3. **Append 语义测试**: 迁移阈值、sealed block 边界、timestamp 顺序
4. **Queue 边界测试**: push/poll/ack 到已关闭的 queue/consumer
5. **Journal 0x13 测试**: append 操作的 journal 记录

### P1 (应该修复)

1. **Read Operations**: deleted/filler 记录的行为
2. **Background Tasks**: retention reclaim 详细验证
3. **Iterator**: 跨 segment/block 的查询
4. **Lazy Allocation**: index segment 扩容
5. **FFI 测试**: C 调用约定、错误码、内存安全
6. **Python Wrapper**: 完整覆盖所有 FFI 函数

### P2 (可以优化)

1. **测试隔离**: 使用 tempfile 或 UUID
2. **测试命名**: 统一规范
3. **Negative Test**: 错误路径覆盖
4. **Fuzz Testing**: 关键路径模糊测试
5. **Coverage**: 引入覆盖率度量
6. **性能测试**: 基准测试和性能回归检测

---

## 八、总结

timslite 项目拥有 **303 个测试**，覆盖了大部分核心功能。但与设计文档对比，存在以下主要缺口:

1. **压缩流程测试严重不足**: 设计文档描述的压缩状态机和 invariant 没有充分测试
2. **Cache 失效测试缺失**: 多个应该使缓存失效的操作没有测试验证
3. **Append 语义测试不完整**: 迁移阈值、边界条件等没有测试
4. **Queue/Journal 边界测试不足**: 错误路径和边界条件缺少测试
5. **FFI/Python 层测试完全缺失**: 无法验证跨语言调用的正确性

建议按优先级逐步补充测试，首先关注 P0 项，确保核心功能的正确性和可靠性。

---

*审查完成日期: 2026-06-11*  
*审查基准: design.md + docs/design/ 全部 20 个设计文档*
