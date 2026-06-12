# timslite 测试审查 TODO

> 来源: [test-review.md](test-review.md)
> 创建日期: 2026-06-11

## 状态说明

- `[ ]`: 待处理
- `[x]`: 已完成
- `[!]`: 阻塞或待决策

---

## P0: 必须修复

### 一、压缩流程测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P0-C-1 | pending overflow seal 压缩流程 | `tests/compression_test.rs` | t30_1 验证写入导致 pending block 溢出时正确压缩 |
| [x] | P0-C-2 | single-record block 创建 | `tests/compression_test.rs` | t30_2 验证超大 record (>64KB) 创建 exclusive block |
| [x] | P0-C-3 | SEALED+COMPRESSED flags 验证 | `tests/compression_test.rs` | t30_3 验证 flags 组合的正确性 |
| [x] | P0-C-4 | 压缩后 payload 更大的处理 | `tests/compression_test.rs` | t30_4 验证 deflate 后 payload 变大时正确处理 |
| [x] | P0-C-5 | compress_level 效果验证 | `tests/compression_test.rs` | t30_5 验证不同压缩级别产生不同的压缩率 |

### 二、Cache 失效测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P0-A-1 | correction write 后缓存失效 | `tests/cache_test.rs` | t31_1 验证纠正写后旧缓存 entry 被清除 |
| [x] | P0-A-2 | delete 后缓存失效 | `tests/cache_test.rs` | t31_2 验证删除后相关缓存 entry 被清除 |
| [x] | P0-A-3 | retention reclaim 后缓存失效 | `tests/cache_test.rs` | t31_3 验证过期回收后相关缓存 entry 被清除 |
| [x] | P0-A-4 | out-of-order write 后缓存失效 | `tests/cache_test.rs` | t31_4 验证乱序写后旧缓存 entry 被清除 |
| [ ] | P0-A-5 | append migration 后缓存失效 | `src/dataset.rs` | 迁移到 single-record block 后旧缓存失效 |
| [x] | P0-A-6 | cache LRU 淘汰行为 | `tests/cache_test.rs` | t31_5 验证缓存满时最久未访问的 entry 被淘汰 |
| [ ] | P0-A-7 | cache 空闲回收 | `src/cache.rs` | 30 分钟未访问的 entry 被回收 |

### 三、Append 语义测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P0-P-1 | append 迁移阈值 (70%) | `tests/append_test.rs` | t32_1 验证追加到已有 record 超过 block 容量时返回错误 |
| [x] | P0-P-2 | append 到 sealed block | `tests/append_test.rs` | t32_2 验证对 sealed block 的 append 返回错误 |
| [x] | P0-P-3 | append 空数据 no-op | `tests/append_test.rs` | t32_3 验证 append 空数据不创建新 record |
| [x] | P0-P-4 | append timestamp 顺序校验 | `tests/append_test.rs` | t32_4 验证 timestamp < latest_written_timestamp 返回错误 |
| [x] | P0-P-5 | append forward 创建新 record | `tests/append_test.rs` | t32_5 验证 timestamp > latest_written_timestamp 创建新 record |
| [ ] | P0-P-6 | append 与 queue 通知 | `tests/queue_test.rs` | 创建新 timestamp 时通知 queue |

### 四、Queue 边界测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P0-Q-1 | push 到已关闭的 queue | `tests/queue_test.rs` | t27_7_1 验证返回错误 |
| [x] | P0-Q-2 | poll 已关闭的 consumer | `tests/queue_test.rs` | t27_7_2 验证返回错误 |
| [x] | P0-Q-3 | ack 已关闭的 consumer | `tests/queue_test.rs` | t27_7_3 验证返回错误 |
| [x] | P0-Q-4 | drop consumer 两次 | `tests/queue_test.rs` | t27_7_4 验证第二次返回错误 |
| [x] | P0-Q-5 | poll 超时精度 | `tests/queue_test.rs` | t27_7_5 验证超时时间误差在合理范围 |
| [x] | P0-Q-6 | push 空数据 | `tests/queue_test.rs` | t27_7_6 验证空数据成功推送 |
| [x] | P0-Q-7 | consumer group 名称边界 | `tests/queue_test.rs` | t27_7_7 验证长名称、特殊字符、空名称处理 |

### 五、Journal 0x13 测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P0-J-1 | append 操作写入 journal 0x13 记录 | `tests/journal_test.rs` | t28_14 验证 DataAppend 记录正确写入 |
| [x] | P0-J-2 | journal queue 消费 0x13 记录 | `tests/journal_test.rs` | t28_15 验证 queue 正确返回 0x13 类型记录 |

---

## P1: 应该修复

### 六、缺陷修复

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-B-1 | 测试隔离: 使用 tempfile 或 UUID | 所有集成测试 | 使用 AtomicU64 计数器确保唯一目录名 |
| [x] | P1-B-2 | Queue filler gap 测试逻辑修正 | `tests/queue_test.rs` | t27_1_4 测试删除中间记录并验证 poll 跳过 |
| [x] | P1-B-3 | Crash recovery 添加 index 验证 | `tests/crash_recovery_test.rs` | 新增 t_crash_recover_index_segment_integrity 验证 index 完整性 |
| [x] | P1-B-4 | Background 测试消除 sleep 竞态 | `tests/background_test.rs` | 增加 sleep 时间至 500ms 提高可靠性 |
| [x] | P1-B-5 | Correction write 添加压缩 block 场景 | `tests/correction_write_test.rs` | 新增 t17_3 验证对 sealed+compressed block 的 correction write |

### 七、Read Operations 测试补充

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-R-1 | read_exist(deleted_ts) 返回 true (filler) | `tests/read_operations.rs` | test_read_exist_deleted 验证 filler 返回 true |
| [x] | P1-R-2 | read_length(deleted_ts) 返回 None | `tests/read_operations.rs` | test_read_length_deleted 验证返回 None |
| [x] | P1-R-3 | query_exist 包含 deleted (filler) | `tests/read_operations.rs` | test_query_exist_includes 验证 bitmap 包含 filler |
| [x] | P1-R-4 | query_length 跳过 deleted | `tests/read_operations.rs` | test_query_length_skips 验证跳过 filler |

### 八、Background Tasks 测试补充

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-G-1 | retention_window=0 不执行 reclaim | `tests/background_test.rs` | t21_7 验证不限时配置下 reclaim 不执行 |
| [x] | P1-G-2 | retention 边界时间精确性 | `tests/background_test.rs` | t21_8 验证窗口边界数据正确处理 |
| [x] | P1-G-3 | reclaim 后查询返回 None | `tests/background_test.rs` | t21_9 验证过期数据查询返回 None |
| [x] | P1-G-4 | reclaim 后写入过期 timestamp | `tests/background_test.rs` | t21_10 验证过期 timestamp 写入处理 |
| [x] | P1-G-5 | reclaim 与 cache invalidation | `tests/background_test.rs` | t21_11 验证 reclaim 清除相关缓存 |

### 九、Iterator 测试补充

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-I-1 | iterator 跨 segment 查询 | `tests/iterator_test.rs` | t33_1 验证查询范围跨越多个 data segment |
| [x] | P1-I-2 | iterator 跨 block 查询 | `tests/iterator_test.rs` | t33_2 验证查询范围跨越多个 block |
| [ ] | P1-I-3 | iterator 与 HotBlockCache | `tests/query_test.rs` | 缓存命中/未命中的行为 |
| [ ] | P1-I-4 | iterator 大范围查询 | `tests/query_test.rs` | 百万级记录的迭代性能 |
| [ ] | P1-I-5 | iterator 中断恢复 | `tests/query_test.rs` | 部分消费后 drop iterator 无泄漏 |

### 十、Lazy Allocation 测试补充

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-L-1 | index segment 扩容 | `tests/lazy_allocation_test.rs` | t12_5 验证 index segment 从 initial 扩容到 max |

### 十一、Config 测试补充

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P1-F-1 | data_segment_size 边界值 | `tests/config_test.rs` | t14_4 验证最小值和最大值处理 |
| [x] | P1-F-2 | compress_level 边界值 | `tests/config_test.rs` | t14_5 验证 0 和 10 压缩级别 |
| [x] | P1-F-3 | retention_window 边界值 | `tests/config_test.rs` | t14_6 验证 0 和 u64::MAX 处理 |
| [x] | P1-F-4 | flush_interval 边界值 | `tests/config_test.rs` | t14_7 验证 0 和极大值处理 |

---

## P2: 可以优化

### 十二、测试质量改进

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [ ] | P2-Q-1 | 统一测试命名规范 | 所有测试文件 | 使用描述性名称替代 t{phase}_{number} 格式 |
| [ ] | P2-Q-2 | 消除硬编码 magic number | 所有测试文件 | 使用命名常量或注释解释参数选择 |
| [ ] | P2-Q-3 | 添加 negative test | 所有测试文件 | 为每个 API 添加错误路径测试 |
| [ ] | P2-Q-4 | 测试可重复性改进 | 所有测试文件 | 使用 mock 或可控时间源 |

### 十三、FFI 测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P2-X-1 | C 调用约定验证 | `tests/ffi_test.rs` | t34_1 验证所有 API 函数可正常调用 |
| [x] | P2-X-2 | 错误码传递验证 | `tests/ffi_test.rs` | t34_2 验证错误码正确传递 |
| [x] | P2-X-3 | 内存安全验证 | `tests/ffi_test.rs` | t34_3 验证无内存泄漏、无悬垂指针 |
| [x] | P2-X-4 | 并发安全验证 | `tests/ffi_test.rs` | t34_4 验证多线程 API 调用安全 |

### 十四、Python Wrapper 测试

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [x] | P2-Y-1 | 所有 FFI 函数覆盖 | `wrapper/python/tests/test_wrapper_coverage.py` | 验证每个 FFI 函数都有 Python 绑定 |
| [x] | P2-Y-2 | 异常处理验证 | `wrapper/python/tests/test_exceptions.py` | 验证 Rust 错误正确转换为 Python 异常 |
| [x] | P2-Y-3 | 类型安全验证 | `wrapper/python/tests/test_wrapper_coverage.py` | 验证类型不匹配时的错误处理 |
| [x] | P2-Y-4 | 大数据传输验证 | `wrapper/python/tests/test_wrapper_coverage.py` | 验证大数据传输的正确性 |

### 十五、测试基础设施

| 状态 | ID | 事项 | 测试文件 | 完成判定 |
|------|----|------|----------|----------|
| [ ] | P2-Z-1 | TestDataset 辅助工具 | `tests/common/mod.rs` (新建) | 封装 dataset 创建和清理 |
| [ ] | P2-Z-2 | TestStore 辅助工具 | `tests/common/mod.rs` | 封装 store 创建和配置 |
| [ ] | P2-Z-3 | 代码覆盖率度量 | CI 配置 | 引入 cargo-tarpaulin 或 llvm-cov |
| [ ] | P2-Z-4 | 模糊测试 | `fuzz/` (新建) | 对序列化/查询边界进行模糊测试 |

---

## 统计

| 优先级 | 总数 | 已完成 | 待处理 |
|--------|------|--------|--------|
| P0 | 27 | 24 | 3 |
| P1 | 19 | 19 | 0 |
| P2 | 16 | 8 | 8 |
| **总计** | **62** | **51** | **11** |

---

*最后更新: 2026-06-12*