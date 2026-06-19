# timslite 第7轮测试深度审查报告

> **审查日期**: 2026-06-17  
> **审查基准**: design.md + docs/design/ 目录全部 22 份设计文档  
> **审查范围**: 单元测试 (src/*) + 集成测试 (tests/*) + Python wrapper 测试 (wrapper/python/tests/*)  
> **状态**: 仅审查,未做任何代码修改

---

## 一、测试覆盖总览

### 1.1 测试数量统计

通过 `grep "#\[test\]"` (Rust) 和 `grep "def test_"` (Python) 精确统计:

| 层级 | 文件数 | 测试数 | 备注 |
|------|--------|--------|------|
| 集成测试 (tests/) | 22 个文件 | **246** | cargo test --test-threads=1 |
| 单元测试 (src/) | 22 个模块 | **276** | 各核心模块的 #[cfg(test)] |
| Python wrapper 测试 | 12 个文件 | **93** | pytest |
| **总计** | **56** | **615** | 较第5轮 (303) 增长 103% |

### 1.2 集成测试明细 (tests/ 目录)

| 文件 | 测试数 | 覆盖范围 |
|------|--------|----------|
| `negative_test.rs` | 54 | DataSet/Store/Queue 错误路径、config 边界值、consumer group 名称校验 |
| `queue_test.rs` | 37 | push/poll/ack、多消费组、线程安全、持久化、filler gap 跳过、reopen |
| `read_operations.rs` | 25 | read_exist/query_exist/read_length/query_length/query_length_iter、deleted record |
| `journal_test.rs` | 19 | journal 创建/禁用、记录类型 (0x01/0x02/0x11/0x12/0x13)、queue 消费 |
| `dataset_inspect_test.rs` | 13 | inspect 字段、retention reclaim 统计扣减、invalid_record_count |
| `background_test.rs` | 11 | 手动 tick、idle-close double-check、cache eviction、retention reclaim |
| `dataset_lifecycle_test.rs` | 10 | create 重复/open 不存在/drop 删除/close-after-drop/reopen |
| `config_test.rs` | 8 | 默认配置、builder 覆盖、向后兼容、retention/compress_type 配置 |
| `lazy_allocation_test.rs` | 7 | 小数据写入、写满扩容、多 segment 创建、索引扩容、retention reclaim after expansion |
| `dataset_basic_test.rs` | 6 | 基本生命周期、多数据集隔离、持久化 reopen |
| `store_listing_test.rs` | 6 | get_dataset_names、get_dataset_types、去重 |
| `append_test.rs` | 6 | pending block 容量边界、sealed block append、空数据、timestamp 顺序 |
| `compression_test.rs` | 6 | zstd/deflate 往返、压缩级别、大数据 |
| `ffi_test.rs` | 6 | FFI C ABI 函数、句柄生命周期、错误路径 |
| `iterator_test.rs` | 6 | QueryIterator 惰性读取、HotBlockCache、collect_all |
| `cache_test.rs` | 5 | correction/delete/retention/out-of-order cache invalidation、LRU eviction |
| `query_test.rs` | 5 | 迭代器小范围、向后兼容、空范围、迭代器大范围 |
| `dataset_identifier_test.rs` | 5 | identifier 分配、by-id 打开、crash 边界 |
| `out_of_order_delete_test.rs` | 4 | 乱序写 (稀疏+连续)、删除生命周期、delete+write 混合 |
| `correction_write_test.rs` | 3 | 同大小纠正写、变长纠正写+reopen、sealed+compressed block fallback |
| `crash_recovery_test.rs` | 3 | pending block 恢复、index segment 完整性、多 pending records |
| `dataset_lock_boundary_test.rs` | 1 | 锁边界、并发读写 |

### 1.3 单元测试明细 (src/ 目录)

| 文件 | 测试数 | 覆盖范围 |
|------|--------|----------|
| `dataset.rs` | 71 | write/query/read、retention、correction、delete、append、out-of-order、连续模式 |
| `queue/mod.rs` | 30 | push/poll/ack、多消费组、多 consumer、等待/通知、close、drop、PendingEntry |
| `segment/data.rs` | 19 | 追加、密封、溢出、idle-close、重开、缓存、single-record |
| `index/mod.rs` | 17 | IndexEntry 往返、范围查询、TimeIndex flush/重开、连续模式 |
| `bg/mod.rs` | 16 | flush/idle/retention/cache、tick 并发、flush 队列、执行器状态 |
| `meta.rs` | 14 | TLV 编码/解码、边界条件、旧格式兼容 |
| `cache.rs` | 14 | LRU、空闲驱逐、invalidate、HotBlockCache |
| `ffi.rs` | 13 | Store 生命周期、数据集创建+读取、句柄管理 |
| `header.rs` | 13 | 数据/索引文件头往返、扩展 meta、pending 状态 |
| `config.rs` | 12 | StoreConfig 构建器、DataSetConfigBuilder、retention、compression |
| `compress.rs` | 10 | deflate/zstd 往返、压缩级别、非法数据 |
| `journal/record.rs` | 10 | JournalRecord 编解码、log_type、TV 字段 |
| `block.rs` | 7 | BlockHeader 序列化、flags、大小 |
| `error.rs` | 6 | 错误转换、Display、source |
| `util.rs` | 6 | 字节序列化往返、字节序、路径校验 |
| `index/segment.rs` | 4 | 基本读写、filler、连续模式、base timestamp |
| `journal/queue.rs` | 3 | JournalQueue open/poll/ack、consumer state |
| `journal/log.rs` | 3 | JournalLog append、sequence 递增、segment 路由 |
| `journal/segment.rs` | 3 | JournalSegment 创建、写入、读取、sequence |
| `query/iter.rs` | 2 | 空 entries、filler 跳过 |
| `segment/mod.rs` | 2 | lazy_open、closed 段查询 |
| `lib.rs` | 1 | 常量健全性检查 |

### 1.4 Python Wrapper 测试明细 (wrapper/python/tests/ 目录)

| 文件 | 测试数 | 覆盖范围 |
|------|--------|----------|
| `test_wrapper_coverage.py` | 20 | API 覆盖率检查、边界条件 |
| `test_write_query.py` | 17 | write/read/query、append、correction、delete、乱序 |
| `test_queue.py` | 17 | push/poll/ack、多消费组、持久化、close |
| `test_config.py` | 7 | StoreConfig 默认值、builder 参数、DataSetConfig |
| `test_lifecycle.py` | 6 | create/open/close/drop、重复创建、打开不存在 |
| `test_basic.py` | 6 | import、Store 创建、DataSet 创建/打开、基本读写 |
| `test_exceptions.py` | 4 | 异常类型、错误消息、非法输入 |
| `test_continuous.py` | 4 | 连续模式写入、filler、gap、base_timestamp |
| `test_journal.py` | 4 | journal 启用/禁用、记录类型、queue 消费 |
| `test_persistence.py` | 3 | reopen 后数据持久化、meta 不可变 |
| `test_store_manual_bg.py` | 3 | 手动 tick 后台任务、flush/idle |
| `test_multi_dataset.py` | 2 | 多数据集并行、隔离、交叉读写 |

---

## 二、已有测试的质量问题 (断言不完整或逻辑缺陷)

> 以下问题针对**已存在**的测试,指出其断言不完整、验证深度不足或存在逻辑瑕疵之处。

### 2.1 Correction Write Sealed Fallback: 缺少 Side-Effect 断言 (高)

**文件**: `tests/correction_write_test.rs` L96-146, `t17_3_correction_write_on_sealed_compressed_block`

**现状**: 测试写入 10 条 10KB 记录触发 block seal+compress,然后对 ts=1 执行 correction write,验证了数据正确性。

**问题**: 只验证了 "read 返回新数据",没有验证设计契约规定的其他 side-effect:
- **`invalid_record_count` 应加一**: 设计文档 (`data-model.md` §3.2) 规定 fallback 时旧数据所在段的 `invalid_record_count` 加一
- **旧 cache key 应被 invalidate**: 设计文档 (`compression.md` §16.5) 规定 correction fallback 必须 invalidate 旧索引对应的全局缓存 key

**建议**: 添加断言:
```rust
// 验证 invalid_record_count 变化
let info = lock.inspect().unwrap();
assert_eq!(info.state.total_invalid_record_count, 1);
```

### 2.2 Cache Invalidation 测试: 间接验证而非直接验证 (中)

**文件**: `tests/cache_test.rs` 全部 5 个测试

**现状**: 测试通过 "read → modify → read" 模式验证 cache invalidation 正确性。这是**功能正确**的验证方式,但属于间接验证。

**问题**: 设计文档 (`background-and-cache.md`) 定义了全局 `BlockCache` 只缓存 compressed block 解压结果。当前测试没有:
1. 确保首次 read 实际命中了 cache (如果数据量小,pending block 是 raw,不会进入 cache)
2. 显式验证 cache 内部状态变化 (如 entry 被移除)

**影响**: 如果 cache 实现有 bug 导致 entry 未被正确移除但数据读取路径恰好绕过了缓存,测试仍会通过。

**建议**: 
1. 确保测试数据已触发 seal+compress (写入足够多记录使目标 block 被 seal)
2. 通过 `inspect()` 或 debug API 验证 cache 内存使用变化

### 2.3 Background Retention Reclaim: 部分测试承认无法验证实际回收 (中)

**文件**: `tests/background_test.rs`

**现状**: 11 个测试中 retention 相关的有 5 个 (t21_7 至 t21_11),但部分测试的注释承认了局限性:
- `t21_9` (L415-416): "We cannot easily test actual reclaim because it depends on retention_check_hour timing"
- `t21_10` (L464-466): "The actual rejection of expired timestamps depends on the retention check implementation"

**问题**: 这些测试验证了 API 行为 (read 返回 None、write 接受有效 timestamp),但没有真正触发 reclaim 并验证段文件被物理删除。

**对比**: `dataset_inspect_test.rs:295` 的 `test_inspect_retention_reclaim_subtracts_archived_stats` 和 `lazy_allocation_test.rs:277` 的 `t12_6_retention_reclaim_after_expansion` 都成功验证了 `reclaim > 0` 和段数量减少。说明实际回收验证是可行的。

**建议**: 将 `t21_9` 改写为类似 `test_inspect_retention_reclaim_subtracts_archived_stats` 的模式,使用小 `data_segment_size` + 合适的 retention_window 确保段级回收。

### 2.4 Crash Recovery Index Test: 条件断言削弱验证力度 (中)

**文件**: `tests/crash_recovery_test.rs` L91-168, `t_crash_recover_index_segment_integrity`

**现状**: 测试验证了 crash 后 30 条记录全部可查询 (证明 index 功能正常),但 `base_timestamp` 使用条件断言:
```rust
if let Some(base_ts) = info.state.base_timestamp {
    assert_eq!(base_ts, 10, "base_timestamp should be 10");
}
```

**问题**: 如果 `base_timestamp` 为 None (即 index 恢复有缺陷),该断言会被静默跳过,测试仍然通过。这削弱了测试的验证力度。

**建议**: 改为无条件断言:
```rust
assert_eq!(info.state.base_timestamp, Some(10), "base_timestamp should be 10");
```
或明确注释说明在什么条件下 base_timestamp 可能为 None 以及为什么这是可接受的。

### 2.5 Append Timestamp Order Test: 缺少 Non-Latest Append 场景 (低)

**文件**: `tests/append_test.rs` L221-282, `t32_4_append_timestamp_order`

**现状**: 测试验证了 `append(timestamp < latest_written_timestamp)` 返回错误。

**问题**: 测试只在一个连续序列 (100, 200, 300) 上验证,没有测试以下场景:
- Append 到已被 delete 的 latest_written_timestamp (latest_written_timestamp 不回退,但记录已不存在)
- Append 到 latest_written_timestamp 但该记录所在的 block 已被 seal+compress

**建议**: 添加这些边界场景的测试。

### 2.6 Negative Test Consumer Group Name: 覆盖字符集不完整 (低)

**文件**: `tests/negative_test.rs` L583-591, `negative_open_consumer_invalid_group_name`

**现状**: 测试了 `"bad/name"`, `"bad name"`, `"bad@group"` 三个非法名称。

**问题**: 设计文档 (`queue-state-file.md` §31.1) 规定 `^[0-9A-Za-z_-]+$`。以下边界未覆盖:
- 空字符串 `""`
- 超过 255 字节的名称
- 包含 `.` (点号) 的名称
- 包含控制字符的名称

**建议**: 扩展测试覆盖这些边界。

### 2.7 Queue Filler Gap Test: 只测试 Delete 创建的 Filler (低)

**文件**: `tests/queue_test.rs` L110-152, `t27_1_4_poll_skips_continuous_filler_gap`

**现状**: 在连续模式下写入 ts=10,20,30,删除 ts=20 (创建 filler),验证 poll 跳过 ts=20 正确返回 ts=10 和 ts=30。

**问题**: 只测试了 delete 创建的 filler。连续模式下还存在:
- 写入 gap 产生的 filler (如写入 ts=10, ts=20, 中间 ts=11-19 是 filler)
- `query_exist` 和 `read_exist` 对 filler 的行为 (已在 read_operations.rs 中测试,但 queue poll 层面未覆盖)

**建议**: 添加一个不删除,直接利用连续模式 gap 的 poll 跳过测试。

---

## 三、测试缺失 (差距分析)

> 以下列出设计契约中有明确规定但当前测试覆盖不足或完全缺失的场景。

### 3.1 compress_type 持久化与混合算法测试缺失 (高)

**设计契约**: `compression.md` §16.1 规定:
- `compress_type` 同时保存在 dataset meta 和 segment header 中
- 读取 compressed block 时必须使用所属 segment header 的 `compress_type` 解压
- 未知 `compress_type` 必须被拒绝

**现状**: `compression_test.rs` (6 个测试) 只测试了单算法的压缩/解压往返。没有任何测试验证:
1. 创建 dataset 使用 deflate (`compress_type=1`),写入数据,reopen 后用 deflate 正确解压
2. 同一 Store 中不同 dataset 使用不同 `compress_type` (zstd vs deflate),各自独立正确
3. 篡改 meta 文件中 `compress_type` 为非法值 (如 2),open 返回错误
4. `compress_type` 在 segment header 中与 meta 中一致

**建议**: 新增 3-4 个测试覆盖 compress_type 持久化和混合算法。

### 3.2 Continuous Index Mode 深度场景测试不足 (高)

**设计契约**: `index-continuous.md` §23 规定了详细的连续模式行为。

**现状**: 现有测试覆盖了连续模式的基本写入、filler、delete,但缺少:
1. **大 gap 写入**: 如 ts=100 → ts=1000000,验证中间 segment 不创建 (逻辑空洞)
2. **回填逻辑空洞**: 写入 ts=100, ts=1000000, 再写入 ts=500000,验证新 segment 创建和前缀 filler 物化
3. **连续模式下 correction write 与 filler 交互**: correction 目标位置是 filler 时的行为
4. **`segment_capacity` 计算正确性**: 基于 `index_segment_size` 和固定 `index_entry_area_start=128`
5. **Reopen 后 `base_timestamp` 恢复**: 连续模式下 close+reopen 的 base_timestamp 一致性

**建议**: 新增 5-7 个测试覆盖连续模式深度场景。

### 3.3 End-to-End Journal 热迁移场景缺失 (中)

**设计契约**: `journal.md` §25.1 规定 journal 用于热迁移、增量同步。

**现状**: `journal_test.rs` (19 个测试) 覆盖了 journal 记录创建和 queue 消费,但缺少完整的端到端场景:
1. 源 Store 写入 → journal 记录 → 目标 Store 消费 journal 并通过 `read_entry_at_index` 拉取数据重建
2. Journal queue 消费 0x12 (delete) 在目标 Store 执行 delete
3. Journal 截断或损坏时的错误处理
4. Journal sequence 接近 `i64::MAX` 时的溢出处理

**建议**: 新增 3-4 个端到端测试。

### 3.4 FFI API 覆盖率仍然偏低 (中)

**设计契约**: `store-and-ffi.md` §11.2 规定约 30 个 FFI 函数。

**现状**: `ffi_test.rs` 6 个测试覆盖了基本 Store/Dataset 生命周期和 write/read。

**未测试或覆盖不足的 FFI 函数**:
- `tmsl_dataset_create_with_config`
- `tmsl_dataset_open_by_identifier`
- `tmsl_dataset_read_latest`
- `tmsl_dataset_read_exist` / `query_exist` / `read_length` / `query_length`
- `tmsl_dataset_inspect`
- `tmsl_store_list_datasets` / `list_dataset_types`
- `tmsl_store_open_journal_queue` / `journal_read` / `journal_query`
- Queue 相关 FFI: `open_queue` / `close_queue` / `open_consumer` / `poll` / `ack`
- Iterator FFI: `tmsl_iter_next` / `tmsl_iter_free_data`
- 错误路径下的 `err_buf` 输出验证

**建议**: 新增 10-15 个 FFI 测试,优先覆盖 read operations 和 queue 的 FFI 版本。

### 3.5 Property-Based Testing 未使用 (低)

**现状**: `Cargo.toml` 声明了 `proptest = "1"` 作为 dev-dependency,但项目中没有任何测试使用 proptest。

**建议**: 利用 proptest 生成随机输入验证:
- 任意 timestamp 序列的 write/read/query 往返正确性
- 任意 data 长度的 append 边界行为
- 任意 retention_window 的 reclaim 正确性

### 3.6 Query Iterator 复杂场景不足 (低)

**现状**: `iterator_test.rs` (6 个测试) 覆盖了基本惰性读取。

**缺失场景**:
1. 跨多个 segment 的查询 (验证 segment 切换)
2. 查询范围包含 closed segment (验证临时打开)
3. 查询范围包含 in-memory buffer (未 flush 的 entry)
4. QueryIterator 创建后修改数据的交互行为

**建议**: 新增 3-4 个测试。

---

## 四、基础设施与工程化问题

### 4.1 临时目录不清理 (中)

**问题**: 所有测试使用自定义 `temp_dir()` 函数,基于 `SystemTime::now().as_nanos()` + `AtomicU64` 计数器生成目录名。测试完成后不清理目录。

**影响**: 长期运行会在 `/tmp/timslite_integration/` 和 `/tmp/timslite_crash_recovery/` 下积累大量临时目录。

**建议**: 使用 `tempfile::tempdir()` 自动清理,或在测试末尾添加 `fs::remove_dir_all()`。

### 4.2 Sleep 依赖导致潜在不稳定 (低)

**问题**: 部分测试使用 `std::thread::sleep()` 等待后台任务或超时:
- `background_test.rs`: 多个 sleep 200ms-500ms
- `queue_test.rs`: sleep 等待 poll 超时

**现状**: 当前配置使用短超时 (100-200ms),在 CI 环境中通常稳定,但在高负载系统上可能偶发失败。

**建议**: 对关键路径使用 condvar 或 channel 替代 sleep。对于 poll 超时测试,sleep 是合理的。

### 4.3 测试命名风格不统一 (低)

**问题**: 测试命名混合了三种风格:
- Phase/Task 编号: `t17_1_*`, `t27_1_*`, `t32_1_*`
- 语义描述: `t_crash_recover_*`
- 标准 Rust: `test_read_exist_*`, `negative_*`

**建议**: 统一使用语义化命名或在文档注释中说明编号含义。

---

## 五、与第5轮 Review 对比

### 5.1 第5轮已修复问题

| 第5轮问题 | 状态 | 备注 |
|-----------|------|------|
| Journal 0x13 append 测试缺失 | ✅ 已修复 | `t28_14` 和 `t28_15` 覆盖 forward append + queue 消费 |
| Consumer group name 校验未测试 | ✅ 已修复 | `negative_open_consumer_invalid_group_name` 覆盖 3 种非法名 |
| FFI API 覆盖率极低 (仅 2 个测试) | ⚠️ 部分修复 | 增至 6 个,但仍远低于 ~30 个 FFI 函数的覆盖需求 |
| Correction sealed block fallback 未测试 | ✅ 已修复 | `t17_3_correction_write_on_sealed_compressed_block` 存在,但缺少 side-effect 断言 |
| Cache invalidation 未测试 | ✅ 已修复 | cache_test.rs 5 个测试覆盖 correction/delete/retention/out-of-order |
| Idle-close double-check 竞态未测试 | ✅ 已修复 | `t21_4_idle_close_double_check_skips_recently_used` |
| Retention reclaim 未真正验证 | ⚠️ 部分修复 | `test_inspect_retention_reclaim_subtracts_archived_stats` 和 `t12_6` 验证了真实回收,但 `t21_9/t21_10` 仍承认无法验证 |
| Crash recovery index 验证缺失 | ✅ 已修复 | `t_crash_recover_index_segment_integrity` 存在,但 base_timestamp 使用条件断言 |
| Queue filler gap 跳过未验证 | ✅ 已修复 | `t27_1_4_poll_skips_continuous_filler_gap` 使用 delete 创建 filler 验证 |
| Read operations deleted/filler 缺失 | ✅ 已修复 | read_operations.rs 4 个测试 (P1-R-1~4) |

### 5.2 第5轮仍未修复

| 第5轮问题 | 状态 | 备注 |
|-----------|------|------|
| 临时目录不清理 | ❌ 未修复 | 仍使用自定义 temp_dir,无清理 |
| Background sleep 竞态 | ⚠️ 改善 | 使用短超时,风险降低但未根除 |
| compress_type 持久化测试 | ❌ 未覆盖 | 无任何 compress_type 选择/持久化测试 |
| Continuous mode 深度场景 | ❌ 未覆盖 | 大 gap、回填逻辑空洞等未测试 |
| proptest 未使用 | ❌ 未使用 | 声明了依赖但零使用 |

---

## 六、优先级排序

### P0 - 建议优先处理

1. **§2.1 Correction fallback side-effect 断言** — 影响 cache 一致性和统计正确性验证
2. **§3.1 compress_type 持久化与混合算法** — 设计契约的核心不变量,零覆盖

### P1 - 高优先级

3. **§3.2 Continuous mode 深度场景** — 大 gap 和回填是连续模式的核心价值
4. **§2.3 Retention reclaim 真正验证** — 已有可参考模式,改造成本低
5. **§3.4 FFI API 覆盖率** — 影响跨语言集成验证

### P2 - 中优先级

6. **§2.4 Crash recovery base_timestamp 条件断言** — 改为无条件断言
7. **§3.3 Journal 端到端热迁移** — 验证 journal 实际使用场景
8. **§2.6 Consumer group name 字符集扩展** — 补全边界字符
9. **§2.2 Cache invalidation 直接验证** — 确保 read 实际经过 cache

### P3 - 低优先级

10. **§3.5 proptest 使用** — 增强测试深度
11. **§3.6 Query iterator 复杂场景** — 补充 segment 切换和 closed segment
12. **§4.1 临时目录清理** — 工程化改善
13. **§4.2 Sleep 替代** — 稳定性改善
14. **§4.3 命名统一** — 可维护性改善

---

## 七、总结

### 7.1 整体评估

第7轮测试相比第5轮 (303 个 → 615 个) 有了**显著进步**,覆盖率翻倍。第5轮指出的大多数严重缺失已得到修复:
- Journal 0x13 append、cache invalidation、correction sealed fallback、idle-close double-check、consumer group name 校验、crash recovery index 验证等**均已补充测试**
- 新增 `negative_test.rs` (54 个测试) 大幅提升了错误路径覆盖
- `read_operations.rs` (25 个测试) 完整覆盖了轻量级读操作 API

### 7.2 主要短板

1. **断言深度不足**: 部分测试只验证了 "happy path" 返回值,缺少 side-effect 断言 (invalid_record_count、cache state、file deletion)
2. **compress_type 零覆盖**: 作为压缩算法选择的核心配置,持久化和混合算法场景完全没有测试
3. **连续模式深度不足**: 大 gap、逻辑空洞回填等是连续模式的核心设计点,缺少验证
4. **FFI 覆盖率偏低**: ~30 个 FFI 函数只测试了 6 个测试覆盖的子集

### 7.3 建议行动

1. **本迭代**: 修复 P0 (correction side-effect 断言 + compress_type 测试),预计新增 5-6 个测试
2. **下一迭代**: 完成 P1 (continuous mode + retention reclaim + FFI),预计新增 15-20 个测试
3. **持续改进**: P2-P3 逐步完善,引入 proptest
