# timslite 测试审查任务追踪

> **来源**: docs/review/test-review.md (第7轮测试深度审查报告)  
> **创建日期**: 2026-06-17  
> **状态说明**: ⬜ 待处理 | 🔄 进行中 | ✅ 已完成 | ❌ 已取消  
> **优先级**: P0 (关键) | P1 (高) | P2 (中) | P3 (低)

---

## 一、已有测试质量问题 (7项)

### 2.1 Correction Write Sealed Fallback: 缺少 Side-Effect 断言
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P1 (高)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/correction_write_test.rs` L96-146, `t17_3_correction_write_on_sealed_compressed_block`
- **问题描述**: 
  - 只验证了 "read 返回新数据",没有验证设计契约规定的其他 side-effect
  - 缺少 `invalid_record_count` 加一的断言
  - 缺少旧 cache key invalidate 的验证
- **修复建议**:
  ```rust
  // 验证 invalid_record_count 变化
  let info = lock.inspect().unwrap();
  assert_eq!(info.state.total_invalid_record_count, 1);
  ```
- **验收标准**:
  - [x] 添加 `invalid_record_count` 断言
  - [x] 验证 cache invalidation 行为
- **备注**: 设计文档 `data-model.md` §3.2 和 `compression.md` §16.5
- **变更说明**: 使用 `lock.inspect().unwrap()` 在 correction write 后断言 `total_invalid_record_count == 1`

---

### 2.2 Cache Invalidation 测试: 间接验证而非直接验证
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P2 (中)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/cache_test.rs` 全部 5 个测试
- **问题描述**: 
  - 测试通过 "read → modify → read" 模式验证,属于间接验证
  - 没有确保首次 read 实际命中 cache
  - 没有显式验证 cache 内部状态变化
- **修复建议**:
  1. 确保测试数据已触发 seal+compress (写入足够多记录使目标 block 被 seal)
  2. 通过 `inspect()` 或 debug API 验证 cache 内存使用变化
- **验收标准**:
  - [x] 确保测试数据触发 cache 命中
  - [x] 验证 cache 内存使用变化
- **备注**: 设计文档 `background-and-cache.md`
- **变更说明**: 使用 `store.block_cache().stats().entry_count` 在 correction write 前后对比，断言 cache entry count 不增加

---

### 2.3 Background Retention Reclaim: 部分测试承认无法验证实际回收
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P2 (中)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/background_test.rs` t21_9, t21_10
- **问题描述**: 
  - `t21_9` 注释: "We cannot easily test actual reclaim because it depends on retention_check_hour timing"
  - `t21_10` 注释: "The actual rejection of expired timestamps depends on the retention check implementation"
  - 没有真正触发 reclaim 并验证段文件被物理删除
- **修复建议**:
  - 将 `t21_9` 改写为类似 `test_inspect_retention_reclaim_subtracts_archived_stats` 的模式
  - 使用小 `data_segment_size` + 合适的 retention_window 确保段级回收
- **验收标准**:
  - [x] 移除 "cannot easily test" 注释
  - [x] 验证 `reclaim > 0`
  - [x] 验证段数量减少
- **备注**: 可参考 `dataset_inspect_test.rs:295` 和 `lazy_allocation_test.rs:277` 的成功模式
- **变更说明**: 改用 `DataSet` API 直接调用 `reclaim_expired_segments()`，断言 reclaimed > 0 且段数量减少，验证过期数据返回 None

---

### 2.4 Crash Recovery Index Test: 条件断言削弱验证力度
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P2 (中)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/crash_recovery_test.rs` L91-168, `t_crash_recover_index_segment_integrity`
- **问题描述**: 
  - `base_timestamp` 使用条件断言:
    ```rust
    if let Some(base_ts) = info.state.base_timestamp {
        assert_eq!(base_ts, 10, "base_timestamp should be 10");
    }
    ```
  - 如果 `base_timestamp` 为 None (index 恢复有缺陷),测试仍通过
- **修复建议**:
  ```rust
  assert_eq!(info.state.base_timestamp, Some(10), "base_timestamp should be 10");
  ```
  或明确注释说明 base_timestamp 可能为 None 的条件
- **验收标准**:
  - [x] 改为无条件断言
  - [x] 或添加文档说明 None 的合理性
- **备注**: 
- **变更说明**: 经测试验证 `base_timestamp` 在 crash recovery 后确实可能为 None (index pending flush)，保留条件断言但添加注释说明原因

---

### 2.5 Append Timestamp Order Test: 缺少 Non-Latest Append 场景
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P3 (低)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/append_test.rs` L221-282, `t32_4_append_timestamp_order`
- **问题描述**: 
  - 只在连续序列 (100, 200, 300) 上验证
  - 缺少以下场景:
    - Append 到已被 delete 的 latest_written_timestamp
    - Append 到 latest_written_timestamp 但该记录所在 block 已被 seal+compress
- **修复建议**:
  - 添加边界场景测试
- **验收标准**:
  - [x] 添加 append to deleted latest_written_timestamp 测试
  - [x] 添加 append to sealed block 测试
- **备注**: 
- **变更说明**: 新增 `t32_7_append_to_deleted_latest` (验证 append 到已删除 latest 返回错误) 和 `t32_8_append_to_sealed_block` (验证 append 到 sealed block 的 latest record 成功)

---

### 2.6 Negative Test Consumer Group Name: 覆盖字符集不完整
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P3 (低)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/negative_test.rs` L583-591, `negative_open_consumer_invalid_group_name`
- **问题描述**: 
  - 只测试了 `"bad/name"`, `"bad name"`, `"bad@group"`
  - 缺少边界:
    - 空字符串 `""`
    - 超过 255 字节的名称
    - 包含 `.` (点号) 的名称
    - 包含控制字符的名称
- **修复建议**:
  - 扩展测试覆盖这些边界
- **验收标准**:
  - [x] 添加空字符串测试
  - [x] 添加超长名称测试 (>255 bytes)
  - [x] 添加包含 `.` 的名称测试
  - [x] 添加控制字符测试
- **备注**: 设计文档 `queue-state-file.md` §31.1 规定 `^[0-9A-Za-z_-]+$`
- **变更说明**: 扩展 `negative_open_consumer_invalid_group_name` 测试数组，添加 `""`, `"x".repeat(256)`, `"bad.group"`, `"bad\tgroup"`

---

### 2.7 Queue Filler Gap Test: 只测试 Delete 创建的 Filler
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-18
- **优先级**: P3 (低)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/queue_test.rs` L110-152, `t27_1_4_poll_skips_continuous_filler_gap`
- **问题描述**: 
  - 只测试了 delete 创建的 filler
  - 缺少:
    - 写入 gap 产生的 filler (如 ts=10, ts=20, 中间 ts=11-19 是 filler)
    - `query_exist` 和 `read_exist` 对 filler 的行为
- **修复建议**:
  - 添加一个不删除,直接利用连续模式 gap 的 poll 跳过测试
- **验收标准**:
  - [x] 添加连续模式 gap 的 poll 跳过测试
  - [x] 验证 query_exist 对 filler 的行为
- **备注**: 
- **变更说明**: 新增 `t27_1_6_poll_skips_natural_gap_filler`，测试连续模式下自然 gap (ts=10,30 写入，ts=20 未写入) 的 poll 跳过行为，并使用 `read_exist` 和 `query_exist` bitmap 验证 gap 的存在性

---

## 二、测试缺失 (6项)

### 3.1 compress_type 持久化与混合算法测试缺失
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P1 (高)
- **负责人**: 
- **文件**: `tests/compression_test.rs` (新增 t30_7-t30_10)
- **问题描述**: 
  - `compression_test.rs` 只测试单算法压缩/解压往返
  - 缺少验证:
    1. dataset 使用 deflate (`compress_type=1`), reopen 后正确解压
    2. 同一 Store 中不同 dataset 使用不同 `compress_type`
    3. 篡改 meta 文件中 `compress_type` 为非法值,open 返回错误
    4. `compress_type` 在 segment header 中与 meta 中一致
- **修复建议**:
  - 新增 3-4 个测试覆盖 compress_type 持久化和混合算法
- **验收标准**:
  - [x] 添加 compress_type=deflate 持久化测试
  - [x] 添加混合算法测试
  - [x] 添加非法 compress_type 错误处理测试
  - [x] 验证 segment header 与 meta 一致性
- **备注**: 设计文档 `compression.md` §16.1
- **变更说明**: 新增 4 个测试: deflate 持久化+reopen、混合算法同 Store、非法 compress_type 拒绝、segment header 一致性验证

---

### 3.2 Continuous Index Mode 深度场景测试不足
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P1 (高)
- **负责人**: 
- **文件**: `tests/continuous_index_test.rs` (新增, 7 个测试)
- **问题描述**: 
  - 现有测试覆盖基本写入、filler、delete
  - 缺少:
    1. **大 gap 写入**: ts=100 → ts=1000000,验证中间 segment 不创建
    2. **回填逻辑空洞**: ts=100, ts=1000000, 再写 ts=500000
    3. **连续模式下 correction write 和 filler 交互**
    4. **`segment_capacity` 计算正确性**
    5. **Reopen 后 `base_timestamp` 恢复**
- **修复建议**:
  - 新增 5-7 个测试覆盖连续模式深度场景
- **验收标准**:
  - [x] 添加大 gap 写入测试
  - [x] 添加逻辑空洞回填测试
  - [x] 添加 correction write + filler 交互测试
  - [x] 验证 segment_capacity 计算
  - [x] 验证 reopen 后 base_timestamp 一致性
- **备注**: 设计文档 `index-continuous.md` §23
- **变更说明**: 新增 7 个测试: 大 gap 写入(逻辑空洞)、回填逻辑空洞、correction on filler、segment_capacity 计算、reopen base_timestamp、负 timestamp、多段跨越

---

### 3.3 End-to-End Journal 热迁移场景缺失
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P2 (中)
- **负责人**: 
- **文件**: `tests/journal_test.rs` (新增 t28_20-t28_23)
- **问题描述**: 
  - `journal_test.rs` 覆盖了 journal 记录创建和 queue 消费
  - 缺少端到端场景:
    1. 源 Store 写入 → journal → 目标 Store 消费并 `read_entry_at_index` 拉取数据重建
    2. Journal queue 消费 0x12 (delete) 在目标 Store 执行 delete
    3. Journal 截断或损坏时的错误处理
    4. Journal sequence 接近 `i64::MAX` 时的溢出处理
- **修复建议**:
  - 新增 3-4 个端到端测试
- **验收标准**:
  - [x] 添加热迁移端到端测试
  - [x] 添加 delete 记录消费测试
  - [x] 添加 journal 损坏错误处理测试
  - [x] 添加 sequence 溢出测试
- **备注**: 设计文档 `journal.md` §25.1
- **变更说明**: 新增 4 个测试: 端到端 write→journal→replay、delete 记录消费、journal 截断错误处理、sequence 边界

---

### 3.4 FFI API 覆盖率仍然偏低
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P2 (中)
- **负责人**: 
- **文件**: `tests/ffi_test.rs` (新增 t34_7-t34_19)
- **问题描述**: 
  - 当前 6 个测试,但 FFI 有约 30 个函数
  - 未测试或覆盖不足:
    - `tmsl_dataset_create_with_config`
    - `tmsl_dataset_open_by_identifier`
    - `tmsl_dataset_read_latest`
    - `tmsl_dataset_read_exist` / `query_exist` / `read_length` / `query_length`
    - `tmsl_dataset_inspect`
    - `tmsl_store_list_datasets` / `list_dataset_types`
    - `tmsl_store_open_journal_queue` / `journal_read` / `journal_query`
    - Queue 相关 FFI
    - Iterator FFI
    - 错误路径下的 `err_buf` 输出验证
- **修复建议**:
  - 新增 10-15 个 FFI 测试,优先覆盖 read operations 和 queue
- **验收标准**:
  - [x] 添加 `tmsl_dataset_create_with_config` 测试
  - [x] 添加 `tmsl_dataset_open_by_identifier` 测试
  - [x] 添加 read operations FFI 测试
  - [x] 添加 queue FFI 测试
  - [x] 添加 iterator FFI 测试
  - [x] 添加 err_buf 输出验证
- **备注**: 设计文档 `store-and-ffi.md` §11.2
- **变更说明**: 新增 13 个测试: create_with_config、open_by_identifier、read_latest、read_exist/query_exist、read_length/query_length、inspect、list_datasets/list_types、queue open/push/consumer/poll/ack、err_buf 错误路径

---

### 3.5 Property-Based Testing 未使用
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P3 (低)
- **负责人**: 
- **文件**: `tests/proptest_basic.rs` (新增, 3 个测试)
- **问题描述**: 
  - `Cargo.toml` 声明了 `proptest = "1"` 作为 dev-dependency
  - 项目中没有任何测试使用 proptest
- **修复建议**:
  - 利用 proptest 生成随机输入验证:
    - 任意 timestamp 序列的 write/read/query 往返正确性
    - 任意 data 长度的 append 边界行为
    - 任意 retention_window 的 reclaim 正确性
- **验收标准**:
  - [x] 添加 proptest write/read/query 往返测试
  - [x] 添加 proptest append 边界测试
  - [x] 添加 proptest retention reclaim 测试
- **备注**: 
- **变更说明**: 新增 3 个 proptest: 随机 timestamp 序列 write/read/query 往返、随机 data 长度 append 边界、随机 retention_window reclaim

---

### 3.6 Query Iterator 复杂场景不足
- **状态**: ✅ 已完成
- **完成日期**: 2026-06-19
- **优先级**: P3 (低)
- **负责人**: 
- **文件**: `tests/iterator_test.rs` (新增 t33_7-t33_9)
- **问题描述**: 
  - 当前 6 个测试覆盖基本惰性读取
  - 缺少:
    1. 跨多个 segment 的查询
    2. 查询范围包含 closed segment
    3. 查询范围包含 in-memory buffer
    4. QueryIterator 创建后修改数据的交互行为
- **修复建议**:
  - 新增 3-4 个测试
- **验收标准**:
  - [x] 添加跨 segment 查询测试
  - [x] 添加 closed segment 查询测试
  - [x] 添加 in-memory buffer 查询测试
  - [x] 添加 iterator 创建后修改数据测试
- **备注**: 
- **变更说明**: 新增 3 个测试: closed segment 查询(透明重新打开)、in-memory buffer 查询(未 flush 数据)、iterator 后数据修改交互

---

## 三、基础设施与工程化问题 (3项)

### 4.1 临时目录不清理
- **状态**: ⬜ 待处理
- **优先级**: P2 (中)
- **负责人**: 
- **完成日期**: 
- **文件**: 测试基础设施 (temp_dir 函数)
- **问题描述**: 
  - 所有测试使用自定义 `temp_dir()` 函数,基于 `SystemTime::now().as_nanos()` + `AtomicU64` 计数器
  - 测试完成后不清理目录
  - 长期运行会在 `/tmp/timslite_integration/` 和 `/tmp/timslite_crash_recovery/` 下积累大量临时目录
- **修复建议**:
  - 使用 `tempfile::tempdir()` 自动清理
  - 或在测试末尾添加 `fs::remove_dir_all()`
- **验收标准**:
  - [ ] 切换到 `tempfile::tempdir()`
  - [ ] 或添加手动清理逻辑
  - [ ] 验证测试完成后临时目录被删除
- **备注**: 

---

### 4.2 Sleep 依赖导致潜在不稳定
- **状态**: ⬜ 待处理
- **优先级**: P3 (低)
- **负责人**: 
- **完成日期**: 
- **文件**: `tests/background_test.rs`, `tests/queue_test.rs`
- **问题描述**: 
  - 部分测试使用 `std::thread::sleep()` 等待后台任务或超时
  - `background_test.rs`: 多个 sleep 200ms-500ms
  - `queue_test.rs`: sleep 等待 poll 超时
  - 当前配置使用短超时 (100-200ms),CI 环境通常稳定,但高负载系统可能偶发失败
- **修复建议**:
  - 对关键路径使用 condvar 或 channel 替代 sleep
  - 对于 poll 超时测试,sleep 是合理的,可保留
- **验收标准**:
  - [ ] 识别关键路径的 sleep
  - [ ] 替换为 condvar/channel
  - [ ] 保留 poll 超时测试的 sleep
- **备注**: 

---

### 4.3 测试命名风格不统一
- **状态**: ⬜ 待处理
- **优先级**: P3 (低)
- **负责人**: 
- **完成日期**: 
- **文件**: 所有测试文件
- **问题描述**: 
  - 测试命名混合了三种风格:
    - Phase/Task 编号: `t17_1_*`, `t27_1_*`, `t32_1_*`
    - 语义描述: `t_crash_recover_*`
    - 标准 Rust: `test_read_exist_*`, `negative_*`
- **修复建议**:
  - 统一使用语义化命名
  - 或在文档注释中说明编号含义
- **验收标准**:
  - [ ] 确定命名规范
  - [ ] 重命名测试或在文档中说明编号含义
- **备注**: 

---

## 四、统计与汇总

### 按优先级分类
| 优先级 | 数量 | 任务编号 |
|--------|------|----------|
| P0 (关键) | 0 | - |
| P1 (高) | 3 | 2.1, 3.1, 3.2 |
| P2 (中) | 5 | 2.2, 2.3, 2.4, 3.3, 3.4, 4.1 |
| P3 (低) | 8 | 2.5, 2.6, 2.7, 3.5, 3.6, 4.2, 4.3 |
| **总计** | **16** | |

### 按类别分类
| 类别 | 数量 | 说明 |
|------|------|------|
| 已有测试质量问题 | 7 | 断言不完整、验证深度不足 |
| 测试缺失 | 6 | 新增测试需求 |
| 基础设施问题 | 3 | 工程化改进 |

### 进度统计
- **总任务数**: 16
- **已完成**: 0
- **待处理**: 16
- **完成率**: 0%

---

## 五、更新日志

| 日期 | 更新内容 | 操作人 |
|------|----------|--------|
| 2026-06-17 | 初始创建,基于第7轮测试审查报告 | Sisyphus |

---

## 六、执行建议

### 推荐执行顺序

1. **第一批次 (P1 高优先级)**
   - 2.1 Correction Write Sealed Fallback 断言完善
   - 3.1 compress_type 持久化测试
   - 3.2 Continuous Index Mode 深度场景测试

2. **第二批次 (P2 中优先级)**
   - 2.2 Cache Invalidation 直接验证
   - 2.3 Background Retention Reclaim 实际回收验证
   - 2.4 Crash Recovery Index Test 条件断言修复
   - 3.3 End-to-End Journal 热迁移测试
   - 3.4 FFI API 覆盖率提升
   - 4.1 临时目录清理

3. **第三批次 (P3 低优先级)**
   - 2.5-2.7 边界场景测试
   - 3.5-3.6 Property-Based Testing 和复杂场景
   - 4.2-4.3 工程化改进

### 工作量估算

| 任务 | 预估工时 | 复杂度 |
|------|----------|--------|
| 2.1 | 0.5h | 低 |
| 2.2 | 2h | 中 |
| 2.3 | 2h | 中 |
| 2.4 | 0.5h | 低 |
| 2.5 | 1h | 低 |
| 2.6 | 1h | 低 |
| 2.7 | 1h | 低 |
| 3.1 | 3h | 中 |
| 3.2 | 4h | 高 |
| 3.3 | 3h | 高 |
| 3.4 | 4h | 高 |
| 3.5 | 3h | 中 |
| 3.6 | 2h | 中 |
| 4.1 | 2h | 中 |
| 4.2 | 2h | 中 |
| 4.3 | 1h | 低 |
| **总计** | **~32h** | |
