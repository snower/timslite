# Phase 24: 连续索引稀疏 filler 分段

> 目标: 修复设计审查 P0-2。连续模式不再按真实 timestamp 跨度全量物化 filler, 改为 `base_timestamp + segment_capacity + time_step=1` 的稀疏逻辑网格; 大 gap 中间完整 index segment 不创建、不填充, 只在必要边界分段内物化 filler。

## 24.1 问题背景

旧连续索引设计在 `timestamp > latest_written_timestamp` 时按 `(latest + 1)..timestamp` 逐条创建 filler。真实业务时间戳通常以秒为单位, 写入间隔可能跨小时、天甚至更久; 若按真实时间跨度全量物化, 单次写入的 CPU、内存、磁盘访问都会随 gap 线性增长, 与高性能时序存储目标冲突。

仅在 flush 后删除纯 filler segment 不能解决该问题, 因为写入路径已经付出了循环、buffer、segment 创建和 mmap 写入成本。

## 24.2 设计结论

连续模式保持 O(1) 定位, 但引入两类缺失表示:

| 类型 | 表示 | 是否占磁盘 | 读/查行为 | 回填行为 |
|------|------|------------|-----------|----------|
| 已物化 filler | `block_offset = u64::MAX`, `in_block_offset = u16::MAX` | 是 | 返回 None 或跳过 | 覆盖为真实 entry |
| 逻辑空洞 | index segment 不存在, 或 `entry_index >= wrote_count` | 否 | 返回 None 或跳过 | 按需创建/扩展目标 segment |

坐标规则:

```text
segment_capacity = floor((index_segment_size - INDEX_HEADER_SIZE) / INDEX_ENTRY_SIZE)
time_step        = 1
base_timestamp   = first real write timestamp
segment_ordinal  = floor((ts - base_timestamp) / segment_capacity)
segment_start    = base_timestamp + segment_ordinal * segment_capacity
entry_index      = ts - segment_start
```

## 24.3 写入规则

1. 第一次真实写入: 持久化 `base_timestamp = timestamp`, 写入真实 entry, 不补 filler。
2. 同分段正序写入: 从上一个存在的写入 timestamp + 1 到当前 timestamp - 1 物化 filler。
3. 跨分段正序写入: 只物化上一个写入所在分段尾部和当前写入所在分段前缀; 中间完整分段保持逻辑空洞。
4. 回填写入:
   - 已有真实 entry: 追加新数据并覆盖索引, 旧数据段 `invalid_record_count += 1`
   - 已物化 filler: 覆盖为真实 entry, `invalid_record_count` 不变
   - 逻辑空洞: 按需创建目标分段, 物化该分段内必要前缀, 写入真实 entry
   - `timestamp < base_timestamp`: 返回错误

一次正序写入最坏 filler 访问量 `< 2 * segment_capacity - 2`, 不随 `timestamp - latest_written_timestamp` 增长。

## 24.4 文档更新

- [x] `docs/design/index-continuous.md`: 重写为稀疏连续索引设计。
- [x] `docs/design/time-index.md`: 补充 `base_timestamp`、`time_step`、逻辑空洞和稀疏分段 API。
- [x] `docs/design/dataset-operations.md`: 更新正序写、乱序回填、读取、删除的逻辑空洞语义。
- [x] `docs/design/query-iterator.md`: 明确逻辑空洞不生成查询 source。
- [x] `design.md` / `docs/design/architecture.md`: 更新索引说明。
- [x] `docs/review/design-review-todo.md`: 将 P0-2 标记为处理中, 记录文档已调整、代码待实现。

## 24.5 实现待办

- [ ] `TimeIndex` 持久化并加载 `base_timestamp`。
- [ ] 新增连续模式 `segment_start_for(timestamp)` 逻辑, 文件名使用逻辑分段起点。
- [ ] 替换 `DataSet::write` 中全量 `(latest+1)..timestamp` filler 循环。
- [ ] 实现跨分段写入时只填上段尾部和当前段前缀。
- [ ] 实现逻辑空洞回填: 按需创建目标 index segment 并物化必要前缀。
- [ ] 更新 `find_entry` / `find_and_delete_entry` / `query_range_indices` 对缺失 segment 和 `entry_index >= wrote_count` 返回 None/skip。
- [ ] 调整 reopen 恢复逻辑: 读取 `base_timestamp`, 从已物化 entry 恢复 `latest_written_timestamp`。
- [ ] 保留 `remove_pure_filler_segments()` 作为兼容清理, 不再依赖它处理大 gap。

## 24.6 测试计划

- [ ] first write 不填充从 0 或 epoch 到首个 timestamp 的 filler。
- [ ] 同分段 gap 仍产生必要 filler, `read/query` 跳过 filler。
- [ ] 跨多个分段的大 gap 只创建前后两个边界分段, 中间分段文件不存在。
- [ ] 大 gap 单次写入 filler 访问量小于两个 index segment 容量。
- [ ] 回填中间逻辑空洞时只创建目标分段并返回正确数据。
- [ ] reopen 后 `base_timestamp`、segment 路由、latest timestamp 恢复一致。
- [ ] retention 删除老分段后不破坏 `base_timestamp` 与新分段路由。

## 24.7 验收标准

- [ ] 设计审查 P0-2 对应代码实现完成。
- [ ] `cargo fmt -- --check` 通过。
- [ ] `cargo clippy --all-targets -- -D warnings` 通过。
- [ ] `cargo test -- --test-threads=1` 通过。
