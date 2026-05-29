# timslite 设计审查待办追踪

创建日期: 2026-05-29

来源: [`docs/review/design-review.md`](design-review.md)

状态说明:
- `[ ]` 未完成
- `[~]` 处理中
- `[x]` 已完成
- `[-]` 不处理/延期

更新规则:
- 每处理一个事项, 更新对应复选框和“处理记录”。
- 若修改了设计文档、计划文档或源码, 在处理记录中写明相关文件。
- 完成状态应以实际文档更新、代码实现或验证结果为准。

## P0 必须优先处理

| 状态 | ID | 事项 | 验收标准 | 处理记录 |
|------|----|------|----------|----------|
| [ ] | P0-1 | 修正超大 record 与 `data_len: u16` 编码冲突 | 明确选择限制 `data_len <= u16::MAX` 或升级/扩展 record 长度编码; 设计文档、常量、校验规则一致 | |
| [ ] | P0-2 | 重设计连续索引 filler 机制 | 连续模式不再按 Unix 秒/毫秒 timestamp 全量物化 filler; 明确 `base_timestamp`、`time_step`、gap 表示和 first write 行为 | |
| [ ] | P0-3 | 统一 Header 可扩展性与固定数据区起点 | 明确采用固定 header 或可变 header; `DATA_HEADER_SIZE`/`INDEX_HEADER_SIZE`、TLV/state 扩展和兼容策略无矛盾 | |
| [ ] | P0-4 | 解决纠正写入与 BlockCache 不可变假设冲突 | 明确 pending/sealed raw/sealed compressed 的可写、可读、可缓存规则; 设计缓存失效或限制纠正写入只作用于 pending block | |
| [ ] | P0-5 | 补齐 crash safety 事务边界 | 明确数据、索引、header 多字段写入顺序; 设计 commit/recovery/checksum 或一致点恢复策略; 修正“最多损失 flush 间隔”的表述 | |

## P1 高优先级

| 状态 | ID | 事项 | 验收标准 | 处理记录 |
|------|----|------|----------|----------|
| [ ] | P1-1 | 统一 `block_offset` 坐标系 | 文档中区分 `segment_file_offset`、`block_global_offset`、`block_segment_offset`、`file_byte_offset`; 给出转换公式 | |
| [ ] | P1-2 | 完整定义 retention 对 read/delete/write/query 的影响 | `read`、`query`、`delete`、乱序写入和过期数据的行为一致; 混合过期/未过期 index segment 有处理策略 | |
| [ ] | P1-3 | 修正后台任务锁顺序设计 | 文档中只有一种锁顺序; 明确 tick/后台线程/Store 操作是否允许交叉调用, 并补充无死锁约束 | |
| [ ] | P1-4 | 补齐 FFI 配置与句柄生命周期设计 | 定义 `StoreConfigFFI`、`tmsl_store_open_with_config`、子句柄失效规则、通用数据释放函数; 同步头文件和设计文档 | |
| [ ] | P1-5 | 收敛 QueryIterator 惰性化目标与现状 | 区分当前实现和目标设计; 若声明 64KB 级内存, 需实现 source cursor 而非全量 `Vec<IndexEntry>` | |
| [ ] | P1-6 | 统一压缩状态机 | 明确 pending overflow、idle-close/recovery、single-record 的压缩和 flag 规则; 说明 idle sealed raw 是否会被后续压缩 | |

## P2 中优先级

| 状态 | ID | 事项 | 验收标准 | 处理记录 |
|------|----|------|----------|----------|
| [ ] | P2-1 | 明确 DataSetMeta open 校验语义 | 明确 meta 是唯一真源还是与 StoreConfig 比较; 说明 `block_max_size` 是否持久化 | |
| [ ] | P2-2 | 明确 retention 调度时区 | `retention_check_hour` 标明 UTC 或 local hour; 若是 local, 给出跨平台实现/降级策略 | |
| [ ] | P2-3 | 增加 dataset name/type 合法性和转义规则 | 明确字符集、长度、路径穿越防护、Windows 保留名处理或编码方案 | |
| [ ] | P2-4 | 补充 compaction 设计或明确不支持 | 若支持, 定义触发阈值、索引重写、并发隔离和 crash recovery; 若不支持, 文档明确 `invalid_record_count` 只统计 | |
| [ ] | P2-5 | 修正文档与构建配置漂移 | `cargo-and-config.md`、`design-decisions.md`、`architecture.md` 与当前 Cargo/module/header 实际状态一致 | |

## 建议处理顺序

1. [ ] 锁定文件格式: record 长度编码、header size 策略、block offset 坐标系。
2. [ ] 锁定一致性模型: 数据/索引写入顺序、pending recovery、cache invalidation、crash recovery。
3. [ ] 重写连续索引设计: 时间粒度、gap 表示、跳段查找、first write 行为。
4. [ ] 补齐 FFI 配置和生命周期: `StoreConfigFFI`、子句柄失效规则、通用 free 函数。
5. [ ] 整理性能设计: 真正惰性的 QueryIterator、retention/compaction、benchmark 文档。

