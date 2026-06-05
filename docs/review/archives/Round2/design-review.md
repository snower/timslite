# Design Review Round 2

日期: 2026-06-04

范围: 本轮仅审查 `design.md` 与 `docs/design/*.md` 中的设计描述, 未修改任何设计文档或代码实现。第一轮 review 已归档, 本文件记录第二轮发现的问题、风险与优化建议。

## 总体结论

本轮未发现必须立即推翻整体架构的 P0 级问题。当前设计的主线仍然清晰: mmap-backed segment、Block 聚合、延迟压缩、时间索引、Store/FFI 门面、journal/queue 扩展。但最近新增的 `append`、journal、queue、read-only `.journal`、retention 与可变 header 设计穿过多个子系统, 部分文档未完全闭合, 主要风险集中在:

1. 同一字段或 API 在不同文档中存在坐标、单位或顺序语义不一致。
2. journal 被定位为热迁移/恢复工具基础, 但记录格式仍是 pointer/change-log, 不是自包含 redo log。
3. `append` 新语义尚未完整串到 queue 通知、只读限制、journal 边界和 single_record flag 命名中。
4. 目录名合法性规则只覆盖 dataset name/type, 没有覆盖 queue consumer group。

## P1 Findings

### P1-1: `retention_ms` 单位语义仍不一致, 且后台文档残留非饱和减法

证据:

- `docs/design/meta-format.md:54` 将 TLV `0x08 retention_ms` 描述为 "数据有效期 (毫秒, 0=不限)"。
- `docs/design/meta-format.md:71` 又写成 "与 timestamp 同单位"。
- `docs/design/dataset-operations.md:574-576` 说明 `retention_ms` 的单位与 timestamp 相同, 通常为毫秒。
- `docs/design/store-and-ffi.md:341` 示例按毫秒传入 `30 * 86400 * 1000`。
- `docs/design/background-and-cache.md:136` 与 `:153` 仍写 `latest_written_timestamp - retention_ms`, 而 `docs/design/dataset-operations.md:581-586` 已要求 `saturating_sub`。

影响:

如果业务 timestamp 采用秒级, 但调用方按字段名或 FFI 示例传入毫秒级 retention, 过期窗口会放大 1000 倍; 反之则可能过早删除数据。后台设计若按普通减法理解, 在 `latest < retention` 时还会产生下溢风险, 与读写路径的 `saturating_sub` 语义冲突。

建议:

统一字段名与单位语义。二选一:

- 若 retention 必须跟随 timestamp 单位, 建议把文档中的 "ms/毫秒" 全部改为 "timestamp unit", 并考虑后续 API 名称避免 `retention_ms` 误导。
- 若 retention 固定毫秒, 则必须明确 timestamp 也必须是 unix ms, 或在 retention 计算处做单位转换。

后台文档中所有阈值计算应统一写为 `latest_written_timestamp.saturating_sub(retention_ms)`。

### P1-2: `wrote_position` 在 data-model 与 data-segment 中坐标系冲突

证据:

- `docs/design/data-model.md:190` 定义 data segment state 的 `wrote_position` 为 "文件内绝对偏移, 含动态 header_len"。
- `docs/design/data-model.md:255` 再次强调 `wrote_position >= header_len`。
- `docs/design/data-segment.md:62` 的 `DataSegment.wrote_position` 却是 "从 header_len 起算的数据区内已用字节数"。
- `docs/design/dataset-operations.md:371` 判断 append tail 时使用 `(block_offset - segment.file_offset) + ... == segment.wrote_position`, 这是数据区相对坐标。

影响:

可变 header 后, `block_offset` 是数据区逻辑 offset, 这一点已经统一。但 `wrote_position` 仍有 "文件内绝对偏移" 与 "数据区相对已用字节数" 两种解释。append/correction 的 tail 校验、segment full 判断、持久化 state 恢复都依赖该字段。如果实现者按不同文档编写, 会出现 off-by-header_len, 轻则拒绝合法 append, 重则把新 block 写到错误位置。

建议:

明确拆分两个名称:

- on-disk state 若保存文件内绝对偏移, 命名为 `file_wrote_position`, 校验 `header_len <= file_wrote_position <= file_len`。
- runtime 若保存数据区相对位置, 命名为 `data_wrote_position`, 转换公式为 `data_wrote_position = file_wrote_position - header_len`。

append/correction 文档中应明确使用哪一个坐标, 避免直接写 `segment.wrote_position`。

### P1-3: Journal 记录格式不足以支撑自包含热迁移/故障恢复

证据:

- `docs/design/journal.md:7` 将 Journal 定位为可服务于热迁移、增量同步、审计和故障恢复工具。
- `docs/design/journal.md:173-200` 的 `0x11` 写入记录只包含 name/type 和 18 字节 `index_info`, 不包含实际 payload、payload length、checksum 或 record version。
- `docs/design/journal.md:227-264` 的 `0x13 append` 只包含最终 `index_info` 与本次 append 的 `data_offset/data_len`, 不包含 append bytes, append 迁移时也不包含迁移后的完整 record。
- `docs/design/journal.md:477-479` 又说明恢复工具不能假设 journal 覆盖所有成功操作, 严格恢复需另行引入 WAL/commit marker/checksum。

影响:

当前 journal 更像 "操作索引指针日志", 不是自包含 redo/change-data log。热迁移消费者若只拿到 journal record, 仍必须能在源 dataset 中按 `index_info` 读取业务数据; 如果源数据已被 retention 回收、被后续 correction/append 迁移覆盖、或者源机器不可达, 则无法仅靠 journal 重放。`0x13` append 原地追加还存在另一个歧义: 消费端如果已经处理过旧 record, 新 journal 只告诉 offset/len, 但不携带新增字节, 需要再次读源 record 才能补齐。

建议:

明确选择:

- 如果 Journal v1 只承诺 pointer-based change log, 文档应把 "热迁移/故障恢复" 降级为 "源 dataset 可访问且未被 retention/checkpoint 清除时的辅助日志", 并定义 consumer 必须通过 `read_entry_at_index` 或类似 API 拉取数据。
- 如果目标是跨进程/跨机器可靠迁移, `0x11/0x13` 至少需要包含数据 payload 或 payload digest + 可稳定读取的 record version/checksum; append record 还需说明是传增量 bytes 还是传完整 record snapshot。

### P1-4: `append` 没有完整串入普通 queue 的通知与消费语义

证据:

- `docs/design/queue-overview.md:208-221` 只规定 `dataset.write()` 在正常写入 (`timestamp > old_latest`) 成功后触发 consumer 通知, correction/out-of-order 不通知。
- `docs/design/dataset-operations.md:321-325` 规定 `append(timestamp > latest)` 会复用正常正序 write 路径创建新 record 并推进 latest。
- `docs/design/dataset-operations.md:354-364` 规定 `append(timestamp == latest)` 可原地增长最新 record, latest 不变。
- `docs/design/journal.md:344` 规定 append 创建新 record 也写 `0x13`, 不写 `0x11`。

影响:

普通 DatasetQueue 的生产模型是按 `latest_written_timestamp` 连续消费。新增 append 后至少有三种结果:

- `timestamp > latest`: 创建新 record, 理应像正常写入一样唤醒 queue consumer。
- `timestamp == latest` 原地追加: 已经 poll/ack 过该 timestamp 的 consumer 是否需要再次看到更新, 未定义。
- `timestamp == latest` 迁移: 同一 timestamp 的数据位置变化, 是否通知或如何避免重复/漏处理, 未定义。

如果实现只在 `write()` hook 通知, `append(timestamp > latest)` 可能因外部语义是 append 而漏通知; 如果所有 append 都通知, 已消费的 timestamp 可能不会被 queue 再次分配, 通知也没有效果。

建议:

为普通 queue 增补 append 专属语义:

- append 创建新 timestamp: 必须 notify, 与 normal write 等价。
- append 修改已有 latest: 默认不重新投递, 或引入 update/revision 机制; 二者必须明确。
- journal queue 可继续以 journal sequence 作为独立递增 timestamp, 每条 `0x13` 都投递。

### P1-5: `.journal/logs` 只读限制遗漏 `append`

证据:

- `docs/design/architecture.md:74` 说 public API 可受控打开 `.journal/logs` read/query/open_queue, 但不能 create/write/delete/drop。
- `docs/design/store-and-ffi.md:77` 同样写 public create/write/delete/drop 必须拒绝 `.journal`, 未列出 append。
- `docs/design/journal.md:407-408` 的允许/禁止清单中允许 read/query/query_iter/latest/open_queue/close, 禁止 write/delete/drop_dataset/create_dataset, 也未列出 append。
- `docs/design/store-and-ffi.md:293-294` 已新增 `tmsl_dataset_append`。

影响:

`.journal/logs` 的 public handle 是只读内部 dataset。新增 append API 后, 如果文档和实现没有把 append 纳入只读拒绝路径, 外部可以通过 `tmsl_dataset_append` 或 Rust handle 修改 journal dataset, 破坏 journal sequence 连续性和审计可信度。

建议:

所有 read-only/internal dataset 的禁止操作统一列为 create/write/append/delete/drop/push, 并在 FFI 层、Store 门面层、DataSet handle 层都说明检查点。

### P1-6: Queue consumer `group_name` 缺少路径合法性规则

证据:

- `docs/design/queue-overview.md:25-27` 和 `docs/design/queue-state-file.md:7-9` 直接使用 `{group_name}` 作为 `queue/{group_name}` 状态文件名。
- `docs/design/queue-overview.md:52-55` 暴露 `open_consumer(group_name)` 与 `drop_consumer(group_name)`。
- `docs/design/store-and-ffi.md:66-75` 与 `docs/design/architecture.md:72` 只定义 dataset name/type 的合法字符规则, 没有覆盖 queue group_name。

影响:

消费组名如果不限制, 会引入路径穿越、Windows 保留名、控制字符、超长路径等问题。该风险与 dataset name/type 完全同类, 但 queue 状态文件也持久化在数据目录内, 所以不能默认安全。

建议:

复用 dataset name/type 的规则: 非空, 整体匹配 `^[0-9A-Za-z_-]+$`, 禁止 `.`, `..`, `/`, `\`, 空白、控制字符、非 ASCII 和 Windows 保留路径。所有 queue open/drop/FFI 入口在拼接路径前校验。

### P1-7: 空 append no-op 与核心 append 顺序契约冲突

证据:

- 用户层核心语义是 `timestamp < latest_written_timestamp` 时 append 返回错误。
- `docs/design/dataset-operations.md:313-319` 当前流程先检查 `data.len() == 0 -> Ok(())`, 再检查 `timestamp < latest_written_timestamp -> Error`。
- `docs/design/dataset-operations.md:369` 又强调 `timestamp < latest_written_timestamp` 不回退为乱序写入。

影响:

按当前分支顺序, `append(old_timestamp, empty)` 会返回成功, 而非 "append timestamp is older than latest"。这会让调用方无法用 append 的返回值判断请求是否违反顺序约束, 也会让 queue/journal 语义出现例外: 这是一次被接受但无记录、无 journal、无 latest 推进的旧 timestamp 操作。

建议:

先执行 timestamp 顺序/retention 校验, 再对空 data 做 no-op。或者明确声明空 append 是全局 no-op, 不参与顺序语义; 但这会削弱 API 一致性, 不建议。

## P2 Findings

### P2-1: `single_record` flag 的含义与 append 70% 迁移后不再等同于 "超大 record"

证据:

- `docs/design/data-model.md:20` 说单条 record 编码后超过 64KB 才独占 Block。
- `docs/design/data-model.md:24` 又说 append 后 record 编码超过 `BLOCK_MAX_SIZE * 70 / 100` 即迁移为独占 block, 当前约 45875 字节, 低于 64KB。
- `docs/design/data-model.md:51`、`docs/design/compression.md:56` 将 `single_record` 注释为 "独占 record 的超大 block"。
- `docs/design/compression.md:10` 与 `:31-32` 仍把独占 block 归到 "超大 record"。

影响:

append 迁移后, `SINGLE_RECORD` 可能表示 "低于 64KB 但为了避免普通聚合 block 继续增长而独占存放"。如果实现或后续工具仍把该 flag 理解为 "payload 必然大于 64KB", 可能出现错误校验、错误统计或错误压缩路径。

建议:

把 flag 语义改为 "exclusive/single-record block", 与大小脱钩。文档中分别说明两种来源:

- write 时 record 编码超过 64KB。
- append 后 record 编码超过 70% 迁移阈值。

### P2-2: Store Rust API 列表不完整, append 被列出但 write/delete/read/query 等同级操作缺失

证据:

- `docs/design/store-and-ffi.md:36-47` 的 `impl Store` 只列出 open/create/open_dataset/open_journal_queue/append_dataset/close/drop/close/tick, 没列出 write/read/query/delete 或 queue API。
- `docs/design/store-and-ffi.md:291-297` 的 FFI 层却列出了 write/append/delete。
- `docs/design/journal.md:331-344` 以 `Store::append_dataset` 作为 append journal hook 入口。

影响:

Journal 设计要求 Store 门面负责 hook, 但 Store API 文档没有完整列出所有需要 hook 的业务操作。读者难以判断 Rust public API 的权威入口到底是 Store 还是 DataSet handle, 也容易造成 FFI 通过 DataSet handle 写入而绕过 Store hook 的误解。

建议:

补齐 Store 层操作表, 明确每个 public write-like API 的 journal/cache/queue 责任:

- `write_dataset`
- `append_dataset`
- `delete_dataset_record`
- `read_dataset`
- `query_dataset/query_iter`
- `open_dataset_queue/open_journal_queue`

同时说明 FFI 的 dataset handle 内部是否携带 Store context, 以保证 write/append/delete 都能访问 journal 与 global cache。

### P2-3: 直接持有 `DataSet` 的 public 边界与 journal 完整性关系仍偏模糊

证据:

- `docs/design/dataset-operations.md:198` 写明绕过 Store 直接调用 `DataSet::write` 默认不具备 journal 语义, 除非显式传入 journal sink。
- `docs/design/journal.md:326` 同样说明直接持有 `DataSet` 调用 `write` 默认不写 journal。
- `docs/design/journal.md:523-525` 又要求 Store/DataSet hook 的窄接口覆盖 create/drop/write/delete/append, 且 Journal 不放入 DataSet 本体。

影响:

如果 `DataSet` 是公开 Rust API, 或 Python/Go/C 包装层直接持有 DataSet 并暴露 write/append/delete, 那么启用 journal 后仍可能出现部分 public 写入不入 journal 的情况。对于 "journal 可用于热迁移/审计" 的目标, 这会形成不可见写入来源。

建议:

明确 public write surface:

- 要么所有外部写入必须经过 Store/handle facade, `DataSet` 写接口降为 crate-private/internal。
- 要么 `DataSet` 写接口必须接受 `JournalSink`/`StoreContext`, 并在缺失时明确返回错误或标记为 non-journal internal path。

### P2-4: correction 变长覆盖仍残留 "移动后续字节" 的过宽描述

证据:

- `docs/design/data-segment.md:204` 要求 correction 目标必须是最后 pending raw block 的最末 record。
- `docs/design/data-segment.md:245-248` 前置条件也要求 record 是 block 内最末 record。
- 但 `docs/design/data-segment.md:250` 又写 "若新 data 长度更小, 后续 block 需前移...实现中允许缩小, 只需移动本 block 后的字节"。
- `docs/design/dataset-operations.md:220` 也写 "只需移动后续字节"。

影响:

如果 correction 只允许最新数据段最后 block 的最后 record, 理论上不应存在需要移动的后续 block/record。保留 "移动后续字节" 会误导实现者支持更复杂的中间 record 变长覆盖, 这会牵涉索引重写、缓存失效和 crash 边界, 明显超出当前设计。

建议:

把 correction 变长覆盖限定为 tail-only resize: 不移动任何后续 block; 若发现 record 后仍有字节, 直接返回错误并走回退路径。相关文档删除 "移动后续字节" 说法。

### P2-5: queue 的锁层级说明与 condvar 叙述容易误导

证据:

- `docs/design/queue-overview.md:288-299` 给出严格锁层级: Store -> Dataset -> QueueInner -> ConsumerStateFile -> Condvar。
- `docs/design/queue-overview.md:373-385` 的 poll 流程在无数据时先释放 state, 获取 condvar mutex, 然后释放 dataset lock 进入 wait。
- `docs/design/queue-overview.md:137` 的流程图写 "Condvar 释放锁", 容易被理解为释放 dataset lock, 但实际 Condvar 只关联自己的 guard mutex。

影响:

当前伪代码最终有 `drop(dataset_guard)`, 所以方向是可行的。但锁层级表把 Condvar 放在最内层, 伪代码又在释放 dataset 前获取 condvar mutex, 实现者如果照锁层级严格理解或误解 Condvar 释放的是 dataset mutex, 可能写出死锁或 missed wakeup 更大的实现。

建议:

把 queue wait 协议单独写清:

- 检查数据时持有 dataset + state。
- 无数据时释放 state 和 dataset。
- 再进入 condvar wait。
- wake 后重新获取 dataset/state 并重查。

并说明 condvar mutex 只保护通知 flag, 不属于 dataset/state 锁层级。

### P2-6: Journal TLV `u16 length` 与名称/meta 上限缺少统一校验说明

证据:

- `docs/design/journal.md:100-133` 定义 record length 与 TLV length 均为 2 字节。
- `docs/design/store-and-ffi.md:66-75` 限制 dataset name/type 字符集合, 但没有定义最大长度。
- `docs/design/journal.md:135-171` 的 create/drop journal 记录会写入 name/type 和 meta values。

影响:

`u16 length` 最大 65535 字节。虽然实际 name/type/meta 通常远小于这个值, 但缺少上限会让 encoder 在极端路径上只能临时失败, 或让 dataset 目录名先通过字符校验但在 journal 编码时失败。create/drop 已经完成主操作后再写 journal, 这会增加 "主操作成功但 journal 缺失" 的概率。

建议:

为 dataset name/type、queue group_name、metadata snapshot 定义明确最大长度, 并在主操作前校验 journal 可编码性。若 journal disabled 可放宽, 但 enabled 时应保证成功主操作不会因可预见的长度问题无法写 journal。

## 后续建议

建议后续按以下顺序处理:

1. 先统一基础契约: retention 单位、`wrote_position` 坐标、read-only `.journal` 禁止 append、queue group_name 校验。
2. 再闭合 append 穿透语义: 空 append 分支顺序、queue notify 策略、single_record/exclusive block 命名。
3. 最后决定 journal v1 的产品边界: 明确 pointer-based 辅助日志, 或升级为携带 payload/checksum 的 change-data log。

本轮审查只保存问题和建议, 未对 `docs/design` 或 `src` 做任何调整。
