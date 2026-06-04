# Design Review Round 2 TODO

来源: `docs/review/design-review.md`

创建日期: 2026-06-04

状态标记:

- `[ ]` 未完成
- `[x]` 已完成

维护规则:

1. 每处理一项, 先更新相关 `docs/design/*.md` 设计文档。
2. 若该项涉及实现, 再完成 `src/`、`include/`、wrapper 或测试调整。
3. 验证后将本文件对应任务改为 `[x]`, 并在"处理记录"中写明完成日期、主要文件和验证命令。

## P1 Tasks

| 状态 | ID | 任务 | 主要范围 | 完成标准 |
|------|----|------|----------|----------|
| [x] | P1-1 | 统一 `retention_window` 单位语义, 并消除后台 retention 非饱和减法描述/实现风险 | `meta-format.md`, `dataset-operations.md`, `background-and-cache.md`, `store-and-ffi.md`, config/meta/bg/retention 代码 | 文档只保留 timestamp unit 语义; 所有阈值计算统一为饱和/安全减法; 读/write/delete/append/retention 行为测试覆盖 |
| [x] | P1-2 | 明确 `wrote_position` 的 on-disk 与 runtime 坐标系 | `data-model.md`, `data-segment.md`, `dataset-operations.md`, segment/header/append/correction 代码 | 文件内绝对偏移与数据区相对偏移命名清晰; append/correction tail 校验使用同一坐标; 可变 header 场景有验证 |
| [x] | P1-3 | 重新界定 Journal v1 热迁移/恢复边界, 或升级日志格式为可自包含变更数据 | `journal.md`, queue/journal API, journal encoder/decoder, migration/recovery docs/tests | 明确 journal 是 pointer-based 辅助日志还是 payload-bearing change log; append/write/delete 记录格式和 consumer 读取路径无歧义 |
| [x] | P1-4 | 补齐 `append` 与普通 DatasetQueue 的通知和消费语义 | `queue-overview.md`, `dataset-operations.md`, queue/write/append hook 代码 | `append(ts > latest)` 是否通知明确且实现一致; `append(ts == latest)` 原地追加/迁移是否重新投递明确; journal queue 与普通 queue 行为分离 |
| [x] | P1-5 | `.journal/logs` 只读 public handle 禁止 `append` | `architecture.md`, `store-and-ffi.md`, `journal.md`, Store/FFI/DataSet handle 代码 | read-only/internal dataset 拒绝 create/write/append/delete/drop/push; FFI 与 Rust API 都有保护和测试 |
| [x] | P1-6 | 为 Queue consumer `group_name` 增加合法性和路径安全规则 | `queue-overview.md`, `queue-state-file.md`, queue API/FFI 代码 | `group_name` 非空且匹配 `^[0-9A-Za-z_-]+$`; 禁止路径穿越/控制字符/非 ASCII/Windows 保留路径; open/drop consumer 测试覆盖 |
| [ ] | P1-7 | 调整空 append no-op 的校验顺序, 避免绕过 timestamp 顺序契约 | `dataset-operations.md`, append 代码/tests, journal/queue hook | `timestamp <= 0`、`timestamp < latest`、retention 等契约先校验; 合法空 append 才 no-op; 非法旧 timestamp 空 append 返回错误 |

## P2 Tasks

| 状态 | ID | 任务 | 主要范围 | 完成标准 |
|------|----|------|----------|----------|
| [ ] | P2-1 | 将 `single_record` 语义从 "超大 record" 调整为 "exclusive/single-record block" | `data-model.md`, `compression.md`, `design-decisions.md`, `dataset-operations.md`, block flag docs/code comments | 文档说明 single-record block 可由 >64KB write 或 append 70% 迁移产生; 校验逻辑不假设其一定大于 64KB |
| [ ] | P2-2 | 补齐 Store Rust API 文档, 明确 write/read/query/delete/append/queue 的门面职责 | `store-and-ffi.md`, `journal.md`, public API docs | Store 与 FFI 操作表完整; 每个 write-like API 的 journal/cache/queue 责任清晰 |
| [ ] | P2-3 | 明确直接持有 `DataSet` 的 public 边界与 journal 完整性关系 | `dataset-operations.md`, `journal.md`, Rust/Python/FFI API docs/code | 外部写入是否必须经 Store/handle facade 有明确规则; 直接 DataSet 写入不会静默绕过 journal 或被标记为 internal |
| [ ] | P2-4 | 收窄 correction 变长覆盖描述, 删除 "移动后续字节" 的过宽语义 | `data-segment.md`, `dataset-operations.md`, correction 代码/tests | correction 仅支持 tail-only resize; 若 record 后还有字节则返回错误/回退; 文档不再暗示移动后续 block/record |
| [ ] | P2-5 | 澄清 queue 锁层级与 Condvar wait 协议 | `queue-overview.md`, queue 并发代码/tests | wait 前释放 dataset/state 的顺序明确; Condvar mutex 只保护通知 flag; 文档与实现无死锁/missed wakeup 误导 |
| [ ] | P2-6 | 为 Journal TLV `u16 length` 相关字段定义最大长度和预校验 | `journal.md`, `store-and-ffi.md`, `meta-format.md`, journal encoder/decoder, create/drop path | dataset name/type/meta snapshot/group name 等可编码长度在主操作前校验; journal enabled 时避免可预见的编码失败 |

## 处理记录

| 日期 | ID | 状态 | 说明 | 验证 |
|------|----|------|------|------|
| 2026-06-04 | P1-1 | 已完成 | retention 统一为 timestamp unit 语义, 最终命名为 `retention_window`; 后台阈值文档统一为 `saturating_sub`; 移除旧的单位误导和过渡兼容命名 | `cargo test test_dataset_config_builder_retention_window -- --test-threads=1`; `cargo test -- --test-threads=1` |
| 2026-06-04 | P1-2 | 已完成 | 数据段 header state `wrote_position` 明确保存文件内绝对偏移; 运行时字段改为 `data_wrote_position`, 持久化时写入 `header_size + data_wrote_position` | `cargo test test_header_wrote_position_is_absolute_and_runtime_is_data_relative -- --test-threads=1`; `cargo test -- --test-threads=1` |
| 2026-06-04 | P1-3 | 已完成 | Journal v1 明确降级为 pointer-based 辅助日志; consumer 必须通过源 dataset 的 `read_entry_at_index(index_info)` 读取业务数据, 不承诺自包含 redo | 文档检查: `docs/design/journal.md` |
| 2026-06-04 | P1-4 | 已完成 | append 创建新 timestamp 时普通 queue notify; append 修改已有 latest 不重新投递也不 notify; journal queue 继续按 journal sequence 投递每条 `0x13` | `cargo test test_append_notifies_queue_only_when_creating_new_timestamp -- --test-threads=1` |
| 2026-06-04 | P1-5 | 已完成 | `.journal/logs` read-only public handle 的禁止操作补齐 `append`/`queue_push`; Store/FFI 经 Store append 路径保持拒绝 | `cargo test t28_9_public_journal_handle_rejects_append -- --test-threads=1` |
| 2026-06-04 | P1-6 | 已完成 | Queue consumer `group_name` 复用 dataset 路径安全字符集, open/drop consumer 拼路径前校验 | `cargo test t27_2_4_consumer_group_name_must_be_path_safe -- --test-threads=1` |
| 2026-06-04 | ALL | 未完成 | 基于第二轮 design review 创建追踪清单, 后续仍需处理未完成项 | `rg -n "^### P[0-9]-" docs/review/design-review.md` |
