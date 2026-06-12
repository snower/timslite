# timslite 设计审查 TODO（第 6 轮）

来源: [docs/review/design-review.md](design-review.md)

创建日期: 2026-06-12

## 状态说明

| 标记 | 含义 |
|------|------|
| `[ ]` | 未开始 |
| `[~]` | 处理中或部分完成 |
| `[x]` | 已完成并验证 |
| `[!]` | 暂缓或需重新决策 |

处理原则:

- 本 TODO 用于追踪第 6 轮设计审查中发现的问题修复过程。
- 每个任务完成时，需要同步更新相关设计文档、必要的实现/测试，以及本文档的处理记录。
- 涉及行为或文件格式契约变化时，需同步 `design.md`、对应 `docs/design/*.md`、`plan.md` 或对应 `docs/plan/*.md` checklist。
- 完成任务后先不要 git commit，等待审核确认。

## P0: 必须优先处理

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [x] | P0-1 | 闭合 Queue 状态文件持久化与新版 dirty flush queue 的契约冲突 | `docs/design/background-and-cache.md`, `docs/design/queue-state-file.md`, `docs/design/queue-overview.md`, `src/bg/mod.rs`, `src/dataset.rs`, `src/queue/mod.rs`, queue/background tests | 明确 queue state 是否是一等 flush target；若是，poll/ack/timeout cleanup 后能入队并由后台 flush 周期落盘；若不是，文档明确只在显式 flush/close 等路径同步；测试覆盖无 data/index dirty 时 queue ack/pending 状态的持久化语义 |
| [x] | P0-2 | 定义并实现 `retention_window: u64` 与 `i64` timestamp 的安全计算边界 | `docs/design/meta-format.md`, `docs/design/dataset-operations.md`, `docs/design/data-model.md`, `src/config.rs`, `src/meta.rs`, `src/dataset.rs`, `tests/config_test.rs`, retention/background tests | 明确 `retention_window > i64::MAX` 的语义；builder/meta/FFI/open/create 与阈值计算一致；`u64::MAX` 不会因 cast wrap 导致错误过期或错误物理回收；新增或调整边界测试 |
| [x] | P0-3 | 收敛 on-disk header/meta active contract，移除旧格式常量漂移 | `docs/design/data-model.md`, `docs/design/data-segment.md`, `docs/design/meta-format.md`, `docs/design/design-decisions.md`, `docs/design/architecture.md`, `src/header.rs`, `src/meta.rs` | 当前真源唯一且一致：`DATA_HEADER_SIZE=124`、`INDEX_HEADER_SIZE=128`、segment `file_size:u64`、dataset/segment `compress_type:u8`、dataset meta TLV length 当前口径一致；旧草案字段或常量被删除或明确标记为历史不可实现 |

## P1: 应尽快处理

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [ ] | P1-1 | 统一 Queue `processed_ts` 与 gap/filler 跳过语义 | `docs/design/queue-overview.md`, `docs/design/queue-state-file.md`, `src/queue/mod.rs`, queue tests | 明确消费进度代表“连续逻辑水位”还是“最后已 ack 真实记录”；gap/filler 是否需要持久 skip 状态有明确设计；稀疏记录 poll/ack/reopen 行为测试覆盖 |
| [ ] | P1-2 | 将压缩文档从 deflate-only 改为 selected algorithm active contract | `docs/design/compression.md`, `docs/design/dataset-operations.md`, `docs/design/data-model.md`, `docs/design/store-and-ffi.md`, `design.md`, `docs/design/architecture.md` | 通用流程不再写死 deflate；zstd 默认与 deflate 支持的 `compress_type` 规则位于压缩文档前部；读取使用 segment header `compress_type` 的规则清晰；level 语义按算法说明 |
| [ ] | P1-3 | 明确 public timestamp 契约与 `0`/`-1` sentinel 关系 | `docs/design/data-model.md`, `docs/design/dataset-operations.md`, `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `docs/design/index-continuous.md`, `docs/design/dataset-inspect.md`, FFI/Python docs/tests | 决定 public API 是否只允许 `timestamp > 0`；若是，负 timestamp 仅作为格式层能力保留；若否，提供替代 latest API 并移除冲突 sentinel；inspect 空值表达与 timestamp 契约一致 |
| [ ] | P1-4 | 统一 append 已有 latest record 的迁移阈值/返回错误契约 | `AGENTS.md`, `docs/design/data-model.md`, `docs/design/data-segment.md`, `docs/design/dataset-operations.md`, `docs/review/archives/Round5/test-review-todo.md`, append/cache/journal tests | 明确 active contract 是“不迁移，超出 pending block 返回错误”还是“迁移到 single-record block”；若保留迁移，补齐 journal/cache/queue/fallback 设计；若废弃迁移，清理 70% threshold 残留 |
| [ ] | P1-5 | 固化 FFI 轻量读接口 ABI，尤其 `query_length` 返回布局 | `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `include/timslite.h`, `src/ffi.rs`, FFI/Python wrapper tests | `tmsl_dataset_query_length` 只有一个 C ABI 签名和内存布局；如返回 `(timestamp, data_len)` 数组，需要 `repr(C)`/C struct、alignment、`array_len` 和释放规则明确；旧 `uint32_t*` 口径清除或改名 |
| [ ] | P1-6 | 明确 `query_exist` retention 语义和范围上界 | `docs/design/dataset-read-operations.md`, `docs/design/dataset-operations.md`, `src/dataset.rs`, `src/ffi.rs`, read operations tests | `read_exist/query_exist` 表示“索引物理存在”还是“当前可见存在”有统一定义；range 计算使用 checked arithmetic；超大 bitmap 有明确错误上限；retention/deleted/filler 测试覆盖 |
| [ ] | P1-7 | 统一 flush 默认值与 durability 口径 | `docs/design/background-and-cache.md`, `docs/design/dataset-operations.md`, `docs/design/queue-state-file.md`, `docs/design/memory-and-concurrency.md`, `docs/design/design-decisions.md`, `src/config.rs` | 所有设计文档统一当前默认 flush interval；若 10min 是历史值或建议配置，明确标注；queue state 持久化窗口描述与 P0-1 决策一致 |

## P2: 可以随后优化

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [ ] | P2-1 | 统一 `read_length`/`query_length` 读取 record header 的 8B/12B 描述 | `docs/design/dataset-read-operations.md`, `docs/design/data-model.md`, `src/segment/*` 如需 | record header 统一为 12 bytes；若实现只读 `data_len`，文档说明 timestamp 校验场景仍需读取完整 header |
| [ ] | P2-2 | 补齐 `DataSetInspect` sentinel/nullability 设计 | `docs/design/dataset-inspect.md`, `docs/design/store-and-ffi.md`, `include/timslite.h`, `src/ffi.rs`, inspect tests | FFI inspect 不再依赖含糊的 0 sentinel，或明确 0 不可能为业务 timestamp；必要时增加 `has_*` flag |
| [ ] | P2-3 | 清理 `design.md` 与架构模块说明中的 deflate-only 残留 | `design.md`, `docs/design/architecture.md`, `docs/design/compression.md` | 入口索引和模块说明统一为 zstd default / deflate supported / selected algorithm 口径 |
| [ ] | P2-4 | 补齐 Queue state file `open_existing` 格式校验清单 | `docs/design/queue-state-file.md`, `src/queue/mod.rs`, queue negative tests | 文档和实现校验文件长度、`state_length`、`pending_value_size`、`pending_length` 上限、entry 越界、status 合法值、timestamp 排序/去重要求 |

## 建议处理顺序

1. P0-1: 先决定 queue state 是否进入 dirty flush queue；这会影响 P1-7 的 flush 口径。
2. P0-2: 再处理 retention 边界，避免后续测试继续固化 `u64::MAX` 的未定义行为。
3. P0-3: 收敛文件格式 active contract，给后续压缩/FFI/inspect 修复一个稳定真源。
4. P1-1、P1-3、P1-4: 依次处理 public 行为语义，避免 wrapper 和 queue/journal 测试继续漂移。
5. P1-5、P1-6、P1-7 和 P2 项: 做 ABI、读操作、默认值和文档一致性收尾。

## 处理记录

| 日期 | ID | 状态 | 处理摘要 | 验证 |
|------|----|------|----------|------|
| 2026-06-12 | ALL | [ ] | 根据第 6 轮设计审查报告创建 TODO 跟踪文件，尚未开始修复 | `docs/review/design-review.md` 已读回；待后续逐项处理 |
| 2026-06-12 | P0-1/P0-2/P0-3 | [x] | 更新相关设计文档；引入 `SegmentFlushTarget::QueueState { group_name }` 并让 poll/ack 入队、后台按 target flush；将 `retention_window` 有效上限固定为 `i64::MAX` 并接入 builder/meta/create/open/FFI；收敛旧 header/meta 常量与字段描述 | `cargo test -- --test-threads=1` 通过 |

## 完成统计

| 优先级 | 总数 | 已完成 | 未完成 |
|--------|------|--------|--------|
| P0 | 3 | 3 | 0 |
| P1 | 7 | 0 | 7 |
| P2 | 4 | 0 | 4 |
| 合计 | 14 | 3 | 11 |
