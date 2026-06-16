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
| [x] | P1-1 | 统一 Queue `processed_ts` 与 gap/filler 跳过语义 | `docs/design/queue-overview.md`, `docs/design/queue-state-file.md`, `src/queue/mod.rs`, queue tests | 明确消费进度代表“最后已按投递顺序完成的真实 timestamp/sequence”；gap/filler 不需要持久 skip 状态；稀疏记录 poll/ack/reopen 行为测试覆盖 |
| [x] | P1-2 | 将压缩文档从 deflate-only 改为 selected algorithm active contract | `docs/design/compression.md`, `docs/design/dataset-operations.md`, `docs/design/data-model.md`, `docs/design/store-and-ffi.md`, `design.md`, `docs/design/architecture.md` | 通用流程不再写死 deflate；zstd 默认与 deflate 支持的 `compress_type` 规则位于压缩文档前部；读取使用 segment header `compress_type` 的规则清晰；level 语义按算法说明 |
| [x] | P1-3 | 明确 public timestamp 契约与 `0`/`-1` sentinel 关系 | `docs/design/data-model.md`, `docs/design/dataset-operations.md`, `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `docs/design/index-continuous.md`, `docs/design/dataset-inspect.md`, FFI/Python docs/tests | public timestamp 保持 signed `i64`，`0` 和负数是合法业务 timestamp；新增 Rust/Store/Python `read_latest()` 与 FFI `tmsl_dataset_read_latest`；`read(-1)`/轻量读接口均改为精确 timestamp；empty latest 使用 `Option<i64>` 或 FFI `has_latest_written_timestamp`/返回码表达 |
| [x] | P1-4 | 统一 append 已有 latest record 的容量错误契约 | `AGENTS.md`, `docs/design/data-model.md`, `docs/design/data-segment.md`, `docs/design/dataset-operations.md`, `docs/review/archives/Round5/test-review-todo.md`, append/cache/journal tests | active contract 明确为迁移已废弃：已有 latest record append 只允许 pending raw tail 原地增长，超出普通 pending block 可承载范围直接返回错误；70% threshold 与旧缓存失效残留已清理 |
| [x] | P1-5 | 固化 FFI 轻量读接口 ABI，尤其 `query_length` 返回布局 | `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `include/timslite.h`, `src/ffi.rs`, FFI/Python wrapper tests | `tmsl_dataset_query_length` 只有一个 C ABI 签名和内存布局；如返回 `(timestamp, data_len)` 数组，需要 `repr(C)`/C struct、alignment、`array_len` 和释放规则明确；旧 `uint32_t*` 口径清除或改名 |
| [x] | P1-6 | 明确 `query_exist` retention 语义和范围上界 | `docs/design/dataset-read-operations.md`, `docs/design/dataset-operations.md`, `src/dataset.rs`, `src/ffi.rs`, read operations tests | `read_exist/query_exist` 表示“索引物理存在”还是“当前可见存在”有统一定义；range 计算使用 checked arithmetic；超大 bitmap 有明确错误上限；retention/deleted/filler 测试覆盖 |
| [x] | P1-7 | 统一 flush 默认值与 durability 口径 | `docs/design/background-and-cache.md`, `docs/design/dataset-operations.md`, `docs/design/queue-state-file.md`, `docs/design/memory-and-concurrency.md`, `docs/design/design-decisions.md`, `src/config.rs` | 所有设计文档统一当前默认 flush interval；若 10min 是历史值或建议配置，明确标注；queue state 持久化窗口描述与 P0-1 决策一致 |

## P2: 可以随后优化

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [ ] | P2-1 | 统一 `read_length`/`query_length` 读取 record header 的 8B/12B 描述 | `docs/design/dataset-read-operations.md`, `docs/design/data-model.md`, `src/segment/*` 如需 | record header 统一为 12 bytes；若实现只读 `data_len`，文档说明 timestamp 校验场景仍需读取完整 header |
| [ ] | P2-2 | 补齐 `DataSetInspect` sentinel/nullability 设计 | `docs/design/dataset-inspect.md`, `docs/design/store-and-ffi.md`, `include/timslite.h`, `src/ffi.rs`, inspect tests | FFI inspect 不再依赖含糊的 0 sentinel，或明确 0 不可能为业务 timestamp；必要时增加 `has_*` flag |
| [x] | P2-3 | 清理 `design.md` 与架构模块说明中的 deflate-only 残留 | `design.md`, `docs/design/architecture.md`, `docs/design/compression.md` | 入口索引和模块说明统一为 zstd default / deflate supported / selected algorithm 口径 |
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
| 2026-06-16 | P1-1 | [x] | 将 queue `processed_ts` 定义为按投递顺序完成的最后一个真实 timestamp/sequence；明确 gap/filler 不投递、不 pending、不持久 ack；同步 queue 设计文档与实现注释；补充稀疏 gap ack 后 reopen 不重复消费测试 | `cargo test --test queue_test t27_1_5_sparse_gap_acked_progress_persists_after_reopen -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
| 2026-06-16 | P1-2/P2-3 | [x] | 将 compression active contract 前置为 `compress_type` 选择算法；通用写入/读取流程改为 selected algorithm；明确 zstd 默认、deflate 支持、level 语义和非法值处理；同步入口、架构、FFI/Python 文档注释与计划总览 | `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
| 2026-06-16 | P1-3 | [x] | 将 public timestamp 契约改为 signed `i64` 全域业务值；移除 `-1` latest sentinel；新增 `DataSet::read_latest`、`Store::read_dataset_latest`、FFI `tmsl_dataset_read_latest`、Python `Dataset.read_latest()`；`latest_written_timestamp` 与 inspect state 改为可空/显式 flag；同步 queue initial progress、read tests、FFI/Python wrapper tests 与相关设计/计划文档 | `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `cargo test --manifest-path wrapper/python/Cargo.toml`; `maturin develop --manifest-path wrapper/python/Cargo.toml`; `python -m pytest wrapper/python/tests -q`; `git diff --check` |
| 2026-06-16 | P1-4 | [x] | 确认实现已按“append 迁移废弃”执行：已有 latest record append 只走 pending raw tail 原地增长，超出普通 pending block 容量返回错误；同步 AGENTS 当前契约、Round5 TODO 残留命名和 append 测试命名；保留现有容量边界回归测试 | `cargo test t32_1_append_existing_latest_exceeding_pending_capacity_errors -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
| 2026-06-16 | P1-5 | [x] | 新增 Rust/C ABI `TmslLengthEntry { timestamp, data_len }`；将 `tmsl_dataset_query_length` 返回值改为 `TmslLengthEntry**`；明确 `sizeof=16`、`alignment=8`、`out_array_len` 为元素数量并清除旧 `uint32_t*`/12B 数组描述；补充 FFI typed array 回归测试 | `cargo test test_ffi_query_iterator_and_delete -- --test-threads=1`; `cargo test tmsl_length_entry_layout_matches_c_abi -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
| 2026-06-16 | P1-6 | [x] | 将 `read_exist/query_exist` 统一定义为当前可见数据存在性：过期 timestamp、filler/deleted entry 返回 false/0；`query_exist` 使用 checked range 计算并限制 bitmap 最大 4MiB；保留 bitmap 与原请求范围对齐；同步 Rust/FFI 注释、C header、设计和计划文档；补充 retention、deleted/filler、超限和 FFI 边界测试 | `cargo test --test read_operations query_exist -- --test-threads=1`; `cargo test test_ffi_query_iterator_and_delete -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
| 2026-06-16 | P1-7 | [x] | 将 flush active default 统一为 15s；把 10min 标为早期草案值或历史迁移说明；同步 memory/concurrency、design decisions、dataset operations、phase overview、早期 phase 文档和 Python wrapper design/plan；修正 Python `StoreConfig()` 构造器默认值从 600s 到 15s 并补回归测试 | `python -m pytest wrapper/python/tests/test_config.py::TestConfig::test_store_config_constructor_defaults_match_rust_defaults -q` 先失败后通过；`cargo test -- --test-threads=1`; `cargo fmt -- --check`; `cargo check`; `cargo clippy --all-targets -- -D warnings`; `cargo test --manifest-path wrapper/python/Cargo.toml`; `maturin develop --manifest-path wrapper/python/Cargo.toml`; `python -m pytest wrapper/python/tests -q`; `git diff --check` |

## 完成统计

| 优先级 | 总数 | 已完成 | 未完成 |
|--------|------|--------|--------|
| P0 | 3 | 3 | 0 |
| P1 | 7 | 7 | 0 |
| P2 | 4 | 1 | 3 |
| 合计 | 14 | 11 | 3 |
