# timslite 设计审查 TODO（第 8 轮）

来源: [docs/review/design-review.md](design-review.md)

创建日期: 2026-06-21

## 状态说明

| 标记 | 含义 |
|------|------|
| `[ ]` | 未开始 |
| `[~]` | 处理中或部分完成 |
| `[x]` | 已完成并验证 |
| `[!]` | 暂缓或需重新决策 |

处理原则:

- 本 TODO 用于追踪第 8 轮设计审查中发现的问题修复过程。
- 本轮仅创建 review/todo artifact；后续修复需另行确认后再修改设计、实现或测试。
- 每个任务完成时，需要同步更新相关设计文档、必要实现/测试，以及本文档处理记录。
- 涉及行为或文件格式契约变化时，需同步 `design.md`、对应 `docs/design/*.md`、`plan.md` 或对应 `docs/plan/*.md` checklist。
- 完成任务后先不要 git commit，等待审核确认。

## P0: 必须优先处理

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [x] | P0-1 | 统一 retention reclaim 的 “closed segment” 与 lifecycle `DataSet::close()` 术语，避免后台任务关闭 dataset | `docs/design/background-and-cache.md`, `docs/design/data-segment.md`, `docs/design/time-index.md`, `docs/design/dataset-operations.md`, `docs/design/store-and-ffi.md`, retention/background tests 如需 | retention 文档只使用 `flush()` + `idle_close_segments()` / `idle_close_all()` 表达分段释放；不再写 `DataSet 已 close()` 或裸 `close()`；明确 reclaim 不关闭 queue、不移除 Store registry、不使 handle 失效；必要时补充后台 retention 不关闭 dataset 的回归测试 |

## P1: 应尽快处理

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [x] | P1-1 | 收敛 `query_length_iter` 的公开 Rust/FFI 语义与内存口径 | `docs/design/dataset-read-operations.md`, `docs/design/query-iterator.md`, `docs/design/store-and-ffi.md`, `src/dataset.rs`, `src/ffi.rs`, `src/query/length_iter.rs`, wrapper docs/tests 如需 | 二选一完成：公开 API/FFI 真正提供 source-cursor + HotBlockCache lazy iterator，或文档明确当前 Rust public 是 `query_length()` snapshot iterator、FFI 是 index-entry snapshot；大范围查询内存边界不再被描述为严格流式 |
| [x] | P1-2 | 更新轻量读操作 FFI 文档，移除旧式 `TmslStore* + name/type` 签名 | `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `include/timslite.h` | `read_exist/query_exist/read_length/query_length/query_length_iter` 在设计文档中只有一套当前 C ABI；使用 dataset handle、return code、err_buf、out 参数和 `tmsl_data_free` 释放规则；旧 bool/裸指针/name-type 签名被删除或明确标为历史草案 |
| [x] | P1-3 | 同步 Inspect 文档的 `identifier` 字段与 Store facade 流程 | `docs/design/dataset-inspect.md`, `docs/design/dataset-identifier.md`, `docs/design/store-and-ffi.md`, `include/timslite.h`, wrapper docs/tests 如需 | Rust/FFI/Python `DataSetInfo` 文档均包含 `identifier`; `inspect_dataset(name,type)` 伪代码与当前流程一致：validate、registry hit、读取 identifier、校验 max_identifier、open/inject context、保留 registry、inspect；内存释放说明准确 |
| [x] | P1-4 | 修正 JournalQueue poll 伪代码，保持 retry/visibility timeout 与普通 queue state 一致 | `docs/design/journal-storage.md`, `docs/design/journal.md`, `docs/design/queue-state-file.md`, `src/journal/queue.rs` tests 如需 | JournalQueue 文档明确只重投 retryable pending；未过期 pending 不立即返回；`max_retry_count` 和 `running_expired_seconds` 语义与 `ConsumerStateFile` 一致；“journal sequence 连续”只简化新 sequence 查找，不改变 retry/ack 状态机 |

## P2: 可以随后优化

| 状态 | ID | 任务 | 主要文件 | 验收标准 |
|------|----|------|----------|----------|
| [ ] | P2-1 | 清理 `data-model.md` 中旧 `StoreConfig` / `DataSetConfig` 示例 | `docs/design/data-model.md`, `src/config.rs` | 同一文档内不再存在两套配置字段列表；核心类型示例与 active contract / `src/config.rs` 对齐；若保留历史草案，明确标注不可作为实现依据 |
| [ ] | P2-2 | 将读操作文档中的 public Rust 签名改为 Store-managed `&self` wrapper 口径 | `docs/design/dataset-read-operations.md`, `docs/design/store-and-ffi.md`, `src/dataset.rs` | public `DataSet` 读/轻量读/length API 示例使用 `&self`；如需展示 `DataSetInner`，单独标注为 crate-internal；文档末尾并发边界与前文签名一致 |

## 建议处理顺序

1. P0-1：先修正 retention/close 术语，避免生命周期语义误导后续实现。
2. P1-2、P1-3：优先处理对外 API/ABI 文档，减少 C/Python 集成误读。
3. P1-1、P1-4：再处理迭代器性能口径与 JournalQueue 状态机，必要时决定是改实现还是改文档。
4. P2-1、P2-2：最后清理旧示意代码和 public/internal 签名残留。

## 处理记录

| 日期 | ID | 状态 | 处理摘要 | 验证 |
|------|----|------|----------|------|
| 2026-06-21 | P1-1 | [x] | Public Rust `DataSet::query_length_iter()` 改为 source-cursor iterator, 创建时准备 `QuerySource`, `next()` 时按需读取 record header; FFI 文档明确保持 index-entry snapshot iterator 语义 | 新增 `test_public_query_length_iter_reads_from_source_cursor`; 先在旧实现下失败, 实现后通过; `cargo check`; `cargo test --test read_operations -- --test-threads=1` |
| 2026-06-21 | P1-2 | [x] | 删除 `dataset-read-operations.md` 中旧式 `TmslStore* + name/type` 轻量读 C ABI 签名, 改为引用 `store-and-ffi.md` 与 `include/timslite.h` 作为权威 ABI | 静态检索旧式 `tmsl_dataset_*` Store/name/type 签名 |
| 2026-06-21 | P1-3 | [x] | `dataset-inspect.md` 补齐 Rust/FFI/Python `DataSetInfo.identifier`, 并把 Store facade 伪代码改为 validate/open-or-get/load-and-keep 流程 | 静态检索 `DataSetInfo` identifier 和 inspect 流程说明 |
| 2026-06-21 | P1-4 | [x] | `journal-storage.md` / `journal.md` 的 JournalQueue poll 伪代码改为先处理 retryable pending, 未过期 pending 不重投, retry 超限按完成前缀推进, 再查找新 sequence | 静态检索 retryable pending、未过期 pending 与 sequence 连续性说明 |
| 2026-06-21 | P0-1 | [x] | 统一 retention reclaim 文档术语: 回收前执行 `flush()` + data/index segment `idle_close_all()` 使分段 entry sync+unmap, 明确不调用 lifecycle `DataSet::close()`、不关闭 queue、不移除 Store registry、不使 handle 失效 | 静态检索确认 P0-1 相关 active docs 不再把 retention 前置条件写成 `DataSet 已 close()` 或裸 `close()` 流程 |
| 2026-06-21 | ALL | [ ] | 根据第 8 轮设计审查报告创建 TODO 跟踪文件，尚未开始修复 | `docs/review/design-review.md` 与 `docs/review/design-review-todo.md` 已创建；本轮未修改设计/实现代码 |

## 完成统计

| 优先级 | 总数 | 已完成 | 未完成 |
|--------|------|--------|--------|
| P0 | 1 | 1 | 0 |
| P1 | 4 | 4 | 0 |
| P2 | 2 | 0 | 2 |
| 合计 | 7 | 5 | 2 |
