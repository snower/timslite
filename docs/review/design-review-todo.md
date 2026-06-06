# timslite 设计审查 TODO（第 4 轮）

来源: [design-review.md](design-review.md)

创建日期: 2026-06-06

## 状态说明

- `[ ]`: 待处理
- `[~]`: 处理中
- `[x]`: 已完成
- `[!]`: 阻塞或待决策

处理要求:

- 涉及设计契约变化时，先更新 `design.md` 和 `docs/design/` 相关文档。
- 涉及实现行为变化时，同步更新 Rust/C header/Python wrapper 中受影响的公开边界。
- 涉及 ABI、持久化格式、retention、queue、journal 或 iterator 语义时，补充相应测试或验证记录。
- 完成后再把本文件对应项状态改为 `[x]`，并在“处理记录”中写明关键文件和验证命令。

## 待处理任务

| 状态 | ID | 优先级 | 事项 | 主要范围 | 完成判定 |
|---|---|---:|---|---|---|
| [ ] | P0-1 | P0 | 闭合 FFI 句柄线程安全与 Rust aliasing 契约 | `docs/design/memory-and-concurrency.md`, `docs/design/store-and-ffi.md`, `src/ffi.rs`, `include/timslite.h`, README/AGENTS 如需 | 明确 FFI 单线程/外部同步或内部同步模型；代码不再从同一 raw store 并发构造未受保护的 `&mut Store`；C header 与文档一致 |
| [ ] | P0-2 | P0 | 修正 Queue FFI ABI 缺失与 store 指针类型错误 | `docs/design/store-and-ffi.md`, `docs/design/queue-overview.md`, `src/ffi.rs`, `include/timslite.h`, queue/journal queue tests | 决定 Queue 是否正式进入 C ABI；若进入，所有 queue FFI 使用正确 opaque handle/lifecycle；若不进入，移除或隐藏未完成导出 |
| [ ] | P1-1 | P1 | 重新定义 FFI QueryIterator 的 retention/snapshot/lazy 一致性语义 | `docs/design/query-iterator.md`, `docs/design/background-and-cache.md`, `src/ffi.rs`, `src/query/iter.rs`, iterator tests | 文档与实现一致；若强一致则持有快照或 guard；若弱一致则明确错误/跳过语义并安全处理 segment 删除 |
| [ ] | P1-2 | P1 | 为 segment size 的 `u64` API 与 `u32` on-disk header 建立统一边界校验 | `docs/design/data-model.md`, `docs/design/meta-format.md`, `docs/design/store-and-ffi.md`, `src/config.rs`, `src/meta.rs`, `src/segment/*`, FFI config decode | v1 明确 segment size 上限；builder/meta/FFI/open/create 均拒绝超过 `u32::MAX` 或 initial > max；不再存在 `as u32` 静默截断 |
| [ ] | P1-3 | P1 | 冻结连续索引 grid capacity/header_len 契约 | `docs/design/index-continuous.md`, `docs/design/time-index.md`, `docs/design/meta-format.md`, `src/index/*`, `src/meta.rs`, continuous index tests | 连续索引 dataset 的 `segment_capacity` 不会随未来 header 变化而改变路由；必要时持久化 `index_grid_capacity` 或明确 v1 header_len 不变 |
| [ ] | P1-4 | P1 | 清除 correction 变长覆盖“移动后续字节”的残留矛盾 | `docs/design/data-segment.md`, `docs/design/dataset-operations.md`, `src/segment/data.rs`, correction/append tests | 文档和实现统一为 tail-only/no byte-shift；非 tail/压缩/需要移动字节场景走 fallback append + index update |
| [ ] | P1-5 | P1 | 统一 Queue consumer state 与 gap/filler poll 语义 | `docs/design/queue-overview.md`, `docs/design/queue-state-file.md`, `src/queue/mod.rs`, queue tests | 明确每组一个共享 state file；poll 对 gap/filler 的推进与 `processed_ts` 语义清晰；复杂度说明与实现一致 |
| [ ] | P1-6 | P1 | 统一 retention reclaim 是否更新 `last_used_at` | `docs/design/background-and-cache.md`, `docs/design/dataset-operations.md`, `src/dataset.rs`, background/retention tests | 明确 reclaim 是维护任务还是 dataset activity；文档、实现和 idle-close 行为一致 |
| [ ] | P1-7 | P1 | 同步 Store Rust API 文档中的 mutability 与内部字段 | `docs/design/store-and-ffi.md`, `src/store.rs`, README/AGENTS 如需 | `&self`/`&mut self` 签名、handle registry、read-only handles、background tasks 描述与实现一致 |
| [ ] | P2-1 | P2 | 清理 `design.md` 中残留的 `retention_ms` 导航名称 | `design.md`, `docs/design/meta-format.md` | 所有导航和描述统一使用 `retention_window` / timestamp unit |
| [ ] | P2-2 | P2 | 更新或去重 `data-model.md` 的 StoreConfig/DataSetConfig 片段 | `docs/design/data-model.md`, `docs/design/cargo-and-config.md`, `docs/design/store-and-ffi.md` | 配置字段摘要不再遗漏当前字段；避免多文档重复维护完整 struct |
| [ ] | P2-3 | P2 | 对齐 lazy allocation 扩容 flush/set_len 持久化语义 | `docs/design/lazy-allocation.md`, `src/segment/data.rs`, `src/index/segment.rs` | 扩容是否需要 flush/sync 的对象、时机和 crash safety 被明确定义，并与实现一致 |
| [ ] | P2-4 | P2 | 更新 `cargo-and-config.md` 中仓库结构与 CI/bench 状态 | `docs/design/cargo-and-config.md`, `.github/workflows/`, `benches/`, `Cargo.toml` | 文档不再声称缺少已存在目录；清晰区分现有 CI、推荐本地验证和未来 benchmark 要求 |
| [ ] | P2-5 | P2 | 清理 C header 与 FFI 源码注释乱码 | `include/timslite.h`, `src/ffi.rs` | 外部 header 注释可读；源码分隔注释统一为 ASCII 或有效 UTF-8；不影响 ABI |
| [ ] | P2-6 | P2 | 补充 Journal v1 consumer 滞后读取语义 | `docs/design/journal.md`, journal consumer examples/tests 如需 | 明确 journal pointer 只能读取当前仍可校验的数据，不代表精确历史 payload；严格 replay 留待未来 WAL/version 设计 |

## 处理记录

| 日期 | ID | 状态 | 处理摘要 | 验证 |
|---|---|---|---|---|
| 2026-06-06 | ALL | [ ] | 基于第 4 轮 `design-review.md` 创建初始 TODO 追踪表，尚未执行修复 | `Test-Path docs/review/design-review-todo.md`; `git diff --check -- docs/review/design-review-todo.md` |
