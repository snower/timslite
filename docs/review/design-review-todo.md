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
| [x] | P0-1 | P0 | 闭合 FFI 句柄线程安全与 Rust aliasing 契约 | `docs/design/memory-and-concurrency.md`, `docs/design/store-and-ffi.md`, `src/ffi.rs`, `include/timslite.h`, README/AGENTS 如需 | 明确 FFI 单线程/外部同步或内部同步模型；代码不再从同一 raw store 并发构造未受保护的 `&mut Store`；C header 与文档一致 |
| [x] | P0-2 | P0 | 修正 Queue FFI ABI 缺失与 store 指针类型错误 | `docs/design/store-and-ffi.md`, `docs/design/queue-overview.md`, `src/ffi.rs`, `include/timslite.h`, queue/journal queue tests | 决定 Queue 是否正式进入 C ABI；若进入，所有 queue FFI 使用正确 opaque handle/lifecycle；若不进入，移除或隐藏未完成导出 |
| [x] | P1-1 | P1 | 重新定义 FFI QueryIterator 的 retention/snapshot/lazy 一致性语义 | `docs/design/query-iterator.md`, `docs/design/background-and-cache.md`, `src/ffi.rs`, `src/query/iter.rs`, iterator tests | 文档与实现一致；若强一致则持有快照或 guard；若弱一致则明确错误/跳过语义并安全处理 segment 删除 |
| [x] | P1-2 | P1 | 为 segment size 的 `u64` API 与 `u32` on-disk header 建立统一边界校验 | `docs/design/data-model.md`, `docs/design/meta-format.md`, `docs/design/store-and-ffi.md`, `src/config.rs`, `src/meta.rs`, `src/segment/*`, FFI config decode | v1 明确 segment size 上限；builder/meta/FFI/open/create 均拒绝超过 `u32::MAX` 或 initial > max；不再存在 `as u32` 静默截断 |
| [x] | P1-3 | P1 | 冻结连续索引 grid capacity/header_len 契约 | `docs/design/index-continuous.md`, `docs/design/time-index.md`, `docs/design/data-model.md`, `src/header.rs`, `src/index/*`, continuous index tests | index entry area 固定从 128 字节开始；连续索引 `segment_capacity=(index_segment_size-128)/18`，不随未来 128 字节保留区内的 header 扩展改变路由 |
| [x] | P1-4 | P1 | 清除 correction 变长覆盖“移动后续字节”的残留矛盾 | `docs/design/data-segment.md`, `docs/design/dataset-operations.md`, `src/segment/data.rs`, correction/append tests | 文档和实现统一为 tail-only/no byte-shift；非 tail/压缩/需要移动字节场景走 fallback append + index update |
| [x] | P1-5 | P1 | 统一 Queue consumer state 与 gap/filler poll 语义 | `docs/design/queue-overview.md`, `docs/design/queue-state-file.md`, `src/queue/mod.rs`, queue tests | 明确每组一个共享 state file；poll 对 gap/filler 的推进与 `processed_ts` 语义清晰；复杂度说明与实现一致 |
| [x] | P1-6 | P1 | 统一 retention reclaim 是否更新 `last_used_at` | `docs/design/background-and-cache.md`, `docs/design/dataset-operations.md`, `src/dataset.rs`, background/retention tests | 明确 reclaim 是维护任务还是 dataset activity；文档、实现和 idle-close 行为一致 |
| [x] | P1-7 | P1 | 同步 Store Rust API 文档中的 mutability 与内部字段 | `docs/design/store-and-ffi.md`, `src/store.rs`, README/AGENTS 如需 | `&self`/`&mut self` 签名、handle registry、read-only handles、background tasks 描述与实现一致 |
| [x] | P2-1 | P2 | 清理 `design.md` 中残留的 `retention_ms` 导航名称 | `design.md`, `docs/design/meta-format.md` | 所有导航和描述统一使用 `retention_window` / timestamp unit |
| [x] | P2-2 | P2 | 更新或去重 `data-model.md` 的 StoreConfig/DataSetConfig 片段 | `docs/design/data-model.md`, `docs/design/cargo-and-config.md`, `docs/design/store-and-ffi.md` | 配置字段摘要不再遗漏当前字段；避免多文档重复维护完整 struct |
| [x] | P2-3 | P2 | 对齐 lazy allocation 扩容 flush/set_len 持久化语义 | `docs/design/lazy-allocation.md`, `src/segment/data.rs`, `src/index/segment.rs` | 扩容是否需要 flush/sync 的对象、时机和 crash safety 被明确定义，并与实现一致 |
| [x] | P2-4 | P2 | 更新 `cargo-and-config.md` 中仓库结构与 CI/bench 状态 | `docs/design/cargo-and-config.md`, `.github/workflows/`, `benches/`, `Cargo.toml` | 文档不再声称缺少已存在目录；清晰区分现有 CI、推荐本地验证和未来 benchmark 要求 |
| [x] | P2-5 | P2 | 清理 C header 与 FFI 源码注释乱码 | `include/timslite.h`, `src/ffi.rs` | 外部 header 注释可读；源码分隔注释统一为 ASCII 或有效 UTF-8；不影响 ABI |
| [x] | P2-6 | P2 | 补充 Journal v1 consumer 滞后读取语义 | `docs/design/journal.md`, journal consumer examples/tests 如需 | 明确 journal pointer 只能读取当前仍可校验的数据，不代表精确历史 payload；严格 replay 留待未来 WAL/version 设计 |

## 处理记录

| 日期 | ID | 状态 | 处理摘要 | 验证 |
|---|---|---|---|---|
| 2026-06-06 | P0-1 | [x] | FFI Store 改为 `Arc<Mutex<Store>>` 内部同步, dataset/iterator/queue/consumer 子句柄共享同步入口; 移除 raw `Store*` aliasing 路径 | `cargo test ffi::tests -- --test-threads=1`; `cargo check`; `cargo test -- --test-threads=1`; `cargo clippy -- -D warnings`; `cargo fmt -- --check` |
| 2026-06-06 | P0-2 | [x] | Queue 正式进入 C ABI: `tmsl_queue_open(dataset)`、queue/consumer registry lifecycle、header 声明、普通/journal queue close/push 语义已同步 | `cargo test ffi::tests -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo clippy -- -D warnings` |
| 2026-06-06 | P1-1 | [x] | FFI iterator 改为 query 时复制 `IndexEntry` snapshot, `next` 不再打开 index segment; 文档和 header 补充 snapshot/retention 边界 | `cargo test ffi::tests -- --test-threads=1`; `cargo test -- --test-threads=1` |
| 2026-06-06 | P1-2 | [x] | Segment header `file_size` 改为 `u64`, dataset/segment meta 增加 `compress_type`, 默认压缩算法改为 zstd, 保留 deflate 支持并同步 FFI 配置/header | `cargo test -- --test-threads=1`; `cargo check`; `cargo clippy -- -D warnings`; `cargo fmt -- --check` |
| 2026-06-06 | P1-3 | [x] | index segment 前 128 字节固定保留给 meta/state/扩展, 所有 index entry 从 128 开始; 连续索引容量和 timestamp 路由固定按 128 计算 | `cargo test header::tests -- --test-threads=1`; `cargo test index:: -- --test-threads=1`; `cargo test -- --test-threads=1`; `cargo check`; `cargo clippy -- -D warnings`; `cargo fmt -- --check` |
| 2026-06-11 | P1-4 | [x] | 清理 correction tail-only 变长覆盖文档残留, 明确不移动任何后续 block/record 字节; 现有 `test_overwrite_in_last_block_rejects_non_tail_record` 覆盖该实现契约 | `cargo test segment::data::tests::test_overwrite_in_last_block_rejects_non_tail_record -- --test-threads=1` |
| 2026-06-11 | P1-5 | [x] | Queue consumer 文档改为真实实现模型: shared state file、direct read fast path、`query_index_entries` 跳过 gap/filler、不为 filler 建 pending/自动 ack; 新增连续 filler gap poll 集成测试 | `cargo test t27_1_4_poll_skips_continuous_filler_gap -- --test-threads=1` |
| 2026-06-11 | P1-6 | [x] | retention reclaim 定义为维护任务, 实现保存并恢复 `last_used_at`, 不延长 dataset 热度; 新增回归测试 | `cargo test dataset::tests::test_retention_reclaim_does_not_refresh_last_used_at -- --test-threads=1` |
| 2026-06-11 | P1-7 | [x] | Store Rust API 文档按实现同步 `&mut self`/`&self` 边界, 补充 handle registry 与内部同步 mutability note | `cargo check` |
| 2026-06-11 | P2-1 | [x] | `design.md` 快速导航统一改为 `retention_window`, 移除 `retention_ms` 残留 | `rg -n "retention_ms" design.md docs/design` |
| 2026-06-11 | P2-2 | [x] | `data-model.md` 补充当前有效 StoreConfig/DataSetConfig 字段摘要, 明确旧片段只作示意且字段以 `src/config.rs` 为准 | `rg -n "Active Contract: StoreConfig" docs/design/data-model.md` |
| 2026-06-11 | P2-3 | [x] | 重写 lazy allocation 设计, 将扩容定义为 unmap + `set_len` + remap, 不要求扩容步骤执行 mmap flush; flush 仅同步已写 mmap 内容 | `rg -n "set_len\\(target\\)|mmap.flush" docs/design/lazy-allocation.md` |
| 2026-06-11 | P2-4 | [x] | 更新构建配置文档: 补齐 `zstd`/`proptest`, 记录 `.github/workflows/ci.yml` 和空 `benches/` 目录现状, 区分 CI、本地验证与未来 benchmark | `Test-Path .github`; `Test-Path benches`; `Get-Content Cargo.toml` |
| 2026-06-11 | P2-5 | [x] | 清理 C header 写入分支注释中的编码损坏箭头, `src/ffi.rs` 未发现同类乱码命中 | `rg -n "鈫|�" include/timslite.h src/ffi.rs` |
| 2026-06-11 | P2-6 | [x] | Journal 设计补充 lagging consumer 语义: `0x11/0x12/0x13` 只是 pointer hint, 滞后消费者必须通过源 dataset 校验, 严格 replay 留待未来 WAL/versioned payload | `rg -n "Lagging Consumer" docs/design/journal.md` |
| 2026-06-11 | ALL | [x] | 第 4 轮 P0/P1/P2 设计审查 TODO 均已完成状态更新; 本轮 P2-1..P2-6 以文档契约和 C header 注释修复为主, 未改变运行时 ABI | `cargo fmt -- --check`; `cargo check`; `cargo test -- --test-threads=1`; `cargo clippy --all-targets -- -D warnings`; `git diff --check` |
