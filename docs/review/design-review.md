# timslite 设计审查报告（第 8 轮）

审查日期: 2026-06-21

## 审查范围

本轮仅做设计审查并保存审查结果，不调整设计文档、代码、测试或计划文件。

已阅读与交叉核对:

- `design.md`
- `docs/design/*.md`
- `docs/review/archives/Round6/design-review*.md`
- 为确认部分设计口径是否仍与实现一致，辅助静态查看了 `src/dataset.rs`、`src/store.rs`、`src/config.rs`、`src/query/length_iter.rs`、`src/journal/queue.rs`、`src/ffi.rs`、`include/timslite.h`

## 总体结论

第 6 轮设计审查中的核心契约大多已经收敛：timestamp 全域 `i64`、`.journal/logs` 专用存储、queue state dirty flush、`retention_window <= i64::MAX`、可变 header、`compress_type` 与 zstd 默认等主线已经比较一致。

但当前设计文档仍存在若干新的或残留的契约漂移，集中在后台维护语义、读操作 ABI、inspect/identifier 字段、JournalQueue retry 语义、以及旧示意代码残留。这些问题主要是文档对外承诺与当前 active contract 或实现不一致，若后续按错误片段实现，会影响 dataset 生命周期、外部 C/Python 集成、消费重试语义和大范围查询内存预期。

## P0 问题

### P0-1: Retention reclaim 文档把 segment idle-close 写成 lifecycle `DataSet::close()`，可能误导后台任务关闭打开中的 dataset

证据:

- `docs/design/store-and-ffi.md:106` 明确定义 `DataSet::close()` 是公开 lifecycle close：关闭 queue、flush、释放资源、标记 closed，并通过 runtime context 从 `Store.datasets` registry 移除、使旧 Store handle generation 失效。
- `docs/design/queue-overview.md:340` 也强调后台 idle-close 不调用 `DataSet::close()`，只调用内部 `idle_close_segments()`。
- `docs/design/background-and-cache.md:182-183` 的 retention 执行流程写成 `DataSet::reclaim_expired_segments()` 内部“先 close() (flush + idle_close_all)”。
- `docs/design/background-and-cache.md:193` 继续写“close() 后分段均为 closed 状态”，`docs/design/data-segment.md:431-432` 与 `docs/design/time-index.md:246-247` 也把回收前置条件写成 “DataSet 已 close()”。
- 另一处 active 流程 `docs/design/dataset-operations.md:620-630` 实际写的是保存 `last_used_at`，执行 `flush()`、`time_index.idle_close_all()`、`segments.idle_close_all()`，最后恢复 `last_used_at`。
- 静态核对当前实现：`src/dataset.rs:1729-1754` 的 `reclaim_expired_segments()` 保存 `last_used_at` 后调用 `idle_close_segments()`，并未调用 lifecycle `close()`。

问题:

这里的 “close” 在设计文档里出现了两种含义：一种是 lifecycle close，另一种只是把 segment sync+unmap 变成 closed entry。前者会改变 Store registry、queue 和 handle 生命周期；后者只是维护任务所需的文件状态。当前 background/data-segment/time-index 文档把两者混用，读者按字面实现会让每日 retention reclaim 关闭业务 dataset，甚至关闭 queue 并移除 registry。

影响:

- 后台 retention 可能被误实现为破坏 Store handle 生命周期的任务。
- 打开中的 queue/consumer 可能被维护任务关闭。
- `last_used_at` “不更新”的设计目标会被 `DataSet::close()` 的生命周期副作用破坏。

建议:

- 在 retention 相关文档中禁用裸 `close()` 表述，统一写成 `flush()` + `idle_close_segments()` / `idle_close_all()`。
- `DataSegmentSet` 和 `TimeIndex` 的回收前置条件改为 “所有 segment entry 均已 sync+unmap，处于 closed segment entry 状态”，不要写 “DataSet 已 close()”。
- 在 `background-and-cache.md` 明确 retention reclaim 不关闭 dataset、不中断 Store handle、不关闭 queue，只临时释放分段 mmap 并在检查后删除整段过期文件。

## P1 问题

### P1-1: `query_length_iter` 的设计承诺与公开 Rust/FFI 行为不一致

证据:

- `docs/design/dataset-read-operations.md:244-265` 定义 `query_length_iter` 返回 `QueryLengthIterator`，是惰性范围数据长度迭代器，并支持 HotBlockCache。
- `docs/design/dataset-read-operations.md:298-321` 进一步给出 `QueryLengthIterator { sources, segments, cache, hot_block }` 结构，强调复用 `QuerySource` 和 `HotBlockCache`。
- `src/query/length_iter.rs:13-22` 确实存在内部 `QueryLengthIterator`，并在 `next_entry()` 中按 source 惰性读取长度。
- 但公开 `DataSet` wrapper 当前实现为 `src/dataset.rs:358-364`：先调用 `self.query_length(start_ts, end_ts)?` 收集成 `Vec`，再返回 `IntoIter<Result<(i64, u32)>>`。
- FFI `tmsl_dataset_query_length_iter` 当前在 `src/ffi.rs:1494-1501` 创建时复制 `query_index_entries` snapshot 到 `FfiIterator`，`next` 时再读取 data_len；它不是 docs 中的 `QuerySource` cursor 语义。

问题:

文档把 `query_length_iter` 作为真正的低内存惰性读接口对外描述，但当前公开 Rust wrapper 会一次性收集所有 length 结果，FFI 则一次性复制命中范围的 index entries。二者都不具备文档承诺的完整 source cursor + HotBlockCache 公开语义。

影响:

- 大范围 length 查询的内存预期被高估，调用方可能误以为可以用 `query_length_iter` 避免一次性结果集。
- Rust public API、FFI API 和内部 `QueryLengthIterator` 的边界不清楚，后续 wrapper 文档和测试可能继续固化错误性能口径。

建议:

- 二选一：真正把内部 `QueryLengthIterator` 暴露到 public Rust wrapper，并为 FFI 明确 snapshot vs source-cursor 语义；或更新文档，说明当前 public Rust `query_length_iter` 是 `query_length()` snapshot 的迭代包装，FFI 是 index-entry snapshot。
- 在 `query-iterator.md` 类似的“内存与性能口径”表中补充 `query_length_iter` 的 Rust/FFI 当前边界，避免宣称严格流式。

### P1-2: `dataset-read-operations.md` 的 FFI 小节仍保留旧式 C ABI 签名

证据:

- `docs/design/dataset-read-operations.md:335-367` 仍用 `TmslStore* store, const char* name, const char* type` 描述 `read_exist`、`query_exist`、`read_length`、`query_length_iter`，并使用 `bool` 或裸指针返回。
- 当前主 FFI 文档 `docs/design/store-and-ffi.md:429-466` 使用 dataset handle、`err_buf`、返回码和 out 参数。
- `include/timslite.h:453-518` 与 `src/ffi.rs:1313-1546` 也按 dataset handle + return code + err_buf 实现。

问题:

同一批轻量读接口在不同设计文档中有两套不兼容 C ABI。`dataset-read-operations.md` 的旧签名看起来像仍可按 name/type 直接从 store 调用，且没有错误缓冲和明确的内存释放/返回码语义。

影响:

- C 集成方或 wrapper 实现者若按该小节开发，会得到不存在或 ABI 不匹配的函数。
- `query_exist` / `query_length_iter` 的内存所有权、错误返回和 handle 生命周期口径会被误读。

建议:

- 删除该小节中的旧签名，改为引用 `store-and-ffi.md` 的权威 C ABI。
- 若保留摘要，必须逐项使用当前 `void* dataset`、`int`/`void*` 返回、`err_buf`、out 参数、`tmsl_data_free` 释放规则。

### P1-3: Inspect 文档未同步 Dataset Identifier 字段与实际 Store facade 流程

证据:

- `docs/design/dataset-identifier.md:125` 明确要求 `DataSetInfo` 新增 `identifier: u64`。
- 当前实现和 C header 已包含该字段：`src/dataset.rs:1854-1862`、`include/timslite.h:853-858`。
- 但 `docs/design/dataset-inspect.md:27-65` 的 Rust `DataSetInfo` 示例没有 `identifier`。
- `docs/design/dataset-inspect.md:249-263` 的 `TmslDataSetInfo` 示例也没有 `identifier`。
- `docs/design/dataset-inspect.md:316-330` 的 Python `DataSetInfo` 示例同样缺少 `identifier`。
- `docs/design/dataset-inspect.md:237-240` 的 Store facade 伪代码仍构造 `DataSetHandle::new(name, dataset_type)` 并 `get_dataset`；但当前实现 `src/store.rs:879-905` 是按 name/type 校验、读取 `identifier`、打开未加载 dataset、注入 runtime context 并保留到 registry。

问题:

Inspect 专题没有吸收 Dataset Identifier 设计，且仍残留旧的 handle-by-name 伪代码。当前 active contract 中 handle 是 Store 管理的数值句柄，`inspect_dataset(name,type)` 是 name/type facade，并且需要读 identifier、校验 max_identifier、必要时加载 dataset。

影响:

- Rust/FFI/Python 文档字段与实际 ABI 不一致，尤其外部运维界面会漏掉稳定数字 id。
- 后续实现者可能误以为可以从 name/type 构造 `DataSetHandle`，绕过 Store registry 和 identifier 校验。

建议:

- 在 `dataset-inspect.md` 的 Rust、FFI、Python 结构体中加入 `identifier`。
- 重写 Store facade 伪代码，使其与 `store.rs` 当前流程一致：validate → registry hit inspect → read identifier/meta/open → inject runtime context → registry insert → inspect。
- 同步内存释放说明，明确 `identifier` 是值字段，不参与字符串释放。

### P1-4: JournalQueue poll 伪代码绕过 retry/visibility timeout

证据:

- `docs/design/journal-storage.md:276-281` 写 JournalQueue poll 第一步为“若已有 unacked pending entry，直接 `JournalLog::read(entry.sequence)`”。
- 通用 queue state 契约 `docs/design/queue-state-file.md:147-155` 规定只扫描“已过期或恢复过期”的未 ack entry；未过期 pending 不会立即重投。
- `docs/design/journal.md:337-338` 也写 JournalQueue 未 ack sequence 只有运行超时或 state file reopen 恢复后才会被重试投递，并受 `max_retry_count` 控制。
- 当前实现 `src/journal/queue.rs:244-258` 使用 `take_retryable_pending(config, now)`，只对 retryable pending 读取并返回。

问题:

`journal-storage.md` 的简化伪代码把“已有 unacked pending”与“retryable pending”混为一谈。按该伪代码实现会在同一 consumer group 内对未超时的 pending 立即重复投递，绕过 visibility timeout 和 retry 计数。

影响:

- `running_expired_seconds` 失去意义。
- `max_retry_count` 可能无法按设计工作。
- 多 consumer 同组场景下会产生超出 at-least-once 预期的热重复投递。

建议:

- 将 JournalQueue poll 伪代码改为与 `ConsumerStateFile` 通用流程一致：先 `take_retryable_pending(config, now)`；未过期 pending 只占用 pending，不返回；再从 `processed_ts + 1` 找不在 pending 的下一 sequence。
- 在 `journal-storage.md` 明确 “Journal 没有 filler/gap” 只简化新 sequence 查找，不简化 retry/ack 状态机。

## P2 问题

### P2-1: `data-model.md` 仍保留旧 `StoreConfig` / `DataSetConfig` 示例，和同文件 active contract 冲突

证据:

- `docs/design/data-model.md:306-344` 的“核心类型定义”中，`StoreConfig` 缺少 `compress_type`、`retention_check_hour`、`enable_background_thread`；`DataSetConfig` 缺少 `compress_type`、`index_continuous`、`retention_window`、`enable_journal`、`create_time`。
- 同文件后续 `docs/design/data-model.md:455-496` 又声明 `src/config.rs` 才是权威字段列表，并给出包含上述字段的 active contract。
- 当前实现字段也与 active contract 一致：`src/config.rs:97-123` 和 `src/config.rs:361-373`。

问题:

一个专题文档内同一类型出现“旧示例”和“active contract”两套字段列表。虽然后者说明了权威来源，但前者位于“核心类型定义”章节，更容易被读者当成实现草图。

影响:

- 新增配置、FFI wrapper 或文档维护时容易漏掉 `compress_type`、background thread、retention/journal 等字段。
- 代码示例和 active contract 不一致会降低设计文档作为 contract 的可信度。

建议:

- 删除旧的 `StoreConfig` / `DataSetConfig` 示例，或直接替换为 active contract 版本。
- 若保留“历史草案”，必须明确标记为废弃且不可作为实现依据。

### P2-2: `dataset-read-operations.md` 的 public Rust 签名仍以 `&mut self` 为主，与 Store-managed `Arc<DataSet>` wrapper 边界不一致

证据:

- `docs/design/dataset-read-operations.md:34-35`、`:72-73`、`:92-93`、`:115-116`、`:144-145`、`:185-186`、`:212-213`、`:247-248` 均把公开读接口写成 `&mut self`。
- `docs/design/store-and-ffi.md:9-13` 定义 public `DataSet` 方法通过内部 dataset mutex 提供 Store-managed 操作视图。
- 当前外层 `DataSet` 实现确实使用 `&self` public wrapper，例如 `src/dataset.rs:318-365`。
- `docs/design/dataset-read-operations.md:450-453` 文件末尾又写“公开 DataSet 读操作使用 `&self`，由 DataSet 内部 mutex 保证线程安全”。

问题:

同一读操作文档前面的 API 签名与末尾并发边界互相冲突。内部 `DataSetInner` 使用 `&mut self` 可以成立，但 public `DataSet` wrapper 已是 `&self`。

影响:

- Rust 使用者会误解是否需要独占 mutable dataset 引用。
- 与 `Store::get_dataset -> Arc<DataSet>` 的 public boundary 不一致，容易在 wrapper 文档和示例中继续扩散。

建议:

- 将该文档拆分为 public wrapper 签名和 crate-internal `DataSetInner` 签名。
- 公开 API 示例统一使用 `&self`；如需说明内部实现，再明确标注为 `DataSetInner`。
