# timslite 设计审查报告（第 4 轮）

审查日期: 2026-06-06

## 审查范围

本轮按“只审查、不修复”的边界执行，重点阅读并交叉检查:

- `design.md`
- `docs/design/*.md` 下 18 个专题设计文档
- 与设计契约强相关的实现边界: `src/ffi.rs`, `include/timslite.h`, `src/store.rs`, `src/dataset.rs`, `src/index/*`, `src/segment/*`, `src/queue/*`, `src/config.rs`

第 1-3 轮 review 已归档。本报告仅记录当前仍能观察到的设计缺陷、逻辑矛盾、文档漂移和优化建议；未对设计文档或代码做任何修复性调整。

## 总体结论

核心存储格式、record/block/index/cache/journal/queue 的主线设计已经比前几轮更收敛，尤其是 `data_len: u32`、可变 header、retention 语义、append、journal pointer-based 辅助日志等关键决策已经有较清晰的契约。

本轮最需要优先处理的问题集中在 **FFI 边界** 和 **可扩展格式约束**:

- FFI store/dataset/iterator 句柄的线程安全和 Rust aliasing 语义没有被设计闭合，当前实现还把原始 `Store*` 暴露为可 `Send/Sync` 的句柄来源，风险高。
- Queue FFI 已在 Rust 中导出，但 C header 和 `store-and-ffi.md` 没有一等公民式设计，且队列函数的 store 指针类型与现有 `tmsl_store_open` 返回值不一致。
- FFI `QueryIterator` 文档声称 retention 删除被 DataSet mutex 保护，但实际 iterator 在获取 DataSet 锁之前会自己打开 index segment 文件，快照/并发语义不成立。
- 连续索引把 `segment_capacity` 绑定到 `index_header_len`，但可变 header 又允许未来 header 长度变化；该能力需要冻结在 dataset meta 中，否则格式扩展会改变 timestamp 到 index segment 的映射。

优先级说明:

- `P0`: 可能导致 ABI 不可用、未定义行为、错误数据读取或核心契约无法成立。
- `P1`: 会导致重要设计语义不一致、未来扩展破坏现有数据，或实现者按文档实现会得到错误行为。
- `P2`: 文档漂移、配置说明不完整或验证/维护层面的优化项。

## P0 问题

### P0-1: FFI 句柄线程安全与 Rust aliasing 契约未闭合

相关位置:

- `docs/design/memory-and-concurrency.md`: Store/DataSet 并发模型描述为 Store `RwLock`、DataSet `Mutex`。
- `docs/design/store-and-ffi.md`: FFI 生命周期只描述父子句柄关闭顺序，没有定义同一 store/dataset 句柄是否允许跨线程并发调用。
- `src/ffi.rs:153-178`: `FfiStore`, `FfiDataset`, `FfiIterator` 持有 `*mut Store`，并手写 `unsafe impl Send/Sync`。
- `src/ffi.rs:442`, `479`, `509`, `566`, `776`, `818` 等: 多处从同一个 raw pointer 直接恢复 `&mut Store`。

问题:

设计文档的并发模型发生在 Rust 内部的 `Store`/`DataSet` 锁层级，但 FFI 层把 `Store` 包装为 raw pointer 后没有同步层。当前 FFI 句柄被声明为 `Send/Sync`，这意味着 C 侧很自然会把同一 store/dataset 句柄放到多个线程调用。此时多个 FFI 函数可同时从同一个 `*mut Store` 构造 `&mut Store`，违反 Rust 独占可变引用规则，即使内部部分字段再用 `RwLock`/`Mutex`，外层别名本身仍可能构成未定义行为。

影响:

- C ABI 动态库的核心线程安全语义不明确。
- 并发调用 FFI 时可能出现 UB、数据竞争或锁层级之外的内存安全问题。
- 文档宣称的 DataSet 级串行化不能覆盖 FFI raw pointer 层。

建议:

在设计层先明确 FFI 线程模型，二选一:

1. FFI 句柄为单线程/外部同步句柄: 文档、C header 和 README 必须明确同一 store/dataset/iterator 句柄不得并发调用，并移除或收紧 FFI 内部的 `Send/Sync` 假设。
2. FFI 句柄内部同步: `FfiStore` 持有 `Arc<Mutex<Store>>` 或等价同步封装，所有 FFI public API 经统一锁入口访问 Store，并补充锁顺序和 close/child handle 规则。

### P0-2: Queue FFI 已导出但 ABI 设计缺失，且 store 指针类型错误

相关位置:

- `src/ffi.rs:300-328`: `tmsl_store_open*` 返回的是 `Box<FfiStore>` 转成的 `void*`。
- `src/ffi.rs:860-899`: `tmsl_queue_open/close` 的参数声明为 `store: *mut Store`，并直接 `&mut *store`。
- `src/ffi.rs:880-899`: `tmsl_queue_close` 用 `DataSetHandle(0)` 调用 `store.close_queue(handle)`。
- `src/ffi.rs:907-1035`: 已导出 `tmsl_queue_consumer_open/drop`, `tmsl_queue_push/poll/ack`。
- `include/timslite.h:240-357`: C header 只声明 dataset write/append/read/query/free/close，没有声明任何 `tmsl_queue_*`。
- `docs/design/store-and-ffi.md:232-300`: FFI API 列表没有 queue FFI 声明。

问题:

Rust 层已经导出 queue FFI 符号，但设计文档和 C header 没有形成可调用 ABI。更严重的是，`tmsl_queue_open` 期望 `*mut Store`，而公开入口 `tmsl_store_open` 实际返回 `FfiStore*`。C 侧如果按其它 API 的惯例把 `void* store` 传给 queue 函数，将被错误解释为 `Store*`，属于布局不匹配。`tmsl_queue_close` 又固定关闭 `DataSetHandle(0)`，无法关联真实 queue 所属 dataset。

影响:

- Queue FFI 对 C 调用者实际不可用。
- 若调用者手工声明这些符号并传入正常 store handle，可能触发错误内存访问。
- queue handle、consumer handle、dataset handle 的生命周期和关闭顺序没有 ABI 契约。
- `.journal/logs` 的 `open_queue` 实时消费能力在 FFI 层没有可验证路径。

建议:

先决定 Queue 是否进入正式 C ABI:

- 如果进入: 在 `store-and-ffi.md` 和 `include/timslite.h` 中定义完整 API，所有函数统一接收 `void* store` 或完全不接收 store，不能混用内部 `Store*`。queue handle 必须记录所属 dataset handle/key，close 必须关闭正确 dataset queue 并清理 queue/consumer registry。
- 如果暂不进入: 移除或隐藏未设计完成的 `tmsl_queue_*` 导出，避免形成半公开 ABI。

## P1 问题

### P1-1: FFI QueryIterator 的 retention/snapshot 保护与实际 lazy index 读取不一致

相关位置:

- `docs/design/query-iterator.md`: 描述 FFI iterator 每次 `next` 通过 DataSet lock 读取，retention 删除 segment 受 DataSet mutex 保护。
- `src/ffi.rs:263-274`: `next_iter_index_entry` 在没有 DataSet 锁的情况下推进 `QuerySource`。
- `src/query/iter.rs:20-30`: `QuerySource::SegmentFile` 只保存 path/start/position 等信息。
- `src/query/iter.rs:65-100`: `QuerySource::next_entry` 会自行 `IndexSegment::open(path, ...)` 并读取 index entry。
- `src/ffi.rs:811-821`: FFI `tmsl_iter_next` 先获得 index entry，之后才锁 DataSet 并调用 `read_entry_at_index`。

问题:

设计文档把 FFI iterator 的并发安全建立在“每次 next 锁 DataSet”上，但实际流程是先由 iterator 自己打开 index segment 文件并读取 entry，随后才锁 DataSet 读取数据。这导致 retention reclaim、drop/close、out-of-order index 更新等操作都可能在 iterator 的 index source 与 data read 之间插入。

影响:

- 文档承诺的 retention 保护不成立: index segment 可能在 iterator 打开前或读取中被删除。
- FFI iterator 不具备清晰快照语义: query 创建时收集的是 source/path，而不是固定的 index entry 集合。
- 在高并发读写或后台 reclaim 下，可能出现 `NotFound`、漏读、重复读或读取到与 query 创建时不同的数据版本。

建议:

设计上需要明确 FFI iterator 的一致性等级:

- 强一致/快照: query 创建时复制 index entry 列表，或持有阻止 retention/drop 的 dataset read session/guard。
- 弱一致/lazy: 文档必须明确 segment 可能消失、iterator 可能返回错误或跳过，并要求实现对 index file missing 做受控处理，而不是声称由 DataSet mutex 保护。

### P1-2: Segment size 暴露为 `u64`，但 on-disk header 仍是 `u32`，缺少边界契约

相关位置:

- `docs/design/data-model.md:126`: `file_size` 属于 `u32 LE`，超过 `u32::MAX` 必须拒绝。
- `docs/design/data-model.md:179`: segment header `file_size` 是 4 字节 `u32 LE`。
- `docs/design/meta-format.md`: `data_segment_size` 和 `index_segment_size` 是 `u64 LE`。
- `docs/design/store-and-ffi.md:232-241`: FFI dataset config 暴露 `data_segment_size/index_segment_size: u64`。
- `src/config.rs:31-38`, `110-130`: StoreConfig builder 接收 `u64` segment size，没有上界校验。
- `src/segment/data.rs:82`, `src/index/segment.rs:103`: 创建 header 时将 `max_file_size as u32` 写入 metadata。

问题:

持久化 meta 允许 `u64` segment size，Store/Rust/FFI 配置也允许 `u64`，但 segment file header 的 `file_size` 是 `u32`。设计只在 data-model 里泛化说要拒绝 `u32::MAX` 以上值，没有把该约束落到 StoreConfig/DataSetConfig/FFI/meta open 的正式配置契约中。实现路径目前存在 `as u32` 截断风险。

影响:

- 用户通过 FFI 或 Rust builder 设置大于 4GiB 的 segment size 时，meta 与 segment header 可能不一致。
- reopen、mmap 扩容、segment 路由和 crash 恢复都依赖 segment size，截断会导致严重数据定位错误。

建议:

设计中明确:

- 当前文件格式 v1 的 `data_segment_size`、`index_segment_size`、`initial_*_segment_size` 均必须 `<= u32::MAX`，且 initial size 也不能超过 max size。
- StoreConfig/DataSetConfig builder、FFI config decode、DataSetMeta create/open 都必须执行统一校验。
- 若未来需要超过 4GiB segment，必须通过 header 版本升级把 `file_size` 扩展为 `u64`，不能在 v1 中隐式允许。

### P1-3: 连续索引 `segment_capacity` 依赖可变 header 长度，未来 header 扩展会改变 timestamp 路由

相关位置:

- `docs/design/index-continuous.md:20-24`: `segment_capacity = floor((index_segment_size - index_header_len) / index_entry_size)`。
- `docs/design/index-continuous.md:37`: 当前 v1 新建文件 `index_header_len` 为 52。
- `docs/design/data-model.md`: segment header 已设计为可变长度 header，未来 TLV/state 可以扩展。
- `src/index/mod.rs:51-52`: 新建 TimeIndex 使用常量 `INDEX_HEADER_SIZE`。
- `src/index/mod.rs:812-815`: reopen 时从第一个已存在 index segment 的 header size 恢复 `index_header_size`。

问题:

连续索引的文件名和 entry index 由 `segment_capacity` 决定，而 `segment_capacity` 又取决于 `index_header_len`。如果未来 header 扩展导致 index segment header 长度变化，同一个 timestamp 会被映射到不同 segment_start/entry_index。当前实现 reopen 时取第一个 segment 的 header size，虽然能维持已有 dataset 的旧 header 计算，但设计并没有声明“连续索引 dataset 的 grid capacity/header_len 必须冻结”。

影响:

- 未来格式升级时，新写入 segment 若使用新 header_len，可能与老 segment 的连续网格不兼容。
- 中间 segment 未创建的 sparse gap 场景下，单靠现有文件 header 不能表达整个 dataset 的 grid 规则。
- 可变 header 的扩展目标与连续索引的稳定路由目标存在潜在冲突。

建议:

在设计中把连续索引 grid 明确持久化为 dataset-level contract:

- 创建 continuous dataset 时计算并冻结 `index_grid_capacity` 或 `index_grid_header_len`，写入 DataSetMeta。
- reopen 后所有 continuous segment 路由都使用 meta 中冻结值，而不是根据某个 segment 当前 header 推导。
- 或者明确 v1 内 continuous index segment header_len 不得变化；可变 header 扩展只能用于不影响 capacity 的字段。

### P1-4: Correction 变长覆盖仍有“移动后续字节”的残留矛盾

相关位置:

- `docs/design/dataset-operations.md:215-242`: out-of-order/correction 强调 index 原地覆盖，不移动已有数据，旧 record 通过 invalid count 标记。
- `docs/design/data-segment.md:207-214`: `overwrite_in_last_block` 前置条件后仍提到实现允许缩小并“移动本 block 后的字节”。
- `docs/design/data-segment.md:216-252`: append tail-only 设计要求 record 必须位于 pending raw block 末尾，按 tail 增长更新 counters。

问题:

前几轮已经把 correction fallback 和 append 收敛为 tail-only / no byte-shift 模型，但 `data-segment.md` 中仍残留“缩小时移动后续字节”的描述。这与当前 block/index offset 稳定性设计冲突，也与“不支持 compaction、不移动后续 record”的总体方向冲突。

影响:

- 后续实现者按该段描述实现 shrink，会破坏后续 record 的 `in_block_offset` 或 block 内数据布局。
- 如果 index 不同步重写所有受影响 record，query/read 可能读错位置。

建议:

文档应统一为:

- correction resize 仅允许目标 record 是物理 tail 且变更后仍不需要移动任何后续字节。
- 非 tail、压缩 block、变长后需要移动后续数据的场景全部走 fallback 追加新 record + 更新 index。
- 当前版本不支持任何 block 内 byte-shift compaction。

### P1-5: Queue consumer state 模型与 poll gap/filler 语义仍不一致

相关位置:

- `docs/design/queue-overview.md:42`: `QueueInner.consumers` 被描述为 `HashMap<String, Vec<Arc<ConsumerStateFile>>>`。
- `src/queue/mod.rs:350`: 实现为 `HashMap<String, Arc<Mutex<ConsumerStateFile>>>`，同组共享一个 state file。
- `docs/design/queue-state-file.md:445-447`: filler entry 被 poll 到时应自动 ack 并跳过。
- `docs/design/queue-state-file.md:505-509`: 描述当前实现从 `processed_ts + 1` 到 latest 线性扫描。
- `src/queue/mod.rs:679-689`: 实际在 `read(next_ts)` 为 None 后调用 `query_index_entries(next_ts, i64::MAX)` 查找第一个非 filler、非 pending entry。

问题:

queue 设计文档中同时存在三套不同模型:

1. 每个 group 多个 `ConsumerStateFile` 的 Vec 模型。
2. 每个 group 一个共享 state file 的实现模型。
3. filler 自动 ack 的模型。
4. 通过 query index 跳过 gap/filler 的实际实现模型。

这些差异不仅是结构体签名漂移，还会影响 at-least-once、pending full、多 consumer 同组抢占、连续索引大量 filler、稀疏 timestamp gap 等核心语义。

影响:

- 按文档实现其它语言 wrapper 或 FFI queue 时，会对同组 consumer 是否共享进度产生错误理解。
- filler/gap 如何推进 `processed_ts` 不清楚，可能导致 consumer 永久卡在 gap、或跳过后无法 ack 连续进度。
- `processed_ts` 与“下一个可消费真实 timestamp”之间可能存在大跨度空洞，文档没有明确这是允许长期保留的状态还是需要压缩推进。

建议:

重新统一 queue 文档:

- 明确每个 `group_name` 只有一个 `ConsumerStateFile`，多个 consumer 共享该 state。
- 明确 poll 的 gap 策略: 不对 filler 创建 pending，不自动 ack filler；而是通过 index 查询寻找下一个真实 entry。
- 定义 `processed_ts` 在存在 gap/filler 时是否可以跳跃推进，或者承认它只是“连续 ack 水位”，真实 poll 需通过 index 查找。
- 更新复杂度说明，不能继续说当前实现只做线性 timestamp 扫描。

### P1-6: Retention reclaim 是否更新 `last_used_at` 的文档自相矛盾

相关位置:

- `docs/design/background-and-cache.md:149`: 回收期间不更新 `last_used_at`。
- `docs/design/dataset-operations.md:601-612`: `DataSet::reclaim_expired_segments()` 第 7 步更新 `self.last_used_at = Instant::now()`。
- `src/dataset.rs:977-995`: 实现中 reclaim 完成后更新 `last_used_at`。

问题:

background 文档把 retention reclaim 定义为不应重置 idle 计时，但 dataset-operations 和实现把 reclaim 视为一次 dataset 使用并刷新 `last_used_at`。这会影响 idle-close 与 retention 的交互。

影响:

- 如果按 background 文档实现，retention 后 dataset 可能很快 idle-close。
- 如果按 dataset-operations/当前实现，retention 后 idle-close 会被推迟。
- 后台任务调度和性能评估会出现不同预期。

建议:

设计上选择一个语义并统一:

- 若 retention 是维护任务，不应延长热度: 移除 reclaim 对 `last_used_at` 的刷新。
- 若 retention 打开/删除 segment 也算 dataset 活动: background 文档应删除“不更新”约束，并说明这是为了避免刚被修改的 segment 立即 idle-close。

### P1-7: Store Rust API 文档仍以 `&self` 展示大量 mutating 操作

相关位置:

- `docs/design/store-and-ffi.md:21-65`: Store 结构和 API 列表显示 `create/open/write/append/delete/open_queue/drop/close` 等多为 `&self`。
- `src/store.rs:42-52`: 实际 Store 包含 `handles`, `next_handle_id`, `read_only_handles`, `bg_tasks` 等文档未列字段。
- `src/store.rs:423-489`: `write_dataset/append_dataset/delete_dataset_record` 实际为 `&mut self`。
- `src/store.rs:617-717`: `open_queue/open_consumer/drop_consumer/queue_push/open_journal_queue` 多为 `&mut self`。

问题:

Store 设计文档对 Rust public API 的 mutability 描述已经落后于实现。当前 Store facade 依赖 handle registry，因此很多操作确实需要 `&mut self`。文档仍以 `&self` 展示会误导外部使用者以为 Store 可在 Rust 侧按共享引用并发调用。

影响:

- Rust API 使用者、wrapper 作者和 FFI 设计者会错误估计 Store 的共享/并发能力。
- 与 P0-1 的 FFI aliasing 问题互相放大: 文档看起来允许共享，实际实现需要独占。

建议:

把 `store-and-ffi.md` 中 Store struct/API 作为真实 public contract 维护:

- 同步 `&mut self` 签名。
- 补全 handle registry、read-only handles、background tasks 等字段说明。
- 明确“内部 DataSet 可并发”与“当前 Store facade 句柄操作需要 &mut self”之间的边界。

## P2 问题与优化建议

### P2-1: `design.md` 仍残留 `retention_ms` 导航名称

相关位置:

- `design.md:44`: “元数据格式·retention_ms”。
- `docs/design/meta-format.md`: 字段已改为 `retention_window`，单位为 timestamp unit。

问题与建议:

这是命名漂移，不影响格式本身，但会让读者误以为仍存在毫秒单位字段。建议把导航文案改为 `retention_window` 或“retention timestamp unit”。

### P2-2: `data-model.md` 的配置结构片段不再完整

相关位置:

- `docs/design/data-model.md:305-342`: StoreConfig/DataSetConfig 片段。
- `src/config.rs`: 实际 StoreConfig 包含 `retention_check_hour`, `enable_background_thread`, `enable_journal` 等；DataSetConfig 包含 `initial_*_segment_size`, `retention_window`, `index_continuous` 等。

问题与建议:

配置结构在多个文档中重复维护，`data-model.md` 片段容易落后。建议将 `data-model.md` 的配置结构改为“字段摘要 + 链接到 cargo/config 或 store-and-ffi”，避免复制完整 struct；或者同步为当前真实字段。

### P2-3: Lazy allocation 扩容步骤写着 flush，但实现只 set_len/remap

相关位置:

- `docs/design/lazy-allocation.md:59-68`: 扩容算法第 6 步写 “flush（确保持久化）”。
- `src/segment/data.rs:285-303`: data segment `expand()` 执行 open/set_len/remap/update file_size，没有 flush。
- `src/index/segment.rs:204-225`: index segment `expand()` 同样 set_len/remap/update capacity，没有 flush。

问题:

扩容的持久化语义不一致。`set_len` 改变文件大小与 mmap dirty data flush 是两类行为；文档写“flush”会让实现者误以为扩容必须同步 mmap 内容或文件元数据。

建议:

明确扩容 crash safety:

- 如果只要求文件大小变更由 OS 文件系统语义保证，删除第 6 步 flush。
- 如果要求扩容后立即落盘，需要定义 flush/sync_data/sync_all 的具体对象和性能代价。

### P2-4: `cargo-and-config.md` 对仓库结构的描述已过期

相关位置:

- `docs/design/cargo-and-config.md:55`: 声称仓库尚未创建 `benches/`。
- `docs/design/cargo-and-config.md:61`: 声称仓库尚未包含 `.github/workflows/`。
- 当前仓库实际存在 `benches/` 和 `.github/workflows/`。

问题与建议:

这是验证策略文档漂移。建议更新为当前真实构建/CI/bench 状态，并明确哪些命令是“已存在 CI 必过”、哪些只是“推荐本地验证”。

### P2-5: C header 存在明显编码损坏文本

相关位置:

- `include/timslite.h:225-230`, `281`, `307` 等注释中出现乱码字符。
- `src/ffi.rs:760`, `853` 等注释分隔符也出现乱码。

问题:

这不直接影响 ABI 编译，但 C header 是外部动态库最重要的用户文档之一。乱码会降低可读性，也可能暗示文件编码处理流程不稳定。

建议:

统一源码和 header 编码为 UTF-8，并将 decorative 分隔符改为 ASCII 注释。后续如果导出 C ABI，建议 header 注释优先使用 ASCII 或受控 UTF-8。

### P2-6: Journal v1 的“辅助日志”定位已经清晰，但消费工具的滞后语义还可再显式

相关位置:

- `docs/design/journal.md:7-9`: 已声明 Journal v1 是 pointer-based 辅助日志，不是自包含 redo log；consumer 必须通过 `read_entry_at_index` 拉取源数据。
- `docs/design/journal.md:271`: `0x13` 不携带 append bytes，consumer 需读取完整 record 后按 offset/len 识别本次追加范围。

问题:

当前文档已经说“源数据被回收/删除/覆盖时不可重放”，但对 consumer 滞后时读到“当前值”而非“事件发生时值”的语义还可以更直白。尤其是 correction、delete、append migration 后，旧 journal record 中的 index pointer 可能指向已被覆盖或逻辑上失效的数据。

建议:

在 journal consumer 章节增加一句强约束:

- Journal v1 consumer 不应把 `0x11/0x12/0x13` 解释为精确历史事件 payload；它只能在源 entry 仍可按该 index pointer 校验读取时提取当前可读数据。
- 需要精确历史 replay 的场景必须等待未来 WAL/checksum/record-version 设计。

## 建议处理顺序

1. 先处理 `P0-1` 和 `P0-2`: 这两个都在 FFI 边界，且可能影响 C ABI 是否可安全发布。
2. 接着处理 `P1-1`: 明确 FFI iterator 的快照/弱一致语义，避免 retention 与 query 并发时出现未定义预期。
3. 再处理 `P1-2` 和 `P1-3`: 它们属于 on-disk format 上界和可扩展性，一旦发布后兼容成本较高。
4. 后续统一 queue、retention、Store API 文档漂移，最后清理 P2 级导航、构建配置和编码问题。

## 本轮未执行的事项

- 未修改任何 `docs/design` 文档。
- 未修改任何 Rust/C/Python 代码。
- 未运行 `cargo test` 或 `cargo check`，因为本轮是设计审查落盘，不涉及实现变更。
