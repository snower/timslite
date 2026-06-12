# timslite 设计审查报告（第 6 轮）

审查日期: 2026-06-12

## 审查范围

本轮仅做设计审查，不调整设计文档、代码、测试或计划文件。

已阅读与交叉核对:

- `design.md`
- `docs/design/*.md`
- `docs/review/archives/Round4/design-review*.md`
- `docs/review/archives/Round5/test-review*.md`
- 为确认当前有效常量/配置边界，辅助查看了 `src/config.rs`、`src/header.rs`、`src/dataset.rs`、`src/bg/mod.rs`、`src/queue/mod.rs`、`tests/config_test.rs`

## 总体结论

第 4 轮设计审查中的多数 P0/P1 项已经被文档或实现吸收，但当前设计文档仍存在几类高风险残留:

1. Queue 状态持久化、gap/filler 消费游标与后台 flush 队列之间的契约没有完全闭合。
2. on-disk header / meta / compression active contract 与旧段落并存，存在多个会误导文件格式实现的旧常量。
3. `retention_window: u64` 与 `timestamp/latest_written_timestamp: i64` 的计算边界未定义，且测试允许 `u64::MAX`。
4. 一些外部 API/FFI 文档仍有 ABI 形态、特殊 timestamp、flush 默认值、append 迁移语义等漂移。

这些问题大多不是代码风格或说明文字问题，而是会影响持久化格式、消费进度、数据可见性、FFI ABI 和未来兼容性的设计契约问题。

## P0 问题

### P0-1: Queue 状态文件的持久化入口与新版 dirty flush queue 设计冲突

证据:

- `docs/design/background-and-cache.md:41-50` 定义后台 flush 只 drain Store 级 `flush_queue`，按出现过的 dataset key 执行 `flush_dirty_segments()`。
- `docs/design/background-and-cache.md:78` 进一步强调 `DataSet::flush()` 会清理队列 stale target，而后台 run_flush 处理 dirty segment target。
- `docs/design/queue-state-file.md:310-337` 又定义 queue state file 只通过后台 flush 统一执行 `mmap.flush()`，并声称 flush 会扫描 queue state files。
- 源码辅助核对显示 `DataSet::flush()` 会调用 `flush_queue_state_files()`，但后台 `run_flush()` 调用的是 `ds.flush_dirty_segments()`，不是 `ds.flush()` (`src/dataset.rs:1043-1058`, `src/bg/mod.rs:350-358`)。

问题:

Queue 的 `poll()` 和 `ack()` 只更新内存状态，不立即 sync，这是合理的 at-least-once 设计取舍。但新版后台 flush 已从“扫描所有 dataset 并 flush 全部状态”改为“drain data/index dirty target 队列”。queue state 变更本身没有被设计为 dirty target，也没有被加入 `flush_queue`。因此在没有 data/index segment dirty target 的情况下，queue ack/pending 状态可能长期不被后台 flush，同步频率不再等于文档承诺的 flush interval。

影响:

- 消费组 `processed_ts`、pending 列表和 ack 状态的持久化可能依赖后续 dataset 写入、显式 `DataSet::flush()` 或 close，而非后台 flush 周期。
- crash 后 replay 窗口可能远大于设计声称的 flush 间隔。
- pending timeout cleanup 也可能因为 queue state 未被后台 flush 路径访问而延后。

建议:

- 在设计上明确 queue state file 是否是 flush target 的一等对象。
- 若是，应引入 `SegmentFlushTarget::QueueState { group_name }` 或 dataset-level queue dirty target，并在 poll/ack/timeout cleanup 后入队。
- 若不是，应修改 queue 文档，明确 queue state 只在 `DataSet::flush()`/close/显式 queue flush 中同步，后台 dirty flush 不保证 queue state 周期落盘。
- 不建议继续保留“Queue 状态文件与 Dataset 分段文件采用相同 Sync 策略”这句泛化表述，除非 queue state dirty 入队协议补齐。

### P0-2: `retention_window: u64` 与 signed timestamp 阈值计算未定义，极大值可能导致错误过期/回收

证据:

- `docs/design/meta-format.md:56`、`docs/design/dataset-operations.md:594-606` 将 `retention_window` 定义为 `u64`，并用 `latest_written_timestamp.saturating_sub(retention_window)` 计算阈值。
- `docs/design/data-model.md:124` 定义 timestamp、时间范围为 `i64 LE`。
- `tests/config_test.rs:197-213` 明确允许创建 `retention_window = u64::MAX` 的数据集。
- 源码辅助核对显示当前计算会把 `retention_window as i64` 传给 `i64::saturating_sub` (`src/dataset.rs:1196-1208`)。

问题:

设计文档没有定义 `u64 retention_window` 如何安全转换到 `i64 timestamp` 坐标系。若允许 `u64::MAX`，它既可以被理解为“极大保留窗口”，也可能在实现中因 cast 变成负数，从而让 `latest - retention` 变成异常大的阈值。阈值一旦错误变大，读路径可能把大量有效数据判为过期，后台 retention reclaim 也可能物理删除本不该删除的 segment。

影响:

- 这是数据可见性和物理回收级别的风险。
- 由于 retention reclaim 是破坏性删除，错误阈值可能造成不可恢复数据丢失。
- 当前文档中“当 latest < retention_window 时 threshold = 0”只覆盖小正数窗口，不覆盖 `retention_window > i64::MAX`。

建议:

- 在设计中二选一:
  - 将 `retention_window` 上限定义为 `i64::MAX`，builder/meta/FFI/open 均拒绝超过该值。
  - 或定义完整的 mixed signed/unsigned 饱和规则，例如 `retention_window >= latest as u64` 时 threshold 为最小可见边界，不发生 cast wrap。
- 为 `u64::MAX` 明确语义: 拒绝、等价无限窗口，或等价不回收。不能只允许持久化而不定义计算。

### P0-3: on-disk header/meta active contract 与旧常量并存，文件格式契约不唯一

证据:

- `docs/design/data-model.md:225-256` 定义 v1 data header 为 124 bytes、index entry area 为 128 bytes。
- `docs/design/data-segment.md:334` 仍写“新建 v1 文件的 `header_len` 为 116”。
- `docs/design/design-decisions.md:11` 仍写“v1 data=116B, index=52B”。
- `docs/design/data-model.md:175-181` 的早期表格仍写 segment header `file_size` 是 4 字节 `u32`，而 `docs/design/data-model.md:431-448` 的 active contract 又改为 `file_size: u64` 和 `compress_type`。
- `docs/design/data-model.md:152` 写 Meta TLV 区“当前 33 bytes”，但同文件 `docs/design/data-model.md:229-245` 的默认 header 计算已是 41 bytes。
- `docs/design/meta-format.md:29` 仍写当前 v1 `meta_values` 固定 74 字节，而 `src/meta.rs` 注释和 active TLV 集合加入 `compress_type` 后已变成 78 字节。

问题:

持久化格式文档中同时存在旧 layout、新 active contract 和局部补丁式说明。读者无法仅凭设计文档判断当前真源到底是:

- data header 116 还是 124;
- index header 52 还是 fixed 128;
- segment header `file_size` 是 `u32` 还是 `u64`;
- meta 是否必须包含 `compress_type`;
- 当前 TLV 长度到底是 33/41/74/78 哪个口径。

影响:

- 这类漂移容易让后续实现、FFI header、Python wrapper 或迁移工具按旧格式写文件。
- 一旦外部用户按错误 header/offset 理解历史文件，兼容成本很高。
- 第 4 轮 P1-2/P1-3 已标记完成，但文档旧常量仍残留，会削弱完成状态的可信度。

建议:

- 在每个相关文档顶部设置“当前 active on-disk contract”小节，旧草案片段移入“历史草案/已废弃”或直接删去。
- 对 `DATA_HEADER_SIZE=124`、`INDEX_HEADER_SIZE=128`、segment `file_size:u64`、dataset/segment `compress_type:u8`、dataset meta TLV length=78 建立单一表格。
- 避免在示意 struct 中保留旧字段宽度，除非明确标记为“旧草案，不可实现”。

## P1 问题

### P1-1: Queue `processed_ts` 与 gap/filler 跳过语义仍未统一

证据:

- `docs/design/queue-overview.md:73-80` 说明 poll 从 `processed_ts + 1` 直接读，miss 后通过索引选择第一个真实 record，filler/gap 不投递、不自动 ack。
- `docs/design/queue-overview.md:406` 再次强调 filler/gap 不 pending、不自动 ack。
- `docs/design/queue-state-file.md:21` 却把 `processed_ts` 定义为“已处理的连续最大时间戳”。
- `docs/design/queue-state-file.md:220-230` 的伪代码只检查 `processed_ts + 1` 是否有 acked pending entry。

问题:

如果数据集中存在真实记录 `10` 和 `20`，中间 `11..19` 是 gap/filler，设计允许 poll 跳过 gap 直接投递 `20`。但 queue-state-file 的 `processed_ts` 连续推进算法要求 `processed_ts + 1` 有 acked entry，ack `20` 后不能从 `10` 推进到 `20`。如果实现选择推进到 `20`，那么 `processed_ts` 就不是“连续最大时间戳”，而是“已 ack 的最后一个真实投递 timestamp”。

影响:

- 消费进度语义不稳定: 它到底代表逻辑连续水位，还是真实 record 水位。
- crash/reopen 后可能重复消费已 ack 的稀疏记录，或在 gap 后无法推进。
- 多 consumer pending 去重和 query 起点都会依赖这个字段，设计必须唯一。

建议:

- 明确 queue consumer 是只保证真实 record 顺序消费，不保证覆盖每个逻辑 timestamp。
- 将 `processed_ts` 重命名或重新定义为 `last_acked_real_ts`，并说明 gap/filler 不需要持久化 ack。
- 如果仍要保持“连续最大时间戳”，则 poll 跳过 gap 时必须把 gap/filler 作为已跳过状态持久化，或者定义可压缩 skip range。

### P1-2: 压缩算法文档仍以 deflate 为主，与 `compress_type`/zstd active contract 冲突

证据:

- `docs/design/compression.md:5-10`、`docs/design/compression.md:23-32`、`docs/design/compression.md:78` 多处写死 `miniz_oxide`、deflate、deflate level。
- `docs/design/dataset-operations.md:122` 和 `docs/design/dataset-operations.md:516` 仍写 deflate 压缩/解压。
- `docs/design/compression.md:84-100` 又补充 active contract: `compress_type=0` 默认 zstd，`1` 为 deflate。
- `docs/design/data-model.md:435-438`、`docs/design/store-and-ffi.md:470-478` 也定义了 zstd 默认。

问题:

主流程仍按 deflate 写，active contract 在文末补充。读者可能以为所有 sealed block 都必须 deflate，而不是按 segment header `compress_type` 选择算法。`compress_level` 的范围和语义也被 deflate 文案主导，但 zstd level 语义不同。

影响:

- 新实现者可能忽略 segment header 中的 `compress_type`，导致读写算法不匹配。
- 文档无法指导混合算法段的读取策略。
- 对外配置和 FFI 文档虽然有 `compress_type`，但核心 compression 文档仍在讲旧模型。

建议:

- 把 `compress_type` active contract 移到 compression 文档开头。
- 所有流程统一写“selected algorithm compress/decompress”，不要在通用路径写死 deflate。
- 单独列出 zstd/deflate 的 level 映射、默认值和非法值处理。

### P1-3: timestamp 正负、`0` 空值、`-1` latest 快捷值的公共契约不完整

证据:

- `docs/design/data-model.md:124` 写 timestamp 为 signed `i64`，并“允许业务使用负 timestamp”。
- `docs/design/dataset-operations.md:167`、`docs/design/dataset-operations.md:340`、`docs/design/dataset-operations.md:418` 写 write/append/delete 均拒绝 `timestamp <= 0`。
- `docs/design/dataset-operations.md:76` 定义 `latest_written_timestamp` 的 `0` 表示空数据集。
- `docs/design/dataset-operations.md:540-544` 和 `docs/design/store-and-ffi.md:340` 定义 `timestamp=-1` 是读取 latest 的特殊值。
- `docs/design/index-continuous.md:59` 同样在连续模式写入流程中拒绝 `timestamp <= 0`。

问题:

设计同时表达了三种互相牵制的口径:

- 磁盘 timestamp 是 signed，且允许业务负 timestamp。
- public write/append/delete 只允许正 timestamp。
- read API 把 `-1` 作为 latest sentinel，`0` 作为空 dataset sentinel。

如果 public API 只允许正 timestamp，则“允许业务使用负 timestamp”应改为“文件格式保留 signed 表达能力，但当前 public API 保留 `<=0` 作为内部/快捷语义”。如果确实要支持负 timestamp，则 `read(-1)` 和 `latest=0` sentinel 都会与合法业务时间冲突，需要替代 API 或额外 option 标记。

影响:

- FFI/Python 调用方无法判断负时间戳是合法数据还是保留值。
- `DataSetInspect` 中 `base_timestamp` 使用 0 sentinel (`docs/design/dataset-inspect.md:185`) 也依赖“0 不可作为业务时间戳”的隐含前提。
- retention 中 “latest < retention_window 时 threshold=0” 也依赖正时间戳模型。

建议:

- 明确当前公共契约是否为 `timestamp > 0`。
- 若是，所有“允许负 timestamp”改成格式层说明，不作为 public API 承诺。
- 若否，新增 `read_latest()`/FFI 专用 latest API，移除 `-1` sentinel，并为 empty latest 使用 `Option<i64>` 或显式状态。

### P1-4: Append 迁移阈值契约互相冲突

证据:

- `AGENTS.md:122-123` 当前存储契约写明: 追加到已有 latest record 后如果超过 append migration threshold，需要迁移整条逻辑 record 到 single-record block；70% threshold 只适用于追加已有 latest record。
- `docs/design/data-model.md:24` 写 append 不再因为比例阈值迁移为独占 block，超过普通 block 可承载范围则返回错误。
- `docs/design/data-segment.md:255`、`docs/design/data-segment.md:299-300`、`docs/design/dataset-operations.md:375-376`、`docs/design/dataset-operations.md:394` 均按“不迁移，返回错误”描述。
- `docs/review/archives/Round5/test-review-todo.md:43` 的任务标题仍叫“append 迁移阈值 (70%)”，但说明变成“超过 block 容量时返回错误”。

问题:

append 已有 latest record 的增长策略存在两个互斥版本:

- 迁移到 single-record block。
- 不迁移，普通 pending block 放不下时返回错误。

影响:

- Journal `0x13` 的 `index_info`、`data_offset`、queue notify、cache invalidation 在迁移/不迁移两种设计下不同。
- 测试和文档标题会误导后续补齐 cache invalidation 或 wrapper 行为。

建议:

- 选择一个 active contract。
- 若迁移已废弃，应同步 AGENTS/current storage contract 和 Round5 TODO 残留命名。
- 若迁移仍是目标，应补齐迁移算法、旧缓存失效、journal append_info、single-record block 写入和失败回滚边界。

### P1-5: FFI 轻量读接口 ABI 在两个设计文档中不一致

证据:

- `docs/design/dataset-read-operations.md:331-334` 将 `tmsl_dataset_query_length` 描述为返回 `uint32_t*`，只包含长度数组。
- `docs/design/store-and-ffi.md:371-376` 将同一函数描述为通过 `out_array: *mut *mut c_void` 返回 12 字节元素 `(timestamp: i64, data_len: u32)`。
- `docs/design/dataset-read-operations.md:206-208` 的 Rust 返回值是 `Vec<(i64, u32)>`。

问题:

C ABI 对同一函数的返回内存布局没有唯一契约。仅返回 `uint32_t*` 会丢失 timestamp，返回 12 字节 packed pair 又需要明确对齐、padding、释放函数和元素读取方式。

影响:

- C/Python wrapper 可能按错误布局解析内存。
- ABI 一旦发布，修正成本高。

建议:

- 定义 `#[repr(C)]` / C header 中的 `TmslLengthEntry { int64_t timestamp; uint32_t data_len; }`，并明确 `sizeof`、alignment、array_len 含义。
- 删除 `uint32_t*` 旧签名，或另设只返回长度的函数名。

### P1-6: `query_exist` 的 retention 与范围上界契约不清晰

证据:

- `docs/design/dataset-read-operations.md:154-158` 写 `query_exist` 会先 `clamp_query_range()` 到 retention 窗口。
- `docs/design/dataset-read-operations.md:159-162` 又强调它“不读取数据段、不限制范围大小”。
- `docs/design/dataset-operations.md:54-60` 将 read_exist/query_exist 定位为索引存在性检查，其中 read_exist 明确“不检查 retention”。
- 源码辅助核对显示 `query_exist` 直接计算 `count = (end_ts - start_ts + 1) as usize`，没有 clamp 或 checked range (`src/dataset.rs:948-960`)。

问题:

存在性 API 到底表示“索引物理存在”还是“当前 retention 可见范围内存在”，文档没有统一。除此之外，“不限制范围大小”在 FFI/Rust 中都存在整数溢出和巨量 bitmap 分配风险。

影响:

- 调用方可能把过期 entry 当成仍可读数据，或相反，无法用 query_exist 判断索引物理状态。
- `end_ts - start_ts + 1` 在 i64 边界会溢出；超大范围会导致内存耗尽。

建议:

- 区分 `query_exist` 和 `query_visible_exist`，或明确所有 exist API 是否忽略 retention。
- 设计 range 上限和 checked arithmetic 行为，例如超过最大 bitmap 字节数返回 `InvalidData`。

### P1-7: Flush 默认值仍在 15s 和 10min 之间漂移

证据:

- `docs/design/background-and-cache.md:11`、`docs/design/background-and-cache.md:41` 写默认 flush 15s。
- `src/config.rs:62` 当前默认也是 15s。
- `docs/design/dataset-operations.md:321` 写 flush 默认 10 分钟。
- `docs/design/queue-state-file.md:337` 写 flush 每 10 分钟。
- `docs/design/memory-and-concurrency.md:43` 和 `docs/design/design-decisions.md:33` 仍写 10min。

问题:

flush interval 是 crash replay 窗口、queue 状态持久化窗口、后台测试和运维调优的重要参数。文档同时存在两个默认值，会让使用方误判 durability/performance 取舍。

建议:

- 统一为当前默认 15s，若 10min 是历史或建议配置，应明确标注。
- 设计决策表中的默认值也要同步，否则它会被视为架构级真源。

## P2 问题与优化建议

### P2-1: `read_length`/`query_length` 的“只读 header”描述有 8B/12B 口径差异

证据:

- `docs/design/dataset-read-operations.md:23` 写 read_length 系列读取 record header “8 bytes”。
- `docs/design/dataset-read-operations.md:275-279` 和 `docs/design/data-model.md:75-79` 定义 record header 是 `data_len: u32 + timestamp: i64 = 12 bytes`。

建议:

- 统一为 12 bytes。
- 如果优化只读取 `data_len` 4 bytes，应明确仍需 timestamp 校验时读取完整 12 bytes，不能把 8 bytes 作为 record header。

### P2-2: `DataSetInspect` 的 sentinel/nullability 依赖隐含正 timestamp 前提

证据:

- `docs/design/dataset-inspect.md:185` 定义 FFI `base_timestamp` 用 0 表示 None。
- `docs/design/dataset-inspect.md:335-342` 对 min/max timestamp 和 base_timestamp 的空值转换写“0 或 None”。

问题:

如果最终公共 timestamp 契约明确为 `timestamp > 0`，0 sentinel 可以成立。但若保留 signed/负 timestamp 能力，FFI inspect 的 0 sentinel 不够表达 `Some(0)`，也无法区分空段 sentinel 与真实 0。

建议:

- 跟随 P1-3 的 timestamp 结论统一。
- FFI inspect 更稳妥的做法是增加 `has_base_timestamp`、`has_min_timestamp`、`has_max_timestamp` 这类显式 flag。

### P2-3: `design.md` 与模块说明仍残留 deflate-only 描述

证据:

- `design.md:23` 写压缩策略是 `miniz_oxide deflate`。
- `docs/design/architecture.md:109` 写 `compress.rs` 是 `deflate_compress/decompress + size comparison`。

建议:

- 改成“selected compression algorithm (zstd default, deflate supported)”一类中性描述。

### P2-4: Queue state file open_existing 缺少格式校验清单

证据:

- `docs/design/queue-state-file.md:88-119` 的 open_existing 伪代码校验 magic/version，但没有列出 state_length、pending_value_size、pending_length 上界、文件长度等校验。

建议:

- 设计中补齐:
  - 文件长度必须为 4096。
  - `state_length == 8`。
  - `pending_value_size == 17`。
  - `pending_length <= 239`。
  - `21 + pending_length * 17 <= 4096`。
  - pending status 只能是 0/1。
  - pending timestamps 的排序/去重要求。

## 建议处理顺序

1. 先处理 P0-1 和 P0-2: 一个影响 queue 消费进度持久化，一个影响 retention 物理回收安全。
2. 接着处理 P0-3: 将 on-disk active contract 收敛成唯一真源，避免后续修复继续建立在旧格式片段上。
3. 然后处理 P1-1、P1-3、P1-4: 这些都是 public 行为语义，需要在测试和 wrapper 继续扩展前定稿。
4. 最后统一 FFI 轻量读 ABI、flush 默认值、compression 文案和 inspect/nullability 细节。

## 本轮未做事项

- 未修改任何设计文档、源码、测试或计划文件。
- 未运行 cargo 测试；本轮是设计审查，不验证实现正确性。
- 未生成 TODO 文件；若需要，可在确认本报告后再拆分为 `docs/review/design-review-todo.md`。
