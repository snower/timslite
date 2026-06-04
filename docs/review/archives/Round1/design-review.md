# timslite 设计审查报告

审查日期: 2026-05-29

审查范围:
- `design.md`
- `docs/design/*.md`
- 对少量公开接口和核心实现做了交叉抽查, 用于识别设计文档与现状的漂移

本报告只记录设计缺陷、逻辑风险、文档不一致和优化建议。未对现有设计文档、计划文档或源码做调整。

## 结论概览

当前设计已经覆盖了 mmap 段文件、Block 聚合、索引、FFI、后台任务、懒分配、删除和 retention 等主要模块, 但仍有若干影响文件格式正确性、数据一致性和长期可维护性的关键问题。最需要优先收敛的是:

1. 单条超大 record 与 `data_len: u16` 编码冲突, 存在格式层面的数据截断风险。
2. 连续索引模式按每个整数 timestamp 填 filler, 对 Unix 秒/毫秒时间戳不可扩展。
3. Header 声称 TLV/state 可扩展, 但数据区起点又固定为 116/52 字节, 兼容性模型自相矛盾。
4. 纠正写入允许修改 sealed raw block, 与 BlockCache 的不可变缓存假设冲突。
5. mmap 多字段写入、数据/索引更新、in-memory index flush 缺少事务边界, crash 安全表述过于乐观。
6. QueryIterator 文档目标是“索引与数据都惰性”, 但设计和现有 FFI 路径仍保留全量 `IndexEntry` 向量。

## 严重问题

### P0-1: 超大 record 设计与 `data_len: u16` 无法同时成立

证据:
- `data-model.md:20` 允许单条 record 超过 64KB, 并作为独占 Block 存储。
- `data-model.md:56-64` 定义每条 record 的 `data_len` 为 `u16`。
- `data-model.md:75-82` 定义 `in_block_offset` 为 `u16`。
- `index-continuous.md:95` 还把 `0xFFFF` 作为 `in_block_offset` 哨兵, 假定合法偏移不会达到该值。

问题:
- `u16` 最大只能表示 65535。若 `data.len() > 65535`, record payload 中的长度字段无法表达真实长度。
- `BlockHeader.block_payload_size` 是 `u32`, 但内部 record 长度仍会溢出或截断。
- `BLOCK_FLAG_SINGLE_RECORD` 只能说明该 block 只有一条记录, 不能解决 record 自身长度编码问题。
- 如果未来允许 `block_max_size` 配置大于 64KB, `in_block_offset: u16` 也会失去边界保证。

建议:
- 二选一并写入文件格式约束:
  - 方案 A: 明确 `data_len <= u16::MAX`, 超过直接返回错误, 同时删除“单条 record 可超过 64KB”的承诺。
  - 方案 B: 为 `SINGLE_RECORD` 定义专用编码, 例如 `data_len = 0xFFFF` 后接 `u32/u64 actual_len`, 或把所有 record 的 `data_len` 升级为 `u32`。
- 明确 `block_max_size <= 65536` 是否是硬约束, 并在 StoreConfig/DataSetMeta 中校验。

### P0-2: 连续索引 filler 机制对真实时间戳不可扩展

证据:
- `index-continuous.md:30-39` 规定连续模式在 `latest+1..timestamp-1` 范围内逐个填充 filler。
- `dataset-operations.md:162` 说明连续模式总是存在 filler 条目。
- `dataset-operations.md:418` 又说明 retention 单位应与 timestamp 一致, 通常可能是毫秒。

问题:
- 如果第一条写入就是 Unix 秒级 `1700000000`, 从 `latest=0` 填到该值会生成约 17 亿条 filler。
- 如果使用 Unix 毫秒, 数量会达到万亿级, 不可能落地。
- `index-continuous.md:113-124` 提到“纯 filler segment 跳过创建”, 但设计仍以逐条 filler 为基础, 没有定义 range filler、稀疏区间或跳段索引。
- 该机制会让连续模式在常见时间戳单位下不可用, 也会让 `latest_written_timestamp` 恢复和 O(1) 查找依赖不可接受的物化成本。

建议:
- 为连续模式引入显式粒度和基准, 例如 `time_step`, `base_timestamp`, `max_gap_fill`。
- 对大 gap 使用区间元数据表示, 不逐条写 filler。
- 明确第一条写入不从 0 填充, 而是把该 timestamp 作为第一个连续段的基准。
- O(1) 查找应基于“连续段描述 + 文件内 entry 索引”, 而不是全局 timestamp 差值直接展开。

### P0-3: Header 可扩展性与固定数据区起点矛盾

证据:
- `data-model.md:98-103` 设计了可变 `meta_length` 和 `state_length`。
- `data-model.md:178-183` 又固定 `DATA_HEADER_SIZE = 116`, `INDEX_HEADER_SIZE = 52`, 并把数据区起点固定为这两个常量。
- `data-model.md:189-193` 声称未来新增 meta/state 字段后旧版本可跳过并继续读取。
- `design-decisions.md:11` 和 `design-decisions.md:25` 仍写着 100 字节 header、state 7x8B, 与当前 116/52 字节设计不一致。

问题:
- 如果未来增加 TLV 或 state 字段, header 实际长度会超过 116/52, 后续 Block/IndexEntry 起点会后移。
- 旧代码若仍用固定常量读取数据区, 会把新增 header 字节误当作 Block 或 IndexEntry。
- 如果坚持固定起点, `meta_length/state_length` 实际只能在预留区内变化, 但文档没有定义预留上限和 padding。

建议:
- 选择一种格式策略:
  - 固定 header: 预留固定大小, TLV/state 不得超出预留区, 未用部分 padding。
  - 可变 header: 文件头持久化 `header_size`, 所有 `wrote_position`、block offset、index offset 都基于 `header_size` 动态计算。
- 清理 `design-decisions.md` 中 100B/7x8B 的旧描述。

### P0-4: 缓存不可变假设与纠正写入冲突

证据:
- `background-and-cache.md:384-391` 规定 BlockCache 只缓存 seal block payload, 假设 seal 后不可变。
- `data-model.md:22`, `data-segment.md:176`, `index-continuous.md:76` 都允许纠正写入直接修改 pending block 或 `SEALED` 但未压缩的 block。
- `dataset-operations.md:169` 又把“block 已密封”列为回退原因, 与前述“SEALED 无 COMPRESSED 可原地修改”冲突。

问题:
- sealed raw block 可能已经被查询读入 BlockCache。随后纠正写入若原地修改该 block, 全局缓存会继续返回旧数据。
- pending block 是否可读、是否可缓存没有形成一致规则。文档说只缓存 sealed block, 但读取最新数据必须能够读取 pending block。
- “SEALED 是否允许修改”在不同文档中互相矛盾。

建议:
- 明确状态机:
  - pending: 可写、可读但禁止进入全局缓存。
  - sealed raw: 如果允许纠正写入, 必须在写入前后按 `(segment_file_offset, block_offset)` 失效缓存。
  - sealed compressed: 不原地改, 只能 append + update index。
- 或者更简单: 纠正写入只允许 pending block, sealed block 全部走 append fallback。
- 在设计中补充 cache invalidation API, 并把 correction/delete/out-of-order 与缓存一致性串起来。

### P0-5: crash safety 缺少事务边界, 现有表述过于乐观

证据:
- `memory-and-concurrency.md:54-64` 认为最多损失 flush 间隔内未 sync 的数据, reopen 可安全密封 pending。
- `dataset-operations.md:213-234` 的乱序写入需要追加新数据、覆盖旧索引、增加旧段 `invalid_record_count`。
- `dataset-operations.md:242-250` flush 仅 msync, 不改变 block 状态。
- `time-index.md:14-17` 和 `query-iterator.md:15-18` 说明索引存在 in-memory buffer。

问题:
- mmap 多字段更新不是事务。BlockHeader、payload、DataFileMetadata state、IndexEntry、invalid_record_count 任一处 partial write 都可能形成不一致。
- 数据写入与索引写入之间没有 commit marker。crash 可能留下“数据已落盘但索引丢失”或“索引指向未完整数据”的状态。
- TimeIndex in-memory buffer 在 flush threshold 前没有落盘。数据 mmap 可能被 OS 写回, 但索引仍只在内存中, reopen 后无法定位这批数据。
- “meta 文件一次性写入, 不存在部分写入问题”的说法缺少 temp file + fsync + rename + directory fsync 这类原子创建协议。

建议:
- 定义最小事务协议:
  - append record 时先写 payload, 再写 block header/state, 最后写 index commit。
  - IndexEntry 可增加 valid/commit 标志或段级 commit position。
  - reopen 时可以扫描 data block 重建缺失索引, 或至少截断到最后一致点。
- 为 meta 创建采用临时文件、fsync、rename、fsync parent dir。
- 给 header 和 block 增加 checksum 或 version/sequence, 用于识别 torn write。

## 高优先级问题

### P1-1: `block_offset` 坐标系在文档中不一致

证据:
- `data-model.md:81` 说 `block_offset` 是 Block 在数据段中的绝对字节偏移, 指向 BlockHeader。
- `data-segment.md:217` 说通过 `time_index.find_entry` 获取 `(block_offset, in_block_offset)`。
- `dataset-operations.md:189-192` 又把 `block_offset - seg.file_offset` 作为段内相对偏移。
- `dataset-operations.md:234` 用 `file_offset = (block_offset / segment_size) * segment_size` 路由旧数据段。

问题:
- “绝对字节偏移”可能被理解为含文件头的文件内 offset, 也可能是跨 segment 的逻辑数据区 offset。
- 设计中同时出现 `seg_offset`, `block_rel_offset`, `new_block_offset`, `block_offset` 等名字, 但缺少一个统一坐标定义。
- 这会影响 FFI 稳定性、缓存 key、segment 路由、debug 工具和未来格式兼容。

建议:
- 明确命名:
  - `segment_file_offset`: 段文件名使用的逻辑起始 offset。
  - `block_global_offset`: 跨数据集数据流、相对数据区起点的全局 offset, 用于 IndexEntry。
  - `block_segment_offset`: 单个 segment 内相对 `DATA_HEADER_SIZE` 的 offset。
  - `file_byte_offset`: 真实文件内 byte offset, 等于 `DATA_HEADER_SIZE + block_segment_offset`。
- 在 `data-model.md` 的 IndexEntry 章节给出转换公式。

### P1-2: retention 只钳制 query, 没有定义 read/delete/write 与过期数据的关系

证据:
- `dataset-operations.md:450-465` 只描述 `query_iter` 对过期范围做 start clamp。
- `dataset-operations.md:363-391` 的 `read(timestamp)` 没有说明 retention clamp。
- `dataset-operations.md:434-441` 回收会物理删除过期 data/index segment。
- `time-index.md:173-174` 要求数据段与索引段成对回收。

问题:
- 如果一个 index segment 同时包含过期和未过期条目, 它不能整体删除；但过期 data segment 可能已经删除。此时 `read(old_ts)` 仍可能从索引找到旧 entry, 再读到不存在的数据段。
- `delete(old_ts)` 和乱序重写过期 timestamp 的语义未定义。
- “数据段与索引段成对回收”与二者不同分段边界不天然一致。

建议:
- 对所有读路径统一 retention 语义: `read(ts)` 若 `ts < threshold` 直接返回 None/Expired。
- 明确过期 timestamp 是否允许 delete 或 out-of-order rewrite。
- 设计索引回收时保留混合 index segment 的处理策略, 例如 segment 内 tombstone、二级 min/max、或延迟删除 data 直到没有活跃索引引用。

### P1-3: 后台任务锁顺序文档自相矛盾

证据:
- `background-and-cache.md:304-312` 写锁顺序为 `datasets` -> `state` -> `DataSet`。
- `background-and-cache.md:192-199` 和 `background-and-cache.md:242-251` 又描述后台线程/tick 先拿 `state` 锁, 再执行任务。
- `background-and-cache.md:299-302` 说明 tick 和 `next_background_delay` 会因 `state` 锁互相阻塞。

问题:
- 死锁证明依赖全局锁顺序, 但文档中给出的顺序和实际执行描述相反。
- 如果未来 Store 操作在持有 `datasets.write()` 时调用 tick 或 next_delay, 就会形成潜在循环等待。
- 文档同时说 `next_background_delay()` 只短暂阻塞, 又承认 state 锁可能覆盖 flush/retention 全执行周期。

建议:
- 固化一个唯一锁顺序, 并让所有 API 遵守。
- 更推荐后台执行时不要在长耗时 IO 期间持有 `state` 锁: 先计算 due tasks, 释放 state, 执行任务, 再以 CAS/sequence 更新状态；或者明确接受长阻塞并更新文档。
- 增加“禁止在持有 datasets write lock 时调用后台任务 API”的设计约束。

### P1-4: FFI 配置与生命周期设计不完整

证据:
- `store-and-ffi.md:113-115` 要求用 `tmsl_store_open_with_config` 禁用后台线程, 且称声明已存在、实现待补齐。
- `store-and-ffi.md:170` 在 FFI 列表中列出 `tmsl_store_open_with_config`。
- 抽查公开头和 FFI 实现只看到 `tmsl_store_open/close/tick/next_delay`: `include/timslite.h:30`, `src/ffi.rs:92`, 未见 `open_with_config`。
- `store-and-ffi.md:190-193` 的 `tmsl_dataset_create` 不包含 `initial_data_segment_size` 和 `initial_index_segment_size`, 但 meta 已把二者作为创建参数持久化, 见 `meta-format.md:48-49`。

问题:
- C 调用方无法设置 `enable_background_thread=false`、cache、idle、initial segment size、block_max_size 等关键参数。
- FFI dataset/iterator 持有 `store_ptr` 裸指针, 文档没有规定 store 关闭后子句柄如何失效, 也没有引用计数或运行时检测策略。
- `tmsl_iter_free_data` 被 read 和 iterator 复用, 名称上会误导 read 调用方。

建议:
- 完整设计 `StoreConfigFFI` 和版本字段, 并同步 `include/timslite.h`、FFI 文档、实现计划。
- 设计父子句柄生命周期: store close 前必须关闭所有 dataset/iter, 或 store 持有 Arc 并让子句柄延长生命周期。
- 增加 `tmsl_data_free` 作为通用释放函数, 保留旧名作为兼容别名。

### P1-5: QueryIterator 目标与设计/现状不一致

证据:
- `query-iterator.md:54-56` 目标是索引按需取出、FFI 不需要每条 malloc。
- `dataset-operations.md:323-358` 写新版流程通过 `TimeIndex.prepare_query()` 返回 source 列表, 不加载实际数据。
- `query-iterator.md:346-348` 估算 100 万条记录查询只需约 64KB, FFI Iterator 不持有全数据。
- 现有抽查显示 `DataSet::query_iter` 仍调用 `time_index.query()` 得到 `Vec<IndexEntry>`: `src/dataset.rs:379-389`；`FfiIterator` 仍持有 `entries: Vec<IndexEntry>`: `src/ffi.rs:77-81`。

问题:
- 即使数据按需读取, 大范围查询仍会一次性持有全部 IndexEntry, 100 万条约 18MB 起步, 与 64KB 目标不一致。
- `TimeIndex::query()` 仍会 sort + dedup, 大范围查询 CPU 和内存仍是 O(n)。
- FFI “零拷贝”是目标, 但文档又推荐先保持 malloc 方案, 需要把目标、当前方案、未来方案分开写清楚。

建议:
- 将 QueryIterator 文档拆成“当前已实现”和“目标设计”两部分。
- 真正实现 source iterator: in-memory slice、open segment range、closed segment cursor, 每次只读取下一个 IndexEntry。
- FFI 可先保持 malloc, 但性能表不能宣称零拷贝或 64KB 上限。

### P1-6: 压缩状态机不一致

证据:
- `compression.md:8` 说压缩后不缩小则保留 raw, 不设 COMPRESSED。
- `compression.md:10` 和 `compression.md:27-28` 又说超大 record 独占 block 后立即 `SEALED | COMPRESSED | SINGLE_RECORD`。
- `compression.md:9` 说 idle-close 不压缩, 压缩延迟至 next write overflow。

问题:
- 超大 record 若压缩无收益, 是否仍强制 COMPRESSED 不清楚。
- idle-close 产生的 sealed raw block 已经不再是 pending, 后续 write overflow 不会再处理它；因此“压缩延迟至 next write overflow”不成立, 它会长期保持 raw, 除非另有后台压缩/compaction。

建议:
- 明确每个状态转换:
  - pending overflow: seal, 尝试压缩, 无收益保留 raw。
  - idle-close/recover: seal raw, 后续是否压缩由单独后台任务决定。
  - single record: 尝试压缩, 根据收益设置 COMPRESSED。
- 如果希望 idle sealed raw 后续也被压缩, 需要设计 background compression 或 compaction。

## 中优先级问题

### P2-1: DataSetMeta 的“不一致校验”语义不清

证据:
- `meta-format.md:96-106` 写 open 时对 `data_segment_size/index_segment_size/compress_level/index_continuous/initial_*/retention_ms` 做一致性处理。
- `dataset-operations.md:8-13` 又强调创建和打开分离, 打开时从 meta 文件读取, 不可修改。

问题:
- open API 没有传入这些 DataSet 参数, 因此“不一致”到底和谁比不清楚。
- 如果和 StoreConfig 默认值比, 会导致用不同 StoreConfig 打开旧数据集时误报或误警。
- `block_max_size` 没有进入 DataSetMeta, 但它影响写入时 block 切分和 `in_block_offset` 约束；是否允许跨 reopen 改变没有设计说明。

建议:
- 明确 meta 是唯一真源。open 不应因为 Store 默认配置不同而报错。
- 把 `block_max_size` 是否持久化作为显式决策写入 meta-format。
- 若某些 StoreConfig 只是“新建数据集默认值”, 应单独命名为 create defaults。

### P2-2: retention 调度时间点缺少时区定义

证据:
- `store-and-ffi.md:61-81` 把 `retention_check_hour` 描述为每日小时。
- `background-and-cache.md:105-124` 使用 `SystemTime` 到 UNIX epoch 后按 86400 取模计算。

问题:
- 该算法实际上按 UTC 日边界计算, 不是本地时区的“午夜/指定小时”。
- 对嵌入式或跨时区部署, “每天 0 点”会有歧义。

建议:
- 明确 `retention_check_hour` 是 UTC hour 还是 local hour。
- 如果要本地时间, 需要引入 timezone/chrono 策略, 或允许调用方直接用手动 tick 控制。

### P2-3: 目录名缺少合法性和转义规则

证据:
- `architecture.md:29-54` 直接把 `{dataset_name}/{dataset_type}` 作为目录。
- `store-and-ffi.md:240-245` C 示例也直接传入字符串。

问题:
- 名称中若包含路径分隔符、`..`、Windows 保留名、控制字符或过长组件, 会破坏目录隔离保证。
- FFI 调用方传入非预期路径字符时, 可能越过 `data_dir`。

建议:
- 设计 dataset name/type 的字符集和长度上限。
- 或使用 percent/base64url escaping, 并在 meta 中保存原始名称。
- Store 层必须校验 canonical path 仍在 `data_dir` 下。

### P2-4: 缺少 compaction 设计, `invalid_record_count` 只能统计不能回收

证据:
- `dataset-operations.md:281-287` 说明 delete/out-of-order 只增加 `invalid_record_count`, 未来 compaction 才会物理清除。
- `data-model.md:133-141` 已把 `invalid_record_count` 放入持久 state。

问题:
- 高频纠正、乱序更新和 delete 会让数据段不断膨胀, 但 retention 只能按时间段删除整段。
- `invalid_record_count` 没有触发阈值、compaction 输出段、索引重写、并发读写隔离设计。

建议:
- 增加 compaction 专题设计或明确当前不支持。
- 定义 compaction 触发条件、目标段写入、索引批量重写、crash recovery、旧段删除协议。

### P2-5: 文档与构建配置存在漂移

证据:
- `cargo-and-config.md:19-21` 声明 `[[bench]] name = "timslite_benchmarks"`。
- 抽查 `Cargo.toml` 只有 `criterion` dev-dependency, 未见 `[[bench]]`: `Cargo.toml:17`。
- `architecture.md:75-96` 模块结构没有列出当前 `src/query/*`, 但 `design.md:23` 已有查询迭代器专题。

问题:
- 构建文档可能让维护者误以为 `cargo bench` 有明确 bench target。
- 模块索引和实际设计专题不一致, 会降低后续维护效率。

建议:
- 在计划或文档中明确 benchmark 是待建还是已建。
- 更新架构模块树, 纳入 `query/mod.rs`, `query/iter.rs`, `query/hot_block.rs`。

## 其它优化建议

1. 为文件格式增加 `format_version` 迁移章节: 每次变更 header、IndexEntry、Record encoding 都必须写迁移策略。
2. 为所有 on-disk integer 明确 signed/unsigned、endianness、溢出策略和最大值校验。
3. 为 `latest_written_timestamp` 区分“最高时间戳”和“最新有效记录”。当前删除 latest 后 `read(-1)` 返回 None 是合理选择, 但应在 API 语义中更醒目地说明。
4. 为 read/query 明确是否允许读取 pending block, 并定义 pending block 的缓存禁用规则。
5. 为 `madvise` 写清楚平台兼容策略。`memory-and-concurrency.md:5-6` 提到 SEQUENTIAL/WILLNEED, 但没有说明 Windows 和非 Unix 平台的降级行为。
6. FFI 应明确空数据 `data_len=0` 的 malloc/free 约定, 避免 C 侧把 null 当错误或 Rust 侧把 `malloc(0)` 的 null 当失败。

## 建议的修复顺序

1. 先锁定文件格式: record 长度编码、header size 策略、block offset 坐标系。
2. 再锁定一致性模型: 数据/索引写入顺序、pending recovery、cache invalidation、crash recovery。
3. 然后重写连续索引设计: 时间粒度、gap 表示、跳段查找、first write 行为。
4. 接着补齐 FFI 配置和生命周期: `StoreConfigFFI`, 子句柄失效规则, 通用 free 函数。
5. 最后整理性能设计: 真正惰性的 QueryIterator、retention/compaction、benchmark 文档。

