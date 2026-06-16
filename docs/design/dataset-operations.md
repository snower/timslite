# 数据集操作 - DataSet 生命周期 + 写入/读取流程

## 八、DataSet: 数据集

### 8.1 生命周期: create / open / close / drop

> **核心原则**: 创建和打开分离。参数仅在创建时传入, 打开时从 meta 文件读取, 不可修改。

```rust
struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,
    config: DataSetConfig,     // 从 meta 文件读取 (创建时写入, 之后不可变)
    retention_window: u64,     // 数据保留窗口 (timestamp unit, 0=不限)
    segments: DataSegmentSet,
    time_index: TimeIndex,
    runtime_context: DataSetRuntimeContext, // Store 注入的 BlockCache + JournalSink
    last_used_at: Instant,
    latest_written_timestamp: Option<i64>,  // 写入过的最大 timestamp, 不是最新有效 record, 同时作为回收基准
}

struct DataSetRuntimeContext {
    block_cache: Option<Arc<BlockCache>>,
    journal: Option<Arc<dyn DataSetJournalSink>>,
    read_only: bool,
}

impl DataSet {
    /// 创建新数据集 (显式创建, 已存在返回错误)
    fn create(
        id: DataSetKey, base_dir: PathBuf,
        data_segment_size: u64, index_segment_size: u64,
        compress_level: u8,
        index_continuous: u8,
        initial_data_segment_size: u64, initial_index_segment_size: u64,
        retention_window: u64,
    ) -> io::Result<Self>;

    /// 打开已有数据集 (参数从 meta 文件读取, 不能设置)
    fn open(id: DataSetKey, base_dir: PathBuf) -> io::Result<Self>;

    /// 关闭数据集 (flush + 关闭所有 segment)
    fn close(&mut self) -> io::Result<()>;

    /// 删除整个数据集 (删除目录及所有文件)
    fn drop_dataset(base_dir: &Path) -> io::Result<()>;

    fn write(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;
    fn read(&mut self, timestamp: i64) -> io::Result<Option<(i64, Vec<u8>)>>;
    fn read_latest(&mut self) -> io::Result<Option<(i64, Vec<u8>)>>;
    fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<(i64, Vec<u8>)>>;
    fn query_iter(&mut self, start_ts: i64, end_ts: i64) -> io::Result<QueryIterator<'_>>;

    // 轻量级读操作 (仅索引或 record header)
    /// 检查索引 entry 是否存在 (包括 filler)。timestamp 为精确业务时间戳。
    /// 不读取数据段，不检查 retention，性能最优。
    fn read_exist(&self, timestamp: i64) -> io::Result<bool>;

    /// 范围索引存在性检查，返回位图。位 i 代表 (start_ts + i) 是否存在。
    /// 不读取数据段，不限制范围大小，调用方需自行解析位图。
    fn query_exist(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<u8>>;

    /// 读取单条记录的逻辑数据长度。timestamp 为精确业务时间戳。
    /// 跳过 filler 和过期记录，仅读取 record header (12 bytes)。
    fn read_length(&mut self, timestamp: i64) -> io::Result<Option<u32>>;

    /// 范围查询数据长度，返回有效记录列表。跳过 filler 和过期记录。
    /// 每个元素为 (timestamp, data_len)，按时间戳排序。
    fn query_length(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<(i64, u32)>>;

    /// 惰性范围数据长度迭代器。支持 HotBlockCache，按需读取 record header。
    fn query_length_iter(&mut self, start_ts: i64, end_ts: i64) -> io::Result<QueryLengthIterator<'_>>;

    fn flush(&mut self) -> io::Result<()>;
    fn config(&self) -> &DataSetConfig;

    /// 写入过的最大时间戳 (None = 数据集为空)
    /// open 时从最后一个索引分段文件的最后一条 entry 恢复; 写入时在内存中维护
    fn latest_written_timestamp(&self) -> Option<i64>;

    /// 删除指定时间戳的记录 (索引标记为哨兵, 数据段 invalid_record_count++)
    fn delete(&mut self, timestamp: i64) -> io::Result<()>;

    /// 向记录追加数据: 不存在则按正序写入创建; 仅允许追加到最新未压缩末尾记录
    fn append(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;

    /// 回收超过有效期的分段文件 (需先 close)
    /// retention_window=0 时跳过; retention_window > 0 时计算过期阈值并删除过期分段
    fn reclaim_expired_segments(&mut self) -> io::Result<usize>;

    /// 获取 retention_window 配置
    fn retention_window(&self) -> u64;
}
```

`DataSet::create/open` 直接构造的独立实例默认没有全局 cache 和 journal sink。由 `Store` 创建、打开或扫描加载的普通业务实例必须在放入 registry 前注入 `DataSetRuntimeContext`, 因此外部即使通过 `Store::get_dataset` 直接持有 `Arc<Mutex<DataSet>>`, 再调用 `DataSet::write/append/delete/read/query` 也应获得与 Store 门面一致的 cache/journal 行为。`.journal/logs` 不再作为 `DataSet` 暴露, 由 `JournalManager` 管理专用 `JournalLog`。`*_with_cache`、`*_with_cache_outcome` 等只作为 crate 内部辅助接口存在, 不属于 public 边界。

## 九、写入流程详解 (Block 聚合 + 延迟压缩)

```
写入 record(timestamp, data)
    │
    ├─ record_size = 4 + 8 + data.len()
    │
    ├─ record_size > 64KB? ──Yes──→ 独占 Block
    │    │                            1. 密封当前 pending (如果有)
    │    │                            2. 使用 selected algorithm 压缩 record payload
    │    │                            3. 写入 BlockHeader(flags=SEALED|COMPRESSED|SINGLE_RECORD)
    │    │                            4. 返回
    │
    No
    │
    ├─ 有 pending Block? ──No───→ 创建新 pending Block
    │    │                           flags=0, 不压缩
    │    │                           写入 record (raw)
    │    │                           设置 pending 状态
    │    │                           返回
    │
    Yes
    │
    ├─ pending_size + record_size > 64KB? ──Yes──→ 强制压缩并密封 pending Block
    │    │                                             1. 读取 raw payload
    │    │                                             2. selected algorithm 压缩
    │    │                                             3. 写回 compressed payload
    │    │                                             4. flags = SEALED|COMPRESSED
    │    │                                             5. 清除 pending
    │    │                                             6. 创建新 pending, 追加 record
    │    │                                             7. 返回
    │
    No
    │
    └─ 追加 record 到 pending (raw, 不压缩)
       更新 BlockHeader
       返回
```

**append 可见性边界**:

append record 必须遵守“payload → block header/state → index”的发布顺序。原因是查询只能从 `TimeIndex` 获得 `(block_offset, in_block_offset)` 后才会访问 data segment:

1. 若 payload 或 block header/state 写入失败, 不写 index, 该 record 对查询不可见。
2. 若 index 已写入, 正常运行期间同一 `DataSet` 的 mutex 保证查询只能在 append 完成后看到该 entry。
3. crash/reopen 后不保证 data/index mmap 文件的落盘顺序; 因此通过 index 读取时必须校验 block 边界和 record 内嵌 timestamp, 校验失败时按缺失/损坏处理, 不能返回旧数据或错误数据。

本设计不引入事务、WAL 或二次提交状态。最近写入可以丢失, 但索引不得先于 payload/header 发布。

**block offset 坐标约定**:

`TimeIndex` 中的 `IndexEntry.block_offset` 是 Block 在数据流中的逻辑全局偏移, 相对各数据段数据区起点, 不包含 header。`DataSegmentSet` 路由时转换为:

```text
segment.file_offset = (block_offset / data_segment_size) * data_segment_size
block_segment_offset = block_offset - segment.file_offset
physical_file_offset = segment.header_len + block_segment_offset
```

`DataSegment` 内部读写只使用 `block_segment_offset`; 任何物理文件 seek/mmap 位置必须加 `header_len`。

**meta 真源与固定 block 上限**:

`DataSet::open()` 只从 `{dataset}/meta` 读取创建参数, 不接收也不比较当前 `StoreConfig` 的 dataset 默认值。`block_max_size` 不是 dataset 参数, 普通聚合 Block payload 上限固定为 `BLOCK_MAX_SIZE=65536`; 超过该上限的单条 record 使用独占 block。

### 9.1 时间戳验证与写入分支

```
DataSet::write(timestamp, data):
    │
    ├─ if retention_window > 0 && timestamp < retention_threshold:
    │      ├─ latest_written_timestamp = Some(latest) 且 timestamp < latest → Error("timestamp expired")
    │      └─ 其它情况不可达 (threshold 基于 latest 计算, 正序写入不会小于 threshold)
    │
    ├─ if latest_written_timestamp == Some(timestamp) (纠正写入, 两种模式通用):
    │    │
    │    ├─ 1. time_index.find_entry(timestamp)
    │    │      → 获取 (block_offset, in_block_offset)
    │    │
    │    ├─ 2. 尝试验证该 record 是"最新数据段的最后一个 pending raw block 的最后一条 record"
    │    │      ├─ 必须是最后一段 + block 为该段最后一个 block
    │    │      ├─ block.flags 必须等于 0 (不能含 SEALED 或 COMPRESSED)
    │    │      └─ record 必须是 block 内最后一条 (in_block_offset + RECORD_HEADER_SIZE + old_data_len == payload_size)
    │    │
    │    ├─ 3. segments.overwrite_in_last_block(block_offset, in_block_offset, timestamp, new_data)
    │    │      ├─ 成功 → 返回 Ok(())
    │    │      │        (支持改变 data 长度, 更新 5 个字段, 索引条目不变)
    │    │      └─ 失败 → 目标 block 无法原地修改 (已 sealed/compressed 或非法位置)
    │    │           └─ **回退到乱序写入**: append 到最新段 + update_entry + invalid_record_count++ + invalidate 旧缓存 key
    │    │
    │    └─ 索引条目不变 (仅当原地覆盖成功时), latest_written_timestamp 不变
    │
    ├─ if latest_written_timestamp = Some(latest) 且 timestamp < latest (乱序写入, 两种模式通用):
    │    │
    │    ├─ 1. 新数据 append 到最新数据段 → (segment.file_offset, block_segment_offset, in_block_offset)
    │    │
    │    ├─ 2. time_index.update_entry / upsert_sparse_continuous_entry
    │    │      → 返回 old_entry: Option<IndexEntry> (用于判断是否需要 invalid_record_count++)
    │    │
    │    ├─ 3. 根据索引更新结果:
    │    │      ├─ 条目存在且引用数据 (block_offset ≠ FILLER):
    │    │      │    ├─ 原地覆盖索引条目 (block_offset + in_block_offset)
    │    │      │    ├─ 定位旧数据所在数据段 (block_offset → segment)
    │    │      │    ├─ invalidate 旧索引对应的全局 BlockCache key
    │    │      │    └─ 该段 invalid_record_count += 1
    │    │      │
    │    │      ├─ 条目存在且为 filler (仅连续模式):
    │    │      │    └─ 原地覆盖为真实条目 (invalid_record_count 不变)
    │    │      │
    │    │      ├─ 连续模式逻辑空洞:
    │    │      │    └─ 按需创建/扩展目标 index segment, 物化前缀 filler 后写入真实条目
    │    │      │
    │    │      └─ 非连续模式条目不存在:
    │    │           └─ Error("out-of-order write requires existing index entry")
    │    │
    │    └─ 成功 → return Ok(())
    │       (latest_written_timestamp 不变)
    │
    ├─ latest_written_timestamp 为 None 或 timestamp > latest (正序写入):
    │    │
    │    ├─ 非连续模式: 正常写入 + 追加索引
    │    └─ 连续模式: 稀疏 filler 写入 + 正常写入
    │
    └─ latest_written_timestamp = Some(timestamp) (仅正序写入时更新)
```

> **Journal hook**: 成功写入需要返回最终 `IndexEntry(timestamp, block_offset, in_block_offset)`。`DataSet::write` 在主写入和 index 发布成功后通过自身 `DataSetRuntimeContext.journal` 向 `JournalManager` 写入 `0x11` 日志; Store 门面只负责调用 DataSet public API, 不再重复追加 journal。若该 DataSet 是独立创建且未注入 journal sink, hook 为 no-op。

**单条 record 上限**: `DataSet::write` 必须拒绝 `data.len() > 4MiB`。该限制适用于普通聚合 block 和 exclusive/single-record block, 与 `data_len:u32` 的磁盘编码能力无关。

**public timestamp 契约**: `timestamp` 是 signed `i64` 业务时间戳, `0` 和负数都是合法值。`read(-1)` 不再有 latest 快捷语义, 而是读取精确 timestamp `-1`; 获取最新写入 timestamp 对应记录必须使用 `read_latest()`。

**纠正写入**: 当 `latest_written_timestamp == Some(timestamp)` 时, 允许覆盖之前写入的同时间戳数据 (数据纠正场景)。

**乱序写入**: 当 `latest_written_timestamp = Some(latest)` 且 `timestamp < latest` 时, 数据追加到最新数据段 (正常写入到 pending block), 同时更新该时间戳对应的索引位置。非连续模式要求索引中已有真实条目; 连续模式允许目标位置是已有真实 entry、已物化 filler 或逻辑空洞。逻辑空洞会按需创建目标 index segment, 只物化该分段内到目标 timestamp 前一位的 filler, 再写入真实 entry。

**retention 写入约束**: 当 `retention_window > 0` 且 `latest_written_timestamp = Some(latest)` 时, `timestamp < latest.saturating_sub(retention_window as i64)` 的乱序写入被视为过期写入, 不允许回填、替换 filler 或覆盖旧 entry, 返回 `Expired` 错误。`retention_window` 在 builder、FFI、create/open/meta decode 阶段必须已校验为 `0..=i64::MAX`, 因此该 cast 不允许发生 wrap。正序写入仍允许推进 `latest_written_timestamp`, 并可能使更多旧数据进入过期窗口。

**连续模式稀疏 filler 规则**:

1. 第一次真实写入: `TimeIndex` 初始化内存态 `base_timestamp = timestamp`, 不补任何 filler; flush 后首个 index segment 文件名承载该基准。
2. 同一 index segment 内正序写入: 从上一个存在的写入 timestamp + 1 物化 filler 到当前 timestamp - 1。
3. 跨 index segment 正序写入: 只物化上一个写入所在分段未写满的尾部, 以及当前写入所在分段前面无数据的前缀; 中间完整分段不创建。
4. 回填逻辑空洞: 只创建目标 timestamp 所属分段, 并物化该分段内必要前缀。
5. `time_step` 固定为 1 个 timestamp 单位; 调用方通常以秒为单位写入时, filler 即按秒递增。

**原地覆盖策略 (In-Place Overwrite, 支持变长)**:

1. **前提**: 最新写入的记录必须仍位于 **最新数据段** 的 **最后一个 pending raw block (`flags=0`)** 的最后一条位置。
2. **回退 (非错误)**: 如果最后一个 block 已经 `SEALED|COMPRESSED`、record 不是最后一条, 或最新数据段无打开的映射 — 无法原地修改时, 自动回退为**乱序写入**: 新数据追加到最新数据段 (新的 pending block), 索引条目原地更新为新的 (block_offset, in_block_offset), 同时旧数据所在段的 `invalid_record_count += 1`, 并 invalidate 旧索引对应的全局缓存 key
3. **支持 tail-only 变 size**: 新 data 可以比原 data 大或小, 但仅限 record 已经是 block payload 的最后一段字节; 不移动任何后续 block/record 字节。若校验发现 record 后仍有字节, 原地覆盖返回错误并由 correction 路径回退为乱序写入。
4. **索引不变**: block_offset + in_block_offset 仍指向同一 record 起始位置, data_len (u32) 更新为新长度
5. **索引条目不变**: 索引中的 block_offset/in_block_offset 字段无需修改
6. **latest_written_timestamp**: 不变

**需要更新的 5 个字段**:

| 字段 | 层级 | 变化量 |
|------|------|--------|
| BlockHeader.payload_size (u32) | block header | `+ delta` (block 内 payload 长度变化) |
| BlockHeader.uncompressed_size (u32) | block header | `+ delta` (block 内原始数据长度变化) |
| DataSegment.pending_wrote_position (u64) | 段状态 | `+ delta` |
| DataSegment.total_uncompressed_size (u64) | 段状态 | `+ delta` |
| DataSegment.data_wrote_position (u64, runtime) / header `wrote_position` (u64, on-disk) | 段状态 | runtime `+ delta`; header 保存 `header_len + data_wrote_position` |

其中 `delta = new_record_bytes - old_record_bytes = new_data.len() - old_data_len` (record_overhead 固定为 12)

**overwrite_in_last_block 实现逻辑**:
```rust
// DataSegmentSet::overwrite_in_last_block(block_offset, in_block_offset, new_data):
//   1. 定位到最新数据段 (seg = self.segments.last_mut())
//      验证 block_offset 落在该段且为段内最后一个 block
//      block_segment_offset = block_offset - seg.file_offset
//      block.start = seg.header_len + block_segment_offset
//   2. 读取 block header (16B at block.start)
//      - 检查 flags == 0 (若含 SEALED 或 COMPRESSED → 返回错误, 由 correct_write 捕获并回退到乱序写入)
//      - 计算 record 在 payload 中的位置
//      - 验证 record 是 block 内最后一条:
//        in_block_offset + 12 + old_data_len == payload_size
//      - 若否, 返回错误 (只支持最新 block 的最末 record)
//   3. 计算 delta = new_data.len() - old_data_len (i32)
//   4. 更新 mmap 中 record 的 data_len (u32) 和 data 字节
//   5. 更新 block header: payload_size += delta, uncompressed_size += delta
//   6. 更新段内计数字段:
//      - data_wrote_position += delta
//      - total_uncompressed_size += delta
//      - pending_wrote_position += delta; 更新 file header 中 pending_wrote_position
//   7. 更新 file header 中 wrote_position = header_len + data_wrote_position
```

**乱序写入机制 (Out-of-Order Write)**:

当 `latest_written_timestamp = Some(latest)` 且 `timestamp < latest` 时, 数据不会写入到其时间戳对应的位置, 而是**追加到最新数据段**的最新位置, 同时原地更新索引中的现有条目:

```
// DataSegmentSet::append_record + TimeIndex::update_entry / upsert_sparse_continuous_entry:
//   1. 新数据追加到最新数据段 (正常写入到 pending block 或创建新 block)
//      → (segment.file_offset, block_segment_offset, in_block_offset)
//   2. 更新索引:
//      → 非连续模式: 查找现有索引条目, 原地覆盖 18 字节为新的 (block_offset, in_block_offset)
//      → 连续模式: 目标可为真实 entry / filler / 逻辑空洞; 逻辑空洞按需创建 segment
//      → 返回 old_entry: Option<IndexEntry>
//   3. if old_entry 存在且 block_offset ≠ FILLER (旧索引引用了实际数据):
//        cache.invalidate(cache_key(old_entry.block_offset))
//        old_segment = segments.locate_segment(old_entry.block_offset)
//        old_segment.invalid_record_count += 1
//      else if old_entry 存在且为 filler (仅连续模式):
//        // 无实际数据被替代, invalid_record_count 不变
//      else (连续模式逻辑空洞):
//        // 无旧索引和旧数据, invalid_record_count 不变
```

> **索引原地更新**: 索引条目 18 字节通过 mmap 直接覆盖, 不改变条目总数。
> - **连续模式**: 先用 `base_timestamp` 计算逻辑 `seg_start_ts` 和 `entry_index`; 如果 segment 不存在或 `entry_index >= wrote_count`, 该位置是逻辑空洞
> - **非连续模式**: 在 in_memory_buffer 中线性搜索, 或在已打开的 IndexSegment 中二分查找; 若目标在 closed segment 中, 临时打开 → 覆盖 → idle_close
> - **崩溃边界**: 18 字节索引条目不是原子事务写入。本库不保证 crash 后保留该次更新; reopen/query 必须依靠 entry 边界、filler sentinel 和 record timestamp 校验避免返回错位数据。
>
> **invalid_record_count 更新**: 通过 `block_offset` 计算旧数据所在数据段 (段路由: `segment.file_offset = (block_offset / segment_size) × segment_size`), 再对该段 `invalid_record_count` 字段 +1。段可能已关闭, 需通过 `lazy_open` 临时打开以更新 mmap state 字段。
>
> **缓存一致性**: 全局 `BlockCache` 只允许缓存 `SEALED|COMPRESSED` block 的解压结果。乱序写入覆盖旧索引、纠正写回退到乱序写入、删除记录时, 都必须根据旧 `block_offset` 换算出 `(segment.file_offset, block_offset - segment.file_offset)` 后调用 `BlockCache::invalidate`。pending raw block 正常不会存在于全局缓存, 但 invalidate 是幂等操作, 可作为防御性清理。

### 9.2 Flush 行为 (mmap sync only)

```
flush (配置化，默认10分钟):
  for each dataset:
    for each open segment (data + index):
      1. mmap.flush() (msync / MS_SYNC)
      2. 不密封 pending block
      3. 不压缩任何数据
  注: flush 仅确保数据持久化到磁盘，不改变 block 状态
      pending block 继续保持 raw 状态留在 mmap 中
```

> **关键区别**: flush ≠ seal。flush 和 idle-close 都不改变 block 状态；密封/压缩只发生在 next write 导致 pending overflow 或 exclusive/single-record block 创建时。

### 9.3 追加操作 (DataSet::append)

**目标**: append 是独立于 correction write 的 API。它不覆盖现有 record, 而是在逻辑 record 的 data 尾部追加新 bytes。若 dataset 为空或该 timestamp 大于 `latest_written_timestamp.unwrap()`, append 创建一条新 record; 若 timestamp 已存在, 只允许追加到当前最大时间戳对应的最新末尾 record。

```
DataSet::append(timestamp, data):
    │
    ├─ if latest_written_timestamp = Some(latest) 且 timestamp < latest
    │      └─ Error("append timestamp is older than latest")
    │
    ├─ if retention_window > 0 && timestamp < retention_threshold
    │      └─ Error("timestamp expired")
    │
    ├─ if data.len() == 0 → Ok(())
    │      (合法空 append 不写数据、不写 journal; timestamp 顺序/retention 校验必须先执行)
    │
    ├─ if latest_written_timestamp is None or timestamp > latest_written_timestamp.unwrap()
    │      ├─ 校验 data.len() <= 4MiB
    │      ├─ 复用正常正序 write 路径创建新 record
    │      ├─ latest_written_timestamp = Some(timestamp)
    │      └─ 返回 AppendOutcome(index_entry, data_offset=0, data_len=data.len())
    │
    └─ latest_written_timestamp == Some(timestamp)
           │
           ├─ time_index.find_entry(timestamp)
           │      ├─ 不存在 / filler / deleted → Error("latest record not found")
           │      └─ 获取 latest index entry
           │
           ├─ 根据 block_offset 打开所属数据段并读取目标 block header
           │
           ├─ if block 已 SEALED|COMPRESSED 或 BLOCK_FLAG_COMPRESSED=1
           │      └─ Error("cannot append to compressed block")
           │
           ├─ 校验目标 record 是 block payload 中最后一条 record
           │   且 record 末尾等于当前数据段运行时 data_wrote_position
           │      └─ 否则 Error("latest record is not at segment tail")
           │
           ├─ final_data_len = old_data_len + data.len()
           │   if final_data_len > 4MiB → Error("record too large")
           │
           ├─ if 12 + final_data_len > BLOCK_MAX_SIZE
           │      └─ Error("record too large for pending block")
           │
           └─ 原地追加:
                  ├─ old record.data_len += data.len()
                  ├─ 在 old data 后复制 append bytes
                  ├─ block_payload_size += data.len()
                  ├─ block.uncompressed_size += data.len()
                  ├─ segment.pending_wrote_position += data.len()
                  ├─ segment.total_uncompressed_size += data.len()
                  ├─ segment.data_wrote_position += data.len()
                  ├─ record_count / min_timestamp / max_timestamp / index entry 不变
                  └─ 返回 AppendOutcome(old_index_entry, data_offset=old_data_len,
                                      data_len=data.len())
```

约束与说明:

1. `timestamp < latest_written_timestamp.unwrap()` 不回退为乱序写入。append 的语义是“尾部追加”, 旧 timestamp 的 record 可能位于 compressed block、历史段或中间位置, 不具备稳定追加边界。
2. `latest_written_timestamp == Some(timestamp)` 时, compressed block 一律返回错误。append 修改已有 latest record 只允许在未压缩且可验证的末尾 record 上原地增长, 不再触发比例阈值迁移; dataset 为空或 `timestamp > latest_written_timestamp.unwrap()` 创建新 record 的 append 复用 normal write 路径。
3. “record 在分段文件最末尾位置”定义为: `(block_offset - segment.file_offset) + BLOCK_HEADER_SIZE + in_block_offset + 12 + old_data_len == segment.data_wrote_position`。这里使用运行时数据区相对坐标; header state 中持久化的 `wrote_position` 必须保存为 `segment.header_len + segment.data_wrote_position`。实现需要同时校验它是 block 内最后一条 record, 防止 block 内部还有后续 record。
4. 原地追加不修改索引, 因为 `block_offset` 和 `in_block_offset` 仍指向同一 record 起点。
5. 全局 `BlockCache` 只缓存 compressed block。原地 append 目标是 pending raw block, 正常不会在全局缓存中。
6. 普通 DatasetQueue 只按 timestamp 递增投递。`timestamp > latest` 创建新 record 的 append 必须 notify, 与 normal write 等价; `timestamp == latest` 修改已有 latest record 不重新投递、不 notify。
7. `DataSet::append` 成功后通过自身 `DataSetRuntimeContext.journal` 写 journal `0x13`。`timestamp > latest` 创建新 record 的 append 也写 `0x13`, 其中 `data_offset=0`。JournalQueue 使用独立递增 journal sequence, 因此每条 `0x13` 都会投递给 journal queue consumer。

建议新增内部返回值:

```rust
pub(crate) struct AppendOutcome {
    pub index_entry: IndexEntry,
    pub data_offset: u32,
    pub data_len: u32,
}
```

### 9.4 删除操作 (DataSet::delete)

**语义**: 将指定时间戳对应的记录从索引中移除 (标记为哨兵), 数据段中的物理数据保留但 `invalid_record_count` 递增, 表示该 record 不再有效。

```
DataSet::delete(timestamp):
    │
    ├─ if latest_written_timestamp is None → Error("no data")
    │
    ├─ if retention_window > 0 && timestamp < retention_threshold
    │      └─ Error("timestamp expired")
    │
    ├─ time_index.find_and_delete_entry(timestamp)
    │    │
    │    ├─ 查找索引条目 (三级搜索: in_memory_buffer → open segments → closed segments):
    │    │    ├─ 连续模式: O(1) 直接计算位置
    │    │    └─ 非连续模式: 二分查找 / in_memory_buffer 线性搜索
    │    │
    │    ├─ 条目存在且引用真实数据 (block_offset ≠ FILLER):
    │    │    ├─ 将索引条目覆盖为哨兵: block_offset = 0xFFFFFFFFFFFFFFFF, in_block_offset = 0xFFFF
    │    │    │   (timestamp 字段保持不变, 查询路径跳过 sentinel 条目)
    │    │    ├─ invalidate 旧索引对应的全局 BlockCache key
    │    │    ├─ 定位旧数据所在数据段: segment = locate_segment(old_block_offset)
    │    │    ├─ segment.invalid_record_count += 1
    │    │    └─ 更新段 file header state
    │    │
    │    └─ 条目不存在 / 条目为 filler / 连续模式逻辑空洞:
    │         └─ Error("not found") — 无可删除的记录
    │
    └─ return Ok(())
```

> **Journal hook**: 成功删除需要返回删除前的真实 `IndexEntry`。`DataSet::delete` 在索引标记为 filler 且 cache 失效完成后通过自身 `DataSetRuntimeContext.journal` 向 `JournalManager` 写入 `0x12` 日志。不存在、已删除 filler 或过期不可操作的删除失败路径不写 journal。

> **查询影响**: 删除后, 查询路径自动跳过 `block_offset == 0xFFFFFFFFFFFFFFFF` 的哨兵条目, 被删除的记录不会出现在查询结果中。无需修改查询逻辑。
>
> **retention 约束**: 过期 timestamp 不允许 delete, 即使旧索引条目或旧数据段尚未被物理回收。调用方应将该错误视为“已超出可操作窗口”, 而不是继续打开旧 segment 查找。
>
> **物理数据保留**: 被删除的 record 物理上仍存在于数据段 block 中, 不影响后续 block 的读写。当前版本只通过 retention reclaim 按整个过期分段删除文件; 不支持基于 `invalid_record_count` 的 compaction 或部分空间回收。
>
> **缓存一致性**: delete 使旧 record 对查询不可见, 必须 invalidate 旧索引指向的全局缓存 key。若旧 block 未压缩或未进入缓存, invalidate 为无副作用 no-op。
>
> **崩溃边界**: 与写入操作一致, delete 的索引覆盖和 `invalid_record_count` 递增不是事务。crash 后可能丢失本次 delete 或只持久化部分状态; 查询路径以索引 sentinel 为可见性边界, 不承诺事务级删除持久性。
>
> **与 `invalid_record_count` 的关系**: 每次 delete 操作使旧数据段的 `invalid_record_count += 1`。该计数器可用于:
> - 诊断: 监控段内无效记录占比 (`invalid_record_count / record_count`)
> - 统计: 评估 correction/out-of-order/delete 造成的无效记录规模
>
> 当前版本不定义 compaction 触发阈值、目标段写入、索引重写、并发隔离或 crash recovery 协议, 因此 `invalid_record_count` 不能触发物理回收。compaction 作为后续文件格式/并发协议版本再设计。

## 十、读取流程详解 (含缓存)

### 10.1 旧版流程 (全量加载, 已弃用)

```
查询 [start_ts, end_ts]
    │
    ├─ 1. TimeIndex.query()
    │      → Vec<IndexEntry(ts, block_offset, in_block_offset)>
    │
    ├─ 2. 对每个 entry:
    │      ├─ 通过 block_offset 定位 data segment
    │      ├─ 读 BlockHeader, 检查 compressed flag
    │      ├─ compressed:
    │      │   ├─ 计算 cache_key = (segment.file_offset, block_offset - segment.file_offset)
    │      │   ├─ 检查全局缓存池
    │      │   └─ Miss → 解压 entire block payload → 存入缓存池
    │      ├─ pending raw → 读取 raw block payload, 不进入全局缓存
    │      ├─ in_block_offset → 定位到 [data_len:4]
    │      ├─ 读 data_len, timestamp, data
    │      └─ 返回
    │
    └─ 3. 按 timestamp 排序返回
```

### 10.2 当前实现流程 (QueryIterator: 数据惰性 + 索引 source cursor)

```
查询 [start_ts, end_ts] → QueryIterator (惰性)
    │
    ├─ 1. TimeIndex.prepare_query_sources()
    │      → 返回按时间顺序排列的 QuerySource 列表
    │      → 内存 buffer 只复制命中范围内的未 flush entry
    │      → index segment 只记录 path + [start_idx, end_idx), 不全量收集 entries
    │
    └─ 调用 next() 时:
           ├─ 2. 从当前 source 获取下一个 IndexEntry
           │      ├─ 当前 source 耗尽 → 切换到下一个 source
           │      ├─ 跳过 filler entries (block_offset == 0xFFFFFFFFFFFFFFFF)
           │      └─ 连续模式未创建的逻辑空洞 segment 不产生 source
           │
           ├─ 3. 检查 HotBlockCache (无锁, 查询级局部缓存)
           │      ├─ Hit (同一个 data segment 且同一个段内 block offset)
           │      │   └─ 直接从 hot_block.extract_record() → return
           │      └─ Miss → 继续 ↓
           │
           ├─ 4. mmap 读取 BlockHeader, 校验 SEALED/COMPRESSED 状态
           │
           ├─ 5. SEALED|COMPRESSED block 才检查全局 BlockCache (RwLock<HashMap>)
           │      ├─ Hit → 放入 HotBlockCache → extract_record → return
           │      └─ Miss → 继续 ↓
           │
           ├─ 6. mmap 读取 payload + 解码
           │      ├─ compressed → 按 segment header compress_type 解压 → 写入全局 BlockCache
           │      └─ pending raw → payload.to_vec(), 不进入全局 BlockCache
           │
           ├─ 7. 更新 HotBlockCache
           │      └─ hot_block = HotBlockCache::new(key, decoded_payload)
           │
           └─ 8. 定位 record 并返回 (timestamp, data)
```

> **关键改进**:
> - **索引 source cursor**: 已落盘的 index segment 只在迭代时逐条读取, 不再把整个查询范围一次性收集到 `Vec<IndexEntry>`
> - **数据惰性化**: `DataSet::query_iter()` 与 FFI iterator 按需读取 record; `DataSet::query()` 作为兼容便利方法仍会 collect 成 `Vec`
> - **HotBlockCache**: 读取循环中保持最后解压的 Block, 同 Block 内连续读取跳过 mmap+解压
> - **无锁热点**: HotBlockCache 属于单个 QueryIterator 实例, 不涉及全局锁竞争
> - **全局缓存不可变性**: 只有 compressed block 的解压 payload 可进入全局 BlockCache; HotBlockCache 为查询局部缓存, 可持有本次读取的 raw payload, 但不跨越写入操作
> - **内存边界**: 当前 Rust QueryIterator 主要持有 source 元数据、未 flush 命中 entry、当前 hot block 和当前 record; FFI `tmsl_iter_next` 仍按条 `malloc` 返回数据。严格 64KB 级常量内存属于后续零拷贝/buffer API 目标, 不能作为当前 FFI 性能承诺
>
> **旧 API 兼容**: `DataSet::query()` 方法保留, 内部改为 `query_iter().collect()`

### 10.3 单时间戳读取 (`read`)

```
read(timestamp) → Option<(i64, Vec<u8>)>
    │
    ├─ 1. timestamp 是精确业务时间戳; `-1` 不再是 latest sentinel
    │
    ├─ 2. if retention_window > 0 && timestamp < retention_threshold
    │      → return Ok(None)
    │
    ├─ 3. TimeIndex.find_entry(timestamp)
    │      → 三级搜索: in_memory_buffer → open segments → closed segments
    │      → 返回 None: 时间戳不存在或连续模式逻辑空洞, 直接返回 Ok(None)
    │
    ├─ 4. 检查 entry.block_offset
    │      └─ == BLOCK_OFFSET_FILLER (0xFFFFFFFFFFFFFFFF)
    │         → 已删除或未写入 (连续模式已物化 filler), 返回 Ok(None)
    │
    └─ 5. segments.read_at_index(entry, cache)
           → 定位数据段, 读 Block + 解压 (如需), 定位 record, 返回 (ts, data)
```

> **与 `query` 的区别**:
> - `read` 查找单个时间戳, 不构建迭代器, 开销更小
> - `read` 返回 `Option`, 未找到时不报错 (区别于 `delete` 的 `NotFound` 错误)
> - `read` 复用 `TimeIndex.find_entry()` (三级搜索), 与 correction-write 路径一致
> - FFI 层 `tmsl_dataset_read` 返回码: 0=成功, 1=未找到, -1=错误
> - `out_data` 由 `libc::malloc` 分配, C 侧通过 `tmsl_data_free` 释放; `tmsl_iter_free_data` 仅作为兼容别名保留
>
> **latest 读取**:
> - `read_latest()` 直接复用内存中的 `latest_written_timestamp: Option<i64>`; `None` 表示 dataset 尚未写入过任何 record
> - 如果最大已写时间戳对应的 index entry 已被 delete 标记为 filler, `read_latest()` 仍返回 `None` (不会回退到更早的有效记录)
> - FFI 使用独立的 `tmsl_dataset_read_latest` API; `tmsl_dataset_read(dataset, -1, ...)` 读取精确 timestamp `-1`
>
> **retention 语义**: 当 `latest_written_timestamp` 为 `Some(latest)` 时, 所有读取路径以 `retention_threshold = latest.saturating_sub(retention_window as i64)` 为可见性下界；为 `None` 时不产生 retention threshold。`retention_window` 在进入计算前必须已校验为 `0..=i64::MAX`。`read(ts)` 若 `ts < retention_threshold` 直接返回 `Ok(None)`; `query/query_iter/query_index_entries` 将 start 钳制到 threshold; `read_entry_at_index(entry)` 若 entry.timestamp 已过期则返回 `Expired` 错误, 防止绕过单时间戳入口读取已过期数据。

### 10.4 `latest_written_timestamp`

数据集实例维护的最高已写时间戳:
- `DataSet::create` 后初始化为 `None`
- 每次正常写入 (`latest_written_timestamp` 为 `None` 或 `timestamp > latest`) 更新为 `Some(timestamp)`
- 纠正写 (`latest_written_timestamp == Some(timestamp)`) / 乱序写 (`timestamp < latest`) / `delete(latest)` 不改变
- `open` 时通过 `recover_latest_timestamp` 从最新索引分段文件的最后一条 entry 恢复; 若该 entry 是 delete/filler 哨兵, 其 timestamp 仍然是 `latest_written_timestamp`
- 运行期若存在未刷盘的 `in_memory_buffer`, 恢复辅助逻辑会把 buffer 中的最大 timestamp 作为兜底候选; 正常 open 路径下该 buffer 为空
- 用于:
  - `read_latest()` 解析到最大已写 timestamp; 若该 entry 不存在、已删除或已过期, 返回 `None`, 不反向搜索更早有效记录
  - 数据保留阈值计算 (`latest.saturating_sub(retention_window as i64)`)
  - 连续模式稀疏 filler 的上一个真实写入边界判定

> **读操作接口总览**: 完整的读操作接口文档（含新增的 read_exist/query_exist/read_length/query_length/query_length_iter）见 [数据集读操作](dataset-read-operations.md)。

## 十一、数据保留 (Retention) 与回收

### 11.1 retention_window 配置

`retention_window` 是数据集级不可变配置, 存储在 `meta` 文件中 (TLV type `0x08`, u64 LE)。其单位必须与业务 timestamp 完全相同, 不绑定秒或毫秒。

| 值 | 含义 |
|---|------|
| `0` | 不限数据有效期, 不触发回收 (默认) |
| `> 0` | 数据保留窗口, 单位必须与业务 timestamp 完全相同 |

> **单位说明**: `retention_window` 不表示固定毫秒。其值必须使用 timestamp unit: 如果业务 timestamp 按秒递增, retention 也按秒; 如果业务 timestamp 按其它单位递增, retention 也按同一单位。调用方需确保二者单位一致。
> **范围说明**: `retention_window` 的磁盘和 FFI 类型是 `u64`, 但有效范围固定为 `0..=i64::MAX`。builder、DataSetMeta 解析、dataset create/open 和 FFI config decode 都必须拒绝超过 `i64::MAX` 的值, 因为过期阈值与 signed `i64` timestamp 同域计算。

### 11.2 过期阈值计算

```
expiration_threshold = latest_written_timestamp.map(|latest| latest.saturating_sub(retention_window as i64))
```

- `latest_written_timestamp`: 数据集写入过的最大时间戳 (从索引最后位置恢复; 不存入 meta); `None` 表示从未写入
- `saturating_sub`: 防止 timestamp < retention_window 时下溢; `retention_window as i64` 在进入计算前已由配置/meta 校验保证安全
- 当 `latest_written_timestamp` 为 `None` 时无过期阈值, 不回收; 当 `latest < retention_window` 时, `saturating_sub` 将 threshold 钳制到 `i64::MIN`

### 11.3 回收流程

```
DataSet::reclaim_expired_segments():
  1. if retention_window == 0 → return Ok(0)
  2. if latest_written_timestamp is None → return Ok(0)
     threshold = latest_written_timestamp.unwrap().saturating_sub(retention_window as i64)
  3. old_last_used_at = self.last_used_at
     self.flush()  -- 确保 in-memory buffer 落盘; flush 内部可能临时 touch
  4. self.time_index.idle_close_all()
     self.segments.idle_close_all()
     确保所有打开分段完成 sync + unmap, 但分段注册表仍保留统一 BTreeMap 条目
  5. self.time_index.reclaim_expired_segments(threshold, index_segment_size)
     逐个检查索引段 last_entry_timestamp() < threshold → 删除
  6. self.segments.reclaim_expired_segments(threshold)
     逐个检查 data segment meta 中的 max_timestamp < threshold → 删除
     删除已归档 data/index segment 时同步扣减或更新 dataset state 文件
  7. self.last_used_at = old_last_used_at
     retention 是维护任务, 不延长 dataset 热度或重置 idle 计时
  8. return Ok(已删除总数)
```

### 11.4 读取与写入约束

当 `retention_window > 0` 时, 所有读路径共享同一个过期阈值:

```rust
retention_threshold = latest_written_timestamp.map(|latest| latest.saturating_sub(retention_window as i64))
```

| 操作 | `timestamp < retention_threshold` 行为 |
|------|----------------------------------------|
| `read(ts)` | 直接返回 `Ok(None)` |
| `read_latest()` | 解析为 latest, 不回退到更早有效记录 |
| `query/query_iter` | `start_ts = max(start_ts, threshold)`; 若范围完全过期则返回空 |
| `query_index_entries` | 与 query 使用相同钳制, 不暴露过期 entry |
| `read_entry_at_index(entry)` | 返回 `Expired` 错误, 防止绕过 timestamp 入口 |
| `delete(ts)` | 返回 `Expired` 错误, 不打开旧索引/旧数据段 |
| `write(ts)` 且 `ts < latest` | 作为过期乱序写入返回 `Expired` 错误 |
| `write(ts)` 且 `ts > latest` | 正序写入允许, 并推进 latest/threshold |
| `append(ts)` 且 `ts < latest` | 返回 append 顺序错误; 若同时过期, 可返回 `Expired` |
| `append(ts)` 且 `ts == latest` | 仅当 latest record 仍是未压缩末尾 record 时可追加; 追加后 latest 不变 |
| `append(ts)` 且 `ts > latest` | 按 append 创建新 record, 推进 latest/threshold |

**效果**: 过期数据即使索引或数据物理文件尚未回收, 也不再通过读路径可见, 且不能被 delete 或 out-of-order rewrite 修改。

### 11.5 约束

- 回收前必须先 `flush()` + `idle_close_all()` 使所有分段进入 closed 集合
- 回收操作是**破坏性**的 (物理删除文件), 不可恢复
- 回收过程中打开的文件必须**检查完成后立即释放**, 不依赖 idle-close
- 数据段和索引段分别按各自分段的时间范围独立回收: 数据段要求 `max_timestamp < threshold`, 索引段要求最后 entry timestamp `< threshold`
- 混合分段 (分段内同时包含过期和未过期 timestamp) 必须保留, 不做部分删除
- 回收不追踪“已回收数据是否仍被索引引用”或“已回收索引是否仍关联数据”; 只保证整个分段全部过期才删除, 查询路径通过 retention 钳制和边界校验避免异常
- 连续模式下, 回收老 index segment 后, 已删除时间范围不可回填; reopen 时以剩余最小分段文件名作为可恢复基准

### 11.6 Compaction 状态

当前版本不支持 compaction。`invalid_record_count` 只作为持久统计字段, 用于诊断无效记录规模, 不参与自动回收、后台任务调度或写入路径决策。过期数据的物理删除仅由 retention reclaim 按整段文件完成; 非过期段内的无效 record 会继续占用磁盘空间, 直到未来版本引入完整的 compaction 设计。

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [查询迭代器](query-iterator.md) | [Store 与 FFI](store-and-ffi.md)
