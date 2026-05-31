# 数据集操作 - DataSet 生命周期 + 写入/读取流程

## 八、DataSet: 数据集

### 8.1 生命周期: create / open / close / drop

> **核心原则**: 创建和打开分离。参数仅在创建时传入, 打开时从 meta 文件读取, 不可修改。

```rust
struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,
    config: DataSetConfig,     // 从 meta 文件读取 (创建时写入, 之后不可变)
    retention_ms: u64,         // 数据有效期 (与 timestamp 同单位, 0=不限)
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
    latest_written_timestamp: i64,  // 最近一次成功真实写入的最高 timestamp, 同时作为回收基准
}

impl DataSet {
    /// 创建新数据集 (显式创建, 已存在返回错误)
    fn create(
        id: DataSetKey, base_dir: PathBuf,
        data_segment_size: u64, index_segment_size: u64,
        compress_level: u8, block_max_size: u32,
        index_continuous: u8,
        initial_data_segment_size: u64, initial_index_segment_size: u64,
        retention_ms: u64,
    ) -> io::Result<Self>;

    /// 打开已有数据集 (参数从 meta 文件读取, 不能设置)
    fn open(
        id: DataSetKey, base_dir: PathBuf, block_max_size: u32,
    ) -> io::Result<Self>;

    /// 关闭数据集 (flush + 关闭所有 segment)
    fn close(&mut self) -> io::Result<()>;

    /// 删除整个数据集 (删除目录及所有文件)
    fn drop_dataset(base_dir: &Path) -> io::Result<()>;

    fn write(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()>;
    fn write_with_cache(&mut self, timestamp: i64, data: &[u8], cache: Option<&BlockCache>) -> io::Result<()>;
    fn read(&mut self, timestamp: i64, cache: Option<&BlockCache>) -> io::Result<Option<(i64, Vec<u8>)>>;
    fn query(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<Vec<(i64, Vec<u8>)>>;
    fn query_iter(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<QueryIterator<'_>>;
    fn flush(&mut self) -> io::Result<()>;
    fn config(&self) -> &DataSetConfig;

    /// 最近成功写入的时间戳 (0 = 数据集为空)
    /// open 时从索引分段恢复; 写入时在内存中维护
    fn latest_written_timestamp(&self) -> i64;

    /// 删除指定时间戳的记录 (索引标记为哨兵, 数据段 invalid_record_count++)
    fn delete(&mut self, timestamp: i64) -> io::Result<()>;
    fn delete_with_cache(&mut self, timestamp: i64, cache: Option<&BlockCache>) -> io::Result<()>;

    /// 回收超过有效期的分段文件 (需先 close)
    /// retention_ms=0 时跳过; retention_ms > 0 时计算过期阈值并删除过期分段
    fn reclaim_expired_segments(&mut self) -> io::Result<usize>;

    /// 获取 retention_ms 配置
    fn retention_ms(&self) -> u64;
}
```

## 九、写入流程详解 (Block 聚合 + 延迟压缩)

```
写入 record(timestamp, data)
    │
    ├─ record_size = 4 + 8 + data.len()
    │
    ├─ record_size > 64KB? ──Yes──→ 独占 Block
    │    │                            1. 密封当前 pending (如果有)
    │    │                            2. 压缩 record payload
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
    ├─ pending_size + record_size > 64KB? ──Yes──→ 密封 pending Block
    │    │                                             1. 读取 raw payload
    │    │                                             2. 压缩 → 比较大小
    │    │                                             3. 写回: compressed 或 raw
    │    │                                             4. flags = SEALED[|COMPRESSED]
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

### 9.1 时间戳验证与写入分支

```
DataSet::write(timestamp, data):
    │
    ├─ if timestamp <= 0 → Error("timestamp must be > 0")
    │
    ├─ if timestamp == latest_written_timestamp 且 latest > 0 (纠正写入, 两种模式通用):
    │    │
    │    ├─ 1. time_index.find_entry(timestamp)
    │    │      → 获取 (block_offset, in_block_offset)
    │    │
    │    ├─ 2. 尝试验证该 record 是"最新数据段的最后一个未压缩 block 的最后一条 record"
    │    │      ├─ 必须是最后一段 + block 为该段最后一个 block
    │    │      ├─ block.flags 不能含 COMPRESSED flag
    │    │      └─ record 必须是 block 内最后一条 (in_block_offset + RECORD_HEADER_SIZE + old_data_len == payload_size)
    │    │
    │    ├─ 3. segments.overwrite_in_last_block(block_offset, in_block_offset, timestamp, new_data)
    │    │      ├─ 成功 → 返回 Ok(())
    │    │      │        (支持改变 data 长度, 更新 5 个字段, 索引条目不变)
    │    │      └─ 失败 → 目标 block 无法原地修改 (已压缩或非法位置)
    │    │           └─ **回退到乱序写入**: append 到最新段 + update_entry + invalid_record_count++ + invalidate 旧缓存 key
    │    │
    │    └─ 索引条目不变 (仅当原地覆盖成功时), latest_written_timestamp 不变
    │
    ├─ if timestamp < latest_written_timestamp (乱序写入, 两种模式通用):
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
    ├─ timestamp > latest (正序写入):
    │    │
    │    ├─ 非连续模式: 正常写入 + 追加索引
    │    └─ 连续模式: 稀疏 filler 写入 + 正常写入
    │
    └─ latest_written_timestamp = timestamp (仅正序写入时更新)
```

**纠正写入**: 当 `timestamp == latest_written_timestamp` 时, 允许覆盖之前写入的同时间戳数据 (数据纠正场景)。

**乱序写入**: 当 `timestamp < latest_written_timestamp` 时, 数据追加到最新数据段 (正常写入到 pending block), 同时更新该时间戳对应的索引位置。非连续模式要求索引中已有真实条目; 连续模式允许目标位置是已有真实 entry、已物化 filler 或逻辑空洞。逻辑空洞会按需创建目标 index segment, 只物化该分段内到目标 timestamp 前一位的 filler, 再写入真实 entry。

**连续模式稀疏 filler 规则**:

1. 第一次真实写入: `TimeIndex` 初始化内存态 `base_timestamp = timestamp`, 不补任何 filler; flush 后首个 index segment 文件名承载该基准。
2. 同一 index segment 内正序写入: 从上一个存在的写入 timestamp + 1 物化 filler 到当前 timestamp - 1。
3. 跨 index segment 正序写入: 只物化上一个写入所在分段未写满的尾部, 以及当前写入所在分段前面无数据的前缀; 中间完整分段不创建。
4. 回填逻辑空洞: 只创建目标 timestamp 所属分段, 并物化该分段内必要前缀。
5. `time_step` 固定为 1 个 timestamp 单位; 调用方通常以秒为单位写入时, filler 即按秒递增。

**原地覆盖策略 (In-Place Overwrite, 支持变长)**:

1. **前提**: 最新写入的记录必然位于 **最新数据段** 的 **最后一个未压缩 block** 的最后一条位置。可能形态:
   - **Pending block** (flags = 0): 尚未密封, 未压缩
   - **Sealed block** (flags = SEALED): 已密封但未压缩 (压缩未受益, seal 时保留原始格式)
2. **回退 (非错误)**: 如果最后一个 block 的 flags 含 `COMPRESSED` (数据已被压缩)、record 不是最后一条, 或最新数据段无打开的映射 — 无法原地修改时, 自动回退为**乱序写入**: 新数据追加到最新数据段 (新的 pending block), 索引条目原地更新为新的 (block_offset, in_block_offset), 同时旧数据所在段的 `invalid_record_count += 1`, 并 invalidate 旧索引对应的全局缓存 key
3. **支持变 size**: 新 data 可以比原 data 大或小, 只需移动后续字节并更新相关计数字段
4. **索引不变**: block_offset + in_block_offset 仍指向同一 record 起始位置, data_len (u32) 更新为新长度
5. **索引条目不变**: 索引中的 block_offset/in_block_offset 字段无需修改
6. **latest_written_timestamp**: 不变

**需要更新的 5 个字段**:

| 字段 | 层级 | 变化量 |
|------|------|--------|
| BlockHeader.payload_size (u32) | block header | `+ delta` (block 内 payload 长度变化) |
| BlockHeader.uncompressed_size (u32) | block header | `+ delta` (block 内原始数据长度变化) |
| DataSegment.pending_wrote_position (u64) | 段状态 | `+ delta` (仅 pending block 场景, sealed 不更新) |
| DataSegment.total_uncompressed_size (u64) | 段状态 | `+ delta` |
| DataSegment.wrote_position (u64) | 段状态 | `+ delta` |

其中 `delta = new_record_bytes - old_record_bytes = new_data.len() - old_data_len` (record_overhead 固定为 12)

**overwrite_in_last_block 实现逻辑**:
```rust
// DataSegmentSet::overwrite_in_last_block(block_offset, in_block_offset, new_data):
//   1. 定位到最新数据段 (seg = self.segments.last_mut())
//      验证 block_offset 落在该段且为段内最后一个 block
//      block_segment_offset = block_offset - seg.file_offset
//      block.start = seg.header_len + block_segment_offset
//   2. 读取 block header (16B at block.start)
//      - 检查 flags & COMPRESSED == 0 (若含 COMPRESSED → 返回错误, 由 correct_write 捕获并回退到乱序写入)
//      - 计算 record 在 payload 中的位置
//      - 验证 record 是 block 内最后一条:
//        in_block_offset + 12 + old_data_len == payload_size
//      - 若否, 返回错误 (只支持最新 block 的最末 record)
//   3. 计算 delta = new_data.len() - old_data_len (i32)
//   4. 更新 mmap 中 record 的 data_len (u32) 和 data 字节
//   5. 更新 block header: payload_size += delta, uncompressed_size += delta
//   6. 更新段内计数字段:
//      - wrote_position += delta
//      - total_uncompressed_size += delta
//      - if block is pending (pending_block_offset matches):
//          pending_wrote_position += delta; 更新 file header 中 pending_wrote_position
//      - else (sealed+uncompressed): 仅更新 file header 中 wrote_position
//   7. 更新 file header 中 wrote_position (update_file_wrote_position)
```

**乱序写入机制 (Out-of-Order Write)**:

当 `timestamp < latest_written_timestamp` 时, 数据不会写入到其时间戳对应的位置, 而是**追加到最新数据段**的最新位置, 同时原地更新索引中的现有条目:

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
> **缓存一致性**: 全局 `BlockCache` 只允许缓存 compressed block 的解压结果。乱序写入覆盖旧索引、纠正写回退到乱序写入、删除记录时, 都必须根据旧 `block_offset` 换算出 `(segment.file_offset, block_offset - segment.file_offset)` 后调用 `BlockCache::invalidate`。pending/raw sealed block 正常不会存在于全局缓存, 但 invalidate 是幂等操作, 可作为防御性清理。

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

> **关键区别**: flush ≠ seal。flush 只 msync，密封/压缩只发生在 block 溢出或 idle-close 时。

### 9.3 删除操作 (DataSet::delete)

**语义**: 将指定时间戳对应的记录从索引中移除 (标记为哨兵), 数据段中的物理数据保留但 `invalid_record_count` 递增, 表示该 record 不再有效。

```
DataSet::delete(timestamp):
    │
    ├─ if timestamp <= 0 → Error("timestamp must be > 0")
    │
    ├─ if latest_written_timestamp == 0 → Error("no data")
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

> **查询影响**: 删除后, 查询路径自动跳过 `block_offset == 0xFFFFFFFFFFFFFFFF` 的哨兵条目, 被删除的记录不会出现在查询结果中。无需修改查询逻辑。
>
> **物理数据保留**: 被删除的 record 物理上仍存在于数据段 block 中, 不影响后续 block 的读写。仅在数据段回收时 (retention reclaim 或 `invalid_record_count` 达到阈值触发 compaction) 才会物理清除。
>
> **缓存一致性**: delete 使旧 record 对查询不可见, 必须 invalidate 旧索引指向的全局缓存 key。若旧 block 未压缩或未进入缓存, invalidate 为无副作用 no-op。
>
> **崩溃边界**: 与写入操作一致, delete 的索引覆盖和 `invalid_record_count` 递增不是事务。crash 后可能丢失本次 delete 或只持久化部分状态; 查询路径以索引 sentinel 为可见性边界, 不承诺事务级删除持久性。
>
> **与 `invalid_record_count` 的关系**: 每次 delete 操作使旧数据段的 `invalid_record_count += 1`。该计数器可用于:
> - 诊断: 监控段内无效记录占比 (`invalid_record_count / record_count`)
> - 未来 compaction: 当无效占比超过阈值时触发段压缩, 物理回收空间

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
    │      ├─ uncompressed → 读取 raw block payload, 不进入全局缓存
    │      ├─ in_block_offset → 定位到 [data_len:4]
    │      ├─ 读 data_len, timestamp, data
    │      └─ 返回
    │
    └─ 3. 按 timestamp 排序返回
```

### 10.2 新版流程 (QueryIterator 惰性查询 + HotBlockCache)

```
查询 [start_ts, end_ts] → QueryIterator (惰性)
    │
    ├─ 1. TimeIndex.prepare_query()
    │      → 返回按时间顺序排列的 QueryDataSource 列表
    │      (不加载实际数据, 只建立 source 映射)
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
           ├─ 4. mmap 读取 BlockHeader, 检查 compressed flag
           │
           ├─ 5. compressed block 才检查全局 BlockCache (RwLock<HashMap>)
           │      ├─ Hit → 放入 HotBlockCache → extract_record → return
           │      └─ Miss → 继续 ↓
           │
           ├─ 6. mmap 读取 payload + 解码
           │      ├─ compressed → deflate_decompress() → 写入全局 BlockCache
           │      └─ uncompressed → payload.to_vec(), 不进入全局 BlockCache
           │
           ├─ 7. 更新 HotBlockCache
           │      └─ hot_block = HotBlockCache::new(key, decoded_payload)
           │
           └─ 8. 定位 record 并返回 (timestamp, data)
```

> **关键改进**:
> - **惰性化**: 索引条目按需从 source 取出, 不再全量收集到 Vec
> - **HotBlockCache**: 读取循环中保持最后解压的 Block, 同 Block 内连续读取跳过 mmap+解压
> - **无锁热点**: HotBlockCache 属于单个 QueryIterator 实例, 不涉及全局锁竞争
> - **全局缓存不可变性**: 只有 compressed block 的解压 payload 可进入全局 BlockCache; HotBlockCache 为查询局部缓存, 可持有本次读取的 raw payload, 但不跨越写入操作
> - **内存节省**: 查询 100 万条记录仅需 ~64KB (1 Block) 内存, 而非 ~100MB
>
> **旧 API 兼容**: `DataSet::query()` 方法保留, 内部改为 `query_iter().collect()`

### 10.3 单时间戳读取 (`read`)

```
read(timestamp) → Option<(i64, Vec<u8>)>
    │
    ├─ 1. 解析 effective_ts
    │      └─ timestamp == -1
    │         → effective_ts = latest_written_timestamp (0 为空 → None)
    │      └─ 其它情况
    │         → effective_ts = timestamp
    │
    ├─ 2. TimeIndex.find_entry(effective_ts)
    │      → 三级搜索: in_memory_buffer → open segments → closed segments
    │      → 返回 None: 时间戳不存在或连续模式逻辑空洞, 直接返回 Ok(None)
    │
    ├─ 3. 检查 entry.block_offset
    │      └─ == BLOCK_OFFSET_FILLER (0xFFFFFFFFFFFFFFFF)
    │         → 已删除或未写入 (连续模式已物化 filler), 返回 Ok(None)
    │
    └─ 4. segments.read_at_index(entry, cache)
           → 定位数据段, 读 Block + 解压 (如需), 定位 record, 返回 (ts, data)
```

> **与 `query` 的区别**:
> - `read` 查找单个时间戳, 不构建迭代器, 开销更小
> - `read` 返回 `Option`, 未找到时不报错 (区别于 `delete` 的 `NotFound` 错误)
> - `read` 复用 `TimeIndex.find_entry()` (三级搜索), 与 correction-write 路径一致
> - FFI 层 `tmsl_dataset_read` 返回码: 0=成功, 1=未找到, -1=错误
> - `out_data` 由 `libc::malloc` 分配, C 侧通过 `tmsl_iter_free_data` 释放 (与迭代器共享分配/释放路径)
>
> **`timestamp = -1` 快捷路径**:
> - 直接复用内存中的 `latest_written_timestamp` (open 时从索引恢复), 省去一次索引查找
> - 如果最新时间戳对应的 index entry 已被 delete 标记为 filler, 仍返回 `None` (不会回退到更早的有效记录)
> - 适合流式消费场景: 每次 "拉最新一条" 而不需要提前知道具体时间戳

### 10.4 `latest_written_timestamp`

数据集实例维护的最高已写时间戳:
- `DataSet::create` 后初始化为 `0`
- 每次正常写入 (`timestamp > latest`) 更新为该 `timestamp`
- 纠正写 (`timestamp == latest`) / 乱序写 (`timestamp < latest`) 不改变
- `open` 时通过 `recover_latest_timestamp` 从索引分段 (closed + open + in_memory_buffer) 恢复
- 用于:
  - `read(-1)` 快捷路径解析最新记录
  - 数据保留阈值计算 (`latest - retention_ms`)
  - 连续模式稀疏 filler 的上一个真实写入边界判定

## 十一、数据保留 (Retention) 与回收

### 11.1 retention_ms 配置

`retention_ms` 是数据集级不可变配置, 存储在 `meta` 文件中 (TLV type `0x08`, u64 LE)。

| 值 | 含义 |
|---|------|
| `0` | 不限数据有效期, 不触发回收 (默认) |
| `> 0` | 数据有效期, 单位与 timestamp 相同 (通常为毫秒) |

> **单位说明**: `retention_ms` 的单位与数据集写入使用的时间戳单位一致。如果时间戳为 unix 毫秒, 则 retention_ms 为毫秒; 如果时间戳为秒, 则为秒。调用方需确保二者单位一致。

### 11.2 过期阈值计算

```
expiration_threshold = latest_written_timestamp.saturating_sub(retention_ms)
```

- `latest_written_timestamp`: 数据集最近一次成功写入的时间戳 (从 meta 加载或从索引恢复)
- `saturating_sub`: 防止 timestamp < retention_ms 时下溢
- 当 `latest_written_timestamp < retention_ms` 时, expiration_threshold = 0 → 无分段满足条件 → 不回收

### 11.3 回收流程

```
DataSet::reclaim_expired_segments():
  1. if retention_ms == 0 → return Ok(0)
  2. threshold = latest_written_timestamp.saturating_sub(retention_ms)
  3. self.flush()  -- 确保 in-memory buffer 落盘
  4. self.time_index.idle_close_all()
     self.segments.idle_close_all()
     确保所有分段进入 closed/closed_index_segments 集合
  5. self.time_index.reclaim_expired_segments(threshold, index_segment_size)
     逐个检查索引段 last_entry_timestamp() < threshold → 删除
  6. self.segments.reclaim_expired_segments(threshold)
     逐个检查 closed_segments[].max_timestamp < threshold → 删除
  7. self.last_used_at = Instant::now()
  8. return Ok(已删除总数)
```

### 11.4 查询约束

当 `retention_ms > 0` 时, 查询范围被自动钳制到数据有效期内:

```rust
fn query_iter(...):
    if retention_ms > 0 && latest_written_timestamp > 0 {
        let expiration_threshold =
            latest_written_timestamp.saturating_sub(retention_ms);
        let effective_start = start_ts.max(expiration_threshold);
        if effective_start > end_ts {
            return empty iterator;  // 查询范围完全在过期区内
        }
        start_ts = effective_start;
    }
```

**效果**: 查询不会返回超出有效期的数据, 即使该数据物理上尚未被回收。

### 11.5 约束

- 回收前必须先 `flush()` + `idle_close_all()` 使所有分段进入 closed 集合
- 回收操作是**破坏性**的 (物理删除文件), 不可恢复
- 回收过程中打开的文件必须**检查完成后立即释放**, 不依赖 idle-close
- 连续模式下, 回收老分段不会破坏新数据 (回收从最老端开始)
- 同一数据集的索引与数据分段必须**成对回收** (相同时间窗口)

---

**相关**: [架构概览](architecture.md) | [数据模型](data-model.md) | [查询迭代器](query-iterator.md) | [Store 与 FFI](store-and-ffi.md)
