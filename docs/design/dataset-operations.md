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
    latest_written_timestamp: i64,  // 用于连续模式判断正序/补数据, 同时作为回收基准
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
    fn read(&mut self, timestamp: i64, cache: Option<&BlockCache>) -> io::Result<Option<(i64, Vec<u8>)>>;
    fn query(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<Vec<(i64, Vec<u8>)>>;
    fn query_iter(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<QueryIterator<'_>>;
    fn flush(&mut self) -> io::Result<()>;
    fn config(&self) -> &DataSetConfig;

    /// 删除指定时间戳的记录 (索引标记为哨兵, 数据段 invalid_record_count++)
    fn delete(&mut self, timestamp: i64) -> io::Result<()>;

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
    ├─ record_size = 2 + 8 + data.len()
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
    │    │      └─ 失败 → 目标 block 无法原地修改 (已压缩、已密封或非法位置)
    │    │           └─ **回退到乱序写入**: append 到最新段 + update_entry + invalid_record_count++
    │    │
    │    └─ 索引条目不变 (仅当原地覆盖成功时), latest_written_timestamp 不变
    │
    ├─ if timestamp < latest_written_timestamp (乱序写入, 两种模式通用):
    │    │
    │    ├─ 1. 新数据 append 到最新数据段 → (seg_offset, new_block_offset, new_in_block_offset)
    │    │
    │    ├─ 2. time_index.update_entry(timestamp, new_block_offset, new_in_block_offset)
    │    │      → 返回 old_entry (旧索引条目, 用于判断是否需要 invalid_record_count++)
    │    │
    │    ├─ 3. 根据 old_entry 状态:
    │    │      ├─ 条目存在且引用数据 (block_offset ≠ FILLER):
    │    │      │    ├─ 原地覆盖索引条目 (block_offset + in_block_offset)
    │    │      │    ├─ 定位旧数据所在数据段 (block_offset → segment)
    │    │      │    └─ 该段 invalid_record_count += 1
    │    │      │
    │    │      ├─ 条目存在且为 filler (仅连续模式):
    │    │      │    └─ 原地覆盖为真实条目 (invalid_record_count 不变)
    │    │      │
    │    │      └─ 条目不存在:
    │    │           └─ Error("out-of-order write requires existing index entry")
    │    │
    │    └─ 成功 → return Ok(())
    │       (latest_written_timestamp 不变)
    │
    ├─ timestamp > latest (正序写入):
    │    │
    │    ├─ 非连续模式: 正常写入 + 追加索引
    │    └─ 连续模式: 填充 filler + 正常写入
    │
    └─ latest_written_timestamp = timestamp (仅正序写入时更新)
```

**纠正写入**: 当 `timestamp == latest_written_timestamp` 时, 允许覆盖之前写入的同时间戳数据 (数据纠正场景)。

**乱序写入**: 当 `timestamp < latest_written_timestamp` 时, 数据追加到最新数据段 (正常写入到 pending block), 同时查找索引中该时间戳的现有条目并原地更新为新的数据位置。**两种模式均要求索引中存在该时间戳的条目** (连续模式总是存在 filler 条目, 非连续模式仅在曾写入过该时间戳时存在)。若索引中无条目则返回错误。

**原地覆盖策略 (In-Place Overwrite, 支持变长)**:

1. **前提**: 最新写入的记录必然位于 **最新数据段** 的 **最后一个未压缩 block** 的最后一条位置。可能形态:
   - **Pending block** (flags = 0): 尚未密封, 未压缩
   - **Sealed block** (flags = SEALED): 已密封但未压缩 (压缩未受益, seal 时保留原始格式)
2. **回退 (非错误)**: 如果最后一个 block 的 flags 含 `COMPRESSED` (数据已被压缩)、block 已密封、record 不是最后一条, 或最新数据段无打开的映射 — 无法原地修改时, 自动回退为**乱序写入**: 新数据追加到最新数据段 (新的 pending block), 索引条目原地更新为新的 (block_offset, in_block_offset), 同时旧数据所在段的 `invalid_record_count += 1`
3. **支持变 size**: 新 data 可以比原 data 大或小, 只需移动后续字节并更新相关计数字段
4. **索引不变**: block_offset + in_block_offset 仍指向同一 record 起始位置, data_len (u16) 更新为新长度
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

其中 `delta = new_record_bytes - old_record_bytes = new_data.len() - old_data_len` (record_overhead 固定为 10)

**overwrite_in_last_block 实现逻辑**:
```rust
// DataSegmentSet::overwrite_in_last_block(block_offset, in_block_offset, new_data):
//   1. 定位到最新数据段 (seg = self.segments.last_mut())
//      验证 block_offset 落在该段且为段内最后一个 block
//      block.start = DATA_HEADER_SIZE + (block_offset - seg.file_offset)
//   2. 读取 block header (16B at block.start)
//      - 检查 flags & COMPRESSED == 0 (若含 COMPRESSED → 返回错误, 由 correct_write 捕获并回退到乱序写入)
//      - 计算 record 在 payload 中的位置
//      - 验证 record 是 block 内最后一条:
//        in_block_offset + 10 + old_data_len == payload_size
//      - 若否, 返回错误 (只支持最新 block 的最末 record)
//   3. 计算 delta = new_data.len() - old_data_len (i32)
//   4. 更新 mmap 中 record 的 data_len (u16) 和 data 字节
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
// DataSegmentSet::append_record + TimeIndex::update_entry:
//   1. 新数据追加到最新数据段 (正常写入到 pending block 或创建新 block)
//      → (seg_offset, new_block_offset, new_in_block_offset)
//   2. time_index.update_entry(timestamp, new_block_offset, new_in_block_offset)
//      → 查找现有索引条目, 原地覆盖 18 字节为新的 (block_offset, in_block_offset)
//      → 返回 old_entry (旧索引条目)
//   3. if old_entry.block_offset ≠ FILLER (旧索引引用了实际数据):
//        old_segment = segments.locate_segment(old_entry.block_offset)
//        old_segment.invalid_record_count += 1
//      else (旧索引为 filler, 仅连续模式):
//        // 无实际数据被替代, invalid_record_count 不变
```

> **索引原地更新**: 索引条目 18 字节通过 mmap 直接覆盖, 不改变条目总数。
> - **连续模式**: O(1) 定位 — `pos = INDEX_HEADER_SIZE + (ts - seg_start_ts) × INDEX_ENTRY_SIZE`
> - **非连续模式**: 在 in_memory_buffer 中线性搜索, 或在已打开的 IndexSegment 中二分查找; 若目标在 closed segment 中, 临时打开 → 覆盖 → idle_close
> - **崩溃安全**: 18 字节非原子写入, 与现有 mmap 写入的崩溃容忍度一致 (最坏损失 = flush 间隔内未 sync 的写入)
>
> **invalid_record_count 更新**: 通过 `block_offset` 计算旧数据所在数据段 (段路由: `file_offset = (block_offset / segment_size) × segment_size`), 再对该段 `invalid_record_count` 字段 +1。段可能已关闭, 需通过 `lazy_open` 临时打开以更新 mmap state 字段。

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
    │    │    ├─ 定位旧数据所在数据段: segment = locate_segment(old_block_offset)
    │    │    ├─ segment.invalid_record_count += 1
    │    │    └─ 更新段 file header state
    │    │
    │    └─ 条目不存在 / 条目为 filler:
    │         └─ Error("not found") — 无可删除的记录
    │
    └─ return Ok(())
```

> **查询影响**: 删除后, 查询路径自动跳过 `block_offset == 0xFFFFFFFFFFFFFFFF` 的哨兵条目, 被删除的记录不会出现在查询结果中。无需修改查询逻辑。
>
> **物理数据保留**: 被删除的 record 物理上仍存在于数据段 block 中, 不影响后续 block 的读写。仅在数据段回收时 (retention reclaim 或 `invalid_record_count` 达到阈值触发 compaction) 才会物理清除。
>
> **崩溃安全**: 与写入操作一致 — 索引覆盖 + `invalid_record_count` 递增均为 mmap 写入, 最坏损失 = flush 间隔内未 sync 的操作。
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
    │      ├─ 计算 cache_key = (segment_path, entry.block_offset)
    │      ├─ 检查全局缓存池:
    │      │   ├─ 命中 → 从缓存读取解压后的 block payload → 跳至定位 record
    │      │   └─ 未命中 → 继续 ↓
    │      │
    │      ├─ 通过 block_offset 定位 data segment
    │      ├─ 读 BlockHeader, 检查 compressed flag
    │      ├─ compressed → 解压 entire block payload → 存入缓存池
    │      ├─ uncompressed → 读取 raw block payload → 存入缓存池
    │      ├─ in_block_offset → 定位到 [data_len:2]
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
           │      └─ 跳过 filler entries (block_offset == 0xFFFFFFFFFFFFFFFF)
           │
           ├─ 3. 检查 HotBlockCache (无锁, 查询级局部缓存)
           │      ├─ Hit (同 segment + 同 block_offset)
           │      │   └─ 直接从 hot_block.extract_record() → return
           │      └─ Miss → 继续 ↓
           │
           ├─ 4. 检查全局 BlockCache (RwLock<HashMap>)
           │      ├─ Hit → 放入 HotBlockCache → extract_record → return
           │      └─ Miss → 继续 ↓
           │
           ├─ 5. mmap 读取 Block + 解压 (如需)
           │      ├─ 读 BlockHeader, 检查 compressed flag
           │      ├─ compressed → deflate_decompress()
           │      └─ uncompressed → payload.to_vec()
           │
           ├─ 6. 更新 HotBlockCache
           │      └─ hot_block = HotBlockCache::new(key, decoded_payload)
           │
           └─ 7. 定位 record 并返回 (timestamp, data)
```

> **关键改进**:
> - **惰性化**: 索引条目按需从 source 取出, 不再全量收集到 Vec
> - **HotBlockCache**: 读取循环中保持最后解压的 Block, 同 Block 内连续读取跳过 mmap+解压
> - **无锁热点**: HotBlockCache 属于单个 QueryIterator 实例, 不涉及全局锁竞争
> - **内存节省**: 查询 100 万条记录仅需 ~64KB (1 Block) 内存, 而非 ~100MB
>
> **旧 API 兼容**: `DataSet::query()` 方法保留, 内部改为 `query_iter().collect()`

### 10.3 单时间戳读取 (`read`)

```
read(timestamp) → Option<(i64, Vec<u8>)>
    │
    ├─ 1. TimeIndex.find_entry(timestamp)
    │      → 三级搜索: in_memory_buffer → open segments → closed segments
    │      → 返回 None: 时间戳不存在, 直接返回 Ok(None)
    │
    ├─ 2. 检查 entry.block_offset
    │      └─ == BLOCK_OFFSET_FILLER (0xFFFFFFFFFFFFFFFF)
    │         → 已删除或未写入 (连续模式 filler), 返回 Ok(None)
    │
    └─ 3. segments.read_at_index(entry, cache)
           → 定位数据段, 读 Block + 解压 (如需), 定位 record, 返回 (ts, data)
```

> **与 `query` 的区别**:
> - `read` 查找单个时间戳, 不构建迭代器, 开销更小
> - `read` 返回 `Option`, 未找到时不报错 (区别于 `delete` 的 `NotFound` 错误)
> - `read` 复用 `TimeIndex.find_entry()` (三级搜索), 与 correction-write 路径一致
> - FFI 层 `tmsl_dataset_read` 返回码: 0=成功, 1=未找到, -1=错误
> - `out_data` 由 `libc::malloc` 分配, C 侧通过 `tmsl_iter_free_data` 释放 (与迭代器共享分配/释放路径)

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
