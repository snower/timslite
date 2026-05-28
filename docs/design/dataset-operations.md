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
    fn query(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<Vec<(i64, Vec<u8>)>>;
    fn query_iter(&mut self, start_ts: i64, end_ts: i64, cache: Option<&BlockCache>) -> io::Result<QueryIterator<'_>>;
    fn flush(&mut self) -> io::Result<()>;
    fn config(&self) -> &DataSetConfig;

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

### 9.1 Flush 行为 (mmap sync only)

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
