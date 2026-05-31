# 查询迭代器优化 — Virtual Iterator + Hot Block Cache

> 目标: 将 query 从"全量加载→内存排序→返回 Vec"改为惰性虚拟迭代, 并在读取循环中保持最后解压的 Block 数据

---

## 一、问题分析

### 1.1 当前流程

```
DataSet::query(start_ts, end_ts)
    │
    ├─ 1. TimeIndex::query() → 收集所有 IndexEntry 到 Vec<IndexEntry>
    │      ├─ 遍历 in_memory_buffer
    │      ├─ 遍历 index_segments (打开每个 segment)
    │      ├─ 遍历 closed_index_segments (打开每个 segment)
    │      └─ sort + dedup
    │
    ├─ 2. 对每个 entry 调用 read_at_index()
    │      ├─ mmap 读取 BlockHeader, 判断是否 compressed
    │      ├─ compressed → 检查全局 BlockCache; miss 时 mmap 读取 + 解压 + 放回缓存
    │      └─ raw → mmap 读取 payload, 不进入全局 BlockCache
    │
    └─ 3. 排序返回 Vec<(i64, Vec<u8>)>
```

### 1.2 三大瓶颈

| 瓶颈 | 现象 | 根因 |
|------|------|------|
| **索引全量加载** | 大范围查询占用大量内存 | `TimeIndex::query()` 立即收集所有 entries 到 Vec |
| **数据全量加载** | 调用方只需要前 N 条却加载全部 | `DataSet::query()` 遍历所有 entries → Vec |
| **FFI 迭代器是伪迭代** | `FfiIterator` 仅持有数组索引 | 实际数据在 `query()` 调用时已全部加载 |

### 1.3 关键洞察

**时序数据读取模式特征:**
- 80%+ 查询为顺序遍历 (按时间范围扫描)
- 同一 Block 内最多可容纳数千条 record (Block 最大 64KB)
- 读取同 Block 内连续 record 时, 重复 mmap 读取和解压完全是浪费

**现有 BlockCache 的局限:**
- 全局共享锁 (`RwLock<HashMap>`) — 每个 record 都要锁竞争
- 缓存粒度是整个解压 Block — 但对顺序读来说, 只需要"上一个 Block"就够了
- 没有跨 `read_at_index` 调用的状态保持 — 每次都是独立调用

---

## 二、设计目标

| 目标 | 说明 |
|------|------|
| **惰性化** | 索引条目按需从 source 取出, 不再全量收集 |
| **Block Hot Cache** | 读取循环中保持最后解压的 Block 数据, 同 Block 内跳过 mmap+解压 |
| **零拷贝传递** | FFI 层不需要 `malloc` 每条 record, 由 C 侧传入 buffer |
| **向后兼容** | 保留 `DataSet::query()` 方法作为 `QueryIterator` 的便利包装 |

---

## 三、QueryIterator 核心设计

### 3.1 数据来源枚举

```rust
/// QueryIterator 的数据来源 (按优先级排列)
enum QueryDataSource<'a> {
    /// 内存中的 index entries (未 flush 的)
    InMemory { entries: &'a [IndexEntry], index: usize },
    /// 打开的数据段中的 index entries
    OpenSegment { segment: &'a IndexSegment, start_idx: usize, end_idx: usize, current_idx: usize },
    /// 关闭的数据段中的 index entries (需要临时打开)
    ClosedSegment { meta: &'a IndexSegmentMeta, segment: Option<IndexSegment>, start_idx: usize, end_idx: usize, current_idx: usize },
}
```

### 3.2 QueryIterator 结构

```rust
pub struct QueryIterator<'a> {
    // 数据集引用
    time_index: &'a TimeIndex,
    segments: &'a mut DataSegmentSet,
    cache: Option<&'a BlockCache>,
    query_range: (i64, i64),      // (start_ts, end_ts)

    // ── 惰性索引遍历状态 ──
    sources: Vec<QueryDataSource<'a>>,   // 所有数据来源 (按时间顺序排列)
    current_source: usize,                // 当前正在遍历的 source 索引

    // ── Block Hot Cache (读取循环级局部缓存) ──
    hot_block: HotBlockCache,

    // 排序缓冲 (仅当前 source 内部需要, 跨 source 已天然有序)
    sorted_current: Vec<IndexEntry>,
    sorted_idx: usize,

    // FFI 安全: 当前返回的数据副本 (防止悬垂指针)
    current_record: Option<(i64, Vec<u8>)>,
}
```

### 3.3 HotBlockCache 结构

```rust
/// 读取循环级局部 Block 缓存
/// 不涉及锁竞争, 属于单个 QueryIterator 实例
struct HotBlockCache {
    /// 当前热点 block 的 cache key
    current_key: Option<CacheKey>,
    /// 解压后的完整 block payload (包含所有 raw records)
    current_data: Vec<u8>,
    /// block 的 payload 总大小 (用于边界检查)
    payload_size: usize,
}

impl HotBlockCache {
    /// 判断给定的 entry 是否在已缓存的 block 中
    fn is_hit(&self, segment_file_offset: u64, block_segment_offset: u64) -> bool {
        self.current_key.as_ref() == Some(&CacheKey::new(segment_file_offset, block_segment_offset))
    }

    /// 从热点缓存中提取单条 record
    fn extract_record(&self, in_block_offset: u16) -> Result<(i64, Vec<u8>)> {
        let pos = in_block_offset as usize;
        // [data_len:4][timestamp:8][data:N]
        if pos + 12 > self.current_data.len() {
            return Err(...);
        }
        let data_len = read_u32_le(&self.current_data[pos..pos+4]) as usize;
        let timestamp = read_i64_le(&self.current_data[pos+4..pos+12]);
        let data = self.current_data[pos+12..pos+12+data_len].to_vec();
        Ok((timestamp, data))
    }
}
```

### 3.4 next() 方法流程

```
QueryIterator::next() → Option<Result<(i64, Vec<u8>)>>
    │
    ├─ 1. 获取下一个非 filler 的 IndexEntry
    │      ├─ 从当前 source 取 entry (按时间顺序)
    │      ├─ 当前 source 耗尽 → 切换到下一个 source
    │      └─ 所有 source 耗尽 → return None
    │      跳过 filler entries; 连续模式逻辑空洞不生成 source
    │
    ├─ 2. 检查 Hot Block Cache
    │      ├─ Hit (同一个 data segment 且同一个段内 block offset)
    │      │   └─ 直接从 hot_block.extract_record() 返回
    │      │
    │      └─ Miss → 继续 ↓
    │
    ├─ 3. Block 读取 + 解压
    │      ├─ 通过 block_offset 找到对应 DataSegment, 并转换为 block_segment_offset
    │      ├─ 读 BlockHeader, 检查 compressed flag
    │      ├─ compressed → 先查全局 BlockCache; miss 时 deflate_decompress() 并写入全局缓存
    │      └─ uncompressed → payload.to_vec(), 只进入 HotBlockCache
    │
    ├─ 4. 更新 Hot Block Cache
    │      └─ hot_block = HotBlockCache::new(key, decoded_payload)
    │
    └─ 5. 定位 record 并返回
           └─ 从 decoded payload[in_block_offset] 提取 (timestamp, data)
```

### 3.5 FFI Iterator 适配

```rust
struct FfiIterator {
    store_ptr: *mut Store,
    handle: DataSetHandle,
    internal_iter: Option<Box<QueryIterator<'static>>>,  // 持有内部查询迭代器
    // 不再持有 Vec<(i64, Vec<u8>)>
}
```

**新 `tmsl_iter_next` 签名 (可选, 或保持原签名):**

```c
// 方案 A: 保持原签名 (向后兼容)
// tmsl_iter_next 内部仍 malloc 数据, 但数据来源是惰性迭代
int tmsl_iter_next(void* iter, long long* out_ts,
                   unsigned char** out_data, size_t* out_len,
                   char* err_buf, size_t err_buf_len);

// 方案 B: 零拷贝版本 (新函数)
// C 侧提供 buffer, 避免 malloc/free
int tmsl_iter_next_buf(void* iter, long long* out_ts,
                       unsigned char* buf, size_t buf_size, size_t* out_written,
                       char* err_buf, size_t err_buf_len);
```

> **推荐**: 先实现方案 A (保持原签名), 后续可选添加方案 B。

---

## 四、索引惰性化策略

### 4.1 TimeIndex 预扫描

```rust
impl TimeIndex {
    /// 准备查询: 返回按时间顺序排列的索引源列表
    /// 不实际加载数据, 只建立 source 映射
    fn prepare_query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<QuerySource>> {
        let mut sources = Vec::new();

        // 1. 内存 buffer (按时间过滤)
        let mem_range = self.filter_memory_range(start_ts, end_ts);
        if !mem_range.is_empty() {
            sources.push(QuerySource::InMemory(mem_range));
        }

        // 2. 打开的 segments (利用连续模式 O(1) 计算范围)
        for seg in &mut self.index_segments {
            seg.ensure_open()?;
            if let Some((start_idx, end_idx)) = seg.query_range_indices(start_ts, end_ts, self.index_continuous) {
                sources.push(QuerySource::OpenSegment { seg, start_idx, end_idx });
            }
        }

        // 3. 关闭的 segments (按时间范围预过滤, 不打开文件)
        for meta in &self.closed_index_segments {
            if self.range_overlaps_segment(start_ts, end_ts, meta) {
                sources.push(QuerySource::ClosedSegment(meta));
            }
        }

        // sources 已按时间顺序 (in_memory → open → closed 按 start_timestamp 排序)
        Ok(sources)
    }
}
```

### 4.2 IndexSegment 范围索引查询

```rust
impl IndexSegment {
    /// 返回 [start_ts, end_ts] 范围内的 entry 索引范围 [start_idx, end_idx)
    /// 连续模式: O(1) 直接计算
    /// 非连续模式: 二分查找
    pub fn query_range_indices(
        &self,
        start_ts: i64, end_ts: i64,
        index_continuous: bool,
    ) -> Option<(usize, usize)> {
        let start_idx = self.lower_bound_cs(start_ts, index_continuous);
        let end_idx = self.upper_bound_cs(end_ts, index_continuous);
        if start_idx >= end_idx {
            None
        } else {
            Some((start_idx, end_idx))
        }
    }
}
```

---

## 五、DataSet API 变更

### 5.1 新增方法

```rust
impl DataSet {
    /// 返回虚拟迭代器 (惰性查询, 不加载全部数据)
    pub fn query_iter(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&BlockCache>,
    ) -> Result<QueryIterator<'_>> {
        QueryIterator::new(self, start_ts, end_ts, cache)
    }
}
```

### 5.2 保留旧方法 (向后兼容)

```rust
impl DataSet {
    /// 查询 records (便利方法, 内部使用 QueryIterator)
    ///
    /// 原有 API 保持不变, 内部改为:
    ///   self.query_iter(start_ts, end_ts, cache)?.collect()
    pub fn query(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&BlockCache>,
    ) -> Result<Vec<(i64, Vec<u8>)>> {
        let mut results = Vec::new();
        let mut iter = self.query_iter(start_ts, end_ts, cache)?;
        while let Some(record) = iter.next()? {
            results.push(record);
        }
        // 结果已按时间顺序 (iterator 保证)
        Ok(results)
    }
}
```

---

## 六、FFI 变更

### 6.1 FfiIterator 变更

```rust
// 旧:
struct FfiIterator {
    entries: Vec<(i64, Vec<u8>)>,  // ← 全量数据
    index: usize,
}

// 新:
struct FfiIterator {
    store_ptr: *mut Store,
    handle: DataSetHandle,
    // query() 时创建 QueryIterator, tmsl_iter_next 按需驱动
    // FfiIterator 自身成为 QueryIterator 的 FFI wrapper
    internal_state: Box<dyn Iterator<Item = Result<(i64, Vec<u8>)>>>,
}
```

### 6.2 函数签名不变

所有 12 个 FFI 函数签名保持不变, 仅内部实现替换:

| 函数 | 变更 |
|------|------|
| `tmsl_dataset_query` | 创建 `QueryIterator` 而非全量 `Vec` |
| `tmsl_iter_next` | 驱动 `QueryIterator::next()` 而非数组索引 |
| `tmsl_iter_close` | 清理 `QueryIterator` 资源 |

---

## 七、性能分析

### 7.1 内存节省

| 场景 | 旧方案 | 新方案 | 节省 |
|------|--------|--------|------|
| 查询 100 万条记录 (每条 100B) | ~100MB 内存 | ~64KB (1 Block) | **99.9%** |
| FFI Iterator 生命周期 | 全数据持有到 close | 无持有 | **100%** |
| IndexEntry 收集 | `Vec<IndexEntry>` (18B × N) | 逐条 yield | **~100%** |

### 7.2 CPU 优化

| 操作 | 旧方案 | 新方案 | 优化 |
|------|--------|--------|------|
| 同 Block 连续读 | 每次查全局 BlockCache (RwLock) | 直接 hit HotBlockCache | **消除锁竞争** |
| 解压开销 | 每 record 可能重复解压 | 同 Block 只解压 1 次 | **N→1** |
| 全局 Cache Miss | 每次独立查 HashMap | 连续读几乎 0 miss | **O(1) vs O(log N)** |

### 7.3 BlockCache 与 HotBlockCache 的关系

```
全局 BlockCache (跨查询共享, LRU, RwLock 保护)
    ↕
QueryIterator::HotBlockCache (单查询内部, 无锁, 仅热点)
    ↕
DataSegment::mmap (文件读取)
```

**协作逻辑:**

```
read_record(block_offset):
    1. 计算 segment.file_offset 和 block_segment_offset = block_offset - segment.file_offset
    2. 检查 HotBlockCache.is_hit(segment.file_offset, block_segment_offset)
       ├─ Yes → 直接提取 record, return
       └─ No  → 继续
    3. mmap 从 segment.header_len + block_segment_offset 读取 BlockHeader, 判断 flags
    4. 若为 compressed, 检查全局 BlockCache.get(segment.file_offset, block_segment_offset)
       ├─ Hit → 存入 HotBlockCache, 提取 record, return
       └─ Miss → 继续
    5. mmap 读取 payload + 解码
       → 存入 HotBlockCache
       → compressed block 存入全局 BlockCache (可选, 取决于 cache 容量)
       → raw block 不进入全局 BlockCache
       → 提取 record, return
```

> **注意**: HotBlockCache 是单次查询内部的局部热点缓存, 不影响全局 BlockCache 的统计。全局 BlockCache 只保存 compressed block 的解压 payload, 因为 compressed block 不允许再被原地修改。

---

## 八、生命周期与安全性

### 8.1 Rust Iterator 生命周期

`QueryIterator<'a>` 的生命周期受限于 `&'a mut DataSet`, 因此:
- Iterator 持有期间, DataSet 的 `&mut` 被借用
- 无法在 iterator 活跃时调用 `write()` 或其他 `&mut` 方法
- 符合 Rust 借用规则, 编译期保证安全

### 8.2 FFI 生命周期

`FfiIterator` 内部使用 `Arc<Mutex<DataSet>>` 引用而非裸借用:
```rust
struct FfiQueryIterator {
    ds: Arc<Mutex<DataSet>>,        // 引用计数持有
    time_index_ref: ...             // 内部引用
    segments_ref: ...               // 内部引用
    cache: Option<&'static BlockCache>,
    ...
}
```

### 8.3 Closed Segment 打开/关闭

`QueryIterator` 遍历 closed segments 时:
- 按需打开 segment (`IndexSegment::open`)
- 查询完毕后立即关闭 (`idle_close`)
- 同一 closed segment 不会被重复打开 (遍历后推进)

---

## 九、边界场景

| 场景 | 处理策略 |
|------|---------|
| 查询范围为空 (start_ts > end_ts) | `QueryIterator` 立即返回 None |
| 范围内全是 filler entries 或逻辑空洞 | 跳过所有 filler, 未创建的空洞分段不生成 source, 返回 None |
| 查询中途 DataSet 被修改 | 借用检查阻止 (编译错误) |
| HotBlockCache 溢出 (single record > 64KB) | 正常处理, hot_block 持有超大 buffer |
| 多线程并发查询同一 DataSet | DataSet 有 Mutex 保护, 串行执行 |
| 查询过程中 segment idle-close | Iterator 持有引用, 阻止全局 idle-close |

---

> **实现优先级**:
> 1. QueryIterator 核心结构 + HotBlockCache (Phase 13.1)
> 2. TimeIndex 惰性化 prepare_query (Phase 13.2)
> 3. DataSet::query_iter + 旧 query 适配 (Phase 13.3)
> 4. FFI 适配器 (Phase 13.4)
> 5. 集成测试 (Phase 13.5)

---

**相关**: [数据集操作](dataset-operations.md) | [时间索引](time-index.md) | [后台任务与缓存](background-and-cache.md)
