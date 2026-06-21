# 查询迭代器 — 当前实现与目标边界

> 目标: 范围查询不再一次性加载所有数据; 索引按 source cursor 逐条推进, record 按需读取, 单次查询内部保留 HotBlockCache。

---

## 一、状态分层

| 层级 | 当前状态 | 说明 |
|------|---------|------|
| `DataSet::query()` | 兼容便利 API | 内部调用 `query_iter().collect_all()`, 因此最终仍返回 `Vec<(i64, Vec<u8>)>` |
| `DataSet::query_iter()` | 已实现惰性读取 | 创建 `QueryIterator`, record 在 `next_entry()` 时读取 |
| `TimeIndex::prepare_query_sources()` | 已实现 source cursor | 落盘 index segment 只保存 path + `[start_idx, end_idx)`, 迭代时逐条读取 `IndexEntry` |
| `HotBlockCache` | 已实现 Rust 查询局部缓存 | 同一查询内复用最近读取的 block payload |
| FFI `tmsl_dataset_query` / `tmsl_iter_next` | 已实现 index-entry snapshot + 数据按需读 | `tmsl_dataset_query` 复制命中范围的 `IndexEntry`; `tmsl_iter_next` 仍为每条 record 分配 `malloc` buffer |
| FFI zero-copy/buffer API | 目标设计 | 后续可添加 `tmsl_iter_next_buf` 由 C 侧提供 buffer |

**边界声明**: Rust `DataSet::query_iter()` 使用 `QuerySource` 保持索引 cursor; FFI iterator 为了跨 C 调用边界提供明确 snapshot 语义, 在 `tmsl_dataset_query` 时复制命中范围的 `Vec<IndexEntry>`。因此不能声称 FFI 查询为严格 64KB 常量内存; 内存上界受查询命中 index entry 数量、当前 record 大小和调用方释放节奏影响。

---

## 二、当前流程

```
DataSet::query_iter(start_ts, end_ts)
    │
    ├─ 1. clamp retention query range
    │
    ├─ 2. TimeIndex::prepare_query_sources()
    │      ├─ InMemory: 复制命中范围内未 flush IndexEntry, 并按 timestamp 排序
    │      ├─ OpenSegment: 计算 [start_idx, end_idx), source 持有 segment path
    │      └─ ClosedSegment: 临时打开计算 [start_idx, end_idx), source 持有 segment path
    │
    └─ 3. QueryIterator::next_entry()
           ├─ 从当前 QuerySource 逐条读取 IndexEntry
           ├─ 跳过 filler/deleted entry
           ├─ 通过 DataSegmentSet 读取对应 record
           ├─ 查询级 HotBlockCache 复用最近 block
           └─ 返回 (timestamp, Vec<u8>)
```

`DataSet::query()` 仅是兼容包装:

```rust
let iter = self.query_iter(start_ts, end_ts)?;
iter.collect_all()
```

因此 `query()` 的返回集合仍由调用方一次性持有; 需要流式消费时必须使用 `query_iter()` 或 FFI iterator。
Store 管理的 DataSet 创建 `QueryIterator` 时会从 `DataSetRuntimeContext` 克隆 `Arc<BlockCache>` 并交给 iterator 持有, 因此 public `query_iter()` 不需要调用方传入 cache, 且 iterator 返回后仍可安全使用全局 compressed block 缓存。

---

## 三、QuerySource

```rust
pub enum QuerySource {
    InMemory {
        entries: Vec<IndexEntry>,
        position: usize,
    },
    SegmentFile {
        path: PathBuf,
        start_timestamp: i64,
        segment_size: u64,
        index_continuous: bool,
        start_idx: usize,
        end_idx: usize,
        position: usize,
        first_timestamp: i64,
        segment: Option<IndexSegment>,
    },
}
```

设计约束:

- `InMemory` 只保存尚未 flush 且命中查询范围的 entry; 这是为了 snapshot 未落盘索引状态。
- `SegmentFile` 不保存 entry 列表, 只保存文件路径和索引范围; 进入该 source 时按需打开 index segment, 每次读取一个 entry。
- source 按 `first_timestamp` 排序。非连续索引通过临时打开 segment 读取范围内首条 entry 的 timestamp。
- 连续索引中未创建的中间空洞 segment 不产生 source。

---

## 四、HotBlockCache

`HotBlockCache` 是单个 `QueryIterator` 内部的局部缓存:

```rust
struct HotBlockCache {
    current_key: Option<CacheKey>,
    current_data: Vec<u8>,
    payload_size: usize,
}
```

协作规则:

- 命中同一个 `(segment_file_offset, block_segment_offset)` 时, 直接从 `current_data` 提取 record。
- miss 时通过 `DataSegmentSet::read_at_index_with_hot_cache()` 读取 block。
- compressed block 可查询全局 `BlockCache`; pending raw block 只允许进入本次查询的 HotBlockCache, 不进入全局缓存。
- HotBlockCache 生命周期不跨越写入操作, 不参与 correction/delete/out-of-order 的全局缓存一致性。

---

## 五、FFI Iterator

当前 FFI iterator 是 index-entry snapshot + 按需 record 读取, 但不是 zero-copy:

```rust
struct FfiIterator {
    store: Arc<Mutex<Store>>,
    handle: DataSetHandle,
    entries: Vec<IndexEntry>,
    position: usize,
}
```

流程:

1. `tmsl_dataset_query` 锁定 FFI Store mutex, 再锁定目标 DataSet, 调用 `query_index_entries(start_ts, end_ts)` 复制当前命中范围的 `IndexEntry` snapshot; filler/deleted entry 可以保留到 `next` 时跳过, 也可以在创建时过滤。
2. `tmsl_iter_next` 只从 snapshot `entries` 中逐条推进, 不再打开或读取 index segment 文件。
3. `tmsl_iter_next` 通过 snapshot entry 锁定对应 DataSet 并调用 `read_entry_at_index` 读取当前 record。
4. 返回数据仍由 `libc::malloc` 分配, C 侧必须用 `tmsl_data_free` 释放。
5. `tmsl_iter_free_data` 是兼容别名。

一致性语义:

- FFI iterator 的索引集合是 `tmsl_dataset_query` 调用时刻的 snapshot, 后续 delete/out-of-order rewrite/correction 不改变该 iterator 已持有的 entry 列表。
- snapshot 不复制 data payload。`tmsl_iter_next` 仍按 snapshot entry 读取源 dataset 当前仍可访问的数据位置。
- 如果 snapshot entry 指向的数据已被 retention reclaim 删除、文件损坏或边界校验失败, `tmsl_iter_next` 返回错误; 它不会重新查询当前 index 来替换结果。
- 因为 iterator 不再直接打开 index segment, index segment 在迭代期间被 retention 删除不会影响 entry 推进; data segment 是否仍可读由 `read_entry_at_index` 的 retention/边界校验决定。

后续可选 API:

```c
int tmsl_iter_next_buf(void* iter, int64_t* out_ts,
                       unsigned char* buf, size_t buf_size,
                       size_t* out_written,
                       char* err_buf, size_t err_buf_len);
```

只有 buffer API 落地后, 才能对 FFI 层声明“无每条 record malloc/free”。

---

## 六、内存与性能口径

### 6.1 Public Rust boundary

`QueryIterator`, `QuerySource`, `SourceIndex`, `IndexEntry`, and physical entry read helpers are internal implementation details. They are used by crate-internal dataset code, queue scan logic, journal source dereference, and FFI iterator state, but they are not re-exported as the normal Rust library API.

External Rust callers use `DataSet::query` / `DataSet::query_length` snapshots, `DataSet::query_length_iter` for source-cursor lazy length iteration, or the Store-level facades. Journal consumers that need to dereference a write/delete/append journal pointer call `Store::read_journal_source_record(dataset_identifier, index_info)` instead of constructing an `IndexEntry`.

| 场景 | 当前实现口径 |
|------|-------------|
| `DataSet::query()` | 最终收集全部结果到 `Vec`, 内存随结果集增长 |
| `DataSet::query_iter()` | 不全量收集落盘 `IndexEntry`; 按需读取 record; 持有当前 hot block |
| FFI iterator | 创建时复制命中范围 `IndexEntry`; 每条 record 仍 malloc 返回 |
| 未 flush index buffer | 命中范围内 entry 会被复制为 snapshot |
| exclusive/single-record block | 当前 record buffer 可超过 64KB, 独占 block 正常返回 |

禁止表述:

- 不得把 FFI 当前实现描述为 zero-copy。
- 不得把当前 FFI 查询描述为严格 64KB 常量内存。
- 不得把 `DataSet::query()` 描述为流式返回; 它只是内部复用 iterator 后 collect。

---

## 七、边界场景

| 场景 | 处理策略 |
|------|---------|
| `start_ts > end_ts` | 返回空 iterator / 空结果 |
| 范围完全过期 | retention clamp 后返回空 iterator / 空结果 |
| source 中为 filler/deleted entry | `next_entry()` / `tmsl_iter_next` 跳过 |
| index segment 尚未创建的连续模式空洞 | 不生成 source |
| index segment 文件在 FFI 迭代期间被 retention 删除 | FFI iterator 已持有 `IndexEntry` snapshot, 不再访问 index segment |
| data segment 文件在 FFI 迭代期间被 retention 删除 | `read_entry_at_index` 返回过期/缺失/损坏错误, 不重新查询当前 index |
| iterator 活跃时写入同一 DataSet | Rust API 由 `&mut DataSet` 借用阻止; FFI 由 FFI Store mutex + DataSet Mutex 串行化 Store/DataSet 入口 |

---

**相关**: [数据集操作](dataset-operations.md) | [时间索引](time-index.md) | [后台任务与缓存](background-and-cache.md)
