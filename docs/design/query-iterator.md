# 查询迭代器 - 当前实现边界

> 目标: 范围查询不一次性加载 record payload。public Rust iterator 持有 Store-managed dataset, 每次推进时重新通过 dataset 的正常 read 流程读取当前可见的 index entry 和 record。

## 一、状态分层

| 层级 | 当前状态 | 说明 |
|------|---------|------|
| `DataSet::query()` | 兼容便利 API | 内部调用 `query_iter().collect_all()`, 最终仍返回 `Vec<(i64, Vec<u8>)>` |
| `DataSet::query_iter()` | dataset-managed lazy iterator | `QueryIterator` 持有 `Arc<Mutex<DataSetInner>>`, `IndexQueryIterator`, `HotBlockCache` |
| `DataSet::query_length_iter()` | dataset-managed lazy length iterator | `QueryLengthIterator` 复用 `IndexQueryIterator` 推进, 只读取 record header/data_len |
| `TimeIndex` | mmap-backed index segments | `add_entry()` 直接写入 index segment; 不维护额外的待刷索引列表 |
| `HotBlockCache` | Rust 查询局部缓存 | 单次 iterator 内复用最近命中的 immutable compressed block |
| C ABI iterator | snapshot wrapper | `wrapper/cffi` 通过 public API 收集结果或 length snapshot, 不暴露 crate-private iterator 内部 |

## 二、当前流程

```text
DataSet::query_iter(start_ts, end_ts)
    -> clamp retention query range
    -> QueryIterator::new(dataset Arc, start_ts, end_ts)

QueryIterator::next_entry()
    -> lock dataset
    -> IndexQueryIterator 通过 DataSetInner/TimeIndex 按当前 segment position 查下一条 index entry
    -> update cursor and next_ts to entry.timestamp + 1
    -> lock dataset
    -> DataSetInner::read_entry_with_hot_cache(entry, hot_block)
    -> skip filler/deleted/expired entries
    -> return (timestamp, Vec<u8>)
```

`QueryLengthIterator` 使用相同推进模型, 但最终调用 `read_entry_data_len_with_hot_cache()` 只读取 record length。

## 三、链式控制 API

`QueryIterator` 和 `QueryLengthIterator` 提供与标准 iterator 风格一致的链式方法:

```rust
dataset.query_iter(start_ts, end_ts)?
    .reverse()
    .skip(100)
    .collect_take(20)?;
```

- `reverse(self) -> Self`: 切换为从大 timestamp 到小 timestamp 推进; 不创建 index snapshot。
- `skip(self, n: usize) -> Self`: 当 receiver 仍是原始 `QueryIterator` / `QueryLengthIterator` 时, 使用 `IndexQueryIterator` 在索引层跳过 `n` 条非 filler entry, 不读取 data segment。若调用方先使用 `filter`、`map` 等标准 iterator adapter, receiver 已变为标准 adapter 类型, 后续 `.skip()` 走标准库 `Iterator::skip()` 语义。
- `collect_take(self, n: usize) -> Result<Vec<_>>`: 最多读取并返回 `n` 条实际结果, 不先 `collect_all()` 再截断。

`skip()` 的计数口径是索引中 `!entry.is_filler()` 的条目。它不会为了确认 data segment 是否存在、record 是否仍可读取而访问 data segment; 后续 `next()` / `collect_take()` 仍按现有读取路径处理 data segment 缺失、过期或校验错误。

`skip()` 是惰性的: 方法调用只记录跳过意图, 第一次真正读取时才通过 dataset/TimeIndex 扫描索引并推进范围边界。`skip().reverse()` 与 `reverse().skip()` 的边界不同:

- `skip(n).reverse()` 先从低 timestamp 侧跳过 `n` 条真实 index entry, 再倒序读取剩余范围。
- `reverse().skip(n)` 先切换到高 timestamp 侧, 再从高 timestamp 侧跳过 `n` 条真实 index entry。

## 四、HotBlockCache

`HotBlockCache` 属于单个 iterator。它不保存可变 raw block, 也不拥有全局 cache entry payload; 对 immutable compressed block, hot cache 只保留全局 `BlockCache` entry data 的 weak reference。全局 cache invalidate 或 dataset close 清理 scope 后, hot cache 命中自然失效。

## 五、边界声明

- Rust public iterator 生命周期受 dataset 管理; dataset 关闭后 iterator 继续读取会返回 dataset closed 类错误。
- iterator 可保存当前 index segment `start_timestamp` 与 entry offset 作为推进提示, 但不持有 `IndexSegment`、mmap 或文件句柄; 每次推进仍通过 dataset 当前 `TimeIndex` 校验 dataset/segment 状态并读取下一条 index entry。
- 后续 delete/out-of-order/correction 可能影响 iterator 之后的读取结果; iterator 不提供创建时 index snapshot 语义。
- FFI iterator 仍以 wrapper 侧 public API 结果为边界, 不暴露 `IndexEntry`、`QueryIterator` 或 `HotBlockCache` 内部类型。

## 六、内存与性能口径

| 场景 | 当前实现口径 |
|------|-------------|
| `DataSet::query()` | collect 全部结果到 `Vec`, 内存随结果集增长 |
| `DataSet::query_iter()` | 持有 dataset handle、index position cursor、当前 hot block 和当前 record |
| `DataSet::query_length_iter()` | 持有 dataset handle、index position cursor 和当前 hot block, 不读取 payload |
| C ABI iterator | wrapper snapshot 语义, 内存上界由 wrapper 收集结果决定 |

`skip()` 可避免被跳过记录的数据段读取、block 解压和重复 record materialization, 但仍需要扫描 index entry 来跳过 filler/deleted 条目。当前 index segment 不维护真实 entry prefix count, 因此 skip 不是无代价跳过所有 filler 的 O(1) 操作。

禁止表述:

- 不得把 `DataSet::query()` 描述为流式返回。
- 不得再声明 Rust iterator 使用旧的 source-list 查询模型。
- 不得把 FFI 当前实现描述为 zero-copy 或严格常量内存。

---

**相关**: [数据集操作](dataset-operations.md) | [时间索引](time-index.md) | [后台任务与缓存](background-and-cache.md)
