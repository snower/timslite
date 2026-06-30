# 查询迭代器 - 当前实现边界

> 目标: 范围查询不一次性加载 record payload。public Rust iterator 持有 Store-managed dataset, 每次推进时重新通过 dataset 的正常 read 流程读取当前可见的 index entry 和 record。

## 一、状态分层

| 层级 | 当前状态 | 说明 |
|------|---------|------|
| `DataSet::query()` | 兼容便利 API | 内部调用 `query_iter().collect_all()`, 最终仍返回 `Vec<(i64, Vec<u8>)>` |
| `DataSet::query_iter()` | dataset-managed lazy iterator | `QueryIterator` 持有 `Arc<Mutex<DataSetInner>>`, `next_ts`, `end_ts`, `HotBlockCache` |
| `DataSet::query_length_iter()` | dataset-managed lazy length iterator | `QueryLengthIterator` 按相同 timestamp cursor 推进, 只读取 record header/data_len |
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
    -> DataSetInner::next_query_index_entry(next_ts, end_ts)
    -> update next_ts to entry.timestamp + 1
    -> lock dataset
    -> DataSetInner::read_entry_with_hot_cache(entry, hot_block)
    -> skip filler/deleted/expired entries
    -> return (timestamp, Vec<u8>)
```

`QueryLengthIterator` 使用相同推进模型, 但最终调用 `read_entry_data_len_with_hot_cache()` 只读取 record length。

## 三、HotBlockCache

`HotBlockCache` 属于单个 iterator。它不保存可变 raw block, 也不拥有全局 cache entry payload; 对 immutable compressed block, hot cache 只保留全局 `BlockCache` entry data 的 weak reference。全局 cache invalidate 或 dataset close 清理 scope 后, hot cache 命中自然失效。

## 四、边界声明

- Rust public iterator 生命周期受 dataset 管理; dataset 关闭后 iterator 继续读取会返回 dataset closed 类错误。
- iterator 不再保存 segment file cursor 列表; 每次推进都通过 dataset 当前 `TimeIndex` 查下一条 index entry。
- 后续 delete/out-of-order/correction 可能影响 iterator 之后的读取结果; iterator 不提供创建时 index snapshot 语义。
- FFI iterator 仍以 wrapper 侧 public API 结果为边界, 不暴露 `IndexEntry`、`QueryIterator` 或 `HotBlockCache` 内部类型。

## 五、内存与性能口径

| 场景 | 当前实现口径 |
|------|-------------|
| `DataSet::query()` | collect 全部结果到 `Vec`, 内存随结果集增长 |
| `DataSet::query_iter()` | 持有 dataset handle、timestamp cursor、当前 hot block 和当前 record |
| `DataSet::query_length_iter()` | 持有 dataset handle、timestamp cursor 和当前 hot block, 不读取 payload |
| C ABI iterator | wrapper snapshot 语义, 内存上界由 wrapper 收集结果决定 |

禁止表述:

- 不得把 `DataSet::query()` 描述为流式返回。
- 不得再声明 Rust iterator 使用旧的 source-list 查询模型。
- 不得把 FFI 当前实现描述为 zero-copy 或严格常量内存。

---

**相关**: [数据集操作](dataset-operations.md) | [时间索引](time-index.md) | [后台任务与缓存](background-and-cache.md)
