# 索引连续存储 (Index Continuous Storage)

> **核心原则**: 连续模式按固定时间步长提供 O(1) 定位能力, 但不会为大跨度时间戳全量物化 filler。缺失时间戳可以由两种方式表示: 已创建分段内的 filler 哨兵条目, 或未创建分段代表的逻辑空洞。

## 23.1 设计动机

当 `index_continuous=true` 时, 索引系统保证:
- 时间步长固定为 `time_step = 1`。业务时间戳单位通常为秒; 如果调用方使用其它整数单位, `time_step=1` 表示该单位的相邻整数。
- 第一次真实写入设置 `base_timestamp`, 不从 0 或 Unix epoch 起始填充。
- 同一已创建 index segment 内, 已物化范围的缺失时间戳使用 **filler entry** 标记无真实数据。
- 跨越大 gap 时, 只物化上一个真实写入所在 segment 的尾部和当前写入所在 segment 的前缀; 中间完整空 segment 不创建、不写 filler。
- 如果后续写入落在逻辑空洞中, 才按需创建该 timestamp 所属 segment, 并只物化该 segment 内到目标 timestamp 为止的必要 filler。

当 `index_continuous=false` 时:
- 索引按实际写入时间戳顺序 append, 无填充。
- 逆序写入 (timestamp < 最新已写入时间戳) 且索引中**存在**该时间戳条目 → **乱序写入** (追加数据到最新段 + 原地更新索引 + 旧段 `invalid_record_count++`)。
- 逆序写入且索引中**不存在**该时间戳条目 → **拒绝** (Error)。
- 相同时间戳 (timestamp == 最新已写入时间戳) → **纠正写入**。

## 23.2 连续模式坐标系

连续模式使用固定的逻辑分段网格:

```text
index_entry_size      = 18
index_entry_area_start = 128
segment_capacity      = floor((index_segment_size - index_entry_area_start) / index_entry_size)
time_step             = 1
base_timestamp        = 第一次真实写入的 timestamp
segment_ordinal(ts)   = floor((ts - base_timestamp) / (segment_capacity * time_step))
segment_start(ts)     = base_timestamp + segment_ordinal(ts) * segment_capacity * time_step
entry_index(ts)       = (ts - segment_start(ts)) / time_step
```

约束:
- `base_timestamp` 只在连续模式下有意义, 由第一次成功真实写入确定。
- 不新增单独的 `base_timestamp` 文件。第一次 flush 生成的首个数值 index segment 文件名就是 `base_timestamp`; reopen 时从现有最小数值分段文件名恢复。
- `segment_capacity` 基于配置的 `index_segment_size` 最大分段大小和固定 `index_entry_area_start=128` 计算, 不随懒分配的当前文件大小、meta/state 实际长度或未来 128 字节保留区内的 header 扩展变化。
- index segment 文件前 128 字节统一保留给 fixed prefix、Meta TLV、state 与未来扩展; 所有 index entry 无论连续/非连续模式都从文件内绝对偏移 128 开始。
- index segment 文件名仍使用 `segment_start` 的 20 位十进制格式。
- 每个 index segment 的 `start_timestamp` 表示该 segment 的逻辑起点, 不一定表示第一条真实数据的时间戳。

## 23.3 缺失时间戳表示

连续模式有两类缺失:

| 类型 | 表示方式 | 是否占磁盘 | 读取语义 | 回填语义 |
|------|----------|------------|----------|----------|
| 已物化 filler | `block_offset=0xFFFFFFFFFFFFFFFF`, `in_block_offset=0xFFFF` | 是 | `read/query` 跳过或返回 None | 直接 overwrite 为真实 entry |
| 逻辑空洞 | 该 timestamp 所属 index segment 不存在, 或 segment 存在但 `entry_index >= wrote_count` | 否 | `read/query` 视为无真实数据 | 按需创建/扩展该 segment, 物化前缀 filler 后写入真实 entry |

> 逻辑空洞是解决 P0-2 的关键: 大 gap 中间完整 segment 不创建, 也不需要后续 flush 再删除。

## 23.4 写入行为

```
DataSet::write(timestamp, data):
  │
  ├─ timestamp 是 signed i64 业务时间戳; 0 和负数都是合法值
  │
  ├─ 情况A: latest_written_timestamp is None or timestamp > latest_written_timestamp.unwrap() (正序写入)
  │    │
  │    ├─ index_continuous=false:
  │    │    └─ append real entry
  │    │
  │    └─ index_continuous=true:
  │         ├─ if latest_written_timestamp is None:
  │         │    ├─ base_timestamp = timestamp (内存态)
  │         │    ├─ create segment at segment_start = timestamp
  │         │    └─ append real entry at entry_index=0
  │         │
  │         └─ else:
  │              ├─ prev_seg = segment_start(latest_written_timestamp)
  │              ├─ curr_seg = segment_start(timestamp)
  │              ├─ if prev_seg == curr_seg:
  │              │    ├─ fill latest+1 .. timestamp-1 in same segment
  │              │    └─ append real entry
  │              └─ else:
  │                   ├─ fill latest+1 .. prev_seg_end in previous segment
  │                   ├─ skip all full middle segments (no files, no fillers)
  │                   ├─ create/open curr_seg
  │                   ├─ fill curr_seg_start .. timestamp-1
  │                   └─ append real entry
  │
  ├─ 情况B: latest_written_timestamp = Some(latest) and timestamp < latest (乱序/回填)
  │    │
  │    ├─ index_continuous=false:
  │    │    └─ 要求已有 entry, 否则 Error
  │    │
  │    └─ index_continuous=true:
  │         ├─ if timestamp < base_timestamp: Error
  │         ├─ 若 entry 已存在且是真实数据: append 新数据 + overwrite entry + old invalid_record_count++
  │         ├─ 若 entry 已存在且是 filler: append 新数据 + overwrite filler
  │         └─ 若 entry 位于逻辑空洞:
  │              ├─ create/open timestamp 所属 segment
  │              ├─ fill segment_start .. timestamp-1 中尚未物化的部分
  │              └─ append real entry
  │
  └─ 情况C: latest_written_timestamp == Some(timestamp)
       └─ 纠正写入; 失败时回退到情况B
```

### 23.4.1 填充上界

设 `C = segment_capacity`。

当 `latest` 与 `timestamp` 跨越任意大 gap 时, 正序写入最多物化:
- `latest` 所在 segment 的尾部: `< C - 1` 条 filler
- `timestamp` 所在 segment 的前缀: `< C - 1` 条 filler

因此一次正序写入最坏 filler 访问量 `< 2*C - 2`, 不随 `timestamp - latest` 增长。中间完整 segment 均为逻辑空洞。

## 23.5 哨兵值设计

| 字段 | 哨兵值 | 含义 | 合法性保证 |
|------|--------|------|-----------|
| `block_offset: u64` | `0xFFFFFFFFFFFFFFFF` | 此位置无真实数据 (filler 或已删除) | 字段语义为数据区逻辑全局 offset; 合法全局偏移远低于 u64::MAX |
| `in_block_offset: u16` | `0xFFFF` | 此位置无真实数据 (filler 或已删除) | 普通聚合 Block 的 payload 硬上限为 64KB, 真实 record 起始偏移不会达到 `0xFFFF`; 超大独占 Block 只含一条 record, offset 固定为 0 |

**哨兵值使用场景**:
- 已物化 filler 条目: 同一 segment 内为保持 O(1) entry_index 定位而写入。
- Delete 条目: `DataSet::delete(timestamp)` 将真实条目覆盖为哨兵值, 旧数据段 `invalid_record_count++`。

**读取时过滤**:
```rust
if segment_missing(timestamp) {
    return None; // 逻辑空洞
}
if entry.block_offset == BLOCK_OFFSET_FILLER {
    return None; // filler / deleted
}
```

## 23.6 查询与读取语义

| 操作 | 已物化 filler | 逻辑空洞 segment | 真实 entry |
|------|---------------|------------------|------------|
| `read(ts)` | `Ok(None)` | `Ok(None)` | 返回数据 |
| `query(start,end)` | 跳过 | 不打开、不创建, 直接跳过 | 返回数据 |
| `delete(ts)` | `NotFound` | `NotFound` | 覆盖为 filler, `invalid_record_count++` |
| 连续模式回填写入 | overwrite filler | 按需创建 segment 并写入 | overwrite real entry, 旧数据失效 |

`query` 不需要为缺失 segment 构造 filler entries。它只遍历:
- in-memory buffer 中与范围相交的 entry;
- 已打开 index segments;
- 已存在的 closed index segments。

未创建的中间 segment 直接视为全 filler 空洞。

## 23.7 重启恢复

```
DataSet::open():
  1. 扫描现有数值 index segment 文件元数据:
       - 文件名解析为 segment_start
       - 读取 wrote_count
       - 找到数值最大的非空 segment 文件, 读取其最后一个已物化 entry 的 timestamp
  2. 连续模式下, base_timestamp = 最小 segment_start
       (无任何 index segment 时为 None, 数据集为空)
  3. latest_written_timestamp = 最新非空 index segment 文件最后一条 entry 的 timestamp; 若没有任何 entry 则为 None
     (包括 delete 后保留的 filler timestamp, 与 read_latest() 不回退到更早有效记录的语义一致)
```

恢复约束:
- 如果 `base_timestamp` 不存在且没有任何 index entry, 数据集为空。
- 如果存在 index segment, 必须使用最小数值文件名作为 `base_timestamp`, 不能创建额外 base 文件。
- retention 删除老 index segment 后, 已删除时间范围不可回填; reopen 时以剩余最小分段文件名作为可恢复基准。混合 index segment 必须保留, 只有整个分段最后一个已物化 timestamp 也早于 retention threshold 时才删除。

## 23.8 连续模式 O(1) 查找

连续模式下, 单时间戳查找:

```text
if base_timestamp is None or ts < base_timestamp:
    return None

segment_start = segment_start(ts)
if segment file does not exist:
    return None

entry_index = entry_index(ts)
if entry_index >= segment.wrote_count:
    return None

read entry at 128 + entry_index * INDEX_ENTRY_SIZE
validate entry.timestamp == ts
```

范围查询仍然是 `O(existing_segments_in_range + returned_entries)`, 不再与大 gap 中未创建 segment 的数量成正比。

## 23.9 实现影响

需要新增或调整的设计接口:
- `TimeIndex::set_or_load_base_timestamp(first_ts)`
- `TimeIndex::segment_start_for(timestamp) -> Option<i64>`
- `TimeIndex::append_sparse_continuous_entry(prev_latest, timestamp, block_offset, in_block_offset)` (`block_offset` 为数据区逻辑全局 offset)
- `TimeIndex::upsert_sparse_continuous_entry(timestamp, block_offset, in_block_offset) -> Option<old_entry>` (`block_offset` 为数据区逻辑全局 offset)
- `IndexSegment::materialize_until(timestamp, real_entry)`

现有 `remove_pure_filler_segments()` 只能作为兼容清理, 不能作为大 gap 的主要策略。新设计必须在写入前跳过中间完整空 segment。

---

**相关**: [时间索引](time-index.md) | [数据集操作](dataset-operations.md) | [设计决策](design-decisions.md)
