# 索引连续存储 (Index Continuous Storage)

> **核心原则**: 索引条目按连续序号增长, 缺失时间戳位置填充哨兵值条目 (filler)。

## 23.1 设计动机

当 `index_continuous=true` 时, 索引系统保证:
- 索引序号严格连续增长 (#1, #2, #3, ...)
- 缺失的时间戳位置填充**哨兵条目 (filler entry)**, 标记无真实数据
- 查询时可通过二分查找精确定位, filler 条目与真实条目同等对待
- 如果后续写入恰好填充了之前的 filler 位置 (匹配 timestamp), filler 被替换为真实数据

当 `index_continuous=false` 时:
- 索引按实际写入时间戳顺序 append, 无填充
- 逆序写入 (timestamp < 最新已写入时间戳) → **拒绝**
- 相同时间戳 (timestamp == 最新已写入时间戳) → **纠正写入** (最新数据段最后一个未压缩 block 的最后一条 record, 就地覆盖 data 字节, 支持变 size, 索引不变)

## 23.2 写入行为

```
DataSet::write(timestamp, data):
  │
  ├─ if timestamp == 0: return Error("timestamp must be > 0")
  │
  ├─ 写入数据到 DataSegmentSet → (seg_offset, block_rel_offset, in_block_offset)
  │
  └─ 索引更新:
       │
       ├─ 情况A: timestamp > latest_written_timestamp (正序写入)
       │    ├─ if index_continuous == true:
       │    │    └─ 填充缺失: for ts in (latest+1)..(timestamp-1):
       │    │         filler_entry = IndexEntry {
       │    │             timestamp: ts,
       │    │             block_offset: 0xFFFFFFFFFFFFFFFF,  // sentinel
       │    │             in_block_offset: 0xFFFF,
       │    │         }
       │    │
       │    └─ 写入真实条目 → latest_written_timestamp = timestamp
       │
       ├─ 情况B: timestamp < latest 且 index_continuous == true (补数据)
       │    ├─ 二分查找 filler entry at timestamp
       │    ├─ 找到 filler → mmap 覆盖写 18 字节 → 替换为真实 entry
       │    └─ 未找到 filler → Error
       │
       ├─ 情况C: timestamp < latest 且 index_continuous == false
       │    └─ Error("out-of-order")
       │
        └─ 情况D: timestamp == latest (纠正写入, 两种模式均适用)
             ├─ 查找索引条目获取 (block_offset, in_block_offset)
             ├─ 验证该 record 位于: 最新数据段 + 最后一个 block (未压缩) + block 的最末 record
             ├─ 若最后一个 block 含 COMPRESSED flag → 不支持 (返回错误)
             ├─ 数据段: 原地覆盖 data 字节, 支持改变 data 长度 (delta = new_len - old_len)
             ├─ 更新 5 个字段: block payload_size/uncompressed_size + 段 wrote_position/total_uncompressed_size/pending_wrote_position (仅 pending)
             ├─ 索引: 不变 (block_offset/in_block_offset 保持原值)
             └─ latest_written_timestamp 不变
```

### 23.2.1 边界条件

| 场景 | 行为 |
|------|------|
| ts < 0 | Error |
| ts = 0 | Error (保留给 index segment 命名) |
| ts = latest_written_timestamp | **纠正写入**: 最新数据段最后一个未压缩 block 的最末 record 原地覆盖, 支持变 size, 需更新 5 个字段 (block payload_size/uncompressed_size + 段 wrote_position/total_uncompressed_size/pending_wrote_position); 若最后 block 含 COMPRESSED → 不支持 |
| ts 对应真实 entry (补数据场景) | Error (不覆盖真实数据, 仅限 filler → 真实) |
| ts 对应 filler | 替换 filler → 真实 entry |
| ts > latest_written_timestamp | 填充 + 正常写入 |

> **纠正写入 (Correction Write)**: 当 `timestamp == latest_written_timestamp` 时, 允许覆盖最近一次写入的数据。该 record 必然位于 **最新数据段的最后一个未压缩 block** (可以是 pending block 或 SEALED 无 COMPRESSED flag 的 block) 的 **最末位置**。通过已有索引条目的 `(block_offset, in_block_offset)` 定位 record 后, 直接 mmap 覆写 data 字节, 支持 `data.len()` 与原 `data_len` 不同 (增长或缩小), 需同步更新 5 个字段 (block 头的 payload_size/uncompressed_size + 段的 pending_wrote_position/total_uncompressed_size/wrote_position)。索引条目完全不变, 不产生孤儿记录。若最后一个 block 含 COMPRESSED flag, 因压缩后大小不可预测, 不支持纠正写入。详见 [数据集操作 §9.1](dataset-operations.md#91-时间戳验证与写入分支)。

## 23.3 配置持久化

新增 `meta` TLV 类型:

| Type (hex) | 名称 | 长度 | 数据类型 | 说明 |
|------------|------|------|---------|------|
| 0x05 | index_continuous | 1 | u8 | 0=非连续, 1=连续存储 |

## 23.4 哨兵值设计

| 字段 | 哨兵值 | 含义 | 合法性保证 |
|------|--------|------|-----------|
| `block_offset: u64` | `0xFFFFFFFFFFFFFFFF` | 此位置无真实数据 (filler) | 合法全局偏移远低于 u64::MAX |
| `in_block_offset: u16` | `0xFFFF` | 此位置无真实数据 (filler) | 合法偏移 ≤ block_max_size = 64KB |

**读取时过滤**:
```rust
for entry in &entries {
    if entry.block_offset == 0xFFFFFFFFFFFFFFFF {
        continue;  // 跳过 filler
    }
    // ... 正常读取 ...
}
```

## 23.5 Index Segment 跳过规则

如果一个 index segment 将全部只包含 filler 条目, 则跳过该 segment 的创建。

```
示例: index_segment 容量 = 50000 条目
  上次写入 ts=50, 新写入 ts=500150
  需填充 499999 个 filler (ts 51..500149)

  填充 ts=51..100000 → 跨 2 个 segment → 全部 filler → 跳过创建
  填充 ts=500001..500149 → 包含真实 entry (ts=500150) → 创建
```

## 23.6 重启恢复

```
DataSet::open():
  ...
  latest = 0
  for seg in all_index_segments:
      if seg.wrote_count > 0:
          last_ts = seg.read_last_entry().timestamp
          if last_ts > latest: latest = last_ts
  for entry in in_memory_buffer:
      if entry.timestamp > latest: latest = entry.timestamp
  latest_written_timestamp = latest
```

## 23.7 连续模式 O(1) 优化

**原理**: 连续模式下, 条目位置可直接从时间戳计算:
- `entry_index = target_ts - start_timestamp`
- `mmap 偏移 = INDEX_HEADER_SIZE + entry_index × INDEX_ENTRY_SIZE`

| 操作 | 非连续模式 | 连续模式 | 收益 |
|------|-----------|---------|------|
| `lower_bound` | O(log n) | O(1) | 消除二分查找 |
| `find_exact` | O(log n) | O(1) | 消除二分查找 |
| `query_range` | O(log n + k) | O(1 + k) | 起始查找降为 O(1) |

单个 4MB index segment 可容纳 ~229,376 条目 (log₂ ≈ 18 次比较) → 优化后 **0 次比较**。

---

**相关**: [时间索引](time-index.md) | [设计决策](design-decisions.md)
