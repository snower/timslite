# Journal 专用存储设计

## 目标

Journal 不再复用标准 `DataSet` 的 data/index 双分段结构。新的 journal 存储是一个专用 append log:

- 保留 mmap 分段文件、Block 聚合、延迟压缩、懒扩容、idle-close 和 queue 消费能力。
- 删除 TimeIndex / IndexSegment, 因为 journal sequence 严格连续递增, 索引信息可由分段文件名和 record_count 推导。
- 仅支持追加写入, 不支持 correction、out-of-order、delete、retention reclaim 或 compaction。
- 单独实现 `journal::segment`、`journal::log` 和 `journal::queue`, 不复用 `DataSet` / `DataSegmentSet` / `DatasetQueue` 的实现。

Journal v1 仍是 pointer-based 辅助日志, 不是事务 WAL。主操作成功但 journal append 失败或 crash 丢失最近日志时, 不回滚主操作。

## 目录布局

```text
{data_dir}/
└── .journal/
    └── logs/
        ├── meta
        ├── data/
        │   ├── 00000000000000000001
        │   ├── 00000000000000004218
        │   └── ...
        └── queue/
            ├── {group_a}
            └── {group_b}
```

`.journal/logs` 不再包含 `index/` 目录。普通 `Store::open` 扫描仍跳过 `.journal`, journal 生命周期由 `JournalManager` 单独管理。

### 文件名

Journal data segment 文件名是该分段第一条 record 的 sequence, 20 位十进制零填充:

```text
base_sequence = 1      -> 00000000000000000001
base_sequence = 4218   -> 00000000000000004218
```

约束:

- sequence 类型使用 `i64`。
- 有效范围为 `1..=i64::MAX`。
- `0` 保留给 queue state 的初始 processed position, 不作为合法 journal record sequence。
- `next_sequence > i64::MAX` 返回 `InvalidData`。

## Journal Sequence

Journal 使用严格连续 sequence, 与 wall-clock 时间无关。

```text
empty journal:
  next_sequence = 1

non-empty journal:
  next_sequence = latest_segment.base_sequence + latest_segment.record_count
  latest_sequence = next_sequence - 1
```

`next_sequence` 是下一条即将分配的 sequence, 不是最新已写 sequence。文档和代码中应统一使用该命名, 避免 off-by-one。

## Segment Registry

`JournalLog` 使用有序 registry 管理所有 journal segment:

```rust
struct JournalLog {
    segments: BTreeMap<i64, JournalSegment>,
    next_sequence: i64,
}
```

定位 sequence:

1. `segments.range(..=sequence).next_back()` 找到 `base_sequence <= sequence` 的候选段。
2. 若 `sequence >= segment.base_sequence + segment.record_count`, 返回 `None`。
3. 否则在该 segment 内扫描 block headers 定位 record。

有序 registry 同时用于:

- 获取最新分段。
- 判断是否需要创建下一分段。
- idle-close 后按需 reopen。
- query 顺序遍历。

## Segment Header

Journal segment 使用可变 header, 与 data segment 保持同一类扩展能力, 但 state 字段精简:

```text
magic/version/header_len
meta TLV:
  segment_file_size: u64
  initial_file_size: u64
  compress_type: u8
  compress_level: u8
state:
  file_size: u64
  wrote_position: u64        # 物理文件内绝对偏移
  record_count: u64
  total_uncompressed_size: u64
  pending_block_offset: u64
  pending_wrote_position: u64
  pending_record_count: u64
```

不保存:

- `invalid_record_count`
- `min_timestamp` / `max_timestamp`
- 任何 index state

## Record 与 Block

Journal record 继续使用统一 record 结构, sequence 写入 timestamp 字段:

```text
Record:
  data_len: u32 LE
  sequence: i64 LE
  payload: [u8; data_len]    # encoded JournalRecord
```

这样可以复用统一 record header 语义, 并在读取时校验 record 内 sequence 是否等于期望值。

BlockHeader 保持 16 字节结构:

```text
payload_size: u32
flags: u16
record_count: u16
uncompressed_size: u32
reserved: u32
```

写入规则:

- 普通 pending block 聚合多条 journal record。
- 当下一条 record 放不进当前 pending block 时, seal + compress 当前 block, 再创建新 block。
- 如果单条 journal record 加上 record header 超过普通 block 容量, 使用 `SINGLE_RECORD` block。
- single-record block 允许超过普通 64KiB block payload 上限, 但仍必须受 journal record 最大长度约束。
- journal TLV outer length 仍为 `u16`, encoded payload 最大 65538 字节 (`log_type:u8 + length:u16 + tlv_list`)。
- `payload + record_header` 超出普通 block 时走 single-record, 不截断。

## Append 流程

```text
JournalLog::append(payload):
  1. validate encoded JournalRecord length
  2. sequence = next_sequence
  3. locate latest writable segment or create first segment
  4. if current segment cannot fit this record:
       flush completed current segment
       create new segment whose base_sequence = sequence
  5. append record(data_len, sequence, payload)
  6. update block header/state and segment record_count
  7. if segment reaches max size after append, mark it complete and flush it
  8. next_sequence += 1
  9. notify JournalQueue consumers
  10. return sequence
```

约束:

- 仅支持 append, 不提供 overwrite/delete。
- append 成功后 sequence 必须连续。
- append 失败不得推进 `next_sequence`。
- 写 journal 不递归写 journal。

## 读取流程

### read(sequence)

```text
JournalLog::read(sequence):
  1. if sequence <= 0 or sequence >= next_sequence: return None
  2. 用 BTreeMap 定位 segment
  3. 从 segment 数据区起点扫描 block header
  4. 若 sequence 不在当前 block 范围:
       current_sequence += block.record_count
       pos += BLOCK_HEADER_SIZE + block.payload_size
       continue
  5. 若 block compressed, 解压 block payload
  6. 在 block payload 内按 record header 顺序跳到目标 record
  7. 校验 record.sequence == sequence
  8. 返回 record.payload
```

定位 block 时只需要读取 block header 和 `record_count`, 不需要读取或解压 payload。只有命中目标 block 后才读取/解压 payload。

### query(start, end)

`query(start, end)` 顺序遍历从 `start` 到 `end` 的 sequence, 使用 segment registry 和 block record_count 跳跃, 返回存在的连续 journal records。由于 journal 不支持删除和 gap, 正常情况下 `[start, min(end, latest_sequence)]` 都存在。

### latest / next

- `latest_sequence()` 返回 `None` 或 `Some(next_sequence - 1)`。
- `next_sequence()` 返回下一条将写入的 sequence。

## Crash Recovery

Journal 不提供事务语义, 但必须保证读取不会返回半写或错误数据。

打开 segment 时:

1. 读取 header state。
2. 从数据区起点扫描 block 链。
3. 对每个 block 校验:
   - block header 完整。
   - `payload_size` 不越过文件实际大小。
   - `record_count > 0`。
   - 命中 pending raw block 时 payload 内 record header/data 完整。
   - compressed block 可解压且 uncompressed size 匹配。
4. 遇到不完整 block、半写 record、越界 payload 或解压失败时, 只保留此前完整前缀。
5. 用扫描得到的 `wrote_position`、`record_count`、pending state 修正内存状态。

如果 header state 比扫描结果更乐观, 以内存扫描结果为准, 并在下一次 flush 时写回修正后的 state。这样通过 journal read/query/queue 不会读取到半写日志。

## Flush 与 Idle-Close

Journal flush 不加入普通 Store dirty flush queue。后台 flush 任务到期时直接调用:

```rust
JournalManager::flush_dirty()
```

执行前检查每个打开 journal segment 的 `is_flushed`:

- dirty: `mmap.flush()`, 然后 `is_flushed=true`
- clean: 跳过

原因:

- journal segment 数量远少于普通 dataset segment 总量。
- journal 是全局串行写入点, 直接 flush 可避免把 journal data target 混入普通 dataset flush queue。
- queue state 文件仍由 JournalQueue 管理, 可复用同一 flush 时机直接同步 dirty state files。

idle-close:

- 与普通 data segment 一样, idle-close 前先 flush dirty mmap。
- idle-close 不 seal、不压缩 pending block。
- 重新打开时恢复 pending raw block。

## JournalQueue

JournalQueue 是独立队列实现, 不复用 `DatasetQueue`, 但复用 `ConsumerStateFile` / `PendingEntry` 的文件格式和 ack 语义。

```rust
pub struct JournalQueue {
    log: Arc<Mutex<JournalLog>>,
    inner: Arc<Mutex<JournalQueueInner>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
}

pub struct JournalQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,
    log: Arc<Mutex<JournalLog>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
    closed: Arc<AtomicBool>,
}
```

新 consumer 的 `processed_ts` 初始值:

- 如果消费后续日志: 使用当前 `latest_sequence.unwrap_or(0)`。
- 如果需要从头消费历史: 后续可提供显式起点 API; v1 默认行为沿用普通 queue, 即新 consumer 从当前末尾开始消费后续 push。

poll 逻辑:

```text
poll(timeout):
  1. 若已有 unacked pending entry, 直接 JournalLog::read(entry.sequence)
  2. next = state.next_poll_ts()   # processed_ts + 1; 初始 0 => 1
  3. 若 next < JournalLog::next_sequence 且不在 pending:
       read(next), add_pending(next), return
  4. 否则释放锁并等待 condvar
```

因为 journal sequence 连续且没有 filler/gap, JournalQueue 不需要 `query_index_entries` 或跳过 filler。

约束:

- producer 只能是 `JournalManager.append_*`。
- 外部 push 始终返回 `InvalidData`。
- 每条成功写入的 `0x01/0x02/0x11/0x12/0x13` 都 notify。
- ack/pending state 仍是 at-least-once 语义。

## Public API 边界

`.journal/logs` 不再作为普通 DataSet handle 暴露。改为专用 journal API:

```rust
impl Store {
    pub fn journal_latest_sequence(&self) -> Result<Option<i64>>;
    pub fn journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn journal_query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub fn open_journal_queue(&mut self) -> Result<JournalQueue>;
}
```

禁用 journal 时, 这些 API 返回 `NotFound("journal is disabled")`。

FFI/Python wrapper 后续应新增对应专用函数/方法, 不再通过 `open_dataset(".journal", "logs")` 访问 journal。

## 模块拆分

```text
src/journal/
├── mod.rs          # JournalManager + record codec facade
├── record.rs       # JournalRecord encode/decode
├── segment.rs      # JournalSegment, mmap segment, block scan/read/append
├── log.rs          # JournalLog, segment registry, sequence routing
└── queue.rs        # JournalQueue + JournalQueueConsumer
```

边界:

- `record.rs` 不依赖 Store/DataSet。
- `segment.rs` 可复用 `BlockHeader`、compress helper、header helper, 但不依赖 `DataSegment`。
- `log.rs` 管理 `.journal/logs/data/` 和 `meta`。
- `queue.rs` 可复用 `ConsumerStateFile`, 但不依赖 `DatasetQueue`。
- `mod.rs` 负责 Store/DataSet hook 接口和 enable/disable 生命周期。
