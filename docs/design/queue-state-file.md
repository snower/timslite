# Queue 模块 — 状态文件格式与同步

## 三十一、消费组状态文件

### 31.1 文件布局

每个消费组对应一个固定 4KB 的 mmap 文件, 存储在:

```text
{data_dir}/{dataset_name}/{dataset_type}/queue/{group_name}
```

JournalQueue 复用同一状态文件格式, 路径为 `{data_dir}/.journal/logs/queue/{group_name}`。Journal sequence 从 `1` 开始, 因此新状态文件的 `processed_ts=0` 与普通 queue 的 `next_poll_ts = processed_ts + 1` 规则兼容。

`group_name` 是文件名, 不做转义或编码。合法值必须非空、最长 255 字节且整体匹配 `^[0-9A-Za-z_-]+$`: 只允许数字、大小写英文字母、`-`、`_`。任何路径分隔符、`.`、空格、控制字符、非 ASCII 字符都不允许。`open_consumer` 与 `drop_consumer` 必须在拼接 `queue/{group_name}` 前校验。

**文件结构**:

```text
Offset  Size    Field                   Description
──────────────────────────────────────────────────────────────
0       4       magic                   "QSTF" (Queue State File)
4       4       version                 u32, 当前 = 1
8       2       state_length            u16, processed_ts 字节数 (固定 8)
10      8       processed_ts            i64, 已处理的连续最大时间戳/sequence
18      2       pending_length          u16, pending entries 数量
20      1       pending_value_size      u8, 单条 pending entry 字节数 (固定 18)
21      -       pending_entries         变长, pending_length * 18 字节
──────────────────────────────────────────────────────────────
```

**总大小**: 固定 4096 字节 (4KB)。

当前仍处于首次开发阶段, 不要求兼容旧草案格式。状态文件版本保持为 `1`, 但当前有效格式的 `pending_value_size` 固定为 18; version 不是旧草案兼容标记, `pending_value_size` 不匹配时视为无效状态文件。

### 31.2 Pending Entry 格式

每条 pending entry 占 18 字节:

```text
Offset  Size    Field                   Description
──────────────────────────────────────────────────────────────
0       8       timestamp               i64, 数据时间戳或 journal sequence
8       8       start_time              i64, 本次处理开始时间 (Unix epoch seconds)
16      1       status                  u8, 0=待 ack, 1=已完成
17      1       retry_count             u8, 已交付的重试次数
──────────────────────────────────────────────────────────────
```

`retry_count` 不包含第一次正常投递。第一次 poll 新数据时写入 `retry_count=0`; 后续由于运行超时或 reopen 恢复导致再次投递时, 在真正返回给调用方前递增一次。

`start_time=0` 是内部恢复标记, 表示该 pending 在本次打开时被强制视为已过期。运行中的新投递和重试投递必须写入当前 Unix seconds。

### 31.3 容量计算

**Header 固定占用**:

- magic: 4 bytes
- version: 4 bytes
- state_length: 2 bytes
- processed_ts: 8 bytes
- pending_length: 2 bytes
- pending_value_size: 1 byte
- **总计**: 21 bytes

**可用空间**:

```text
4096 - 21 = 4075 bytes
```

**最大 pending entries**:

```text
4075 / 18 = 226 entries (取整)
```

**实际使用**:

- 226 * 18 = 4068 bytes
- 剩余 7 bytes 未使用 (padding)

### 31.4 Consumer 配置

普通 `DatasetQueue` 与 `JournalQueue` 使用同一组 consumer 配置:

```rust
pub struct QueueConsumerConfig {
    pub running_expired_seconds: u16,
    pub max_retry_count: u8,
}
```

| 字段 | 默认值 | 范围 | 语义 |
|------|--------|------|------|
| `running_expired_seconds` | `900` | `0..=u16::MAX` | pending 被 poll 后超过该秒数未 ack 时可重试; `0` 表示运行期间永不过期 |
| `max_retry_count` | `3` | `0..=u8::MAX` | 最多允许多少次重试投递; `0` 表示不限制 |

配置是消费组级别的运行时契约。同一个 queue 中同一 `group_name` 已经打开时, 后续 `open_consumer` 只能使用相同配置; 使用不同配置必须返回错误, 避免多个 handle 对同一 state file 采用不同重试语义。

`running_expired_seconds=0` 只关闭运行期间的时间过期。`ConsumerStateFile::open_existing` 从磁盘加载时仍会把所有未 ack pending 标记为恢复过期, 以覆盖程序重启或 queue 重新打开后的 at-least-once 语义。

`max_retry_count` 的边界按“允许重试多少次”解释:

- 第一次正常投递不计入 retry。
- 每次实际重试投递前递增 `retry_count`。
- 当 `max_retry_count > 0 && retry_count >= max_retry_count` 且该 pending 再次需要重试时, 不再返回给 consumer, 而是标记为已完成并按连续完成规则推进 `processed_ts`。
- 因此 `max_retry_count=3` 时, `retry_count=1/2/3` 的三次重试都可以返回; 下一次重试机会才丢弃。

如果同时配置 `running_expired_seconds=0` 与 `max_retry_count=0`, 未 ack pending 在进程持续运行期间不会自动释放。该组合可能导致 pending 永久占满并阻塞后续 poll, 由调用方负责避免。

### 31.5 文件操作

```rust
pub(crate) struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
}

pub(crate) struct PendingEntry {
    timestamp: i64,
    start_time: i64,
    status: u8,
    retry_count: u8,
}
```

`ConsumerStateFile::open_or_create(path, initial_processed_ts)` 负责打开或创建状态文件:

1. 新文件写入 version 1、`pending_value_size=18`、空 pending 列表。
2. 现有文件校验 magic/version/state_length/pending_value_size/pending_length 边界。
3. 现有文件加载后先按连续已完成 pending 推进 `processed_ts` 并清理前缀。
4. 所有仍未 ack 的 pending 保留, 但 `start_time` 置为 `0`, 表示恢复后首次 poll 可立即重试。
5. 恢复动作只更新 state file 缓存, 不把 pending 删除。

`processed_ts` 只能按 pending 列表前缀中连续完成的条目前移。重试超限导致的强制丢弃与业务 ack 一样, 都只是把 entry 标记为完成; 只有此前所有较小 pending 都已完成时才能推进水位。

### 31.6 Poll 与 Retry 流程

poll 的核心顺序:

1. 锁定当前消费组 state file。
2. 扫描 pending 列表, 查找已过期或恢复过期的未 ack entry。
3. 若 entry 可重试:
   - 如果 `max_retry_count > 0 && retry_count >= max_retry_count`, 标记完成并继续扫描。
   - 否则 `retry_count += 1`, `start_time = now`, 释放锁后读取并返回对应数据。
4. 如果没有可重试 pending, 从 `processed_ts + 1` 起查找新的真实 record/sequence。
5. 新数据投递时追加 pending entry: `status=0`, `retry_count=0`, `start_time=now`。
6. 无可投递数据时释放 dataset/state 相关锁, 仅持有 notify mutex 进入 Condvar wait。

未过期 pending 仍保留在 state file 中, 不会被立即重投。同一消费组多个 consumer 可以继续 poll 后续不在 pending 中的真实数据, 直到 pending 容量耗尽或没有新数据。

对于普通 dataset queue, poll 分配新 pending、重试更新、重试超限丢弃和 ack 都必须把当前消费组入队为 `SegmentFlushTarget::QueueState { group_name }`。对于 JournalQueue, 同样的状态变更必须更新 mmap; flush 入口复用 queue state file 的 `sync/flush` 逻辑。

### 31.7 Crash / Reopen 恢复

状态文件是消费进度的持久化缓存, 不是严格事务 WAL。进程 crash 后可能出现:

| 状态 | 说明 | 恢复策略 |
|------|------|----------|
| processed_ts 未更新 | 内存中已 ack, 但未 flush | 重新消费 (at-least-once) |
| pending entry status=0 | 处理中但未 ack | 保留 entry, reopen 时置 `start_time=0`, 下次 poll 按 retry 规则处理 |
| pending entry status=1 | 已 ack/已丢弃但未清理 | 只在连续前缀内推进 `processed_ts` 并清理 |

“consumer 关闭再次开启”不依赖单个 handle 的 Drop 时机识别。只有 `ConsumerStateFile::open_existing` 从磁盘加载状态文件时触发恢复过期, 覆盖程序重启和 queue 重新打开场景。已经在内存中打开的同组 consumer handle 不因新增 handle 自动过期。

---

## 三十二、同步策略

### 32.1 Flush 集成 (唯一 Sync 入口)

Queue 状态文件与 Dataset 分段文件采用相同的 dirty flush queue 策略。每个普通 dataset consumer group state file 是一等 flush target: `SegmentFlushTarget::QueueState { group_name }`。ack、poll 分配新 pending、retry 更新和 retry 超限丢弃后只更新内存状态并把对应 group 入队; 后台 flush 任务 drain 队列后同步对应 state file。

```rust
fn flush_target(dataset: &mut DataSet, target: SegmentFlushTarget) {
    match target {
        SegmentFlushTarget::Data { file_offset } => dataset.sync_data_segment(file_offset),
        SegmentFlushTarget::Index { start_timestamp } => dataset.sync_index_segment(start_timestamp),
        SegmentFlushTarget::QueueState { group_name } => {
            dataset.sync_queue_state_file(&group_name)
        }
        SegmentFlushTarget::DatasetState => dataset.sync_dataset_state_file(),
    }
}
```

后台 flush 不再执行 timeout cleanup, 也不删除未 ack pending。过期判断只发生在 poll 尝试投递时, 这样 `retry_count` 与 pending 占用状态不会被后台任务破坏。

### 32.2 Ack 与 Poll 不执行立即 Sync

普通 dataset queue 的 `ack()` 和 `poll()` 操作后不执行立即 `mmap.flush()`, 仅更新内存中的状态并入队 `QueueState { group_name }`。状态文件与 Dataset 分段文件采用相同的 Sync 策略, 由后台 flush 任务统一执行 `mmap.flush()` (MS_SYNC)。

JournalQueue 没有普通 dataset 的全局 dirty flush queue target, 但必须采用相同的状态文件格式和 retry 规则; JournalQueue close/flush 显式同步当前打开的 group state files。

**Crash 安全**: 与 Dataset 分段文件一致, crash 后可能丢失最近 flush 间隔内的状态变更。Consumer 重新 poll 时, 未持久化的 pending entry 会丢失 (该数据重新可 poll), 未持久化的 ack 也会丢失 (该数据重新消费)。这是 at-least-once 语义的自然保证。

---

## 三十三、容量限制与处理

### 33.1 Pending 满的处理

当 pending entries 达到 226 条上限时, `poll()` 返回 `PendingFull` 错误:

```rust
state.add_pending(pending)?;  // 可能返回 TmslError::PendingFull
```

调用方策略:

- 及时 ack 已处理的数据, 释放连续完成前缀。
- 配置合理的 `running_expired_seconds` 与 `max_retry_count`, 避免未 ack pending 长期占满。
- 增加 consumer 实例数只能提升处理并发, 不能绕过同一 group state file 的 pending 容量上限。

### 33.2 Pending 过期与丢弃

Pending 过期不再通过后台 cleanup 删除。poll 发现过期 pending 时:

1. 未达到 retry 上限: 递增 `retry_count`, 更新 `start_time`, 返回同一 timestamp/sequence。
2. 已达到 retry 上限: 标记完成, 尝试按连续完成前缀推进 `processed_ts`, 然后继续寻找下一个可投递记录。

如果一个较早 pending 未完成, 较晚 pending 即使因 retry 超限被标记完成, 也不能让 `processed_ts` 越过前者。

### 33.3 Filler / Gap 跳过

Queue poll 不会投递 filler entry, 也不会对 filler 自动 ack。consumer 只会看到真实 record:

```rust
loop {
    match consumer.poll(Duration::from_secs(5))? {
        Some((ts, data)) => {
            process(ts, &data);
            consumer.ack(ts)?;
        }
        None => {
            // timeout, no new real record
        }
    }
}
```

---

## 三十四、性能考量

### 34.1 锁竞争

| 操作 | 锁 | 竞争程度 |
|------|-----|----------|
| push | Dataset Mutex | 低 (Dataset 锁已序列化所有写操作) |
| poll | StateFile Mutex + Dataset Mutex | 中 (多 consumer 竞争 state lock) |
| ack | StateFile Mutex | 低 (单 consumer 操作) |

poll 等待时必须释放 Dataset 锁和 StateFile 锁, 进入 Condvar wait 时只持有 notify mutex, 不阻塞 write 或 ack。

### 34.2 Mmap 开销

- 每个消费组 4KB mmap, 常驻内存
- 10 个消费组 = 40KB, 可忽略
- 100 个消费组 = 400KB, 仍然可接受

### 34.3 Sync 开销

- MS_SYNC 每次约 1-5ms (取决于磁盘)
- 高频 ack/poll 场景下, 立即 sync 会成为瓶颈
- 普通 dataset queue 通过 dirty flush queue 批量同步状态文件

### 34.4 Condvar 通知

- `notify_all()` 唤醒所有等待 consumer
- N 个 consumer 同时 poll, push 后全部唤醒, 但只有部分能拿到数据
- 如果 consumer 数量远大于 push 频率, 大部分唤醒是空跑, 但 poll 循环会重新检查 retryable pending 和新数据

### 34.5 poll next-entry efficiency

当前实现不按 timestamp 逐个线性扫描 gap/filler, 而是在 direct read miss 后通过 `query_index_entries(next_ts, i64::MAX)` 查找后续 index entry。

- 正常情况下: `processed_ts + 1` 即可命中, O(1)
- 大量 filler/gap: 由 TimeIndex range 查询跳过未创建的中间 segment 和 filler entry; 不为 filler 创建 pending
- pending 堆积: 最坏 O(pending_length), 但 pending 最多 226 条, 可接受
