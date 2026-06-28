# Queue 模块 — 整体架构与 API

## 二十八、Queue 模块

> **核心设计**: 在 Dataset 之上实现队列语义, 支持多消费组、多 Consumer 实例、持久化消费进度、等待/通知机制。

### 28.1 设计目标

| 目标 | 说明 |
|------|------|
| 队列语义 | push 自动分配 timestamp, poll 按序返回未消费数据 |
| 多消费组 | 同一 Dataset 可开多个独立消费组, 各自维护进度 |
| 多 Consumer | 同一消费组可开多个 Consumer 实例, 共享进度、互斥 poll |
| 持久化进度 | 消费组状态存储在 mmap 文件, crash 后恢复 |
| 等待通知 | poll 无数据时等待, write 后自动唤醒 |
| 非侵入 | 不修改 Dataset 核心逻辑, 通过 hook 集成 |

### 28.2 目录结构

```
{data_dir}/{dataset_name}/{dataset_type}/
├── meta
├── data/
├── index/
└── queue/                    # 新增: queue 目录
    ├── {group_a}             # 消费组 A 的状态文件 (4KB mmap)
    └── {group_b}             # 消费组 B 的状态文件 (4KB mmap)
```
每个消费组对应一个独立的 4KB 状态文件, 存储已处理时间戳和处理中列表。

Journal 使用独立 `JournalQueue`, 不复用 `DatasetQueue`。`JournalQueue` 复用本模块的 `ConsumerStateFile` / `PendingEntry` 文件格式和 at-least-once ack 语义, 但 poll 数据源是 `JournalLog::read(sequence)`, 不依赖 `DataSet::query_index_entries`。journal queue 以 journal sequence 作为独立递增消费序列, 每条成功写入的 journal record 都必须投递。

`group_name` 直接作为 `queue/{group_name}` 状态文件名, 因此必须复用 dataset name/type 的路径安全规则: 非空、最长 255 字节, 且整体匹配 `^[0-9A-Za-z_-]+$`, 只允许数字、大小写英文字母、`-`、`_`; `open_consumer` 和 `drop_consumer` 必须在拼接路径前校验。

consumer retry/visibility timeout 是消费组级配置。`open_consumer(group_name)` 使用默认配置; `open_consumer_with_config(group_name, config)` 可显式设置 `running_expired_seconds` 与 `max_retry_count`。同一 queue 中同一 `group_name` 已经打开时, 后续 open 必须使用相同配置, 否则返回错误。

`processed_ts` 表示该 consumer group 已按投递顺序完成的最后一个真实 record timestamp / journal sequence, 不是连续逻辑时间戳水位。普通 DatasetQueue 的 filler/gap 不投递、不 pending、不持久 ack; poll 从 `processed_ts + 1` 起查找下一条真实记录, direct read miss 后通过索引跳到后续真实且未 pending 的 record。

### 28.3 核心类型

```rust
// ─── QueueInner (DatasetQueue 共享内部状态) ──────────────────────────────

pub(crate) struct QueueInner {
    consumers: HashMap<String, Arc<Mutex<ConsumerStateFile>>>,
    consumer_configs: HashMap<String, QueueConsumerConfig>,
    closed: AtomicBool,                    // close 标志, poll/close 检测
}

pub struct QueueConsumerConfig {
    /// 0 表示运行期间未 ack pending 永不过期; 默认 900 秒; 最大 u16::MAX。
    pub running_expired_seconds: u16,
    /// 0 表示不限制重试次数; 默认 3; 最大 u8::MAX。
    pub max_retry_count: u8,
}

// ─── DatasetQueue (per dataset, singleton handle, Clone-safe) ────────────

pub struct DatasetQueue {
    dataset: Arc<DataSet>,
    inner: Arc<Mutex<QueueInner>>,
    notify: Arc<QueueNotifier>,            // Condvar + lightweight callback slots
}

impl DatasetQueue {
    pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer>;
    pub fn open_consumer_with_config(
        &self,
        group_name: &str,
        config: QueueConsumerConfig,
    ) -> Result<DatasetQueueConsumer>;
    pub fn drop_consumer(&self, group_name: &str) -> Result<()>;
    pub fn push(&self, data: &[u8]) -> Result<i64>;
    pub fn close(&self) -> Result<()>;
}

// ─── DatasetQueueConsumer (per group, multi-instance, Clone-safe) ────────

pub struct DatasetQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,  // 同组共享
    config: QueueConsumerConfig,
    notify: Arc<QueueNotifier>,                 // 共享 DatasetQueue 的 notifier
    dataset: Arc<DataSet>,
    closed: Arc<AtomicBool>,                    // 共享 QueueInner.closed
    poll_callback: Arc<Mutex<Option<QueuePollCallback>>>,
}

impl DatasetQueueConsumer {
    pub fn poll_callback(&self, callback: Option<QueuePollCallback>) -> Result<()>;

    /// Poll for the next real record.
pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
    // 1. Scan pending entries and only return an unacked entry if it is
    //    expired/recovery-expired and still below the retry discard boundary.
    // 2. Retry delivery increments retry_count before returning. If the
    //    retry limit is already exhausted, mark the entry completed and only
    //    advance processed_ts through the delivery-order completed prefix.
    // 3. Otherwise use processed_ts + 1 as the direct-read fast path.
    // 4. If direct read misses because of a sparse gap or filler, query index
    //    entries from next_ts forward and choose the first non-filler entry
    //    that is not already pending.
    // 5. Add only real records to pending with retry_count=0. Filler/gap
    //    timestamps are never delivered and are not auto-acked.
    // 6. If no real record exists, release dataset/state locks and wait on
    //    the condvar until timeout or notification.
}
```

**所有权模型**: `DatasetQueue` 和 `DatasetQueueConsumer` 都是 Clone-safe handle, 内部通过 `Arc` 共享状态。`open_queue()` 返回一个 handle, 重复调用返回相同内部引用的新 handle。

Rust public API ownership boundary: callers open and close ordinary dataset queues through `DataSet::open_queue()` and `DataSet::close_queue()` on the Store-managed `DataSet` returned by `Store::create_dataset*` or `Store::open_dataset*`. `DatasetQueue::new`, `QueueInner`, and `ConsumerStateFile` are crate-internal plumbing; wrapper layers route queue open through the public `DataSet` API.

### 28.4 API 概览

#### Dataset 新增方法

```rust
impl DataSet {
    /// 打开队列 (singleton, 重复调用返回已打开的 queue)
    pub fn open_queue(&self) -> Result<DatasetQueue>;

    /// 关闭队列 (自动关闭所有打开的 consumers)
    pub fn close_queue(&self) -> Result<()>;
}
```
#### DatasetQueue 方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `open_consumer(group_name)` | 使用默认 consumer 配置打开消费组 (不存在则创建状态文件) | `DatasetQueueConsumer` |
| `open_consumer_with_config(group_name, config)` | 使用显式配置打开消费组; 同组已打开时配置必须一致 | `DatasetQueueConsumer` |
| `drop_consumer(group_name)` | 删除消费组 (删除状态文件) | `Result<()>` |
| `push(data)` | 推送数据 (自动分配 timestamp = latest+1); 仅适用于普通 DatasetQueue | `i64` (分配的 timestamp) |
| `close()` | 关闭队列 (自动 drop 所有 consumers) | `Result<()>` |

`QueueConsumerConfig` 通过 builder 构造并校验边界: `running_expired_seconds <= u16::MAX`, `max_retry_count <= u8::MAX`。默认值为 `running_expired_seconds=900`, `max_retry_count=3`。

#### DatasetQueueConsumer 方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `poll(timeout)` | 拉取下一条数据 (无数据时等待) | `Option<(timestamp, data)>` |
| `ack(timestamp)` | 标记已处理 (更新进度) | `Result<()>` |
| `poll_callback(callback)` | 为当前 consumer 实例注册或清除轻量唤醒回调; `None` 清除; 当前实例已有 callback 时再次设置非空 callback 返回错误 | `Result<()>` |

`poll_callback` 是 best-effort 唤醒钩子, 不属于 queue state、pending、ack 或 retry 语义。callback slot 属于 consumer 实例; 同一个 queue 上多个 consumer 实例可以各自注册 callback, 当前 consumer 实例已有 callback 时再次传入 `Some(callback)` 返回错误且不覆盖, 需要先传入 `None` 清除。回调在数据通知完成 `notify_all()` 后由触发通知的线程同步执行, 只用于唤醒外部处理线程; 不保证每条数据正好触发一次, 不保证触发后 `poll(0)` 一定有数据, 也不处理 `poll(0)` 返回 `None` 与注册之间的 lost-wake 窗口。调用方不得在回调内执行耗时处理或依赖精确通知计数。

#### C ABI Queue 方法

Queue 由独立 `wrapper/cffi` crate (`timslitecffi`) 暴露到 C ABI。C 侧不直接持有 Rust `DatasetQueue`, 也不依赖主 crate 的 `DataSetHandle`:

```c
typedef struct TmslQueueConsumerConfigFFI {
    uint32_t version;                  /* must be 1 */
    uint32_t running_expired_seconds;  /* 0..65535, default helper uses 900 */
    uint32_t max_retry_count;          /* 0..255, default helper uses 3 */
} TmslQueueConsumerConfigFFI;
```

`config == NULL` 时使用默认配置。非 NULL 时必须校验 `version == 1`, `running_expired_seconds <= UINT16_MAX`, `max_retry_count <= UINT8_MAX`。

| 函数 | 说明 | 返回值 |
|------|------|--------|
| `tmsl_queue_open(dataset)` | 从 FFI dataset 句柄打开普通 queue | `usize` queue handle, `0` 表示失败 |
| `tmsl_queue_close(queue_handle)` | 释放普通 queue FFI handle 并关闭 dataset queue | `0` 成功, `-1` 错误 |
| `tmsl_queue_consumer_open(queue_handle, group_name)` | 打开消费组, group_name 复用路径安全规则 | `usize` consumer handle, `0` 表示失败 |
| `tmsl_queue_consumer_open_with_config(queue_handle, group_name, config)` | 使用显式 consumer 配置打开普通 queue 消费组 | `usize` consumer handle, `0` 表示失败 |
| `tmsl_queue_consumer_drop(queue_handle, consumer_handle)` | 删除该 consumer 对应消费组并使同组 FFI consumer handle 失效 | `0` 成功, `-1` 错误 |
| `tmsl_queue_push(queue_handle, data, data_len)` | 普通 queue 写入数据并返回自动分配 timestamp | `timestamp`, `-1` 错误 |
| `tmsl_queue_poll(consumer_handle, timeout_ms, ...)` | poll 下一条数据; 成功数据由 `malloc` 分配 | `0` 成功, `-2` 超时, `-1` 错误 |
| `tmsl_queue_ack(consumer_handle, timestamp)` | ack 已 poll 的 timestamp | `0` 成功, `-1` 错误 |
| `tmsl_queue_consumer_poll_callback(consumer_handle, callback, userdata)` | 为当前 consumer 注册轻量唤醒回调; `callback == NULL` 清除; 已有 callback 时重复设置非空 callback 返回错误 | `0` 成功, `-1` 错误 |

FFI queue/consumer 是 `timslitecffi` 自己维护的 wrapper 句柄, 不属于主 `timslite` crate 的 Store registry。`tmsl_queue_close` 会移除该 queue 下所有 FFI consumer handle, 防止 C 侧继续 poll/ack 已关闭 queue。

Journal queue 使用专用 FFI:

| 函数 | 说明 | 返回值 |
|------|------|--------|
| `tmsl_journal_queue_open(store)` | 从 Store 打开专用 journal queue | `usize` queue handle, `0` 表示失败 |
| `tmsl_journal_queue_close(queue_handle)` | 释放 journal queue FFI handle | `0` 成功, `-1` 错误 |
| `tmsl_journal_queue_consumer_open(queue_handle, group_name)` | 打开 journal 消费组 | `usize` consumer handle, `0` 表示失败 |
| `tmsl_journal_queue_consumer_open_with_config(queue_handle, group_name, config)` | 使用显式 consumer 配置打开 journal 消费组 | `usize` consumer handle, `0` 表示失败 |
| `tmsl_journal_queue_poll(consumer_handle, timeout_ms, ...)` | poll 下一条 journal payload | `0` 成功, `-2` 超时, `-1` 错误 |
| `tmsl_journal_queue_ack(consumer_handle, sequence)` | ack 已 poll 的 journal sequence | `0` 成功, `-1` 错误 |
| `tmsl_journal_queue_consumer_poll_callback(consumer_handle, callback, userdata)` | 为当前 journal consumer 注册轻量唤醒回调; `callback == NULL` 清除; 已有 callback 时重复设置非空 callback 返回错误 | `0` 成功, `-1` 错误 |

#### Python Queue 方法

Python wrapper 使用 keyword 参数暴露同一配置:

```python
consumer = queue.open_consumer(
    "workers",
    running_expired_seconds=900,
    max_retry_count=3,
)
consumer.poll_callback(lambda: wake_worker())
consumer.poll_callback(None)
journal_consumer = journal_queue.open_consumer(
    "journal_workers",
    running_expired_seconds=900,
    max_retry_count=3,
)
journal_consumer.poll_callback(lambda: wake_worker())
journal_consumer.poll_callback(None)
```

省略参数时使用默认配置。传入超过 Rust 配置上限的值必须抛出对应 `TmslInvalidDataError`。

### 28.5 生命周期

```
Store.open_dataset()
    ↓
DataSet.open_queue() → DatasetQueue (singleton, repeatable)
    ↓
DatasetQueue.open_consumer("group_a") → Consumer (multi-instance)
    ↓
┌──────────────────────────────────────────────────────────┐
│  Producer                    Consumer                    │
│  ─────────                   ────────                    │
│  queue.push(data)            consumer.poll(timeout)      │
│      ↓                           ↓                       │
│  lock(dataset)               lock(dataset) → wait        │
│  write(latest+1, data)           ↓ (Condvar 释放锁)     │
│  notify_all() ─────────────→ wake up → re-lock           │
│                              read(ts, data)              │
│                                  ↓                       │
│                              ack(ts)                     │
│                                  ↓                       │
│                              update processed_ts         │
└──────────────────────────────────────────────────────────┘
    ↓
DatasetQueue.close() → closed=true + notify_all → 自动 drop 所有 consumers
    ↓
Dataset.close()
```
### 28.6 关键约束

| 约束 | 说明 |
|------|------|
| 单 Queue 实例 | 每个 Dataset 只能有一个 DatasetQueue (singleton) |
| 多 Consumer 实例 | 同一消费组可开多个 Consumer, 共享状态文件 |
| 4KB 状态文件 | 每个消费组的状态文件固定 4KB, QSTF v1 max 226 pending entries |
| 组级配置 | 同一 `group_name` 的活动 consumer 必须使用一致的 `QueueConsumerConfig` |
| Visibility Retry | 未 ack pending 只有运行超时或 reopen 恢复后才可重试; retry 超限后按投递顺序完成前缀规则丢弃 |
| 统一 Sync | 状态文件作为 `SegmentFlushTarget::QueueState { group_name }` 进入 dirty flush queue, 由后台 flush 任务统一执行 MS_SYNC |
| Idle-Close 阻塞 | Queue 打开时 Dataset 不会被 idle-close |
| 仅正常写入 | push 只使用 auto-increment timestamp, 不支持 correction/out-of-order |
| 不通知更新 | correction/out-of-order 写入不触发 consumer 通知 |

### 28.7 错误类型

```rust
pub enum TmslError {
    // ... 现有错误 ...

    /// Queue 已打开 (重复 open_queue)
    QueueAlreadyOpen,

    /// Queue 未打开 (close_queue 或 consumer 操作时)
    QueueNotOpen,

    /// 消费组不存在 (drop_consumer 或 ack 时)
    ConsumerGroupNotFound(String),

    /// 消费组已存在 (open_consumer 时, 如果要求不存在)
    ConsumerGroupExists(String),

    /// 队列已关闭 (poll/ack 操作时)
    QueueClosed,

    /// Pending 列表已满 (poll 时无法分配新 entry)
    PendingFull,
}
```

---

## 二十九、Queue 与 Dataset 集成

### 29.1 Dataset 新增字段

```rust
pub struct DataSet {
    // ... 现有字段 ...
    queue_inner: Option<Arc<Mutex<QueueInner>>>,   // Queue 内部状态 (打开时 Some)
    queue_notify: Option<Arc<QueueNotifier>>,      // Condvar + callback notifier
}
```

- `queue_inner.is_some()` 表示 Queue 已打开, 同时阻止 idle-close
- `queue_notify` 用于 normal write / append-created-new-timestamp hook 唤醒等待 consumer 并触发已注册的轻量 poll callback

### 29.2 Write Hook

`dataset.write()` 成功后, 如果 `queue_notify` 存在且是正常写入, 触发通知。`dataset.append()` 的普通 queue 语义为:

- `latest_written_timestamp is None or timestamp > latest_written_timestamp.unwrap()`: 创建新 timestamp, 与 normal write 等价, 必须 notify。
- `latest_written_timestamp == Some(timestamp)`: 修改已存在 latest record, 不推进 queue timestamp, 不重新投递, 不 notify。
- journal queue 例外: JournalLog 每条 `0x13` 都是新的 journal sequence, 必须 notify。

通知实现:

```rust
// 在 write_with_cache 末尾, 正常写入 (timestamp > old_latest) 成功后:
if let Some(ref notifier) = self.queue_notify {
    notifier.notify_data_available_best_effort();
}
```

**注意**: correction/out-of-order 写入不触发通知, 避免 consumer 处理更新数据。判断依据: 仅在 `old_latest_written_timestamp.is_none() || timestamp > old_latest_written_timestamp.unwrap()` (正常写入分支) 成功后通知。

`DatasetQueue::push()` 内部通过 `DataSet::write_next_queue_record()` 走 normal write 分支, 因此真实数据通知和 callback 由 dataset write hook 触发。`push()` 仍保留一次 waiter-only wake 以维持既有 Condvar 行为, 但不再次执行 callback, 避免单次 push 产生双回调。

### 29.3 Idle-Close 检查

后台任务 idle-check 时, 如果 `dataset.queue_inner.is_some()`, 跳过该 dataset:

```rust
// bg/mod.rs idle-check 逻辑
for (key, dataset_arc) in datasets.iter() {
    let dataset = Arc::clone(dataset_arc);

    // Queue 打开时阻止 idle-close
    if dataset.queue_inner.is_some() {
        continue;
    }

    if dataset.last_used_at.elapsed() >= idle_timeout {
        // ... 执行 idle-close ...
    }
}
```

### 29.4 Close 行为

`DataSet::close()` 是公开 lifecycle close: 如果 queue 仍打开会先关闭 queue, 随后 flush 并释放 data/index segment 等资源, 标记 dataset 已关闭, 并通过 Store runtime context 从 `Store.datasets` registry 移除。后台 idle-close 不调用 `DataSet::close()`, 只调用内部 `idle_close_segments()` 释放分段文件, dataset 仍保持打开且 handle 有效。

```rust
pub fn close(&mut self) -> Result<()> {
    if self.queue_inner.is_some() {
        self.close_queue()?;
    }
    self.idle_close_segments()?;
    self.closed = true;
    // Store-managed dataset also notifies lifecycle hook.
}
```

`DatasetQueue.close()` 流程:

```rust
pub fn close(&self) -> Result<()> {
    // 1. 标记 Dataset queue 关闭, 清空 dataset.queue_inner/queue_notify
    self.dataset.close_queue()?;

    // 2. 唤醒所有等待 consumer (poll 检测到 closed 后退出)
    let (ref guard, ref condvar) = *self.notify;
    let mut flag = guard.lock().unwrap();
    *flag = true;
    condvar.notify_all();
    *flag = false;

    // 3. 状态文件保持打开, 不删除 (下次 open_consumer 可恢复)
    Ok(())
}
```

---

## 三十、并发控制

### 30.1 锁层级

```
Store (RwLock<HashMap>)
    ↓
Dataset (Arc<DataSet>, internal mutex)
    ↓
QueueInner (Arc<Mutex<QueueInner>>)
    ↓
ConsumerStateFile (Arc<Mutex<ConsumerStateFile>>)
```

**严格遵循**: 外层锁未释放时, 不能获取内层锁。避免死锁。

`Condvar pair: (Mutex<bool>, Condvar)` 不属于上述 dataset/state 锁层级。它的 mutex 只保护一个通知 flag, `Condvar::wait_timeout` 只会释放并重新获取这个 notify mutex, 不会释放 `Dataset` 或 `ConsumerStateFile` 锁。进入 wait 前必须已经释放 dataset/state 相关锁。

### 30.2 Push 流程

```rust
pub fn push(&self, data: &[u8]) -> Result<i64> {
    // DataSet internally locks while checking queue state,
    // allocating timestamp = latest + 1, and writing the record.
    let ts = self.dataset.write_next_queue_record(data)?;
    Ok(ts)
}
```

**串行化保证**: DataSet 内部 mutex 已保证所有 write 操作串行, 不需要额外 queue_mutex。

**Journal queue 特例**: JournalQueue 不提供外部 `push(data)`。journal record 只能由 `JournalManager.append_*` 写入; append 成功后通过 JournalQueue notify 机制唤醒等待 consumer。这里的 append 指 journal record 写入 JournalLog, 其 sequence 独立递增, 不等同于业务 dataset 的 `append(ts == latest)` 修改已有 record。

### 30.3 Poll 流程

```rust
/// Poll for the next real record.
pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
    // 1. Scan pending entries and only return an unacked entry if it is
    //    expired/recovery-expired and still below the retry discard boundary.
    // 2. Retry delivery increments retry_count before returning. If the
    //    retry limit is already exhausted, mark the entry completed and only
    //    advance processed_ts through the delivery-order completed prefix.
    // 3. Otherwise use processed_ts + 1 as the direct-read fast path.
    // 4. If direct read misses because of a sparse gap or filler, query index
    //    entries from next_ts forward and choose the first non-filler entry
    //    that is not already pending.
    // 5. Add only real records to pending. Filler/gap timestamps are never
    //    delivered and are not auto-acked.
    // 6. If no real record exists, release dataset/state locks and wait on
    //    the condvar until timeout or notification.
}```

**关键设计**: poll 先检查 retryable pending, 再检查新数据可用性; 无数据时释放 state/dataset 锁后只持有 notify mutex 进入 Condvar wait。write 完成后 notify_all 唤醒所有等待 consumer。唤醒后循环重新检查 retryable pending 和数据可用性。

**Condvar 竞态处理**: notify mutex 只保护通知 flag。poll 在释放 dataset/state 锁后进入 wait; 如果 write 在窗口内完成并设置 flag/notify, poll 唤醒后或下一轮都会重新检查数据。实现必须在 wait 超时后做最后一次 retryable pending 与新数据检查, 避免 missed wakeup 造成可见记录被漏读。

`poll_callback` 不参与上述竞态处理。它只是数据通知后的同步轻量回调, 不记录 generation, 不补偿 lost wake, 不影响 `poll()` 的最后一次检查。需要可靠事件语义的调用方必须仍以 `poll/ack` 状态机为准。

### 30.4 Ack 流程

```rust
pub fn ack(&self, timestamp: i64) -> Result<()> {
    // 1. 检查 closed
    if self.closed.load(Ordering::SeqCst) {
        return Err(TmslError::QueueClosed);
    }

    // 2. 获取 state file 锁
    let mut state = self.state_file.lock().unwrap();

    // 3. 找到对应 pending entry
    let entry = state.find_pending_mut(timestamp)
        .ok_or_else(|| TmslError::NotFound(
            format!("pending entry not found: {}", timestamp)
        ))?;

    // 4. 标记为已 ack
    entry.status = 1;

    // 5. 清理投递顺序完成前缀, 并更新 processed_ts
    state.cleanup_acked();

    // 不执行 sync — 由后台 flush 任务统一同步 (与 Dataset 分段文件一致)
    Ok(())
}
```

**关键**: ack 后只清理 pending 列表中按投递顺序连续完成的前缀, 并把 `processed_ts` 更新为该前缀最后一个真实 timestamp/sequence。gap/filler 不进入 pending, 不需要持久 skip 状态。

ack 成功、poll 分配新 pending entry、retry 更新 `retry_count/start_time`、retry 超限标记完成都必须把当前消费组入队为 `SegmentFlushTarget::QueueState { group_name }`。该入队动作只声明 state file dirty, 不执行立即 `mmap.flush()`; 后台 flush 周期按 group_name 精确同步对应 4KiB state file。

### 30.5 多 Consumer poll 分配

同一消费组多个 Consumer 同时 poll 时, 通过共享的 `ConsumerStateFile` 互斥:

```
Consumer A: poll → lock(state) → find_next_available_entry → ts=100 → add_pending(100) → unlock
Consumer B: poll → lock(state) → find_next_available_entry → ts=102 (100 已在 pending, 101 是 filler/gap) → add_pending(102) → unlock
```

`find_next_available_entry` 从 `processed_ts + 1` 开始, 先尝试 direct read; direct read miss 后通过 `query_index_entries` 寻找第一个真实且不在 pending 中的 entry。filler/gap 不投递、不 pending、不自动 ack; 共享 state file 保证同一 group 多 consumer 不会拿到同一真实 timestamp。

未过期的 pending entry 不会被 `find_next_available_entry` 重投, 但会继续占用 pending 容量并被 `is_in_pending` 跳过。只有满足以下条件之一的未 ack pending 才能被重投:

- `start_time=0`, 表示从磁盘 reopen 时被标记为恢复过期。
- `running_expired_seconds > 0` 且当前时间距离 `start_time` 已达到配置阈值。

重投前先检查 `max_retry_count`: 如果 `max_retry_count > 0 && retry_count >= max_retry_count`, 该 entry 标记为完成并按投递顺序完成前缀推进 `processed_ts`, 不再返回给 consumer; 否则先递增 `retry_count` 并刷新 `start_time`, 再返回对应数据。

### 30.6 drop_consumer 行为

```rust
pub fn drop_consumer(&self, group_name: &str) -> Result<()> {
    let mut inner = self.inner.lock().unwrap();

    // 1. 检查消费组是否存在
    if !inner.consumers.contains_key(group_name) {
        return Err(TmslError::ConsumerGroupNotFound(group_name.to_string()));
    }

    // 2. 从 consumers map 中移除
    inner.consumers.remove(group_name);

    // 3. 删除状态文件 (当所有 Arc 引用释放后)
    // 注意: 如果有 Consumer handle 仍持有 Arc<ConsumerStateFile>,
    // 文件不会立即删除。Consumer 下次操作时会检测到 closed 或 group 不存在。
    let state_path = /* base_dir/queue/group_name */;
    if state_path.exists() {
        std::fs::remove_file(&state_path)?;
    }

    Ok(())
}
```
