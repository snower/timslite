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

### 28.3 核心类型

```rust
// ─── QueueInner (DatasetQueue 共享内部状态) ──────────────────────────────

pub(crate) struct QueueInner {
    consumers: HashMap<String, Vec<Arc<ConsumerStateFile>>>,
    closed: AtomicBool,                    // close 标志, poll/close 检测
}

// ─── DatasetQueue (per dataset, singleton handle, Clone-safe) ────────────

pub struct DatasetQueue {
    dataset: Arc<Mutex<DataSet>>,
    inner: Arc<Mutex<QueueInner>>,
    notify: Arc<(Mutex<bool>, Condvar)>,   // (guard_mutex, condvar) pair
}

impl DatasetQueue {
    pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer>;
    pub fn drop_consumer(&self, group_name: &str) -> Result<()>;
    pub fn push(&self, data: &[u8]) -> Result<i64>;
    pub fn close(&self) -> Result<()>;
}

// ─── DatasetQueueConsumer (per group, multi-instance, Clone-safe) ────────

pub struct DatasetQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,  // 同组共享
    notify: Arc<(Mutex<bool>, Condvar)>,        // 共享 DatasetQueue 的 Condvar
    dataset: Arc<Mutex<DataSet>>,
    closed: Arc<AtomicBool>,                    // 共享 QueueInner.closed
}

impl DatasetQueueConsumer {
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn ack(&self, timestamp: i64) -> Result<()>;
}

// ─── ConsumerStateFile (4KB mmap, shared among same-group consumers) ──────

pub(crate) struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
}

pub(crate) struct PendingEntry {
    pub timestamp: i64,
    pub start_time: i64,        // Unix epoch seconds
    pub status: u8,             // 0=待ack, 1=已ack
}
```

**所有权模型**: `DatasetQueue` 和 `DatasetQueueConsumer` 都是 Clone-safe handle, 内部通过 `Arc` 共享状态。`open_queue()` 返回一个 handle, 重复调用返回相同内部引用的新 handle。

### 28.4 API 概览

#### Dataset 新增方法

```rust
impl DataSet {
    /// 打开队列 (singleton, 重复调用返回已打开的 queue)
    pub fn open_queue(&mut self) -> Result<DatasetQueue>;

    /// 关闭队列 (自动关闭所有打开的 consumers)
    pub fn close_queue(&mut self) -> Result<()>;
}
```

#### DatasetQueue 方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `open_consumer(group_name)` | 打开消费组 (不存在则创建状态文件) | `DatasetQueueConsumer` |
| `drop_consumer(group_name)` | 删除消费组 (删除状态文件) | `Result<()>` |
| `push(data)` | 推送数据 (自动分配 timestamp = latest+1) | `i64` (分配的 timestamp) |
| `close()` | 关闭队列 (自动 drop 所有 consumers) | `Result<()>` |

#### DatasetQueueConsumer 方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `poll(timeout)` | 拉取下一条数据 (无数据时等待) | `Option<(timestamp, data)>` |
| `ack(timestamp)` | 标记已处理 (更新进度) | `Result<()>` |

### 28.5 生命周期

```
Dataset.open()
    ↓
Dataset.open_queue() → DatasetQueue (singleton, repeatable)
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
| 4KB 状态文件 | 每个消费组的状态文件固定 4KB, max 239 pending entries |
| 统一 Sync | 状态文件与 Dataset 分段文件采用相同 Sync 策略, 由后台 flush 任务统一执行 MS_SYNC |
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
    queue_notify: Option<Arc<(Mutex<bool>, Condvar)>>, // Condvar pair
}
```

- `queue_inner.is_some()` 表示 Queue 已打开, 同时阻止 idle-close
- `queue_notify` 用于 write hook 唤醒等待 consumer

### 29.2 Write Hook

`dataset.write()` 成功后, 如果 `queue_notify` 存在且是正常写入, 触发通知:

```rust
// 在 write_with_cache 末尾, 正常写入 (timestamp > old_latest) 成功后:
if let Some(ref notify_pair) = self.queue_notify {
    let (ref guard, ref condvar) = **notify_pair;
    let mut flag = guard.lock().unwrap();
    *flag = true;
    condvar.notify_all();
    *flag = false;
}
```

**注意**: correction/out-of-order 写入不触发通知, 避免 consumer 处理更新数据。判断依据: 仅在 `timestamp > old_latest_written_timestamp` (正常写入分支) 成功后通知。

### 29.3 Idle-Close 检查

后台任务 idle-check 时, 如果 `dataset.queue_inner.is_some()`, 跳过该 dataset:

```rust
// bg/mod.rs idle-check 逻辑
for (key, dataset_arc) in datasets.iter() {
    let dataset = dataset_arc.lock().unwrap();

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

`Dataset.close()` 时, 如果 Queue 仍打开, 自动关闭:

```rust
pub fn close(&mut self) -> Result<()> {
    if self.queue_inner.is_some() {
        self.close_queue()?;
    }
    // ... 现有 close 逻辑 ...
}
```

`DatasetQueue.close()` 流程:

```rust
pub fn close(&self) -> Result<()> {
    // 1. 设置 closed 标志
    let inner = self.inner.lock().unwrap();
    inner.closed.store(true, Ordering::SeqCst);

    // 2. 唤醒所有等待 consumer (poll 检测到 closed 后退出)
    let (ref guard, ref condvar) = *self.notify;
    let mut flag = guard.lock().unwrap();
    *flag = true;
    condvar.notify_all();
    *flag = false;

    // 3. 标记 Dataset queue 关闭
    let mut dataset = self.dataset.lock().unwrap();
    dataset.queue_inner = None;
    dataset.queue_notify = None;

    // 4. 状态文件保持打开, 不删除 (下次 open_consumer 可恢复)
    Ok(())
}
```

---

## 三十、并发控制

### 30.1 锁层级

```
Store (RwLock<HashMap>)
    ↓
Dataset (Arc<Mutex<DataSet>>)
    ↓
QueueInner (Arc<Mutex<QueueInner>>)
    ↓
ConsumerStateFile (Arc<Mutex<ConsumerStateFile>>)
    ↓
Condvar pair: (Mutex<bool>, Condvar)
```

**严格遵循**: 外层锁未释放时, 不能获取内层锁。避免死锁。

### 30.2 Push 流程

```rust
pub fn push(&self, data: &[u8]) -> Result<i64> {
    // 1. 获取 dataset 锁 (保证 latest_written_timestamp 串行)
    let mut dataset = self.dataset.lock().unwrap();

    // 2. 检查 queue 是否打开
    if dataset.queue_inner.is_none() {
        return Err(TmslError::QueueClosed);
    }

    // 3. 计算 timestamp
    let ts = dataset.latest_written_timestamp() + 1;

    // 4. 写入数据 (内部 write hook 会触发 notify_all)
    dataset.write(ts, data)?;

    // 5. 返回分配的 timestamp
    Ok(ts)
}
```

**串行化保证**: Dataset 的 `Mutex<DataSet>` 已保证所有 write 操作串行, 不需要额外 queue_mutex。

### 30.3 Poll 流程

```rust
pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
    let deadline = Instant::now() + timeout;

    loop {
        // 1. 检查 closed 标志
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed);
        }

        // 2. 获取 dataset 锁
        let mut dataset_guard = self.dataset.lock().unwrap();

        // 2a. 再次检查 closed (可能在等锁期间被关闭)
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed);
        }

        // 3. 获取 state file 锁
        let mut state = self.state_file.lock().unwrap();

        // 4. 查找可分配的 entry
        let latest = dataset_guard.latest_written_timestamp();
        let next_ts = Self::find_next_available_ts(&state, latest);

        if let Some(ts) = next_ts {
            // 有数据: 分配 pending entry, 读取数据, 返回
            let pending = PendingEntry {
                timestamp: ts,
                start_time: now_unix_epoch(),
                status: 0,
            };
            state.add_pending(pending)?;
            // 不执行 sync — 由后台 flush 任务统一同步

            // 读取数据 (跳过 filler entries)
            let data = dataset_guard.read(ts, None)?
                .map(|(_, bytes)| bytes)
                .unwrap_or_default();

            return Ok(Some((ts, data)));
        }

        // 5. 无数据: 持有 dataset lock 进入 Condvar wait
        //    Condvar::wait_timeout 自动释放 dataset lock, 唤醒后重新获取
        drop(state);  // 先释放 state lock

        let (ref guard_mutex, ref condvar) = *self.notify;
        let notify_guard = guard_mutex.lock().unwrap();

        // 释放 dataset lock 让 write 可以进入
        // 注意: 这里不能直接 drop dataset_guard, 因为 Condvar 绑定的是 guard_mutex
        // 所以改用 wait_timeout_while 模式:

        // 先释放 dataset lock, 进入 condvar wait
        drop(dataset_guard);

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(None);  // 超时
        }

        let (guard, _timeout_result) = condvar
            .wait_timeout(notify_guard, remaining)
            .unwrap();
        drop(guard);
        // 循环重新检查
    }
}

/// 找到 processed_ts 之后第一个不在 pending 中的 timestamp
fn find_next_available_ts(state: &ConsumerStateFile, latest: i64) -> Option<i64> {
    let mut ts = state.processed_ts + 1;
    while ts <= latest {
        // 检查是否已在 pending 中 (待ack 或已ack 但未清理)
        let in_pending = state.pending_entries.iter()
            .any(|e| e.timestamp == ts);
        if !in_pending {
            return Some(ts);
        }
        ts += 1;
    }
    None  // 没有可分配的数据
}
```

**关键设计**: poll 先检查数据可用性, 无数据时释放 dataset lock 进入 Condvar wait。write 完成后 notify_all 唤醒所有等待 consumer。唤醒后循环重新检查数据可用性。

**Condvar 竞态处理**: poll 在释放 dataset lock 和进入 condvar wait 之间有窗口, write 可能在此窗口内完成 notify_all。但由于 poll 在循环中, 下一轮会检查到新数据。最坏情况: poll 多等一个 timeout 周期。

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

    // 5. 扫描连续 ack, 更新 processed_ts
    state.update_processed_ts();

    // 6. 清理已 ack 的 entries (释放空间)
    state.cleanup_acked();

    // 不执行 sync — 由后台 flush 任务统一同步 (与 Dataset 分段文件一致)
    Ok(())
}
```

**关键**: ack 后扫描从 `processed_ts` 开始的连续 ack 序列, 更新 `processed_ts`。然后清理已 ack 的 entries, 释放空间。

### 30.5 多 Consumer poll 分配

同一消费组多个 Consumer 同时 poll 时, 通过共享的 `ConsumerStateFile` 互斥:

```
Consumer A: poll → lock(state) → find_next_available_ts → ts=100 → add_pending(100) → unlock
Consumer B: poll → lock(state) → find_next_available_ts → ts=101 (100已在pending) → add_pending(101) → unlock
```

`find_next_available_ts` 从 `processed_ts + 1` 开始扫描, 跳过已在 pending 中的 timestamp, 返回第一个可用 timestamp。这保证了多 Consumer 不会拿到相同数据。

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
