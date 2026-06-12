# Queue 模块 — 状态文件格式与同步

## 三十一、消费组状态文件

### 31.1 文件布局

每个消费组对应一个固定 4KB 的 mmap 文件, 存储在:
```
{data_dir}/{dataset_name}/{dataset_type}/queue/{group_name}
```

`group_name` 是文件名, 不做转义或编码。合法值必须非空、最长 255 字节且整体匹配 `^[0-9A-Za-z_-]+$`: 只允许数字、大小写英文字母、`-`、`_`。任何路径分隔符、`.`、空格、控制字符、非 ASCII 字符都不允许。`open_consumer(group_name)` 与 `drop_consumer(group_name)` 必须在拼接 `queue/{group_name}` 前校验。

**文件结构**:
```
Offset  Size    Field                   Description
──────────────────────────────────────────────────────────────
0       4       magic                   "QSTF" (Queue State File)
4       4       version                 u32, 当前 = 1
8       2       state_length            u16, processed_ts 字节数 (固定 8)
10      8       processed_ts            i64, 已处理的连续最大时间戳
18      2       pending_length          u16, pending entries 数量
20      1       pending_value_size      u8, 单条 pending entry 字节数 (固定 17)
21      -       pending_entries         变长, pending_length * 17 字节
──────────────────────────────────────────────────────────────
```

**总大小**: 固定 4096 字节 (4KB)

### 31.2 Pending Entry 格式

每条 pending entry 占 17 字节:

```
Offset  Size    Field                   Description
──────────────────────────────────────────────────────────────
0       8       timestamp               i64, 数据时间戳
8       8       start_time              i64, 开始处理的时间戳 (Unix epoch seconds)
16      1       status                  u8, 0=待ack, 1=已ack
──────────────────────────────────────────────────────────────
```

### 31.3 容量计算

**Header 固定占用**:
- magic: 4 bytes
- version: 4 bytes
- state_length: 2 bytes
- processed_ts: 8 bytes
- pending_length: 2 bytes
- pending_value_size: 1 bytes
- **总计**: 21 bytes

**可用空间**:
```
4096 - 21 = 4075 bytes
```

**最大 pending entries**:
```
4075 / 17 = 239 entries (取整)
```

**实际使用**:
- 239 * 17 = 4063 bytes
- 剩余 12 bytes 未使用 (padding)

### 31.4 文件操作

```rust
pub(crate) struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
}

impl ConsumerStateFile {
    /// 打开现有状态文件或创建新文件
    pub fn open_or_create(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Self::create_new(path, initial_processed_ts)
        }
    }

    fn open_existing(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // 验证 magic
        if &mmap[0..4] != b"QSTF" {
            return Err(TmslError::InvalidMagic("queue state file"));
        }

        // 验证 version
        let version = read_u32(&mmap, 4);
        if version != 1 {
            return Err(TmslError::InvalidVersion(version));
        }

        // 读取 processed_ts
        let processed_ts = read_i64(&mmap, 10);

        // 读取 pending entries
        let pending_length = read_u16(&mmap, 18) as usize;
        let mut pending_entries = Vec::with_capacity(pending_length);
        for i in 0..pending_length {
            let offset = 21 + i * 17;
            pending_entries.push(PendingEntry {
                timestamp: read_i64(&mmap, offset),
                start_time: read_i64(&mmap, offset + 8),
                status: mmap[offset + 16],
            });
        }

        Ok(Self {
            path,
            mmap,
            processed_ts,
            pending_entries,
        })
    }

    fn create_new(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        // 确保 queue/ 目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.set_len(4096)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // 写入 header
        mmap[0..4].copy_from_slice(b"QSTF");
        write_u32(&mut mmap, 4, 1);           // version
        write_u16(&mut mmap, 8, 8);           // state_length = 8 (i64)
        write_i64(&mut mmap, 10, initial_processed_ts);
        write_u16(&mut mmap, 18, 0);          // pending_length = 0
        mmap[20] = 17;                        // pending_value_size = 17

        // 同步到磁盘
        mmap.flush()?;

        Ok(Self {
            path,
            mmap,
            processed_ts: initial_processed_ts,
            pending_entries: Vec::new(),
        })
    }

    /// 同步内存状态到 mmap 并 flush (MS_SYNC)
    pub fn sync(&mut self) -> Result<()> {
        // 更新 processed_ts
        write_i64(&mut self.mmap, 10, self.processed_ts);

        // 更新 pending entries
        let pending_length = self.pending_entries.len() as u16;
        write_u16(&mut self.mmap, 18, pending_length);

        for (i, entry) in self.pending_entries.iter().enumerate() {
            let offset = 21 + i * 17;
            write_i64(&mut self.mmap, offset, entry.timestamp);
            write_i64(&mut self.mmap, offset + 8, entry.start_time);
            self.mmap[offset + 16] = entry.status;
        }

        // 清零剩余区域 (防止旧数据残留)
        let used = 21 + self.pending_entries.len() * 17;
        for byte in &mut self.mmap[used..] {
            *byte = 0;
        }

        // MS_SYNC 同步
        self.mmap.flush()?;
        Ok(())
    }

    /// 添加 pending entry (容量检查)
    pub fn add_pending(&mut self, entry: PendingEntry) -> Result<()> {
        if self.pending_entries.len() >= 239 {
            return Err(TmslError::PendingFull);
        }
        self.pending_entries.push(entry);
        Ok(())
    }

    /// 查找待 ack 的 pending entry
    pub fn find_pending(&self, timestamp: i64) -> Option<&PendingEntry> {
        self.pending_entries.iter()
            .find(|e| e.timestamp == timestamp && e.status == 0)
    }

    /// 查找待 ack 的 pending entry (mut)
    pub fn find_pending_mut(&mut self, timestamp: i64) -> Option<&mut PendingEntry> {
        self.pending_entries.iter_mut()
            .find(|e| e.timestamp == timestamp && e.status == 0)
    }

    /// 检查 timestamp 是否在 pending 中 (任何状态)
    pub fn is_in_pending(&self, timestamp: i64) -> bool {
        self.pending_entries.iter().any(|e| e.timestamp == timestamp)
    }

    /// 清理已 ack 的 entries (释放空间)
    pub fn cleanup_acked(&mut self) {
        self.pending_entries.retain(|e| e.status == 0);
    }

    /// 扫描从 processed_ts 开始的连续 ack 序列, 更新 processed_ts
    pub fn update_processed_ts(&mut self) {
        loop {
            let next = self.processed_ts + 1;
            match self.pending_entries.iter().find(|e| e.timestamp == next) {
                Some(e) if e.status == 1 => {
                    self.processed_ts = next;
                }
                _ => break,
            }
        }
    }

    /// 清理超时的 pending entries (释放空间, 数据重新可 poll)
    pub fn cleanup_timeout(&mut self, timeout_secs: i64) {
        let now = now_unix_epoch();
        self.pending_entries.retain(|e| {
            if e.status == 0 && (now - e.start_time) > timeout_secs {
                false  // 超时, 释放空间
            } else {
                true
            }
        });
    }
}
```

### 31.5 Crash 恢复

**进程 crash 后, 状态文件可能处于以下状态**:

| 状态 | 说明 | 恢复策略 |
|------|------|----------|
| processed_ts 未更新 | 内存中已 ack, 但未 sync | 重新消费 (at-least-once) |
| pending entry status=0 | 处理中但未 ack | 重新可 poll |
| pending entry status=1 | 已 ack 但未清理 | `cleanup_acked()` 清理 |

**open_consumer 恢复逻辑**:

```rust
pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer> {
    // 1. 获取 dataset 锁
    let dataset_guard = self.dataset.lock().unwrap();

    // 2. 构建状态文件路径
    let state_path = dataset_guard.base_dir.join("queue").join(group_name);

    // 3. 获取初始 processed_ts
    let latest = dataset_guard.latest_written_timestamp();

    // 4. 打开或创建状态文件
    let mut state_file = ConsumerStateFile::open_or_create(state_path, latest)?;

    // 5. 恢复: 清理已 ack 但未清理的 entries
    state_file.cleanup_acked();

    // 6. 恢复: 所有 status=0 的 pending entries 保留 (重新可 poll)
    //    不重置 start_time, 由 cleanup_timeout 处理超时

    // 7. 同步恢复后的状态
    state_file.sync()?;

    // 8. 注册到 QueueInner.consumers
    let state_arc = Arc::new(Mutex::new(state_file));
    drop(dataset_guard);

    let mut inner = self.inner.lock().unwrap();
    inner.consumers
        .entry(group_name.to_string())
        .or_insert_with(Vec::new)
        .push(state_arc.clone());

    Ok(DatasetQueueConsumer {
        group_name: group_name.to_string(),
        state_file: state_arc,
        notify: self.notify.clone(),
        dataset: self.dataset.clone(),
        closed: inner.closed.clone(),
    })
}
```

**关键**: crash 后所有 `status=0` 的 pending entries 保留, 重新可 poll。这是 at-least-once 语义的保证。

---

## 三十二、同步策略

### 32.1 Flush 集成 (唯一 Sync 入口)

Queue 状态文件与 Dataset 分段文件采用相同的 dirty flush queue 策略。每个 consumer group state file 是一等 flush target: `SegmentFlushTarget::QueueState { group_name }`。ack、poll 和 timeout cleanup 后只更新内存状态并把对应 group 入队, 不触发立即 sync; 后台 flush 任务 drain 队列后同步对应 state file。

```rust
// bg/mod.rs flush 逻辑
fn flush_target(dataset: &mut DataSet, target: SegmentFlushTarget) {
    match target {
        SegmentFlushTarget::Data { file_offset } => dataset.sync_data_segment(file_offset),
        SegmentFlushTarget::Index { start_timestamp } => dataset.sync_index_segment(start_timestamp),
        SegmentFlushTarget::QueueState { group_name } => {
            dataset.sync_queue_state_file(&group_name)
        }
    }
}
```

**关键**: queue state file 与 data/index segment 共享同一个 dirty flush queue 和 flush 周期。状态变更 (ack/poll/timeout cleanup) 在内存中累积并入队, flush 时按 group 持久化。Crash 后可能丢失最近 flush 间隔内的状态变更, 这与 Dataset 分段文件的 crash 安全保证一致。

### 32.2 Ack 与 Poll 不执行立即 Sync

`ack()` 和 `poll()` 操作后 **不执行立即 Sync**, 仅更新内存中的状态并入队 `QueueState { group_name }`。状态文件与 Dataset 分段文件采用相同的 Sync 策略 — 由后台 flush 任务统一执行 `mmap.flush()` (MS_SYNC)。

**Ack 流程** (仅内存更新):

```rust
pub fn ack(&self, timestamp: i64) -> Result<()> {
    // 1. 检查 closed
    if self.closed.load(Ordering::SeqCst) {
        return Err(TmslError::QueueClosed);
    }

    // 2. 获取 state file 锁
    let mut state = self.state_file.lock().unwrap();

    // 3. 标记为已 ack
    let entry = state.find_pending_mut(timestamp)
        .ok_or(TmslError::NotFound(format!("pending entry: {}", timestamp)))?;
    entry.status = 1;

    // 4. 更新 processed_ts (扫描连续 ack)
    state.update_processed_ts();

    // 5. 清理已 ack entries
    state.cleanup_acked();

    // 不执行 sync — 由后台 flush 任务统一同步
    Ok(())
}
```

**Poll 分配** (仅内存更新):

```rust
// poll 内部, 分配 pending entry 后:
let pending = PendingEntry {
    timestamp: ts,
    start_time: now_unix_epoch(),
    status: 0,
};
state.add_pending(pending)?;
// 不执行 sync — 由后台 flush 任务统一同步

// 读取数据并返回
let data = dataset_guard.read(ts, None)?
    .map(|(_, bytes)| bytes)
    .unwrap_or_default();
return Ok(Some((ts, data)));
```

**Crash 安全**: 与 Dataset 分段文件一致, crash 后可能丢失最近 flush 间隔内的状态变更。Consumer 重新 poll 时, 未持久化的 pending entry 会丢失 (该数据重新可 poll), 未持久化的 ack 也会丢失 (该数据重新消费)。这是 at-least-once 语义的自然保证。

---

## 三十三、容量限制与处理

### 33.1 Pending 满的处理

当 pending entries 达到 239 条上限时, `poll()` 返回 `PendingFull` 错误:

```rust
// poll 内部分配 pending 时:
state.add_pending(pending)?;  // 可能返回 TmslError::PendingFull
```

**调用方策略**:
- 立即 ack 已处理的数据, 释放空间
- 等待一段时间后重试
- 增加 consumer 实例数, 分散处理负载

### 33.2 Pending 超时清理

如果 pending entry 长时间未 ack (默认 5 分钟 = 300 秒), 主动清理释放空间:

```rust
/// 清理超时 pending entries
/// timeout_secs: 超时时间 (默认 300 秒 = 5 分钟)
pub fn cleanup_timeout(&mut self, timeout_secs: i64) {
    let now = now_unix_epoch();
    self.pending_entries.retain(|e| {
        if e.status == 0 && (now - e.start_time) > timeout_secs {
            false  // 超时, 释放空间, 数据重新可 poll
        } else {
            true
        }
    });
}
```

**调用时机**: 后台 flush 任务执行时, 同时检查并清理超时 pending entries:

```rust
// bg/mod.rs flush 逻辑中:
for state_arc in state_arcs {
    if let Ok(mut state) = state_arc.lock() {
        state.cleanup_timeout(300);  // 5 分钟超时
        let _ = state.sync();
    }
}
```

**注意**: 超时清理会导致数据重新可 poll (at-least-once 语义)。Consumer 会再次收到相同数据。

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
| poll | Dataset Mutex + StateFile Mutex | 中 (多 consumer 竞争 state lock) |
| ack | StateFile Mutex | 低 (单 consumer 操作) |

**优化**: poll 等待时释放 Dataset 锁, 进入 Condvar wait, 不阻塞 write。

### 34.2 Mmap 开销

- 每个消费组 4KB mmap, 常驻内存
- 10 个消费组 = 40KB, 可忽略
- 100 个消费组 = 400KB, 仍然可接受

### 34.3 Sync 开销

- MS_SYNC 每次约 1-5ms (取决于磁盘)
- 高频 ack/poll 场景下, sync 可能成为瓶颈
- 如果需要更高吞吐, 可以改为 MS_ASYNC + 定时 sync (由 flush 任务覆盖)

### 34.4 Condvar 通知

- `notify_all()` 唤醒所有等待 consumer
- N 个 consumer 同时 poll, push 后全部唤醒, 但只有部分能拿到数据 (取决于数据量)
- 如果 consumer 数量远大于 push 频率, 大部分唤醒是空跑 (spurious wakeup), 但 poll 循环会重新进入 wait

### 34.5 poll next-entry efficiency

当前实现不按 timestamp 逐个线性扫描 gap/filler, 而是在 direct read miss 后通过 `query_index_entries(next_ts, i64::MAX)` 查找后续 index entry。

- 正常情况下: `processed_ts + 1` 即可命中, O(1)
- 大量 filler/gap: 由 TimeIndex range 查询跳过未创建的中间 segment 和 filler entry; 不为 filler 创建 pending。
- pending 堆积: 最坏 O(pending_length), 但 pending 最多 239 条, 可接受
