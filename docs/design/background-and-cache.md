# 后台任务与缓存池

## 十七、后台任务

> **核心设计**: 单一线程执行 flush 和 idle check 两个任务, 通过动态计算下一次唤醒时间来避免轮询浪费。

### 17.0 单线程统一循环

| 任务 | 间隔 | 行为 |
|------|------|------|
| Flush | 可配置, 默认 15s | drain Store 级共享待 flush 队列同步普通 dataset dirty data/index/queue state; 直接调用 JournalManager 同步 dirty journal segment / journal queue state |
| Idle Check | 60s | 扫描 dataset last_used_at, ≥30min → sync + unmmap + close |
| Cache Eviction | 60s | 扫描缓存池, last_access_at ≥30min → 回收 + 释放内存 → LRU 检查 |
| Retention Reclaim | 每日, 默认 0 点 | 扫描 retention_window > 0 的 dataset, 回收过期分段 |

**线程模型**:
```
后台单线程:
  loop:
    1. 计算下一次 flush, idle check, cache eviction, retention reclaim 的到期时间
    2. wait_timeout = min(next_flush, next_idle, next_cache_eviction, next_retention) - now
    3. shutdown_rx.recv_timeout(wait_timeout)
       - 收到信号 → break
       - 超时 → 继续执行到期任务
    4. 如果 now >= next_flush → 执行 flush
    5. 如果 now >= next_idle → 执行 idle check (dataset idle-close)
    6. 如果 now >= next_cache_eviction → 执行缓存回收
    7. 如果 now >= next_retention → 执行 retention reclaim
```

**优势**:
- 减少线程数量 (2 → 1)
- 无固定轮询间隔 (动态计算, 精确到毫秒)
- 单一 shutdown channel (简化资源管理)
- 三个任务共享 datasets 读锁 (减少锁竞争)
- retention reclaim 使用 system clock 计算下次触发时间 (非 monotonic, 依赖 wall clock)

### 17.1 Flush 行为

```
flush (默认每 15 秒):
  1. 从 Store 级共享 flush_queue drain 全部 DataSetFlushTarget
  2. 按 dataset key 去重, 不遍历未出现在队列中的普通 dataset
  3. 对每个出现在队列中的普通 dataset:
     a. 通过 datasets HashMap 精确 get 普通 dataset; journal 不进入该队列
     b. 执行该 dataset 的 flush_dirty_segments()
        - 先把该 dataset 的 in-memory index buffer flush_to_disk()
        - 收集该 dataset 当前仍 dirty 的 data/index segment
        - 仅对这些 dirty segment target 执行 mmap.flush() — MS_SYNC
       - 如果 target 已在 idle-close 中被 flush+close, 或分段已经不存在, 则跳过
  4. 若 journal enabled, 直接调用 JournalManager::flush_dirty()
     - 检查 journal segment / journal queue state 是否 dirty
     - 仅同步 dirty 对象
  注: flush 不密封 pending block, 不压缩
```

每个 data/index segment 在内存中维护两个非持久字段:

- `is_flushed`: 当前 mmap 内容是否已通过 `mmap.flush()` 同步。创建、打开、成功 flush 后为 `true`; 任意 mmap 写入后置为 `false`。
- `queued_for_flush`: 当前 dirty segment 是否已经进入等待 flush 队列。`is_flushed` 从 `true` 变为 `false` 时加入队列; 后续写入保持 dirty 但不重复入队。

Store 持有一个全局共享 `VecDeque<DataSetFlushTarget>` 等待队列, `DataSetRuntimeContext` 仅保存该队列的 `Arc` 引用。队列项必须带 dataset key, 以便后台 flush 不扫描全部 dataset:

```rust
struct DataSetFlushTarget {
    dataset: DataSetKey,
    segment: SegmentFlushTarget,
}

enum SegmentFlushTarget {
    Data { file_offset: u64 },
    Index { start_timestamp: i64 },
    QueueState { group_name: String },
}
```

`SegmentFlushTarget` 的逻辑定位键:

- data segment: `file_offset`
- index segment: `start_timestamp`
- queue state file: `group_name`

后台 `run_flush()` drain 全局队列后按 dataset key 分组, 再逐个执行队列中出现过的精确 target; 不遍历全部 dataset。data/index target 分别同步对应 segment, queue state target 同步对应 consumer group state file。`DataSet::flush()` 同步当前 dataset 的所有打开 data/index segment 和已打开 queue state files, 并清除全局队列中属于当前 dataset 的 stale target; 低层 `DataSet::create/open` 绕过 Store 且没有 runtime context 时, `flush()` 退化为同步所有打开 data/index segment 和 queue state files, 保持直接使用 API 的可用性。

Journal 不把 segment 写入加入 Store 级 `flush_queue`。Journal 是全局单一 append log, 后台 flush 到期时直接调用 `JournalManager::flush_dirty()`; 该方法内部检查 `is_flushed`, clean segment/state file 直接跳过。这样既避免普通 flush queue 被 journal 高频写入污染, 也避免后台为了 journal 扫描普通 dataset。

创建新分段文件时, 写入路径必须先把前一个已经完结的分段文件直接 `flush()`:

- data segment rollover: 当前段无法继续承载新 record, 创建下一 data segment 前同步旧段。
- index segment rollover: 当前 index segment 达到 max size 或连续模式进入下一 grid segment, 创建下一 index segment 前同步旧段。

这个直接 flush 不改变 pending block 状态, 只减少已完结分段在后台 flush 间隔内的异常窗口。

### 17.2 Idle-Close 行为

```
idle-check (每 60s):
  1. 读锁遍历 datasets
     收集 last_used_at.elapsed() >= idle_timeout 的 dataset keys
  2. 对每个 idle dataset key:
     写锁获取 → 获取 dataset 引用
     ⚠️ Double-check: 获取写锁后再次检查 last_used_at.elapsed() >= idle_timeout
  3. 对每个打开的 segment:
     a. mmap.flush() (MS_SYNC)
     b. 不改变 block header, 不密封 pending, 不清除 header pending state
     c. munmap + close file
```

> **Race Condition 防护**: 后台线程读锁收集 idle datasets → 在获取写锁前, 前台写操作可能命中该 dataset → 更新 `last_used_at` → 写锁获取后必须重新检查。

### 17.5 BackgroundTasks 结构

```rust
pub struct BackgroundTasks {
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}
```

### 17.6 mmap 生命周期

```
┌─────────┐  write/read    ┌────────┐   idle 30min   ┌────────┐
│ closed  │ ─────────────→ │  open  │ ──────────────→ │ closed │
│         │ ←─ on-demand ──│(mmap) │                 │(unmap) │
└─────────┘                └────────┘                 └────────┘
    ↑                          │
    │      flush (15s dirty queue) │ msync only
    └──────────────────────────┘
```

### 17.7 Pending Block 恢复

```
reopen 时 pending block 恢复流程:
   1. 读取 FileMetadata, 校验 magic/version
   2. 检查 pending_block_offset != u64::MAX
   3. 恢复流程:
      a. 从 header 恢复 pending 状态
      b. 验证偏移有效性
      c. 保持 block.flags = 0, 恢复为 pending raw block
      d. wrote_position 保持 header 中的已写位置
      e. 返回 OpenReady
```

### 17.8 Retention Reclaim (数据保留回收)

**触发调度**:
- 基于 `StoreConfig.retention_check_hour` (u8, 0-23, 默认 0 = UTC 00:00)
- `retention_check_hour` 明确定义为 **UTC hour**, 不使用本地时区, 不处理 DST
- 使用 `SystemTime` 相对 UNIX epoch 的秒数按 UTC 日边界计算到下一个目标时间点的等待时长
- 每日触发一次, 触发后 `next_retention` 推进 24 小时

**时间计算**:
```rust
fn next_retention_time(check_hour: u8) -> Instant {
    let now = SystemTime::now();
    let today = now.duration_since(UNIX_EPOCH).unwrap();
    let today_secs = today.as_secs();
    // UTC day start + check_hour * 3600
    let day_start = today_secs - (today_secs % 86400);
    let target = day_start + check_hour as u64 * 3600;
    let wait_secs = if target > today_secs {
        target - today_secs
    } else {
        // 今天目标已过, 等到明天
        target + 86400 - today_secs
    };
    Instant::now() + Duration::from_secs(wait_secs)
}
```

**执行流程**:
```
retention-reclaim (每日 retention_check_hour):
  1. 读锁遍历 datasets, 收集 retention_window > 0 的 dataset keys + retention_window
  2. 对每个 retention 启用的 dataset:
     a. Read lock → 获取 dataset Arc 引用
     b. Lock individual dataset mutex
     c. 调用 DataSet::reclaim_expired_segments()
        - 先 close() (flush + idle_close_all)
        - 计算 threshold = latest_written_timestamp.saturating_sub(retention_window as i64)
        - 删除 data 分段 (max_timestamp < threshold)
        - 删除 index 分段 (last_entry_timestamp < threshold)
     d. 释放 dataset mutex
  3. 释放 datasets map read lock
  4. log::info!("[bg retention] reclaimed N segments across M datasets")
```

**关键约束**:
- 回收过程中**不保留打开的 mmap**: close() 后分段均为 closed 状态, 检查文件后立即释放
- **不在 idle-close 中回收**: 回收是独立的、显式的操作, 不依赖 idle 超时
- 若 foreground 线程正在使用某个 dataset, retention reclaim 会阻塞等待 (mutex)
- 回收期间打开的索引文件必须**检查后立即释放** (read-only mmap → drop → fs::remove_file)
- 回收期间不更新 `last_used_at` (回收不应重置 idle 计时)

**数据集级过期判断**:
```
expiration_threshold = ds.latest_written_timestamp.saturating_sub(ds.retention_window)
```

**分段级过期判断**:

| 分段类型 | 判断依据 | 条件 |
|---------|---------|------|
| 数据分段 (DataSegment) | `closed_segments[].max_timestamp` (header 中维护) | `max_timestamp < expiration_threshold` |
| 索引分段 (IndexSegment) | `last_entry_timestamp()` (读取文件最后一个 index entry 的 ts) | `last_ts < expiration_threshold` |

### 17.9 任务执行器统一化 (Unified Executor Pattern)

**核心问题**: 现有后台线程将调度状态 (`last_flush`, `next_retention` 等) 作为线程局部变量保存在 spawn 的闭包内。若仅引入无线程的 "外部触发" 分支, 两套路径将维护各自的调度状态, 且外部调用与后台线程并发时会产生竞态。

**解决方案**: 将调度状态抽取到共享的 `ExecutorState`, 由 `BackgroundTasks` 持有;后台线程和外部 API 均共享同一执行引擎, 通过 `Mutex` 串行化所有任务执行 (无论来源)。线程启用与否仅影响是否自动循环调用, 不影响执行逻辑。

```rust
struct ExecutorState {
    last_flush: Instant,
    last_idle_check: Instant,
    last_cache_eviction: Instant,
    next_retention: Instant,
}

pub struct BackgroundTasks {
    state: Mutex<ExecutorState>,                 // 共享调度状态
    datasets: Arc<RwLock<DatasetMap>>,
    block_cache: Arc<BlockCache>,
    flush_interval: Duration,
    idle_timeout: Duration,
    cache_idle_timeout: Duration,
    retention_check_hour: u8,
    // 以下为线程相关字段, enable_background_thread=false 时为 None
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}
```

**线程启用模式**: `enable_background_thread=true` (默认)
```
后台线程 loop:
  1. 获取 state 锁, 读取所有 last_* 值, 计算 wait_time = min(next_*) - now, 释放锁
  2. recv_timeout(wait_time) — 等待信号或超时
  3. 收到 shutdown → break; 超时 → 继续
  4. 调用 tick(): 在 state 锁内预约到期任务, 释放 state 锁后执行 IO/缓存/回收
  5. 回到 1
```

**线程关闭模式**: `enable_background_thread=false`
- `Store::open` 不创建后台线程, 状态仅做惰性初始化 (last_* = now, next_retention = 当前计算值)
- 由外部调用 `Store::tick_background_tasks()` 主动驱动, 调度逻辑同上
- `next_background_delay()` 仍可正常工作 (读取惰性状态, 无副作用)

**关键一致性保证**: 状态锁 `state: Mutex<ExecutorState>` 只保护调度状态, 不包住长耗时 IO:
1. `tick` 先在 state 锁内把到期任务标记为 running, 并更新对应 `last_*`/`next_retention` 作为调度预约点。
2. 释放 state 锁后执行 flush / idle-close / cache eviction / retention reclaim, 执行体内部再获取 `datasets`/`DataSet`/`BlockCache` 锁。
3. 任务完成后短暂获取 state 锁清除 running 标记; 同一任务 running 期间, 其它 tick 不会重复执行该任务。
4. `next_background_delay()` 只读取 state 快照, 不等待后台 IO 完成。

### 17.10 外部手动执行 (External Manual Execution)

#### 17.10.1 `Store::tick_background_tasks() -> TickResult`

同步执行一次完整的后台任务到期检查与执行流程。

```rust
pub struct TickResult {
    /// 本次 tick 中实际被执行的任务数量 (0..=4)
    pub executed_tasks: usize,
    /// 距离下一次任务到期的剩余时间;
    /// 若本次未执行任何任务, 调用方应在 >= next_delay 后再次调用
    pub next_delay: Duration,
}

impl Store {
    /// 同步执行一次后台任务检查。
    ///
    /// 根据配置的间隔判断每个任务 (flush / idle / cache / retention) 是否到期,
    /// 到期则立即执行。返回本次执行的任务数量 + 距离下一次任一任务到期的剩余时间。
    ///
    /// `enable_background_thread=true` 下也可调用, 与后台线程互斥串行执行。
    /// `enable_background_thread=false` 时, 必须由调用方周期性地调用此方法驱动后台逻辑。
    pub fn tick_background_tasks(&self) -> Result<TickResult>;
}
```

**执行流程**:
```
tick_background_tasks():
  1. 获取 state 锁
  2. 读取 (last_flush, last_idle_check, last_cache_eviction, next_retention)
  3. now = Instant::now()
  4. 计算各任务到期时刻:
       next_flush      = last_flush + flush_interval
       next_idle       = last_idle_check + 60s
       next_cache      = last_cache_eviction + 60s
  5. 对到期且未 running 的任务设置 running=true, 并更新对应 last_* 或 next_retention
  6. 计算 next_delay = min(next_flush, next_idle, next_cache, next_retention) - now
  7. 释放 state 锁
  8. 在锁外按固定顺序执行被预约的任务
  9. 重新短暂获取 state 锁, 清除对应 running 标记
 10. 返回 TickResult { executed_tasks, next_delay }
```

**任务执行顺序与更新**:
| 任务 | 到期判断 | 执行体 | 状态更新 |
|------|---------|--------|---------|
| Flush | `now >= last_flush + flush_interval && !flush_running` | drain 全局 dirty queue, 按 dataset key 精确定位后执行 `flush_dirty_segments()` | 预约时 `last_flush = now`, `flush_running = true`; 完成后 `flush_running = false` |
| Idle Check | `now >= last_idle_check + 60s && !idle_running` | 收集 idle keys → `ds.lock()` + double-check close | 预约时 `last_idle_check = now`, `idle_running = true`; 完成后 `idle_running = false` |
| Cache Eviction | `cache enabled && now >= last_cache_eviction + 60s && !cache_running` | `block_cache.evict_idle(idle_timeout)` | 预约时 `last_cache_eviction = now`, `cache_running = true`; 完成后 `cache_running = false` |
| Retention Reclaim | `now >= next_retention && !retention_running` | `ds.reclaim_expired_segments()` | 预约时 `next_retention = next_retention_time(hour)`, `retention_running = true`; 完成后 `retention_running = false` |

#### 17.10.2 `Store::next_background_delay() -> Result<Duration>`

```rust
impl Store {
    /// 返回距离下一次后台任务执行应等待的时间。
    ///
    /// 仅计算, 不执行任何任务。只在读取调度快照时短暂获取 state 锁,
    /// 不等待后台任务的 IO/segment/cache 操作完成。
    /// 若后台线程未启用, 调用方应以此值调度下一次 tick_background_tasks() 调用。
    pub fn next_background_delay(&self) -> Result<Duration>;
}
```

**计算逻辑**:
```
next_background_delay():
  1. 获取 state 锁
  2. 读取快照
  3. 释放锁
  4. next_delay = min(last_flush + flush_interval,
                      last_idle_check + 60s,
                      last_cache_eviction + 60s,
                      next_retention) - now
  5. 返回 Ok(Duration) (saturating, 不低于 0); executor 未初始化时返回错误
```

**语义注意**:
- 返回值为 "距离下一次任一任务到期" 的剩余时间, 不代表一定会执行 (需等到该时刻)
- 若上次 tick 执行了多个任务, 各 last_* 已同时更新, 下一次到期时间由各任务的间隔独立决定
- 若后台线程未启用且从未调用 tick, 返回值 = `min(flush_interval, 60s, retention_until)`
- 正在 running 的任务已被预约, `next_background_delay()` 不会为了等待其完成而持有或等待 data/index/cache 锁

### 17.11 并发安全模型 (Concurrency Safety)

**并发场景矩阵**:

| 调用方 A (持有锁) | 调用方 B (请求锁) | 行为 |
|------------------|------------------|------|
| 后台线程正在 flush/idle/retention IO | 外部 `tick_background_tasks` | **不重复同一任务** — state 中 running 标记会跳过已预约任务 |
| 外部 `tick_background_tasks` 正在执行任务 | 后台线程 wake-up 后 tick | **短暂串行调度** — 仅竞争 state 快照; 执行体不持有 state |
| 多个外部调用者同时 tick | 彼此 | **调度阶段互斥** — `Mutex` 保护 running/last_*; 执行体可在锁外运行 |
| `next_background_delay()` (读快照) | 正在执行的 tick | **短暂阻塞** — 只等待 state 锁释放, 不等待任务完成 |

**锁顺序 (Lock Ordering)**:
```
外层 → 内层:

1. `BackgroundTasks::state` (Mutex) ← 仅用于调度预约/快照, 不与其它锁嵌套
2. `Store::datasets` (RwLock)       ← 数据集注册表
3. `DataSet` 内部 (Mutex/字段锁)     ← 单个数据集
```

**顺序约束**:
- `tick`/后台线程: 获取 `state` 锁预约任务 → 释放 `state` → 获取 `datasets.read()` → 逐个获取 `ds.lock()`
- `Store::create/open/close/drop_dataset`: 获取 `datasets.write()` → **不触及** `state` 锁 (不会触发 tick)
- `next_background_delay()`: 仅获取 `state` 锁 → 快速读快照 → 释放

**无死锁保证**: `state` 锁不在持有 `datasets` 或 `DataSet` 锁时获取, `datasets.write()` 路径也不调用 tick/next_delay, 因此不存在 `state ↔ datasets` 或 `state ↔ DataSet` 的循环等待。

**前台操作与 idle close 的 race**: 沿用现有机制 — foreground 写操作更新 `last_used_at`, idle close double-check 验证 idle 超时。tick 调用方 (无论前台/外部) 均遵守此协议。

## 十八、读缓存池 (BlockCache)

> **核心原则**: 全局 `BlockCache` 只缓存**已压缩 block 解压后的 payload**。compressed block 一旦写入就视为不可变; pending raw block 不得进入全局缓存。新格式不允许 sealed raw block 存在。

### 18.1 设计目标

- 避免重复解压同一个 block
- 跨查询复用解压数据
- LRU 淘汰 + idle 回收双策略控制内存上限
- `cache_max_memory=0` 时完全禁用, 零额外开销

### 18.2 数据结构

```rust
pub struct BlockCache {
    max_memory: usize,
    used_memory: AtomicUsize,
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    cache_hit_count: AtomicU64,
    cache_miss_count: AtomicU64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CacheKey {
    segment_file_offset: u64,      // equals segment.file_offset
    block_segment_offset: u64,
}

struct CacheEntry {
    data: Vec<u8>,                 // 解压后的 block payload
    last_access_at: Instant,
    access_count: u64,
    memory_footprint: usize,       // data.len() + ~96 bytes overhead
}
```

### 18.3 缓存接口

```rust
impl BlockCache {
    pub fn new(max_memory: usize) -> Self;
    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>>;
    pub fn put(&self, key: CacheKey, data: Vec<u8>);
    pub fn invalidate(&self, key: &CacheKey) -> bool;
    pub fn evict_idle(&self, idle_timeout: Duration) -> usize;
    pub fn clear(&self);
    pub fn stats(&self) -> CacheStats;
}
```

`invalidate` 用于纠正写入、删除、乱序覆盖索引等会改变 record 可见位置的路径。即使目标 block 当前不在缓存中也必须允许调用, 返回值仅表示是否实际删除了缓存项。

### 18.4 LRU 淘汰策略

```
put 时淘汰流程:
  1. 计算新增内存: new_used = used_memory + entry.memory_footprint
  2. 如果 new_used > max_memory:
     a. 收集所有 entry, 按 last_access_at 排序 (从旧到新)
     b. 依次淘汰最旧的 entry, 直到 used_memory + entry_footprint <= max_memory × 0.85
     c. 留出 15% 余量, 避免每次 put 都触发淘汰
  3. 插入新 entry
```

### 18.5 LRU vs Idle 回收

| 策略 | 触发时机 | 淘汰对象 | 效果 |
|------|----------|----------|------|
| LRU 淘汰 | `put` 时 (used_memory > max_memory) | 最久未访问的 entry | 控制内存上限 |
| Idle 回收 | 后台线程每 60s | 超过 idle_timeout 的 entry | 释放不再访问的内存 |

### 18.6 缓存写入规则

| 操作 | 是否进入缓存 | 原因 |
|------|-------------|------|
| `DataSet::write` | ❌ 不进入 | 写入的是 raw 数据, seal 后才可确定 final 内容 |
| `DataSet::query` / `DataSet::read` 读取 compressed block | ✅ 进入 (解压后) | compressed block 不允许再被原地修改, 解压结果具备全局缓存不可变性 |
| pending raw block 读取 | ❌ 不进入 | 仍可能追加 record 或被 seal, 不具备不可变性 |
| compressed block 读取 | ✅ 进入 (解压后) | 解压操作是 CPU 密集型, 且 block 内容不可变 |

### 18.7 读取与失效协议

全局缓存查询必须在读取 `BlockHeader` 之后进行:

1. 根据索引中的 `block_offset` 定位 data segment, 并计算 `block_segment_offset = block_offset - segment.file_offset`。
2. 使用 `segment.header_len + block_segment_offset` 读取 `BlockHeader`。
3. 若 `flags` 包含 `COMPRESSED`, 才使用 `(segment.file_offset, block_segment_offset)` 查询全局 `BlockCache`。
4. 若全局缓存命中, 从缓存 payload 中复制 record。
5. 若未命中, 从 mmap 读取 payload; compressed block 先解压再写入全局缓存, pending raw block 只返回本次读取结果。

`HotBlockCache` 是单次查询迭代器内部的局部缓存, 生命周期不跨越写入操作; 它可以缓存本次查询中读到的 raw 或 compressed payload, 但不得提升为全局缓存规则。

写入路径必须维护以下一致性:

| 写入路径 | 缓存处理 |
|---------|----------|
| 正常追加 | 不触碰全局缓存 |
| correction 原地修改 | 仅允许修改 pending raw block; 目标 key 可防御性 invalidate, 但按新规则不应存在全局缓存 |
| correction 命中 compressed block | 不允许原地修改, 回退为 out-of-order append, 并 invalidate 旧索引指向的 block key |
| out-of-order 覆盖已有索引 | append 新 block/record 后, invalidate 旧索引指向的 block key |
| delete | 将索引置为 filler 前后均可 invalidate 旧索引指向的 block key |

---

**相关**: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md) | [内存与并发](memory-and-concurrency.md)
