# 后台任务与缓存池

## 十七、后台任务

> **核心设计**: 单一线程执行 flush 和 idle check 两个任务, 通过动态计算下一次唤醒时间来避免轮询浪费。

### 17.0 单线程统一循环

| 任务 | 间隔 | 行为 |
|------|------|------|
| Flush | 可配置, 默认 10min | 遍历所有打开的 segment, mmap.flush() (MS_SYNC) |
| Idle Check | 60s | 扫描 dataset last_used_at, ≥30min → sync + 密封 pending + unmmap + close |
| Cache Eviction | 60s | 扫描缓存池, last_access_at ≥30min → 回收 + 释放内存 → LRU 检查 |
| Retention Reclaim | 每日, 默认 0 点 | 扫描 retention_ms > 0 的 dataset, 回收过期分段 |

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
flush (每 10 分钟):
  for each dataset:
    for each open segment (data + index):
      mmap.flush() — MS_SYNC
  注: flush 不密封 pending block, 不压缩
```

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
     b. 如果 data segment 有 pending block → 密封 (不压缩)
     c. 清除 header pending state
     d. munmap + close file
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
    │      flush (10min)       │ msync only
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
      c. 密封 pending block (FLAGS=SEALED, 不压缩)
      d. 清除 header pending state
      e. wrote_position = sealed block 末尾
      f. 返回 OpenReady
```

### 17.8 Retention Reclaim (数据保留回收)

**触发调度**:
- 基于 `StoreConfig.retention_check_hour` (u8, 0-23, 默认 0=午夜)
- 使用 `SystemTime` 计算到下一个目标时间点的等待时长
- 每日触发一次, 触发后 `next_retention` 推进 24 小时

**时间计算**:
```rust
fn next_retention_time(check_hour: u8) -> Instant {
    let now = SystemTime::now();
    let today = now.duration_since(UNIX_EPOCH).unwrap();
    let today_secs = today.as_secs();
    // 今天目标时间 = 今天 0 点 + check_hour * 3600
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
  1. 读锁遍历 datasets, 收集 retention_ms > 0 的 dataset keys + retention_ms
  2. 对每个 retention 启用的 dataset:
     a. Read lock → 获取 dataset Arc 引用
     b. Lock individual dataset mutex
     c. 调用 DataSet::reclaim_expired_segments()
        - 先 close() (flush + idle_close_all)
        - 计算 threshold = latest_written_timestamp - retention_ms
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
expiration_threshold = ds.latest_written_timestamp - ds.retention_ms
```

**分段级过期判断**:

| 分段类型 | 判断依据 | 条件 |
|---------|---------|------|
| 数据分段 (DataSegment) | `closed_segments[].max_timestamp` (header 中维护) | `max_timestamp < expiration_threshold` |
| 索引分段 (IndexSegment) | `last_entry_timestamp()` (读取文件最后一个 index entry 的 ts) | `last_ts < expiration_threshold` |

## 十八、读缓存池 (BlockCache)

> **核心原则**: 只缓存**解压后的 seal block payload**。写入不进入缓存, 只有读取时解压后的数据才加入。

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
    segment_file_offset: u64,
    block_offset: u64,
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
    pub fn evict_idle(&self, idle_timeout: Duration) -> usize;
    pub fn clear(&self);
    pub fn stats(&self) -> CacheStats;
}
```

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
| `DataSet::query` | ✅ 进入 (解压后) | 解压后的 seal block 数据不可变, 安全缓存 |
| 未压缩 block 读取 | ✅ 进入 | raw payload 直接从 mmap 复制到缓存 |
| 压缩 block 读取 | ✅ 进入 (解压后) | 解压操作是 CPU 密集型, 缓存价值最高 |

---

**相关**: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md) | [内存与并发](memory-and-concurrency.md)
