# Phase 16: 数据保留 (Retention) — 有效期回收 + 查询约束

> **目标**: 为每个数据集添加 `retention_window` 与 `timestamp_units_per_seconds: u64` 不可变配置, 支持 legacy 与 wall-clock retention, Store 级可配置每日回收时间点, 后台线程执行回收任务删除过期分段文件, 查询自动钳制到有效时间范围内。

## 1. 背景与动机

### 1.1 当前问题

时序数据积累会导致磁盘空间持续增长, 缺乏自动清理机制。用户需要:

- **按时间维度回收**: 旧数据超过一定期限后自动删除, 释放磁盘空间
- **可控调度**: 回收操作不应在高峰期运行, 应可指定每日执行时间
- **查询正确性**: 查询结果不应包含已过期但尚未回收的数据

### 1.2 设计方案

| 维度 | 设计决策 |
|------|---------|
| retention 存储 | 数据集 meta 文件保存 `retention_window` 与 `timestamp_units_per_seconds: u64` (0=legacy scale) |
| 回收调度 | StoreConfig 新增 `retention_check_hour` (u8, 0-23, 默认 0=午夜) |
| 回收基准 | scale 为 `0` 时为 `latest_written_timestamp.saturating_sub(retention_window)`; 非零时为持久化 monotonic floor 与 scaled Unix-time candidate 的最大值 |
| 回收粒度 | 整个分段文件 (数据段/索引段), 不拆分 block |
| 查询约束 | `query_iter()` 自动钳制 `start_ts = max(start_ts, expiration_threshold)`; 仅 `timestamp < expiration_threshold` 过期 |
| 锁策略 | 回收前 close() dataset, 回收期间不保持 mmap 打开 |

## 2. 改动清单

### 2.1 `src/meta.rs` — DataSetMeta

**新增字段**:
- `pub retention_window: u64` — 数据有效期 (与 timestamp 同单位, 0=不限)
- `pub timestamp_units_per_seconds: u64` — 不可变 wall-clock scale, `0` 保持 legacy retention
- `pub persisted_retention_floor: i64` — dataset state 中持久化的 nonzero-scale 单调 floor, 不属于 `DataSetMeta`

**变更**:
- `DataSetMeta::new()`: 接收 `retention_window` 与 `timestamp_units_per_seconds`, 两者均为不可变配置
- `DataSetMeta::to_bytes()` / `from_bytes()`: 持久化并恢复两个配置值; 缺失 `timestamp_units_per_seconds` 时默认为 `0`, 保持 legacy 行为
- `persisted_retention_floor` 存在 dataset state 中, 不属于 immutable meta; 新 floor 必须先持久化并 flush

### 2.2 `src/config.rs` — StoreConfig + DataSetConfig

**StoreConfig 新增字段**:
```rust
pub retention_check_hour: u8,  // 每日回收执行时间点 (0-23, 默认 0=午夜)
```

**StoreConfigBuilder 新增方法**:
```rust
pub fn retention_check_hour(mut self, hour: u8) -> Self
```
- `hour.clamp(0, 23)` 容错处理

**Default**:
- `retention_check_hour: 0`

**DataSetConfig 新增字段**:
```rust
    pub retention_window: u64,
    pub timestamp_units_per_seconds: u64,
```

**DataSetConfigBuilder 新增方法**:
```rust
    pub fn retention_window(mut self, window: u64) -> Self
    pub fn timestamp_units_per_seconds(mut self, units: u64) -> Self
```

### 2.3 `src/dataset.rs` — DataSet

**新增字段**:
- `retention_window: u64` — 从 meta 读取或 create 时传入
- `timestamp_units_per_seconds: u64` — 从 meta 读取或 create 时传入
- `persisted_retention_floor: i64` — 从 state 恢复, 只可单调推进

**DataSet::create()**:
- 新增 `retention_window: u64` 和 `timestamp_units_per_seconds: u64` 参数
- 写入 meta 时包含两个不可变配置

**DataSet::open()**:
- 从 meta 读取 retention_window 与 timestamp_units_per_seconds
- 从 state 恢复 persisted_retention_floor

**DataSet::query_iter()**:
```rust
pub fn query_iter(...) {
    let mut start_ts = start_ts;
    if self.retention_window > 0 {
        let threshold = self.effective_retention_floor()?;
        if start_ts < threshold {
            start_ts = threshold;
        }
        if start_ts > end_ts {
            return Ok(QueryIterator::empty(...));  // 完全过期
        }
    }
    // Only timestamps strictly below threshold are expired.
}
```

**DataSet::reclaim_expired_segments() (新增)**:
```rust
pub fn reclaim_expired_segments(&mut self) -> Result<usize> {
    if self.retention_window == 0 { return Ok(0); }
    let threshold = if self.timestamp_units_per_seconds == 0 {
        let Some(latest) = self.latest_written_timestamp else { return Ok(0); };
        latest.saturating_sub(self.retention_window as i64)
    } else {
        // current Unix seconds × units uses checked i128 arithmetic.
        // A pre-epoch clock or an i64 conversion overflow is InvalidData.
        let now_units = checked_unix_seconds_times_units()?;
        let candidate_floor = now_units.saturating_sub(self.retention_window as i64);
        let floor = self.persisted_retention_floor.max(candidate_floor);
        if floor > self.persisted_retention_floor {
            self.persisted_retention_floor = floor;
            self.flush_retention_state()?; // durable before physical reclaim
        }
        floor
    };

    // 1. Flush and idle-close all data/index segments before reclaiming files.
    // This only syncs and unmaps segments; it does not close the DataSet lifecycle.
    self.flush()?;
    self.segments.idle_close_all()?;
    self.time_index.idle_close_all()?;

    // 2. Reclaim index segments
    let idx_reclaimed = self.time_index.reclaim_expired_segments(
        threshold, self.config.index_segment_size
    )?;

    // 3. Reclaim data segments
    let data_reclaimed = self.segments.reclaim_expired_segments(threshold)?;

    Ok(idx_reclaimed + data_reclaimed)
}
```

**DataSet::retention_window() (新增 getter)**:
```rust
    pub fn retention_window(&self) -> u64 { self.retention_window }
```

### 2.4 `src/segment/mod.rs` — DataSegmentSet

**DataSegmentSet::reclaim_expired_segments() (新增)**:
```rust
pub fn reclaim_expired_segments(&mut self, threshold: i64) -> Result<usize> {
    let mut reclaimed = 0;
    let before_len = self.closed_segments.len();
    self.closed_segments.retain(|meta| {
        if meta.max_timestamp < threshold {
            let _ = std::fs::remove_file(&meta.path);
            log::info!("[retention] deleted data segment: {:?}", meta.path);
            false  // remove
        } else {
            true   // keep
        }
    });
    reclaimed = before_len - self.closed_segments.len();
    Ok(reclaimed)
}
```

- 使用 data segment registry 中缓存的 `max_timestamp`, 无需打开文件
- `retain()` 同时完成筛选和删除

### 2.5 `src/index/mod.rs` — TimeIndex

**TimeIndex::reclaim_expired_segments() (新增)**:
```rust
pub fn reclaim_expired_segments(
    &mut self, threshold: i64, max_file_size: u64,
) -> Result<usize> {
    let mut reclaimed = 0;
    let before_len = self.index_segments.len();
    self.index_segments.retain(|_start, entry| {
        let meta = entry.meta();
        match IndexSegment::last_entry_timestamp(&meta.path, max_file_size) {
            Ok(last_ts) => {
                if last_ts < threshold {
                    let _ = std::fs::remove_file(&meta.path);
                    log::info!("[retention] deleted index segment: {:?}", meta.path);
                    false  // remove
                } else { true }
            }
            Err(_) => true,  // 读取失败时保留 (安全)
        }
    });
    reclaimed = before_len - self.index_segments.len();
    Ok(reclaimed)
}
```

### 2.6 `src/index/segment.rs` — IndexSegment

**IndexSegment::last_entry_timestamp() (新增自由函数)**:
```rust
/// 读取索引段文件中最后一个条目的 timestamp, 立即释放 mmap+file.
/// 返回 Ok(last_ts). 空段/读取失败返回 Err.
pub fn last_entry_timestamp(path: &Path, max_file_size: u64) -> Result<i64> {
    let file = std::fs::OpenOptions::new().read(true).open(path)?;
    let file_len = file.metadata()?.len();
    // Parse the variable-length index header and its persisted state first.
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let header = read_index_segment_header(&mmap)?;
    let header_len = header.header_len as usize;
    let wrote_pos = header.wrote_position;
    if file_len < header_len as u64 || wrote_pos < header_len as u64 {
        return Err(...);
    }

    // wrote_position is relative to the physical file and is persisted in
    // the variable-length header state. Validate it before deriving entries.
    if wrote_pos > file_len || (wrote_pos - header_len as u64) % INDEX_ENTRY_SIZE as u64 != 0 {
        return Err(...);
    }
    let wrote_count = (wrote_pos - header_len as u64) / INDEX_ENTRY_SIZE as u64;

    let result = if wrote_count == 0 {
        Err(TmslError::InvalidData("empty index segment".into()))
    } else {
        let last_offset = header_len
            + (wrote_count - 1) * INDEX_ENTRY_SIZE;
        let ts = i64::from_le_bytes(
            mmap[last_offset..last_offset + 8].try_into().unwrap()
        );
        Ok(ts)
    };

    drop(mmap);
    drop(file);
    result
}
```

### 2.7 `src/bg/mod.rs` — BackgroundTasks

**启动参数新增**:
- `retention_check_hour: u8`

**新增时间计算**:
```rust
fn next_retention_time(check_hour: u8) -> Instant {
    let now_system = SystemTime::now();
    let secs_since_epoch = now_system.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let today_start = secs_since_epoch - (secs_since_epoch % 86400);
    let target = today_start + check_hour as u64 * 3600;
    let wait = if target > secs_since_epoch {
        target - secs_since_epoch
    } else {
        target + 86400 - secs_since_epoch
    };
    Instant::now() + Duration::from_secs(wait)
}
```

**主循环变更**:
- 新增 `let mut next_retention = next_retention_time(retention_check_hour);`
- `wait_time` 计算纳入 `next_retention`
- 新增 retention reclaim 执行分支

**回收执行逻辑**:
```rust
if Instant::now() >= next_retention {
    // 1. 读锁: 收集 retention_window > 0 的 dataset keys
    let retention_datasets: Vec<(DataSetKey, u64)> = {
        let guard = datasets.read().unwrap();
        guard.iter()
            .filter_map(|(k, ds_arc)| {
                let ds = ds_arc.lock().ok()?;
                if ds.retention_window() > 0 { Some((k.clone(), ds.retention_window())) }
                else { None }
            })
            .collect()
    };

    // 2. 逐个回收
    for (key, _retention_window) in retention_datasets {
        let ds_arc = {
            let guard = match datasets.read() {
                Ok(g) => g, Err(_) => continue,
            };
            match guard.get(&key) {
                Some(ds) => Arc::clone(ds), None => continue,
            }
        };
        let mut ds = match ds_arc.lock() {
            Ok(ds) => ds, Err(_) => continue,
        };
        match ds.reclaim_expired_segments() {
            Ok(n) if n > 0 => log::info!("[bg retention] {:?}: reclaimed {} segments", key, n),
            Err(e) => log::error!("[bg retention] {:?}: reclaim failed: {}", key, e),
            _ => {}
        }
    }
    next_retention = next_retention_time(retention_check_hour) + Duration::from_secs(86400);
}
```

### 2.8 `src/store.rs` — Store

**Store::open()**:
- 传递 `config.retention_check_hour` 到 `BackgroundTasks::start()`
- create_dataset_with_config() 传递 `config.retention_window` 与 `config.timestamp_units_per_seconds`

**Store::create_dataset()**:
- 向后兼容: retention_window = 0 时禁用 retention; 旧 meta 缺少 scale 时以 timestamp_units_per_seconds = 0 使用 legacy 行为

**Store::create_dataset_with_config()**:
- 从 DataSetConfig 提取 retention_window 与 timestamp_units_per_seconds 传递到 DataSet::create()

### 2.9 `wrapper/cffi/src/lib.rs` — FFI

**tmsl_dataset_create()**:
- 新增 `retention_window: u64` 和 `timestamp_units_per_seconds: u64` 参数
- 传递到 `store.create_dataset()` 或通过 DataSetConfigBuilder

**向后兼容**:
- `tmsl_store_open()` 使用 StoreConfig::default() (retention_check_hour=0)
- 旧 FFI 调用者传入 `timestamp_units_per_seconds = 0` 时保持 legacy retention

### 2.10 `wrapper/cffi/include/timslite.h` — C 头文件

**更新函数声明**:
```c
void* tmsl_dataset_create(
    void* store, const char* name, const char* dataset_type,
    uint64_t data_segment_size, uint64_t index_segment_size,
    unsigned char compress_level, unsigned char index_continuous,
    uint64_t retention_window,
    uint64_t timestamp_units_per_seconds,
    char* err_buf, size_t err_buf_len);
```

## 3. 测试计划

### 3.1 单元测试

| 测试 | 文件 | 描述 |
|------|------|------|
| `test_meta_retention_config_roundtrip` | meta.rs | retention_window 和 timestamp_units_per_seconds 序列化/反序列化 |
| `test_meta_retention_scale_default_zero` | meta.rs | 缺失 scale 时默认 0, 保持 legacy 行为 |
| `test_config_retention_check_hour` | config.rs | StoreConfig builder 设置 retention_check_hour |
| `test_config_retention_window_and_scale` | config.rs | DataSetConfigBuilder 设置 retention_window 与 timestamp_units_per_seconds |
| `test_dataset_retention_config_stored` | dataset.rs | create → open → 验证 retention 配置一致 |
| `test_next_retention_time` | bg/mod.rs | 计算下次回收时间正确性 |

### 3.2 集成测试

| 测试 | 描述 |
|------|------|
| `t16_1_retention_no_reclaim_when_zero` | retention_window=0 → 不回收且不推进 floor |
| `t16_2_retention_legacy_scale_zero` | timestamp_units_per_seconds=0 → latest-based threshold, 不读取 wall clock |
| `t16_3_retention_wall_clock_threshold` | nonzero scale → checked i128 的 current Unix seconds × units, 再减 retention_window |
| `t16_4_retention_strict_boundary` | threshold 以下过期, threshold 相等仍可读且不回收 |
| `t16_5_retention_monotonic_floor` | 时钟回拨、重启和 failed reclaim 后 floor 均不降低 |
| `t16_6_retention_floor_persisted_before_reclaim` | floor 先写入并 flush state, 随后才允许物理删除 |
| `t16_7_retention_invalid_clock_or_overflow` | epoch 前时钟、i128 乘法或 i64 转换失败均为 InvalidData |
| `t16_8_retention_backward_compat` | 旧 meta 缺少 timestamp_units_per_seconds → 默认 0 并使用 legacy 行为 |

### 3.3 验证清单

- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过
- [x] 旧 FFI 调用适配

## 4. 实施顺序

| Step | 描述 | 依赖 |
|------|------|------|
| 1 | meta.rs: retention_window 与 timestamp_units_per_seconds 持久化 | — |
| 2 | config.rs: retention_check_hour + DataSetConfig retention 配置 | Step 1 |
| 3 | dataset.rs: scale 分支、wall-clock threshold、单调 floor 持久化和 query 钳制 | Step 1, 2 |
| 4 | segment/mod.rs: reclaim_expired_segments | Step 3 |
| 5 | index/segment.rs: last_entry_timestamp | — |
| 6 | index/mod.rs: reclaim_expired_segments | Step 5 |
| 7 | bg/mod.rs: retention_check_hour + 回收任务 | Step 3, 4, 6 |
| 8 | store.rs: 传递新配置 | Step 2, 7 |
| 9 | ffi.rs + timslite.h: FFI 参数扩展 | Step 8 |
| 10 | 测试 + 文档更新 | 全部 |

## 5. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 回收期间前台线程等待 | 写操作延迟 | 回收仅在 dataset mutex 层面阻塞, 不影响其他 dataset |
| 关闭 dataset 后 reopen | 需要 lazy_open 重建 | DataSegmentSet.append() 已有 lazy_open 支持 |
| retention_window 单位或 scale 不一致 | 过早/过晚回收 | retention_window 与 timestamp 同单位; nonzero scale 明确 timestamp units per Unix second |
| 连续模式 back-fill 与回收冲突 | 找不到 filler | 回收后 back-fill 返回 NotFound (预期行为) |
| 回收文件时 Windows 文件锁定 | 删除失败 | read-only mmap + 立即 drop 后 remove |

## 6. 验收标准
- [x] `meta.rs`: retention_window 与 timestamp_units_per_seconds 完整序列化/反序列化, 缺失 scale 时默认 0
- [x] `config.rs`: StoreConfig.retention_check_hour + DataSetConfigBuilder retention 配置
- [x] `dataset.rs`: legacy/wall-clock threshold、持久化单调 floor、query 钳制和 reclaim_expired_segments
- [x] `segment/mod.rs`: DataSegmentSet.reclaim_expired_segments (max_timestamp 判断)
- [x] `index/mod.rs`: TimeIndex.reclaim_expired_segments (last_entry_timestamp 判断)
- [x] `index/segment.rs`: last_entry_timestamp() 读取后立即释放 mmap+file
- [x] `bg/mod.rs`: retention_reclaim 任务每日执行 + next_retention 计算
- [x] `store.rs`: retention_check_hour 传递到 BackgroundTasks
- [x] `ffi.rs + timslite.h`: tmsl_dataset_create 新增 retention_window 与 timestamp_units_per_seconds 参数
- [x] 集成测试: legacy、wall-clock、strict boundary、monotonic floor 与 overflow 测试全部通过
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过
