# Phase 16: 数据保留 (Retention) — 有效期回收 + 查询约束

> **目标**: 为每个数据集添加 `retention_ms` 不可变配置 (数据有效期), Store 级可配置每日回收时间点, 后台线程执行回收任务删除过期分段文件, 查询自动钳制到有效时间范围内。

## 1. 背景与动机

### 1.1 当前问题

时序数据积累会导致磁盘空间持续增长, 缺乏自动清理机制。用户需要:

- **按时间维度回收**: 旧数据超过一定期限后自动删除, 释放磁盘空间
- **可控调度**: 回收操作不应在高峰期运行, 应可指定每日执行时间
- **查询正确性**: 查询结果不应包含已过期但尚未回收的数据

### 1.2 设计方案

| 维度 | 设计决策 |
|------|---------|
| retention 存储 | 数据集 meta 文件新增 TLV `0x08: retention_ms` (u64 LE, 0=不限) |
| 回收调度 | StoreConfig 新增 `retention_check_hour` (u8, 0-23, 默认 0=午夜) |
| 回收基准 | `latest_written_timestamp.saturating_sub(retention_ms)` |
| 回收粒度 | 整个分段文件 (数据段/索引段), 不拆分 block |
| 查询约束 | `query_iter()` 自动钳制 `start_ts = max(start_ts, expiration_threshold)` |
| 锁策略 | 回收前 close() dataset, 回收期间不保持 mmap 打开 |

## 2. 改动清单

### 2.1 `src/meta.rs` — DataSetMeta

**新增字段**:
- `pub retention_ms: u64` — 数据有效期 (与 timestamp 同单位, 0=不限)

**新增常量**:
- `const META_RETENTION_MS: u8 = 0x08;` — TLV type code

**变更**:
- `DataSetMeta::new()`: 新增 `retention_ms: u64` 参数, 默认 0
- `DataSetMeta::to_bytes()`: 序列化新增 TLV entry (retention_ms, u64 LE, 11 bytes: 1+2+8)
- `DataSetMeta::from_bytes()`: 解析 TLV `0x08`, 缺失时默认 0 (向前兼容)

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
pub retention_ms: u64,  // 从 meta 传递, 不参与 builder (不可变配置)
```

**DataSetConfigBuilder 新增方法**:
```rust
pub fn retention_ms(mut self, ms: u64) -> Self
```

### 2.3 `src/dataset.rs` — DataSet

**新增字段**:
- `retention_ms: u64` — 从 meta 读取或 create 时传入

**DataSet::create()**:
- 新增参数 `retention_ms: u64`
- 写入 meta 时包含 retention_ms

**DataSet::open()**:
- 从 meta 读取 retention_ms
- 存入 DataSet.retention_ms

**DataSet::query_iter()**:
```rust
pub fn query_iter(...) {
    let mut start_ts = start_ts;
    if self.retention_ms > 0 && self.latest_written_timestamp > 0 {
        let threshold = self.latest_written_timestamp.saturating_sub(self.retention_ms);
        if start_ts < threshold {
            start_ts = threshold;
        }
        if start_ts > end_ts {
            return Ok(QueryIterator::empty(...));  // 完全过期
        }
    }
    // ... original query logic with adjusted start_ts
}
```

**DataSet::reclaim_expired_segments() (新增)**:
```rust
pub fn reclaim_expired_segments(&mut self) -> Result<usize> {
    if self.retention_ms == 0 { return Ok(0); }
    if self.latest_written_timestamp == 0 { return Ok(0); }

    let threshold = self.latest_written_timestamp.saturating_sub(self.retention_ms);

    // 1. Close dataset (flush + all segments → closed)
    self.close()?;
    // close() already called flush + idle_close_all

    // 2. Reclaim index segments
    let idx_reclaimed = self.time_index.reclaim_expired_segments(
        threshold, self.config.index_segment_size
    )?;

    // 3. Reclaim data segments
    let data_reclaimed = self.segments.reclaim_expired_segments(threshold)?;

    self.last_used_at = Instant::now();
    Ok(idx_reclaimed + data_reclaimed)
}
```

**DataSet::retention_ms() (新增 getter)**:
```rust
pub fn retention_ms(&self) -> u64 { self.retention_ms }
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

- 使用 closed_segments 中缓存的 `max_timestamp`, 无需打开文件
- `retain()` 同时完成筛选和删除

### 2.5 `src/index/mod.rs` — TimeIndex

**TimeIndex::reclaim_expired_segments() (新增)**:
```rust
pub fn reclaim_expired_segments(
    &mut self, threshold: i64, max_file_size: u64,
) -> Result<usize> {
    let mut reclaimed = 0;
    let before_len = self.closed_index_segments.len();
    self.closed_index_segments.retain(|meta| {
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
    reclaimed = before_len - self.closed_index_segments.len();
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
    if file_len < INDEX_HEADER_SIZE { return Err(...); }

    // read-only mmap (Windows safe)
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let wrote_pos = read_u64_from_mmap(&mmap, 44);
    let wrote_count = (wrote_pos.saturating_sub(INDEX_HEADER_SIZE)) / INDEX_ENTRY_SIZE as u64;

    let result = if wrote_count == 0 {
        Err(TmslError::InvalidData("empty index segment".into()))
    } else {
        let last_offset = INDEX_HEADER_SIZE as usize
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
    // 1. 读锁: 收集 retention_ms > 0 的 dataset keys
    let retention_datasets: Vec<(DataSetKey, u64)> = {
        let guard = datasets.read().unwrap();
        guard.iter()
            .filter_map(|(k, ds_arc)| {
                let ds = ds_arc.lock().ok()?;
                if ds.retention_ms() > 0 { Some((k.clone(), ds.retention_ms())) }
                else { None }
            })
            .collect()
    };

    // 2. 逐个回收
    for (key, _retention_ms) in retention_datasets {
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
- create_dataset_with_config() 传递 `config.retention_ms` (从 DataSetConfigBuilder)

**Store::create_dataset()**:
- 向后兼容: retention_ms = 0 (默认不保留)

**Store::create_dataset_with_config()**:
- 从 DataSetConfig 提取 retention_ms 传递到 DataSet::create()

### 2.9 `src/ffi.rs` — FFI

**tmsl_dataset_create()**:
- 新增参数 `retention_ms: u64`
- 传递到 `store.create_dataset()` 或通过 DataSetConfigBuilder

**向后兼容**:
- `tmsl_store_open()` 使用 StoreConfig::default() (retention_check_hour=0)
- 旧 FFI 调用者需要适配新的 retention_ms 参数

### 2.10 `include/timslite.h` — C 头文件

**更新函数声明**:
```c
void* tmsl_dataset_create(
    void* store, const char* name, const char* dataset_type,
    uint64_t data_segment_size, uint64_t index_segment_size,
    unsigned char compress_level, unsigned char index_continuous,
    uint64_t retention_ms,
    char* err_buf, size_t err_buf_len);
```

## 3. 测试计划

### 3.1 单元测试

| 测试 | 文件 | 描述 |
|------|------|------|
| `test_meta_retention_ms_roundtrip` | meta.rs | retention_ms 序列化/反序列化 |
| `test_meta_retention_ms_default_zero` | meta.rs | 缺失 retention_ms TLV 时默认 0 |
| `test_config_retention_check_hour` | config.rs | StoreConfig builder 设置 retention_check_hour |
| `test_config_retention_ms` | config.rs | DataSetConfigBuilder 设置 retention_ms |
| `test_dataset_retention_ms_stored` | dataset.rs | create → open → 验证 retention_ms 一致 |
| `test_next_retention_time` | bg/mod.rs | 计算下次回收时间正确性 |

### 3.2 集成测试

| 测试 | 描述 |
|------|------|
| `t16_1_retention_no_reclaim_when_zero` | retention_ms=0 → 不回收 (即使过期) |
| `t16_2_retention_reclaim_basic` | 写入数据 → close → reopen → reclaim → 验证文件被删除 |
| `t16_3_retention_query_clamped` | retention 生效 → query 范围被自动钳制 |
| `t16_4_retention_reclaim_partial` | 多分段, 部分过期 → 只删除过期的 |
| `t16_5_retention_reclaim_index_and_data` | 索引+数据成对回收 |
| `t16_6_retention_backward_compat` | 旧格式 meta (无 retention_ms) → open 默认 0 |

### 3.3 验证清单

- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过
- [x] 旧 FFI 调用适配

## 4. 实施顺序

| Step | 描述 | 依赖 |
|------|------|------|
| 1 | meta.rs: retention_ms TLV + 序列化 | — |
| 2 | config.rs: retention_check_hour + DataSetConfig.retention_ms | Step 1 |
| 3 | dataset.rs: retention_ms 字段 + create/open 传递 + query 钳制 | Step 1, 2 |
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
| retention_ms 单位不一致 | 过早/过晚回收 | 文档明确: 单位与 timestamp 一致 |
| 连续模式 back-fill 与回收冲突 | 找不到 filler | 回收后 back-fill 返回 NotFound (预期行为) |
| 回收文件时 Windows 文件锁定 | 删除失败 | read-only mmap + 立即 drop 后 remove |

## 6. 验收标准

- [ ] `meta.rs`: retention_ms TLV 完整序列化/反序列化, 缺失时默认 0
- [ ] `config.rs`: StoreConfig.retention_check_hour + DataSetConfigBuilder.retention_ms
- [ ] `dataset.rs`: retention_ms 持久化 + query 钳制 + reclaim_expired_segments
- [ ] `segment/mod.rs`: DataSegmentSet.reclaim_expired_segments (max_timestamp 判断)
- [ ] `index/mod.rs`: TimeIndex.reclaim_expired_segments (last_entry_timestamp 判断)
- [ ] `index/segment.rs`: last_entry_timestamp() 读取后立即释放 mmap+file
- [ ] `bg/mod.rs`: retention_reclaim 任务每日执行 + next_retention 计算
- [ ] `store.rs`: retention_check_hour 传递到 BackgroundTasks
- [ ] `ffi.rs + timslite.h`: tmsl_dataset_create 新增 retention_ms 参数
- [ ] 集成测试: 6 个新测试全部通过
- [ ] `cargo clippy -- -D warnings` clean
- [ ] `cargo test -- --test-threads=1` 全部通过
