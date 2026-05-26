# Phase 9: 读缓存池 (BlockCache)

**目标**: 全局读缓存池, LRU + idle 回收, 解压后 block payload 缓存

---

## 9.1 src/cache.rs — BlockCache 结构定义

```rust
pub struct BlockCache {
    entries: HashMap<CacheKey, CacheEntry>,
    used_memory: usize,
    max_memory: usize,
    hits: u64,
    misses: u64,
}

struct CacheEntry {
    data: Vec<u8>,         // 解压后的 block payload
    last_accessed: Instant,
    size: usize,
}

struct CacheKey {
    dataset_key: DataSetKey,
    block_offset: u64,
}
```

## 9.2 BlockCache::get — 缓存查询

- 根据 (dataset_key, block_offset) 查询缓存
- 命中: 更新 last_accessed, 返回数据副本, hits++
- 未命中: misses++, 返回 None

## 9.3 BlockCache::put — 缓存写入 + LRU 淘汰

- 插入新 entry, 更新 used_memory
- 如果 used_memory > max_memory × 85% → 淘汰最少使用的 entry
- `cache_max_memory=0` → 全部跳过 (缓存禁用)

## 9.4 BlockCache::evict_idle — 后台回收

- 扫描所有 entry, 移除 last_accessed ≥ idle_timeout 的条目
- 由后台任务循环定期调用 (每 60s)

## 9.5 读取流程集成

- DataSet::query 读取 block 前, 先查 BlockCache
- 缓存命中 → 直接使用, 跳过 mmap 读取 + 解压
- 缓存未命中 → mmap 读取 + 解压 → 写入缓存 → 使用数据

## 9.6 后台线程集成

- bg/mod.rs idle check 循环中调用 `cache.evict_idle()`
- 与 flush + idle segment close 在同一线程顺序执行

## 9.7 StoreConfig 扩展

- 新增 `cache_max_memory: usize` (默认 256MB)
- 新增 `cache_idle_timeout: Duration` (默认 30 分钟)

## 验收标准

- [x] 单元测试: put/get roundtrip, 命中/未命中计数
- [x] 单元测试: LRU 淘汰 → used_memory ≤ max_memory × 85%
- [x] 单元测试: idle 回收 → 30min 后条目被移除
- [x] 单元测试: `cache_max_memory=0` → put/get 无效果
- [x] 集成测试: 所有 74 tests pass

---

**导航**: [← Phase 8](phase-08-tests-perf.md) | [→ Phase 10](phase-10-continuous-storage.md)
