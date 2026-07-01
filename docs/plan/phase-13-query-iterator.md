# Phase 13: 查询迭代器 + HotBlockCache

**目标**: 将 query 从"全量加载→内存排序→返回 Vec"改为惰性虚拟迭代, 并在读取循环中保持最后解压的 Block 数据, 大幅降低内存占用并加速顺序读取

**依赖**: Phase 4 (时间索引), Phase 9 (BlockCache), Phase 11 (O(1) 查询优化)

---

## 13.1 src/query/iter.rs — QueryIterator 核心结构

```rust
/// 虚拟查询迭代器 — 惰性遍历时间范围内的 record
pub struct QueryIterator<'a> {
    // 数据集引用
    dataset: &'a mut DataSet,
    cache: Option<&'a BlockCache>,
    query_range: (i64, i64),      // (start_ts, end_ts)

    // ── 惰性索引遍历状态 ──
    sources: Vec<QueryDataSource>,   // 数据来源列表
    current_source: usize,            // 当前 source 索引

    // ── Block Hot Cache (读取循环级局部缓存) ──
    hot_block: HotBlockCache,
}

/// 惰性索引读取器 trait (供 QueryIterator 内部使用)
trait QueryDataSource {
    /// 返回下一个非 filler 的 IndexEntry, 或 None
    fn next_entry(&mut self) -> Option<IndexEntry>;
}
```

### 实现细节:

- `QueryIterator::new()` 调用 `TimeIndex::prepare_query()` 建立 sources 列表
- `QueryIterator::next()` 遍历当前 source, 跳过 filler, 调用 `read_record_with_hot_cache()`
- 按时间顺序 yield (sources 已按时间排序, source 内部 entries 天然有序)
- `QueryIterator` 实现 `Iterator<Item = Result<(i64, Vec<u8>)>>`

## 13.2 src/query/hot_block.rs — HotBlockCache 结构

```rust
/// 读取循环级局部 Block 缓存 (无锁)
pub struct HotBlockCache {
    current_key: Option<CacheKey>,     // (segment_offset, block_offset)
    current_data: Vec<u8>,              // 解压后的完整 block payload
    payload_size: usize,                // payload 总大小
}
```

### 方法:

| 方法 | 行为 |
|------|------|
| `is_hit(seg_offset, block_offset)` | 判断是否在已缓存的 block 中 |
| `extract_record(in_block_offset)` | 从缓存 payload 提取 `(timestamp, data)` |
| `fill(key, data, payload_size)` | 存入新的 block 数据 |
| `clear()` | 清空缓存 (source 切换时调用) |

## 13.3 src/index/mod.rs — TimeIndex::prepare_query 新增

```rust
impl TimeIndex {
    /// 准备查询: 返回按时间顺序排列的查询源列表
    /// 不加载实际 entry 数据, 只建立 source 映射
    pub fn prepare_query(
        &mut self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<QueryDataSource>> {
        // 1. 内存 buffer (按时间范围过滤)
        // 2. 打开的 segments (query_range_indices 获取索引范围)
        // 3. 关闭的 segments (范围预过滤, 不打开文件)
        // 返回已排序的 sources
    }
}
```

## 13.4 src/index/segment.rs — IndexSegment 范围查询

```rust
impl IndexSegment {
    /// 返回 [start_ts, end_ts] 范围内的 entry 索引范围 [start_idx, end_idx)
    /// 连续模式: O(1) 直接计算
    /// 非连续模式: 两次二分查找 (lower_bound + upper_bound)
    pub fn query_range_indices(
        &self,
        start_ts: i64, end_ts: i64,
        index_continuous: bool,
    ) -> Option<(usize, usize)> {
        let start_idx = self.lower_bound_cs(start_ts, index_continuous);
        let end_idx = self.upper_bound_cs(end_ts, index_continuous);
        if start_idx >= end_idx {
            None
        } else {
            Some((start_idx, end_idx))
        }
    }
}
```

## 13.5 src/segment/data.rs — DataSegment::read_at_index 增强

在现有 `read_at_index` 方法基础上, 新增支持传入 HotBlockCache 的版本:

```rust
impl DataSegment {
    /// 读取 record, 支持 HotBlockCache
    pub fn read_at_index_with_hot_cache(
        &self,
        entry: &ReadIndexEntry,
        hot_block: &mut HotBlockCache,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        // 1. 检查 HotBlockCache.is_hit()
        //    ├─ Hit → hot_block.extract_record() → return
        //    └─ Miss → 继续
        // 2. 检查全局 BlockCache.get()
        //    ├─ Hit → hot_block.fill() → hot_block.extract_record() → return
        //    └─ Miss → 继续
        // 3. mmap 读取 + 解压
        //    → hot_block.fill()
        //    → (可选) BlockCache.put()
        //    → 提取 record → return
    }
}
```

## 13.6 src/dataset.rs — DataSet::query_iter + query 适配

```rust
impl DataSet {
    /// 返回虚拟迭代器 (惰性查询)
    pub fn query_iter(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<&BlockCache>,
    ) -> Result<QueryIterator<'_>> {
        QueryIterator::new(self, start_ts, end_ts, cache)
    }
}
```

**DataSet::query 适配 (向后兼容):**

```rust
pub fn query(
    &mut self,
    start_ts: i64,
    end_ts: i64,
    cache: Option<&BlockCache>,
) -> Result<Vec<(i64, Vec<u8>)>> {
    let iter = self.query_iter(start_ts, end_ts, cache)?;
    iter.collect()  // Iterator 已保证时间顺序, 无需再 sort
}
```

## 13.7 wrapper/cffi/src/lib.rs — FfiIterator 重构

```rust
// 旧: 持有全量 Vec<(i64, Vec<u8>)>
// struct FfiIterator { entries: Vec<...>, index: usize }

// 新: 持有内部查询迭代器
struct FfiIterator {
    rows: Vec<(i64, Vec<u8>)>,
    position: usize,
}

// Rust public QueryLengthIterator 仍可保持 lazy;
// wrapper/cffi data iterator 通过 public DataSet::query() 收集结果。
struct QueryIteratorWrapper {
    ds: Arc<DataSet>,
    cache: Option<*const BlockCache>,
    internal: QueryIteratorState,  // 非生命周期绑定版本
    current_record: Option<(i64, Vec<u8>)>,  // FFI 返回数据的临时持有
}
```

### FFI 函数内部变更:

| 函数 | 变更 |
|------|------|
| `tmsl_dataset_query` | 创建 `QueryIteratorWrapper` 而非全量 `Vec` |
| `tmsl_iter_next` | 驱动 `QueryIteratorWrapper::next()` 而非数组索引 |
| `tmsl_iter_close` | 清理 `QueryIteratorWrapper` 资源 |
| `tmsl_iter_free_data` | 无需变更 (仍释放 malloc 数据) |

## 13.8 src/lib.rs — 模块导出

```rust
mod query;  // 新增模块
pub use query::iter::QueryIterator;
pub use query::hot_block::HotBlockCache;
```

## 验收标准

- [ ] 单元测试: HotBlockCache hit/miss/extract 正确性
- [ ] 单元测试: QueryIterator 惰性遍历 (next 不提前加载)
- [ ] 单元测试: filler entries 正确跳过
- [ ] 单元测试: 数据源切换 (in_memory → open_segment → closed_segment)
- [ ] 单元测试: query() 向后兼容 → 结果正确且时间有序
- [ ] 单元测试: 空范围查询 → Iterator 立即返回 None
- [ ] 集成测试: t13_1_iterator_small_range (查询 10 条, 验证逐条返回)
- [ ] 集成测试: t13_2_iterator_large_range (查询 1000 条, 验证内存占用 < 1MB)
- [ ] 集成测试: t13_3_hot_cache_hit_rate (同 block 连续读 50 条, 验证缓存命中率)
- [ ] 集成测试: t13_4_ffl_iterator_api (FFI query + iter_next 正确性)
- [ ] 集成测试: t13_5_query_backward_compat (旧 API 结果与新 API 一致)
- [ ] `cargo clippy -- -D warnings` clean
- [ ] `cargo test -- --test-threads=1` 全部通过 (新增至少 5 项测试)

## 13.9 迭代器链式控制扩展

已完成的 dataset-managed iterator 模型继续作为边界, 在其上增加 Rust public iterator 控制 API:

- [x] `IndexQueryIterator` 支持正向和反向推进, 反向从更大 timestamp 向更小 timestamp 读取。
- [x] `IndexQueryIterator` 支持索引层 `skip(n)`, 只按非 filler entry 计数, 不读取被跳过记录的数据段。
- [x] `QueryIterator` 支持 `reverse()`、优化 `skip(n)`、`collect_take(n)` 链式调用。
- [x] `QueryLengthIterator` 支持 `reverse()`、优化 `skip(n)`、`collect_take(n)` 链式调用。
- [x] 标准 iterator adapter 语义保持自然分流: 原始 `QueryIterator.skip(n)` 走索引层优化; `QueryIterator.map(...).skip(n)` 等 adapter 链走标准库 `Iterator::skip()`。
- [x] 回归测试覆盖正向 skip、反向 skip、`skip().reverse()`、`reverse().skip()`、`collect_take()`、continuous sparse filler 和 deleted/filler entry。

---

**导航**: [← Phase 12](phase-12-lazy-allocation.md)
