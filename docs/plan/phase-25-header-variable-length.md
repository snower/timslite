# Phase 25: Header 可变长度

> 目标: 修复设计审查 P0-3。文件头保留 v1 默认 `DATA_HEADER_SIZE=116` / `INDEX_HEADER_SIZE=52`, 但打开和读写 segment 时按 `meta_length/state_length` 计算运行时 `header_size`, 消除 TLV/state 扩展与固定数据区起点之间的矛盾。

## 1. 设计概要

```text
header_len = 9 + meta_length + 2 + state_length
```

v1 默认值不变:
- `DATA_HEADER_SIZE = 116` (9 state 字段)
- `INDEX_HEADER_SIZE = 52` (1 state 字段)

打开和读写 segment 时按运行时 `header_len` 计算数据区起点。

## 2. 实现清单

- [x] `src/header.rs`: `DataFileMetadata` / `IndexFileMetadata` 读取动态 state 起点和 `header_size`, state 更新 helper 不再硬编码 42/44 偏移
- [x] `src/segment/data.rs`: `DataSegment` 运行时保存 `header_size`, Block 物理位置和 header state 更新均基于动态 header
- [x] `src/index/segment.rs`, `src/index/mod.rs`, `src/dataset.rs`: `IndexSegment` 与 TimeIndex 已有分段扫描、查询、恢复均基于动态 index header

## 3. 测试

- [x] 扩展 meta 后的 header 解析测试
- [x] DataSegment/IndexSegment 扩展 header reopen 读取测试
- [x] `cargo test -- --test-threads=1` 全部通过 (151 unit + 28 integration)

## 4. 验收

- [x] `cargo fmt -- --check` clean
- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (151 unit + 28 integration, 2 doctests ignored)

## 5. 设计文档更新

- [x] `docs/design/data-model.md`: 定义 `header_len`, 明确数据/索引区从运行时 `header_len` 开始
- [x] `docs/design/data-segment.md`, `docs/design/time-index.md`, `docs/design/index-continuous.md`, `docs/design/lazy-allocation.md`, `docs/design/memory-and-concurrency.md`, `docs/design/dataset-operations.md`: 将固定起点/容量公式调整为运行时 `header_len`

---

**相关**: [数据模型](../design/data-model.md) | [数据段管理](../design/data-segment.md) | [时间索引](../design/time-index.md)
