# Phase 40: Dataset Inspect State Cache

## 概述

优化 `DataSetInspectResult::state` 的统计来源，避免 `total_record_count`、`total_data_size`、`total_uncompressed_size`、`total_invalid_record_count` 只统计当前打开的数据分段。

新增 dataset 目录下与 `meta` 同级的 `state` 文件，保存已经归档的 data/index 分段统计缓存。普通 inspect 读取该缓存并叠加当前 active tail data segment 与 active index segment 的状态，从而返回整个 dataset 的统计信息，而不需要打开所有历史分段。

设计文档:

- [dataset-inspect.md](../design/dataset-inspect.md)
- [background-and-cache.md](../design/background-and-cache.md)
- [data-segment.md](../design/data-segment.md)
- [store-and-ffi.md](../design/store-and-ffi.md)

## 设计确认

- active tail data segment 定义为最高 `file_offset` 的数据分段，与 open/closed 状态无关。
- `state` 文件记录归档水位 `archived_until_offset`，表示已纳入归档统计的 data segment `file_offset` 排他上界。
- `state` 文件保存归档 data segment 的 `total_record_count`、`total_data_size`、`total_uncompressed_size`、`total_invalid_record_count`。
- `min_timestamp` / `max_timestamp` 不从 data segment 统计做加减推导，只在 index segment 新增/删除时维护，inspect 时叠加 active index segment 范围。
- `state` 文件是可重建的持久化缓存，不是数据正确性的唯一真源；普通读写流程不依赖它。
- dataset state 文件为低频 inspect 缓存，变更后由写入、删除或 retention reclaim 路径立即同步，不进入 dirty flush queue。
- 不新增普通 inspect 所需的“读取全部 data segment header stats” helper；后续若需要重建 state 文件，走单独维护流程。
- `DataSetState` 分段字段改为总数 + 打开数: `data_segments`、`open_data_segments`、`index_segments`、`open_index_segments`，不再返回关闭数。

## 文件格式

`{dataset_dir}/state` 为固定 64 bytes 二进制文件，所有多字节整数为 little-endian：

| Offset | 字段 | 类型 | 说明 |
|--------|------|------|------|
| 0 | magic | `[u8; 4]` | ASCII `DSSF` |
| 4 | version | `u32` | 当前为 `1` |
| 8 | archived_until_offset | `u64` | 已归档 data segment offset 排他上界 |
| 16 | min_timestamp | `i64` | 已归档 index segment 最小 timestamp |
| 24 | max_timestamp | `i64` | 已归档 index segment 最大 timestamp |
| 32 | total_record_count | `u64` | 已归档 data segment record 总数 |
| 40 | total_data_size | `u64` | 已归档 data segment 数据区已用字节数 |
| 48 | total_uncompressed_size | `u64` | 已归档 data segment 未压缩逻辑大小 |
| 56 | total_invalid_record_count | `u64` | 已归档 data segment 无效 record 总数 |

## 实现任务

- [x] 设计文档更新
  - [x] `docs/design/dataset-inspect.md`
  - [x] `docs/design/background-and-cache.md`
  - [x] `docs/design/data-segment.md`
  - [x] `docs/design/store-and-ffi.md`
- [x] 计划文档更新
  - [x] `docs/plan/phase-40-dataset-inspect-state.md`
  - [x] `docs/plan/overview.md`
  - [x] `plan.md`
- [x] Rust 实现
  - [x] 新增 dataset state file 类型与 open/create/snapshot/update/sync 逻辑
  - [x] DataSet create/open 初始化并持有 dataset state cache
  - [x] data segment rollover 时归档旧 active tail 统计
  - [x] index segment rollover 时归档旧 active index timestamp 范围
  - [x] retention 删除 data/index segment 时扣减或更新 state
  - [x] delete 命中已归档 data segment 时更新 `total_invalid_record_count`
  - [x] dataset state 文件变更后立即同步
  - [x] `DataSetState` 字段重命名为 `data_segments` / `index_segments`
- [x] FFI / C header 更新
  - [x] `TmslDataSetState` 字段改为 `data_segments` / `index_segments`
  - [x] `wrapper/cffi/include/timslite.h` 同步
- [x] Python wrapper 更新
  - [x] `DataSetState` PyClass 字段改为 `data_segments` / `index_segments`
  - [x] Python tests 同步断言
- [x] 测试
  - [x] 空 dataset state 文件初始化
  - [x] rollover 后归档统计 + active tail 统计
  - [x] inspect 不打开所有历史分段
  - [x] retention 删除归档 data/index segment 后 state 更新
  - [x] delete 命中归档 data segment 后 invalid count 更新
  - [x] dataset state 写入路径即时同步
- [x] 验证
  - [x] `cargo fmt -- --check`
  - [x] `cargo test -- --test-threads=1`
  - [x] `cargo check`
  - [x] `cargo clippy --all-targets -- -D warnings`
  - [x] `cargo test --manifest-path wrapper/python/Cargo.toml -- --test-threads=1`
  - [x] `maturin develop --manifest-path wrapper/python/Cargo.toml`
  - [x] `python -m pytest wrapper/python/tests -q`
  - [x] `git diff --check`

## 验收标准

- inspect 返回的 `total_*` 覆盖整个 dataset，而不是仅覆盖打开分段。
- inspect 返回 data/index 分段总数和打开数，不再返回关闭数。
- 普通 inspect 不打开全部历史 data/index segment。
- dataset state file 变更由当前写入、删除或 retention reclaim 路径立即同步。
- state file 异常不改变普通数据文件读写的正确性边界。

---

## 任务清单

> 以下为 `plan.md` 中 Phase 40 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — dataset state 文件、active tail 统计、即时同步语义、`DataSetState` 字段语义
- [x] 计划文档 — `docs/plan/phase-40-dataset-inspect-state.md`, `docs/plan/overview.md`, `plan.md`
- [x] 实现 — dataset state file、rollover/retention/delete 更新、即时同步
- [x] API 同步 — Rust/FFI/C header/Python 字段改为 `data_segments` / `index_segments`
- [x] 测试 — state file 初始化、归档统计、retention/delete 更新、flush queue、inspect 不全量打开分段
- [x] 验证 — fmt, cargo test, cargo check, clippy, wrapper pytest, diff hygiene
