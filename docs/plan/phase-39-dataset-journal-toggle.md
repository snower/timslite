# Phase 39: Dataset Journal Toggle

## 目标

在全局 `StoreConfig.enable_journal=true` 的情况下, 允许创建单个 dataset 时关闭该 dataset 的 journal 记录。默认保持 `true`, 以维持现有行为。

## 设计契约

- 新增 `DataSetConfig.enable_journal: bool`, 默认 `true`。
- 新增 `DataSetConfigBuilder::enable_journal(bool)`。
- 新增 dataset meta TLV `0x0A enable_journal`, `u8`, canonical 值只能是 `0` 或 `1`。
- 有效 journal 开关定义为 `StoreConfig.enable_journal && DataSetMeta.enable_journal`。
- 有效开关为 false 时, 该 dataset 的 `0x01/0x02/0x11/0x12/0x13` 均不写入 journal。
- dataset 开关不影响全局 `.journal/logs` 是否创建、其它 dataset 的 journal、普通 queue 或 cache 行为。

## 任务

- [x] 更新 `docs/design/journal.md`、`docs/design/meta-format.md`、`docs/design/store-and-ffi.md`、`docs/design/data-model.md`。
- [x] 更新 `docs/plan/overview.md` 和根 `plan.md`。
- [x] 添加 Rust config/meta/journal 行为测试。
- [x] 更新 `src/config.rs`、`src/meta.rs`、`src/dataset.rs`、`src/store.rs`。
- [x] 更新 FFI struct/header/config decode。
- [x] 更新 Python wrapper create_dataset kwargs 与测试。
- [x] 运行格式、测试和 diff hygiene 验证。

---

## 任务清单

> 以下为 `plan.md` 中 Phase 39 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — `docs/design/journal.md`, `docs/design/meta-format.md`, `docs/design/store-and-ffi.md`, `docs/design/data-model.md`
- [x] 计划文档 — `docs/plan/phase-39-dataset-journal-toggle.md`, `docs/plan/overview.md`, `plan.md`
- [x] 实现 — Rust config/meta/dataset/store, FFI/header, Python wrapper
- [x] 测试 — config/meta roundtrip, Store hook integration, FFI/Python create kwargs
- [x] 验证 — fmt, targeted tests, full cargo test, cargo check, wrapper pytest, diff hygiene
