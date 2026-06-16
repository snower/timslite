# Phase 37: Journal Record TV Format

> 目标: 缩小 journal record 体积, 将高频数据变更记录中的 `(name,type)` 字符串替换为 canonical dataset identifier, 并把 record 内部 framing 从 `log_type + outer length + TLV` 调整为 `log_type + TV list`。
>
> 状态: 完成。Journal record TV 格式、canonical identifier 编码、Store/DataSet journal hook 传递与测试已完成。

## 37.0 设计文档

- [x] [Journal 变更日志设计](../design/journal.md)
- [x] [Journal 专用存储设计](../design/journal-storage.md)
- [x] [Dataset Identifier](../design/dataset-identifier.md)
- [x] [Store 与 FFI 设计](../design/store-and-ffi.md)
- [x] [Meta 格式](../design/meta-format.md)

## 37.1 格式契约

- [x] 将 `JournalRecord` payload 从 `log_type + outer_length + TLV list` 改为 `log_type + TV list`。
- [x] `JournalRecord::decode()` 使用传入 payload slice 的长度作为记录边界, 不再读取 outer `length:u16`。
- [x] 实现 identifier canonical/minimal encoding: `0x01`/`0x02`/`0x03`/`0x04` 分别对应 `u8`/`u16`/`u32`/`u64`。
- [x] 拒绝 identifier `0`、非最短编码、缺失 identifier、重复 identifier 和截断 identifier value。
- [x] create/drop 记录保留 identifier、name、type、metadata。
- [x] write/delete/append 记录只保留 identifier, 不再重复 name/type 字符串。
- [x] 按 `log_type` 解析 `0x10`、`0x11`、`0x12` 等 schema-scoped field。
- [x] 已知 `log_type` 内遇到 schema 外 type 时返回 `InvalidData`, 不做未知字段跳过。
- [x] 所有多字节整数继续使用 Little Endian。

## 37.2 实现任务

- [x] 调整 `JournalRecord` 数据结构, 让所有记录携带 `dataset_identifier: u64`。
- [x] create/drop 记录继续携带 `name` 和 `dataset_type`; data write/delete/append 记录不再携带它们。
- [x] 更新 `JournalRecord::encode()` 输出 canonical field order: identifier first, then schema fields by type。
- [x] 更新 `JournalRecord::decode()` 按 `log_type` schema 解析 TV 字段。
- [x] 更新 create/drop 输入校验: identifier 非零、name/type 路径规则、metadata 可被 `u16` length 编码。
- [x] 更新 data write/delete/append 输入校验: identifier 非零、index_info/append_info 固定长度。
- [x] 更新 `JournalManager::append_*` 签名, 让 create/drop 传入 identifier + key, 数据变更只传入 identifier + entry payload。
- [x] 更新 `DataSetJournalSink` 和 Store/DataSet 调用点, 确保 Store 管理的 dataset 在 journal hook 前已有非零 identifier。
- [x] 更新 Rust/FFI/Python 层任何暴露 decoded journal record 的结构或测试辅助。
- [x] 保持低层 `DataSet::create/open` 绕过 Store 时 journal hook 为 no-op。

## 37.3 测试计划

- [x] identifier 编码边界: `1`, `255`, `256`, `65535`, `65536`, `u32::MAX`, `u32::MAX + 1`, `u64::MAX`。
- [x] identifier 反例: `0`, 非最短编码、重复字段、缺失字段、截断 value。
- [x] create/drop codec roundtrip: identifier + name + type + metadata。
- [x] write/delete codec roundtrip: identifier + `index_info`。
- [x] append codec roundtrip: identifier + `index_info` + `append_info`。
- [x] known `log_type` decode 遇到 schema 外 TV type 返回 `InvalidData`。
- [x] 字段顺序错误返回 `InvalidData`。
- [x] journal 集成测试确认 `0x11`/`0x12`/`0x13` encoded payload 不包含 name/type 字符串。
- [x] journal 集成测试确认 `0x01`/`0x02` decoded record 保留 identifier、name、type、metadata。
- [x] Store hook 测试确认数据变更记录使用 dataset 当前 identifier。

## 37.4 验证命令

```bash
cargo fmt -- --check
cargo test journal::record -- --test-threads=1
cargo test journal_test -- --test-threads=1
cargo test -- --test-threads=1
cargo check
git diff --check
```

---

## 任务清单

> 以下为 `plan.md` 中 Phase 37 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — `docs/design/journal.md`、`docs/design/journal-storage.md`、`docs/design/dataset-identifier.md`、`docs/design/store-and-ffi.md`、`docs/design/meta-format.md`
- [x] 实现 — canonical identifier TV 编码、log_type-scoped TV parser、JournalManager/DataSet hook 签名更新
- [x] 测试 — codec 边界/反例、Store hook 集成、数据变更记录不再包含 name/type
- [x] 验证 — `cargo fmt -- --check`, `cargo test journal::record -- --test-threads=1`, `cargo test -- --test-threads=1`, `cargo check`, `git diff --check`
