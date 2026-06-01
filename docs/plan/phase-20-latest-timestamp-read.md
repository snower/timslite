# Phase 20: 最新时间戳读取 (Latest Timestamp Read)

> 目标: 新增获取数据集最新写入时间戳的能力, 并在 `read(-1)` 时自动返回最新记录。

## 1. 设计概要

### 1.1 `DataSet::latest_written_timestamp(&self) -> i64`

返回内存中维护的最新写入时间戳, 空数据集返回 `0`。

### 1.2 `DataSet::read(-1)` 快捷路径

当 `timestamp == -1` 时, 解析为 `latest_written_timestamp`, 空数据集直接返回 `None`。

### 1.3 FFI 接口

```c
// 获取最新写入时间戳
int tmsl_dataset_latest_timestamp(void* ds, int64_t* out_ts, char* err_buf, size_t err_buf_len);
```

## 2. 实现清单

- [x] `dataset.rs`: 新增 `DataSet::latest_written_timestamp(&self) -> i64`
- [x] `dataset.rs`: 修改 `DataSet::read()` — `timestamp == -1` 时解析为 `latest_written_timestamp`
- [x] `ffi.rs`: 新增 `tmsl_dataset_latest_timestamp(...)` FFI 函数
- [x] `ffi.rs`: 修复 `tmsl_dataset_read` 中 `out_ts` 写入 (原为硬编码输入值, 改为写入实际返回的时间戳)
- [x] `include/timslite.h`: 新增 `tmsl_dataset_latest_timestamp` 声明; 更新 `tmsl_dataset_read` 注释
- [x] `wrapper/python/src/dataset.rs`: 新增 `latest_timestamp` 只读属性 + 更新 `read()` docstring

## 3. 测试

### 单元测试
- `test_latest_written_timestamp_after_writes` — 写入后返回最新时间戳
- `test_latest_written_timestamp_after_reopen` — reopen 后保持一致
- `test_read_minus_one_empty_dataset` — 空数据集 `read(-1)` 返回 `None`
- `test_read_minus_one_returns_latest` — `read(-1)` 返回最新记录
- `test_read_minus_one_after_delete_latest` — 删除最新记录后 `read(-1)` 行为
- `test_read_minus_one_after_reopen` — reopen 后 `read(-1)` 仍正确

## 4. 验收

- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (130 unit + 25 integration = 155 tests)

## 5. 设计文档更新

- [x] `docs/design/dataset-operations.md` (§10.3 流程图重写 + §10.4)
- [x] `docs/design/store-and-ffi.md` (FFI 函数列表)

---

**相关**: [数据集操作](../design/dataset-operations.md) | [Store 与 FFI](../design/store-and-ffi.md)
