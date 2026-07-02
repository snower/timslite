# Phase 20: 显式最新记录读取 (Explicit Latest Read)

> 目标: 新增获取数据集最新写入时间戳和最新记录的显式 API。当前 public 写入 timestamp 必须为非负 `i64`; 读/查接口的负值参数表示相对最新 timestamp 的偏移，`read(-1)` 解析为 `latest_written_timestamp`。

## 1. 设计概要

### 1.1 `DataSet::latest_written_timestamp(&self) -> Option<i64>`

返回内存中维护的最大已写入时间戳, 空数据集返回 `None`。删除 latest record 不回退该值。

### 1.2 `DataSet::read_latest()`

读取 `latest_written_timestamp` 对应记录。空 dataset、latest entry 已删除/为 filler/已过期时返回 `None`, 不回退到更早有效记录。

### 1.3 FFI 接口

```c
// 获取最新写入时间戳。返回 0=有值, 1=空 dataset, -1=错误。
int tmsl_dataset_latest_timestamp(void* ds, int64_t* out_ts, char* err_buf, size_t err_buf_len);

// 读取 latest record。返回 0=成功, 1=未找到/空, -1=错误。
int tmsl_dataset_read_latest(void* ds, int64_t* out_ts,
    unsigned char** out_data, size_t* out_data_len,
    char* err_buf, size_t err_buf_len);
```

## 2. 实现清单

- [x] `dataset.rs`: 新增 `DataSet::latest_written_timestamp(&self) -> Option<i64>`
- [x] `dataset.rs`: 新增 `DataSet::read_latest()`; `DataSet::read()` 对非负 timestamp 保持精确读取，负 timestamp 按 latest-relative offset 解析
- [x] `ffi.rs`: 新增 `tmsl_dataset_latest_timestamp(...)` FFI 函数
- [x] `ffi.rs`: 新增 `tmsl_dataset_read_latest(...)` FFI 函数
- [x] `ffi.rs`: 修复 `tmsl_dataset_read` 中 `out_ts` 写入 (原为硬编码输入值, 改为写入实际返回的时间戳)
- [x] `wrapper/cffi/include/timslite.h`: 新增 `tmsl_dataset_latest_timestamp`/`tmsl_dataset_read_latest` 声明; 更新 `tmsl_dataset_read` 注释
- [x] `wrapper/python/src/dataset.rs`: 新增 `latest_timestamp: Optional[int]` 只读属性 + `read_latest()` + 更新 `read()` docstring

## 3. 测试

### 单元测试
- `test_latest_written_timestamp_after_writes` — 写入后返回最新时间戳
- `test_latest_written_timestamp_after_reopen` — reopen 后保持一致
- `test_read_latest_empty_dataset_and_minus_one_offset` — 空数据集 latest 为 `None`, `read(-1)` 返回 `None`
- `test_non_negative_timestamps_and_read_latest` — 0 timestamp 可写可读, 负 timestamp 读参数按 latest-relative offset 解析, `read_latest()` 返回最大已写 timestamp 对应记录
- `test_read_latest_after_delete_latest` — 删除 latest 后 `read_latest()` 返回 `None`, latest timestamp 不回退
- `test_read_latest_after_reopen_and_minus_one_offset` — reopen 后 latest 恢复, `read(-1)` 解析为 latest

## 4. 验收

- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (130 unit + 25 integration = 155 tests)

## 5. 设计文档更新

- [x] `docs/design/dataset-operations.md` (§10.3 流程图重写 + §10.4)
- [x] `docs/design/store-and-ffi.md` (FFI 函数列表)

---

**相关**: [数据集操作](../design/dataset-operations.md) | [Store 与 FFI](../design/store-and-ffi.md)
