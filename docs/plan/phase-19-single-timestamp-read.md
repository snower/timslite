# Phase 19: 单时间戳读取 (Single Timestamp Read)

> 目标: 新增 `DataSet::read(timestamp)` 方法, 允许通过时间戳精确读取单条记录的数据, 并暴露 FFI 接口供 Python 等语言调用。

## 1. 设计概要

### 1.1 `DataSet::read(timestamp) -> Result<Option<(i64, Vec<u8>)>>`

- `timestamp` 是精确 signed `i64` 业务时间戳; `-1` 是普通 timestamp, latest 读取由 Phase 20 的 `read_latest()` 负责
- 通过 `time_index.find_entry(timestamp)` 三级搜索定位索引条目
- 过滤 FILLER 哨兵条目 (连续模式下填充的无效条目)
- 通过 `segments.read_at_index_with_hot_cache()` 读取实际数据
- 删除后的条目 (哨兵) 返回 `None`

### 1.2 FFI 接口

```c
// 读取指定时间戳的单条记录
// 返回: 0=成功找到, 1=未找到, -1=错误
// out_data 由 libc::malloc 分配, 必须通过 tmsl_iter_free_data 释放
int tmsl_dataset_read(void* ds, int64_t timestamp, int64_t* out_ts, uint8_t** out_data, size_t* out_data_len, char* err_buf, size_t err_buf_len);
```

- `out_ts` 写入实际返回的时间戳 (非硬编码输入值)
- 内存所有权: C 侧获取的 buffer 必须通过 FFI 函数释放

## 2. 实现清单

- [x] `dataset.rs`: 新增 `DataSet::read(timestamp, cache) -> Result<Option<(i64, Vec<u8>)>>`
- [x] `ffi.rs`: 新增 `tmsl_dataset_read(...)` FFI 函数, 返回码 0/1/-1
- [x] `wrapper/cffi/include/timslite.h`: 新增 `tmsl_dataset_read` C 函数声明 + doxygen 注释
- [x] 内存所有权: `out_data` 由 `libc::malloc` 分配, 复用 `tmsl_iter_free_data` 释放路径

## 3. 测试

### 单元测试
- `test_read_found` — 基本读取, 返回正确时间和数据
- `test_read_not_found` — 不存在时间戳, 返回 `None`
- `test_read_deleted_returns_none` — 删除后的条目返回 `None`
- `test_read_continuous_filler_returns_none` — 连续模式 filler 条目返回 `None`
- `test_read_after_reopen` — reopen 后仍可正确读取

## 4. 验收

- [x] `cargo clippy --tests -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过

## 5. 设计文档更新

- [x] `docs/design/dataset-operations.md` (signature block)
- [x] `docs/design/store-and-ffi.md` (FFI 函数列表 + 内存所有权说明)

---

**相关**: [数据集操作](../design/dataset-operations.md) | [Store 与 FFI](../design/store-and-ffi.md)
