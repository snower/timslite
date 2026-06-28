# Phase 7: FFI 接口

**目标**: C ABI 接口完整实现, errno-safe, panic-safe

> Current contract: this phase has been superseded by Phase 46. C ABI now lives in the independent `wrapper/cffi` crate (`timslitecffi`); the main `timslite` crate no longer contains `wrapper/cffi/src/lib.rs` or exports C ABI symbols.

---

## 7.1 wrapper/cffi/src/lib.rs — 核心工具

- `catch_unwind` 包装所有 FFI 函数体
- 错误缓冲区写入: `write_err(err_buf, err_len, message)`
- C 侧句柄类型: opaque store/dataset pointers and wrapper-owned queue/consumer numeric handles

## 7.2 FFI: Store 管理

- `tmsl_store_open(data_dir: *const c_char, err_buf: *mut c_char, err_len: size_t) -> *mut c_void`
- `tmsl_store_close(store: *mut c_void, err_buf: *mut c_char, err_len: size_t) -> c_int`

## 7.3 FFI: 数据集管理 — create/open/close/drop

- `tmsl_dataset_create(store, name, dataset_type, data_segment_size, index_segment_size, compress_level, err_buf, err_len) -> *mut c_void`
- `tmsl_dataset_open(store, name, dataset_type, err_buf, err_len) -> *mut c_void`
- `tmsl_dataset_close(dataset, err_buf, err_len) -> c_int`
- `tmsl_dataset_drop(dataset, store, err_buf, err_len) -> c_int`
- `tmsl_dataset_flush(dataset, err_buf, err_len) -> c_int`

## 7.4 FFI: 数据写入

- `tmsl_dataset_write(dataset, timestamp: i64, data: *const u8, data_len: size_t, err_buf, err_len) -> c_int`

## 7.5 FFI: 查询迭代器

- `tmsl_dataset_query(dataset, start_ts: i64, end_ts: i64, err_buf, err_len) -> *mut c_void` (返回迭代器)
- `tmsl_iter_next(iter, ts: *mut i64, data: *mut *mut u8, len: *mut size_t, err_buf, err_len) -> c_int`
- `tmsl_iter_free_data(data: *mut u8) -> void`
- `tmsl_iter_close(iter: *mut c_void) -> void`

## 7.6 头文件生成 (.h)

- 创建 `wrapper/cffi/include/timslite.h` C 头文件, 包含所有 FFI 声明
- 新增 `tmsl_dataset_create`, `tmsl_dataset_drop` 声明

## 验收标准

- [x] 编译: `cargo build --release` → 生成 `libtimslite.dll`/`.so`
- [x] C 程序链接测试: `wrapper/cffi/include/timslite.h` 包含所有 12 个函数声明
- [x] FFI 测试: `create` → write → query → close → open → query → verify (12 个 extern "C" 函数)
- [x] FFI 测试: `create` 已存在 → 返回 -1/null, err_buf 有错误信息
- [x] FFI 测试: `open` 不存在 → 返回 -1/null, err_buf 有错误信息
- [x] FFI 测试: `drop` 后重新 `create` → write → query → verify
- [x] 边界测试: nullptr 参数检查 (已在所有 FFI 函数中实现)
- [x] panic 测试: 所有 FFI 函数使用 `catch_unwind` 包裹

---

**导航**: [← Phase 6](phase-06-store-bg.md) | [→ Phase 8](phase-08-tests-perf.md)
