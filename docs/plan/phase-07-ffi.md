# Phase 7: FFI 接口

**目标**: C ABI 接口完整实现, errno-safe, panic-safe

---

## 7.1 ffi.rs — 核心工具

- `catch_unwind` 包装所有 FFI 函数体
- 错误缓冲区写入: `write_err(err_buf, err_len, message)`
- 句柄类型: `DataSetHandle`, `StoreHandle` 等 opaque pointers

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

- 创建 `include/timslite.h` C 头文件, 包含所有 FFI 声明
- 新增 `tmsl_dataset_create`, `tmsl_dataset_drop` 声明

## 验收标准

- [ ] 编译: `cargo build --release` → 生成 `libtimslite.dll`/`.so`
- [ ] C 程序链接测试: 编译 C 测试程序 → 链接 libtimslite → 运行
- [ ] FFI 测试: `create` → write × 100 → query → verify → `close` → `open` → query → verify (全部 FFI 调用)
- [ ] FFI 测试: `create` 已存在 → 返回 -1, err_buf 有错误信息
- [ ] FFI 测试: `open` 不存在 → 返回 -1, err_buf 有错误信息
- [ ] FFI 测试: `drop` 后重新 `create` → write → query → verify
- [ ] 边界测试: 空 data_dir, 长 name, nullptr 参数 → 返回 -1, err_buf 有错误信息
- [ ] panic 测试: 触发 panic → FFI 返回 -1, 不崩溃

---

**导航**: [← Phase 6](phase-06-store-bg.md) | [→ Phase 8](phase-08-tests-perf.md)
