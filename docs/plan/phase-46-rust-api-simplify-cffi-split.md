# Phase 46: Rust API 简化与 C ABI Wrapper 拆分

## 目标

将主 `timslite` crate 调整为标准 Rust library, 移除主项目内 C FFI 设计实现, 并把 C ABI 迁移到独立 `wrapper/cffi` crate (`timslitecffi`)。

本阶段不考虑兼容性, 以简化 Rust public API、降低主库资源消耗、减少后续维护成本为优先。

## 已完成

- [x] 主 `Cargo.toml` 移除 `cdylib` crate type 和主库 `libc` 依赖。
- [x] 主 crate 删除 `wrapper/cffi/src/lib.rs`, `src/lib.rs` 不再编译或导出 C ABI。
- [x] `Store::create_dataset*`, `Store::open_dataset*`, `Store::open_dataset_by_identifier` 直接返回 `DataSet`。
- [x] 移除 public `DataSetHandle` 和 Store handle registry。
- [x] 移除 Store 上与 Store 职责无关的 record/queue facade API: `write_dataset`, `append_dataset`, `delete_dataset_record`, `read_dataset`, `query_dataset`, `open_queue`, `queue_push`, `queue_poll`, `queue_ack` 等。
- [x] 普通 record API 统一通过 `DataSet::{write, write_now, append, append_now, delete, read, read_latest, query, ...}` 调用。
- [x] 普通 queue API 统一通过 `DataSet::open_queue()`、`DatasetQueue`、`DatasetQueueConsumer` 调用。
- [x] 新增 `wrapper/cffi` 独立 crate, package name 为 `timslitecffi`。
- [x] C header 迁移到 `wrapper/cffi/include/timslite.h`。
- [x] `timslitecffi` 只依赖 `timslite` 的公开 Rust API, 不访问 crate-private 模块。
- [x] 更新 Python wrapper, 改为直接持有 `Arc<DataSet>`。
- [x] 更新 Node.js wrapper, 改为直接持有 `Arc<DataSet>`。
- [x] 更新 Java native wrapper, 改为直接持有 `Arc<DataSet>`。
- [x] 主 Rust tests/benchmarks 改为直接 `DataSet` API。
- [x] 新增 `tests/rust_public_api_test.rs` 覆盖直接 `DataSet` 返回和 queue open。
- [x] `tests/rust_public_api_test.rs` 覆盖 `DataSet::write_now(data)` / `DataSet::append_now(data)` 的 Unix 秒级时间戳 public API。
- [x] 新增 `wrapper/cffi/tests/cffi_api_test.rs` 覆盖 C ABI store/dataset/read/queue roundtrip。
- [x] 更新 README、design.md、Store/C ABI、Queue、Dataset 操作、Dataset Identifier、Query Iterator 等当前设计文档。

## 后续待办

- [ ] 独立 C 链接测试: 使用 C 编译器链接 `timslitecffi` 动态库并调用完整 C ABI 流程。
- [ ] 性能基准补充: 对比简化后的 Rust direct DataSet API 和旧 Store facade 路径的调用开销。

## 验证命令

```bash
cargo check --all-targets
cargo test --test rust_public_api_test -- --test-threads=1
cargo check --manifest-path wrapper/cffi/Cargo.toml --all-targets --offline
cargo test --manifest-path wrapper/cffi/Cargo.toml --offline -- --test-threads=1
cargo check --manifest-path wrapper/python/Cargo.toml --all-targets --offline
cargo check --manifest-path wrapper/nodejs/Cargo.toml --all-targets --offline
cargo check --manifest-path wrapper/java/native/Cargo.toml --all-targets --offline
```

