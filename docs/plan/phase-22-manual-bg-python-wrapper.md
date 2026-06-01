# Phase 22: Manual Background Execution Python Wrapper

> 目标: 为 Phase 21 的新 API 提供 Python FFI 绑定。

## 1. 实现清单

- [x] `wrapper/python/src/config.rs`: `PyStoreConfig::new()` 新增 `retention_check_hour` / `enable_background_thread` 参数 + getter
- [x] `wrapper/python/src/store.rs`: `PyStore::tick_background_tasks()` → 返回 `(executed: int, next_delay_ms: int)` 元组
- [x] `wrapper/python/src/store.rs`: `PyStore::next_background_delay()` → 返回 `int` (毫秒)
- [x] 支持通过 `StoreConfig(enable_background_thread=False, ...)` 配置构造

## 2. 测试

- [x] `tests/test_store_manual_bg.py`: 验证 enable=False + tick 触发 flush + next_delay 返回
- [x] `tests/test_store_manual_bg.py`: 验证 tick 返回值结构正确

## 3. 文档

- [x] `wrapper/python/README.md`: 更新使用示例, 演示手动后台模式

## 4. 验收

- [x] `cargo clippy --lib -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo build --lib` 编译通过

---

**相关**: [Phase 21](phase-21-manual-bg-execution.md)
