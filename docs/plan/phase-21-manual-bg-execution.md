# Phase 21: 后台任务手动执行 (Manual Background Execution)

> 目标: 让调用方能够选择性禁用内置后台线程, 并通过主动 API 驱动后台任务 (flush / idle-close / cache-eviction / retention-reclaim); 同时保证即使启用了后台线程, 外部也可安全调用相同 API。

## 1. 设计概要

### 1.1 统一执行器

抽取调度状态到 `ExecutorState { last_flush, last_idle_check, last_cache_eviction, next_retention }`, 放入 `Arc<Mutex<ExecutorState>>`。`BackgroundTasks::start` 改为持有共享 `state`, 线程 loop 与 `tick` 使用相同执行路径。

### 1.2 `TickResult`

```rust
pub struct TickResult {
    pub executed_tasks: usize,
    pub next_delay: Duration,
}
```

### 1.3 Store API

- `Store::tick_background_tasks() -> Result<TickResult>` — 同步执行一次到期任务检查
- `Store::next_background_delay() -> Result<Duration>` — 仅计算下一次任务到期延迟, 不执行

### 1.4 FFI 接口

```c
int tmsl_store_tick_background_tasks(void* store, int* out_executed, int64_t* out_next_delay_ms, char* err_buf, size_t err_buf_len);
int tmsl_store_next_background_delay(void* store, int64_t* out_next_delay_ms, char* err_buf, size_t err_buf_len);
```

### 1.5 并发安全

`executor.state: Mutex<ExecutorState>` 保证后台线程与外部 `tick` 互斥串行, 无死锁风险 (锁顺序: state → datasets → DataSet)。

## 2. 实现清单

- [x] `config.rs`: `StoreConfig` 新增 `enable_background_thread: bool` (默认 `true`) + builder 方法
- [x] `bg/mod.rs`: `ExecutorState` + `Arc<Mutex<ExecutorState>>` 共享状态
- [x] `bg/mod.rs`: `BackgroundTasks::tick() -> TickResult` 统一执行路径
- [x] `bg/mod.rs`: `BackgroundTasks::new()` 支持无线程模式
- [x] `bg/mod.rs`: `TickResult` 返回类型
- [x] `store.rs`: `Store::open` 按配置启用/禁用线程
- [x] `store.rs`: `Store::tick_background_tasks()` + `Store::next_background_delay()`
- [x] `ffi.rs`: 新增两个 FFI 函数
- [x] `wrapper/cffi/include/timslite.h`: 新增函数声明 + doxygen 注释

## 3. 测试

### 单元测试
- `test_tick_bg_disabled_mode`
- `test_tick_bg_returns_next_delay`
- `test_tick_bg_respects_interval`
- `test_tick_bg_all_four_tasks_due`
- `test_next_delay_no_side_effects`
- `test_thread_enabled_external_tick_safe`
- `test_concurrent_external_ticks_serialized`
- `test_next_delay_during_tick`
- `test_enable_background_thread_default_true`
- `test_thread_disabled_close_safe`

### 集成测试
- `t21_1_manual_bg_lifecycle` — disabled thread → write → tick → verify flush
- `t21_2_manual_bg_next_delay_consistency` — next_delay 与 flush_interval 一致
- `t21_3_manual_bg_concurrent_with_thread` — 启用线程 + 外部 tick 并发, 无数据损坏

## 4. 验收

- [x] `cargo clippy --all-targets -- -D warnings` clean
- [x] `cargo fmt -- --check` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (131 unit + 25 integration = 156 tests)

## 5. 设计文档更新

- [x] `docs/design/background-and-cache.md` (§17.9-17.11)
- [x] `docs/design/store-and-ffi.md` (§11.4-11.5)
- [x] `design.md` 后台任务条目描述更新

---

**相关**: [后台任务与缓存](../design/background-and-cache.md) | [Store 与 FFI](../design/store-and-ffi.md)
