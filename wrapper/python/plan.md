# timslite Python Wrapper — 开发计划

> 基于 `design.md` 详细设计  
> 目标: 通过 PyO3 + maturin 为 timslite 构建 Python 绑定, 包名 `timslite` (PyPI)

---

## 计划状态总览

| Phase | 描述 | 状态 | 详情 |
|-------|------|------|------|
| PY-0 | Rust 层前置修复 (`Store::close()` 线程泄漏) | ✅ 完成 | [src/store.rs](../../src/store.rs) `bg.stop()` 替代 `drop(bg)` |
| PY-1 | 项目骨架 + 构建系统 | ✅ 完成 | `Cargo.toml`, `pyproject.toml`, `src/lib.rs` |
| PY-2 | 异常层次 + 错误映射 | ✅ 完成 | `src/exceptions.rs` — 9 个异常类 + `map_error()` + `wrap()` |
| PY-3 | StoreConfig + Store (核心) | ✅ 完成 | `src/config.rs`, `src/store.rs` |
| PY-4 | Dataset 封装 | ✅ 完成 | `src/dataset.rs` — `Arc<DataSet>` 共享模式 |
| PY-5 | 查询迭代器 (`Arc<DataSet>` 模式) | ✅ 完成 | `src/query.rs` — 预收集 IndexEntry + 懒加载数据 |
| PY-6 | Python 模块导出 + `__init__.py` | ✅ 完成 | `src/lib.rs`, `python/timslite/__init__.py` |
| PY-7 | 集成测试 (8 个测试文件, 39 用例) | ✅ 完成 | `tests/` — 全部通过 |
| PY-8 | CI/CD + 多平台 Wheel 构建 | 🔲 待开始 | GitHub Actions workflow |
| PY-9 | Queue 模块 Python 包装 | ✅ 完成 | `src/queue.rs` — DatasetQueue + Consumer |
| PY-10 | Queue poll callback 包装 | ✅ 完成 | `DatasetQueueConsumer.poll_callback`, `JournalQueueConsumer.poll_callback` |

**验证结果**:
- ✅ `cargo clippy -- -D warnings` — wrapper crate 无警告
- ✅ `cargo clippy -- -D warnings` — 主 crate 无警告
- ✅ `pytest tests/ -v` — **56 tests passed** (42 existing + 14 queue), 0 failed
- ✅ `cargo test -- --test-threads=1` — 主 crate 244 tests passed (200 lib + 44 integration)
- ✅ `maturin develop --release` — 编译并安装成功 (CPython 3.13)

---

## 已知限制

| 限制 | 说明 |
|------|------|
| 连续模式大间隙写入 | `index_continuous=True` + 大间隙 (≥100) 会触发大量 filler 条目创建, 可能导致性能下降 |
| Windows 后台线程清理 | 后台线程被 detach 后仍持有 mmap 文件句柄, 测试完成后可能需要短暂延迟才能删除临时文件 |
| 多 Python 版本 | 当前仅在 CPython 3.13 上验证, 需扩展至 3.9-3.13 |

---

## 技术选型

| 组件 | 选择 | 理由 |
|------|------|------|
| PyO3 版本 | **0.28** | 最新稳定版 (MSRV 1.83, `Python::with_gil` API) |
| 构建工具 | **maturin** 1.x | PyO3 官方推荐, PEP 517 兼容 |
| Python 最低版本 | **3.9** | 支持 `list[tuple[int, bytes]]` 原生类型提示 |
| 测试框架 | **pytest** | Python 生态标准 |
| 依赖 | **timslite (path)** | 直接引用项目根目录, 无需重复编译 |
| crate-type | `cdylib` | 仅生成 Python 扩展模块 (`.so`/`.pyd`/`.dylib`) |

---

## 目录结构 (目标)

```
wrapper/python/
├── Cargo.toml                      # maturin crate, pyo3 0.28
├── pyproject.toml                  # PEP 517 build config
├── plan.md                         # 本文件: 计划状态总览
├── design.md                       # 详细设计文档 (已存在)
├── src/
│   └── lib.rs                      # PyO3 模块入口, 异常注册
│   ├── exceptions.rs               # TmslError 层次 + map_error()
│   ├── config.rs                   # PyStoreConfig #[pyclass]
│   ├── store.rs                    # PyStore #[pyclass]
│   ├── dataset.rs                  # PyDataset #[pyclass]
│   ├── query.rs                    # PyQueryIterator #[pyclass]
│   ├── queue.rs                    # PyDatasetQueue + PyDatasetQueueConsumer #[pyclass]
├── python/
│   └── timslite/
│       └── __init__.py             # 纯 Python 层: 类型重导出
└── tests/
    ├── test_basic.py               # Smoke: import, open, close
    ├── test_lifecycle.py           # create/open/close/drop
    ├── test_write_query.py         # write, query, query_all, iterator
    ├── test_continuous.py          # continuous mode: backfill, gaps
    ├── test_exceptions.py          # All 8 error types
    ├── test_persistence.py         # reopen after close
    ├── test_multi_dataset.py       # isolation between datasets
    └── test_config.py              # StoreConfig + create_dataset kwargs
```

---

## 待完成事项

### Phase PY-1: 项目骨架 + 构建系统 🔲 待开始

- [ ] `wrapper/python/Cargo.toml` — 创建 maturin/PyO3 crate
  - `[package] name = "timslite-python"`, edition = "2021"
  - `[lib] name = "timslite"`, crate-type = `["cdylib"]`
  - `[dependencies] pyo3 = "0.28"` (features: `["extension-module"]`)
  - `[dependencies] timslite = { path = "../.." }`
- [ ] `wrapper/python/pyproject.toml` — PEP 517 构建配置
  - `build-system: requires = ["maturin>=1.0,<2.0"]`, `build-backend = "maturin"`
  - `[project]` name = "timslite", version = "0.1.0", requires-python = ">=3.9"
  - `[tool.maturin]` features = `["pyo3/extension-module"]`
- [ ] `wrapper/python/src/lib.rs` — PyO3 模块入口
  - `use pyo3::prelude::*`
  - `#[pymodule] fn timslite(m: &Bound<'_, PyModule>) -> PyResult<()>` 
  - 注册所有 `#[pyclass]` 和自定义异常
  - 暂不包含实现, 仅声明模块
- [ ] `wrapper/python/python/timslite/__init__.py` — 最小导出
  - `from .timslite import *` (导入编译后的 .so/.pyd 扩展)
- [ ] 验证: `cd wrapper/python && maturin develop` 成功
- [ ] 验证: `python -c "import timslite; print(dir(timslite))"` 显示模块
- [ ] 验证: `cd wrapper/python && maturin develop --release` 编译成功

**验收标准**:
- `maturin develop` 和 `maturin develop --release` 均成功
- `import timslite` 成功, 模块存在
- 无 Rust 编译警告 (`cargo clippy` 对 wrapper crate 无警告)

---

### Phase PY-2: 异常层次 + 错误映射 🔲 待开始

- [ ] `wrapper/python/src/exceptions.rs` — 定义 9 个自定义异常
  - 创建基类 `TmslError` (继承自 Python `Exception`)
  - 创建 8 个子类: `TmslIoError`, `TmslNotFoundError`, `TmslAlreadyExistsError`,
    `TmslInvalidDataError`, `TmslSegmentFullError`, `TmslMmapError`,
    `TmslCompressionError`, `TmslDecompressionError`
  - 使用 `pyo3::create_exception!` 宏定义
  - 提供 `fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` 注册函数
- [ ] 实现 `fn map_error(err: timslite::TmslError) -> PyErr`
  - 匹配 `TmslError` 的 10 个变体, 映射到对应的 Python 异常
  - `TmslError::Io(e)` → `TmslIoError`
  - `TmslError::NotFound(msg)` → `TmslNotFoundError`
  - `TmslError::AlreadyExists(msg)` → `TmslAlreadyExistsError`
  - `TmslError::InvalidData(msg)` → `TmslInvalidDataError`
  - `TmslError::SegmentFull` → `TmslSegmentFullError`
  - `TmslError::MmapError(msg)` → `TmslMmapError`
  - `TmslError::CompressionError(msg)` → `TmslCompressionError`
  - `TmslError::DecompressionError(msg)` → `TmslDecompressionError`
  - `TmslError::InvalidMagic` / `TmslError::InvalidVersion` → `TmslInvalidDataError`
  - 错误消息使用 `err.to_string()` 传递到 Python
- [ ] 在 `lib.rs` 中注册异常到模块
- [ ] 验证: 每个异常可在 Python 中捕获 (`except timslite.TmslNotFoundError`)
- [ ] 验证: 异常携带正确的错误消息字符串
- [ ] 单元测试: `test_basic.py` — 验证所有异常可导入, 继承关系正确

**验收标准**:
- 9 个异常类均可在 Python 中 `import timslite; timslite.TmslError` 访问
- `map_error` 覆盖所有 `TmslError` 变体, 无遗漏
- 异常消息包含 Rust 端的原始错误描述
- `maturin develop` 编译通过, `cargo clippy` 无警告

---

### Phase PY-3: StoreConfig + Store (核心) 🔲 待开始

#### 3.1 StoreConfig

- [ ] `wrapper/python/src/config.rs` — `PyStoreConfig`
  - `#[pyclass(get_all)]` — 所有字段可读 (不需要 set, 配置不可变)
  - `#[new] fn __init__` 接受所有参数作为 keyword-only args (`#[pyo3(signature = (...))]`)
  - 默认值与 `StoreConfig::default()` 一致
  - `@classmethod def default(cls)` — 返回全默认配置
  - Duration 字段接受 `int` (秒), 内部转换为 `std::time::Duration::from_secs()`
- [x] 验证: `StoreConfig()` 使用全部默认值
- [x] 验证: `StoreConfig(flush_interval=300)` 仅覆盖指定字段
- [x] 验证: `StoreConfig.default()` 返回正确配置

#### 3.2 Store

- [ ] `wrapper/python/src/store.rs` — `PyStore`
  - `#[pyclass]` 结构体持有 `Option<Store>`
  - `#[new]` — 不直接调用, 使用 `@classmethod open(...)` 工厂方法
  - `@classmethod fn open(data_dir: &str, config: Option<PyStoreConfig>) -> PyResult<Self>`
    - 创建目录, 扫描已有数据集, 启动后台线程
  - `fn __enter__(&mut self) -> PyResult<Self>` — 返回 self (context manager 入口)
  - `fn __exit__(&mut self, _exc_type, _exc_val, _exc_tb) -> PyResult<()>` — 调用 close
  - `fn close(&mut self) -> PyResult<()>` — consume inner Store
  - `fn __del__(&mut self)` — best-effort close (防止未手动 close 时泄漏)
- [ ] 验证: `with Store.open(tmpdir) as store:` 进入/退出正常
- [ ] 验证: 退出时自动调用 close (数据持久化)
- [ ] 验证: 手动调用 `store.close()` 后再调用操作 → 友好错误

**验收标准**:
- `StoreConfig` 所有当前字段正确, 默认值匹配 Rust 端, 包括 `read_only=None|False|True`
- `Store.open()` 创建/打开存储成功, 自动创建目录
- Context manager (`with` 语句) 正确管理生命周期
- `close()` 后 Store 不可用, 尝试操作返回明确错误
- `maturin develop` 编译通过, `cargo clippy` 无警告
- Python 测试 `test_basic.py` + `test_config.py` 通过

---

### Phase PY-4: Dataset 封装 🔲 待开始

- [ ] 在 `PyStore` 中添加 dataset 追踪机制
  - `PyStore` 内部维护 `HashMap<u64, Arc<DataSet>>` (与 Rust `Store` 并行)
  - `create_dataset/open_dataset` 返回的 dataset 通过 `Arc` 共享
  - 分配 dataset_id (自增 u64) 作为 Python 端标识
- [ ] `wrapper/python/src/store.rs` — 添加 dataset 管理方法
  - `fn create_dataset(&mut self, name: &str, dataset_type: &str, **kwargs) -> PyResult<()>`
    - kwarg 参数: `data_segment_size`, `index_segment_size`, `compress_level`,
      `index_continuous`, `initial_data_segment_size`, `initial_index_segment_size`
    - 使用 `DataSetConfigBuilder::from_store()` + 覆盖构建配置
    - 委托给底层 `Store::create_dataset_with_config()`
    - 不返回 Dataset — 用户需调用 `open_dataset()` 获取
  - `fn open_dataset(&mut self, name: &str, dataset_type: &str) -> PyResult<PyDataset>`
    - 委托给底层 `Store::open_dataset()` 获取 handle
    - 通过 `Store::get_dataset()` 获取 `Arc<DataSet>`
    - 返回封装后的 `PyDataset` (持有 `Arc<DataSet>`)
  - `fn drop_dataset(&mut self, name: &str, dataset_type: &str) -> PyResult<()>`
    - 委托给底层 `Store::drop_dataset_by_name()`
    - 清理 Python 端的 dataset 追踪
- [ ] `wrapper/python/src/dataset.rs` — `PyDataset`
  - `#[pyclass]` 持有 `Arc<DataSet>` + `dataset_id: u64`
  - `fn write(&mut self, timestamp: i64, data: &[u8]) -> PyResult<()>`
    - 调用 `DataSet::write()`, 由 DataSet 内部 mutex 保护
    - `data` 接受 Python `bytes`, 通过 `PyBytes::as_bytes()` 零拷贝获取
  - `fn query(&mut self, start_ts: i64, end_ts: i64) -> PyResult<PyQueryIterator>`
    - 调用 `DataSet::query_index_entries()`, 预收集索引条目
    - 返回懒加载的 `PyQueryIterator` (持有 `Arc<DataSet>` + 条目列表)
  - `fn query_all(&mut self, start_ts: i64, end_ts: i64) -> PyResult<Vec<(i64, Vec<u8>)>>`
    - 便捷方法: 调用 `query()` + 收集全部结果
    - 返回 `list[tuple[int, bytes]]`
  - `fn flush(&mut self) -> PyResult<()>` — 委托 `DataSet::flush()`
  - `#[getter] fn data_dir(&self) -> PyResult<String>` — 返回数据集路径
- [ ] 验证: `store.create_dataset("sensor", "data")` 成功
- [ ] 验证: `store.create_dataset("sensor", "data")` 第二次 → `TmslAlreadyExistsError`
- [ ] 验证: `ds.write(1, b"hello")` 写入成功
- [ ] 验证: `ds.write(-1, b"bad")` → `TmslInvalidDataError`
- [ ] 验证: `store.open_dataset("nonexistent", "data")` → `TmslNotFoundError`

**验收标准**:
- `create_dataset` 支持所有 kwargs, 缺失值继承 Store 默认
- `open_dataset` 返回可操作的 `Dataset` 对象
- `Dataset.write()` 接受 `(int, bytes)` 并正确写入
- `Dataset.query_all()` 返回正确结果列表
- `Dataset.flush()` 可手动刷盘
- 所有错误映射到正确的 Python 异常
- Python 测试 `test_lifecycle.py` + `test_write_query.py` 通过
- `maturin develop` 编译通过, `cargo clippy` 无警告

---

### Phase PY-5: 查询迭代器 (`Arc<DataSet>` 模式) 🔲 待开始

- [ ] `wrapper/python/src/query.rs` — `PyQueryIterator`
  - `#[pyclass]` 结构体:
    ```rust
    struct PyQueryIterator {
        entries: Vec<IndexEntry>,      // 预收集的索引条目
        dataset_arc: Arc<DataSet>,        // 共享数据集引用
        index: usize,                  // 当前遍历位置
    }
    ```
  - `fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self>` — 返回 self
  - `fn __next__(&mut self) -> PyResult<Option<(i64, Vec<u8>)>>`
    - 跳过 filler entries (`block_offset == BLOCK_OFFSET_FILLER`)
    - 调用 `DataSet::read_entry_at_index(entry)`; DataSet 内部加锁
    - 返回 `Some((timestamp, data))` 或 `None` (触发 `StopIteration`)
  - `fn __len__(&self) -> usize` — 返回剩余条目数 (不含 filler)
  - `fn close(&mut self)` — 释放资源 (通常不需要, 提供显式关闭)
- [ ] 在 `PyDataset::query()` 中创建 `PyQueryIterator`:
  - 调用 `DataSet::query_index_entries(start_ts, end_ts)` 收集条目
  - 克隆 `Arc<DataSet>`
  - 返回 `PyQueryIterator { entries, dataset_arc, index: 0 }`
- [ ] 验证: `for ts, data in ds.query(1, 100)` 正确迭代
- [ ] 验证: `list(ds.query(1, 100))` 收集为完整列表
- [ ] 验证: 空查询范围 → 立即 `StopIteration`
- [ ] 验证: filler entries 被正确跳过 (连续模式)
- [ ] 验证: 迭代中途关闭 store → 优雅处理 (不会 panic)

**验收标准**:
- `QueryIterator` 实现 Python `__iter__` / `__next__` 协议
- 遍历结果与 `query_all()` 一致
- Filler entries 自动跳过
- 迭代期间数据块懒加载 (非预加载)
- 中途 GC 或提前退出不会泄漏资源
- Python 测试 `test_write_query.py` iterator 部分通过
- `maturin develop` 编译通过, `cargo clippy` 无警告

---

### Phase PY-6: Python 模块导出 + `__init__.py` 🔲 待开始

- [ ] 完善 `wrapper/python/src/lib.rs` — 完整模块注册
  ```rust
  #[pymodule]
  fn timslite(m: &Bound<'_, PyModule>) -> PyResult<()> {
      exceptions::register(m)?;
      m.add_class::<config::PyStoreConfig>()?;
      m.add_class::<store::PyStore>()?;
      m.add_class::<dataset::PyDataset>()?;
      m.add_class::<query::PyQueryIterator>()?;
      Ok(())
  }
  ```
- [ ] 完善 `wrapper/python/python/timslite/__init__.py`
  - 从编译后的扩展模块导入所有公共符号
  - 提供便捷别名 (如 `Store` = `PyStore`)
  - 添加模块级 docstring
  ```python
  """High-performance time-series data storage."""
  from .timslite import (
      TmslError, TmslIoError, TmslNotFoundError, TmslAlreadyExistsError,
      TmslInvalidDataError, TmslSegmentFullError, TmslMmapError,
      TmslCompressionError, TmslDecompressionError,
      StoreConfig, Store, Dataset, QueryIterator,
  )
  
  __all__ = [
      "TmslError", "TmslIoError", "TmslNotFoundError", "TmslAlreadyExistsError",
      "TmslInvalidDataError", "TmslSegmentFullError", "TmslMmapError",
      "TmslCompressionError", "TmslDecompressionError",
      "StoreConfig", "Store", "Dataset", "QueryIterator",
  ]
  ```
- [ ] 验证: `import timslite` + `dir(timslite)` 包含所有符号
- [ ] 验证: 所有文档字符串 (`__doc__`) 正确显示
- [ ] 验证: `from timslite import Store, Dataset, StoreConfig` 正常

**验收标准**:
- 模块导入后所有 13+ 符号可用 (9 exceptions + 3 classes + 1 iterator)
- `__all__` 列表正确, IDE 自动补全可用
- 模块有清晰的 docstring
- `maturin develop` 编译通过

---

### Phase PY-7: 集成测试 (8 个测试文件) 🔲 待开始

- [ ] `tests/test_basic.py` — Smoke 测试
  - [ ] `test_import` — `import timslite` 成功, 模块有所有导出符号
  - [ ] `test_store_open_close` — `Store.open(tmpdir)`, `close()` 无错误
  - [ ] `test_store_context_manager` — `with Store.open(tmpdir): pass` 正常退出
- [ ] `tests/test_lifecycle.py` — 生命周期测试
  - [ ] `test_create_open_write_close` — 完整 create→open→write→close 流程
  - [ ] `test_create_twice_raises` — 重复 create → `TmslAlreadyExistsError`
  - [ ] `test_open_nonexistent_raises` — open 不存在 → `TmslNotFoundError`
  - [ ] `test_drop_dataset` — drop 后目录消失, 可重新 create
  - [ ] `test_drop_recreate_with_different_params` — drop 后用不同参数 re-create
- [ ] `tests/test_write_query.py` — 写入和查询测试
  - [ ] `test_single_write_query` — 写 1 条, query 返回 1 条
  - [ ] `test_multiple_write_query_range` — 写 100 条, query(3, 7) 返回 5 条
  - [ ] `test_query_empty_range` — query 无数据的范围 → 空迭代
  - [ ] `test_query_all_convenience` — `query_all()` = `list(query())`
  - [ ] `test_iterator_protocol` — `for ts, data in ds.query(...)` 正确枚举
  - [ ] `test_iterator_partial_consumption` — 迭代一半后丢弃, 不崩溃
  - [ ] `test_write_timestamp_zero_rejected` — `write(0, ...)` → `TmslInvalidDataError`
  - [ ] `test_write_negative_timestamp_rejected` — `write(-1, ...)` → `TmslInvalidDataError`
  - [ ] `test_write_out_of_order_rejected` — 非连续模式下逆序写 → `TmslInvalidDataError`
  - [ ] `test_flush_manual` — 写入后 `flush()`, 数据落盘
- [ ] `tests/test_continuous.py` — 连续模式测试
  - [ ] `test_continuous_out_of_order_write` — 允许逆序写入 (back-fill)
  - [ ] `test_continuous_gap_filling` — 跳写产生 filler, 查询自动过滤
  - [ ] `test_continuous_backfill_replaces_filler` — back-fill 替换 filler
  - [ ] `test_continuous_duplicate_timestamp_rejected` — 重复时间戳 → 错误
- [ ] `tests/test_exceptions.py` — 异常类型测试
  - [ ] `test_all_exceptions_importable` — 9 个异常类均可导入
  - [ ] `test_exception_hierarchy` — 所有异常继承 `TmslError`
  - [ ] `test_error_message_passthrough` — 捕获异常, 验证消息包含 Rust 端描述
  - [ ] `test_catch_specific_exception` — `except TmslNotFoundError` 精确捕获
- [ ] `tests/test_persistence.py` — 持久化测试
  - [ ] `test_reopen_after_close` — 关闭后重新 open, 数据完整
  - [ ] `test_data_survives_process_restart` — 写→close (进程退出)→新进程 open→读
  - [ ] `test_meta_file_invariant` — meta 文件在 reopen 时参数一致
- [ ] `tests/test_multi_dataset.py` — 多数据集隔离测试
  - [ ] `test_two_datasets_isolated` — 写 A 不影响 B
  - [ ] `test_same_name_different_type` — 同名不同类型完全隔离
- [ ] `tests/test_config.py` — 配置测试
  - [ ] `test_store_config_defaults` — 所有字段默认值正确
  - [ ] `test_store_config_custom` — 自定义所有字段
  - [ ] `test_create_dataset_with_kwargs` — create_dataset kwargs 覆盖 store 默认
  - [ ] `test_create_dataset_uses_store_defaults` — 不传 kwargs 时用 store 配置
  - [ ] `test_index_continuous_kwarg` — `index_continuous=True` 启用连续模式

**验收标准**:
- 全部 pytest 测试通过 (`pytest tests/ -v`, 预计 ~30+ 测试用例)
- 测试使用 `tempfile.TemporaryDirectory()` 确保隔离
- 无测试间干扰 (每个测试独立临时目录)
- `pytest tests/ -v` 全部绿色

---

### Phase PY-8: CI/CD + 多平台 Wheel 构建 🔲 待开始

- [ ] `.github/workflows/python-ci.yml` — PR/合并时 CI
  - [ ] 触发条件: `push` / `pull_request` 到 `wrapper/python/` 路径变更
  - [ ] 矩阵: OS = `[ubuntu-latest, macos-latest, windows-latest]`
  - [ ] 步骤: checkout → setup Python (3.9-3.13 矩阵) → setup Rust → `maturin build` → `pytest`
  - [ ] lint: `cargo clippy` 对 wrapper crate
  - [ ] 验证 wheel 产物 `.whl` 大小合理
- [ ] `.github/workflows/python-release.yml` — 发布 CI
  - [ ] 触发条件: `push` tag `python-v*`
  - [ ] 构建多平台 wheels (包含 manylinux / macOS universal2 / Windows)
  - [ ] 上传到 PyPI (`pypa/gh-action-pypi-publish`)
- [ ] 本地构建验证:
  - [ ] `maturin build --release` 生成当前平台 wheel
  - [ ] 验证 wheel 可 `pip install` 且 `import timslite` 成功
- [ ] 文档: 在 `wrapper/python/` 或项目 README 中添加 Python 安装说明

**验收标准**:
- CI workflow 在 PR 上自动运行, 所有矩阵通过
- Release workflow 可构建多平台 wheels
- `pip install timslite` (从 wheel) 后可正常使用
- CI/CD 中 clippy 无警告, 测试全部通过

---

## Phase 依赖关系

```
PY-1 (骨架) 
  ├─ PY-2 (异常) 
  │   └─ PY-3 (StoreConfig + Store)
  │       └─ PY-4 (Dataset)
  │           └─ PY-5 (查询迭代器)
  │               └─ PY-6 (模块导出)
  │                   └─ PY-7 (集成测试)
  │                       └─ PY-8 (CI/CD)
```

**说明**: 
- PY-1 是基础设施, 必须先完成
- PY-2 独立于 PY-3, 但需要在 PY-3 之前完成 (因为 Store 操作需要异常映射)
- PY-3 → PY-4 → PY-5 依次依赖 (Store 创建 Dataset, Dataset 返回 QueryIterator)
- PY-6 聚合所有组件, 必须在 PY-2 到 PY-5 之后
- PY-7 测试依赖于 PY-6 (模块必须完整)
- PY-8 CI/CD 可在 PY-7 期间并行准备, 但最终验证依赖测试通过

---

## 风险与应对

| 风险 | 影响 | 概率 | 应对 |
|------|------|------|------|
| PyO3 0.28 API 变更 | 中 | 低 | 参照 PyO3 migration guide, 使用稳定 API |
| DataSet 内部锁迭代器阻塞 | 中 | 低 | `__next__` 每次只调用一次 DataSet public API, 不在 Python 层嵌套锁 |
| Windows 编译问题 | 中 | 中 | 使用 maturin 的 MSVC toolchain, 测试 `x86_64-pc-windows-msvc` |
| manylinux 兼容性 | 中 | 中 | 使用 `pypa/manylinux` Docker 镜像 + maturin 自动处理 |
| GIL 与后台线程冲突 | 高 | 低 | 后台线程不持 GIL, Python 操作在 GIL 下, 无交叉 |

---

## 开发规范

- **无 `as any` / `unwrap()`**: 所有 `Option` / `Result` 必须显式处理
- **错误映射**: 所有 Rust `Result<T>` 在 Python 边界转为 `PyResult<T>`, 错误通过 `map_error` 转换
- **内存安全**: `Arc<DataSet>` 共享不跨越 FFI 传递裸指针
- **测试隔离**: 每个 Python 测试使用 `tempfile.TemporaryDirectory()` 独立临时目录
- **代码风格**: Rust 端遵循项目根目录 `AGENTS.md` (clippy clean, fmt check), Python 端遵循 PEP 8
- **文档**: 所有 PyO3 `#[pyclass]` 和 `#[pymethods]` 必须有 Python 可见的 docstring

---

## 文档结构

```
wrapper/python/
├── Cargo.toml
├── pyproject.toml
├── plan.md                ← 本文件: 计划状态总览
├── design.md              ← 详细设计文档 (已存在)
└── src/
    ├── lib.rs
    ├── exceptions.rs
    ├── config.rs
    ├── store.rs
    ├── dataset.rs
    └── query.rs
```

**维护指南**:
- 完成验收标准后, 将 `🔲` 改为 `✅`
- Phase 全部完成时, 更新上方状态表中对应行的状态
- 子任务按实际进展勾选
- 如需新增子任务, 在对应 Phase 的 "待完成事项" 中添加
