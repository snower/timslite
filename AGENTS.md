# AGENTS.md — timslite

> 高性能 Rust 时序数据存储动态库 — mmap-backed, Block 级聚合, 延迟压缩, C ABI FFI

## 项目概览

timslite 是一个用 Rust 编写的时序数据存储库，编译为 `cdylib` 动态库，通过 C ABI FFI 供 C/C++/Go/Python 等语言调用。核心特性：内存映射存储、Block 级聚合、延迟压缩、懒加载生命周期、时间索引。

## 目录结构

```
timslite/
├── src/
│   ├── lib.rs          # 入口, 公共 API 导出
│   ├── config.rs       # StoreConfig 配置构建器
│   ├── error.rs        # TmslError 错误类型
│   ├── util.rs         # 路径/offset 格式化工具
│   ├── meta.rs         # DataSetMeta TLV 元数据
│   ├── header.rs       # 文件头 100B (magic+version+TLV meta+state)
│   ├── block.rs        # BlockHeader + Block 序列化
│   ├── compress.rs     # miniz_oxide deflate 压缩
│   ├── cache.rs        # BlockCache LRU 读缓存
│   ├── dataset.rs      # DataSet 操作 (create/open/close/drop/write/query)
│   ├── store.rs        # Store 门面 (数据集注册表 + 后台任务管理)
│   ├── ffi.rs          # extern "C" FFI 接口
│   ├── bg/
│   │   └── mod.rs      # BackgroundTasks 统一执行器 (flush + idle + cache + retention; 支持线程/手动双模式)
│   ├── segment/
│   │   ├── mod.rs      # DataSegmentSet (段路由 + 懒加载管理)
│   │   └── data.rs     # DataSegment (mmap 生命周期 + Block 写入/读取)
│   └── index/
│       ├── mod.rs      # TimeIndex (索引段路由 + 时间查询)
│       └── segment.rs  # IndexSegment (二分查找 + 连续模式优化)
├── include/
│   └── timslite.h      # C 头文件 (FFI 函数声明)
├── tests/
│   └── integration_test.rs   # 集成测试
├── benches/                   # 性能基准测试 (待创建)
├── docs/
│   ├── design/         # 详细设计文档 (14 个专题)
│   └── plan/           # 开发计划文档 (overview + 12 phases)
├── plan.md             # 计划状态总览 + 待完成清单
├── design.md           # 设计文档索引 (导航入口)
└── Cargo.toml
```

## 构建与测试

```bash
# 编译动态库
cargo build              # debug
cargo build --release    # release → target/release/libtimslite.{so,dll,dylib}

# 运行测试 (必须单线程)
cargo test -- --test-threads=1

# Clippy (零警告)
cargo clippy -- -D warnings

# 格式检查
cargo fmt -- --check
```

**重要**: 测试必须使用 `--test-threads=1`，因为多个测试共享同一 `tmp` 目录且涉及文件系统操作。

## 代码规范

**重要**: 进行代码编写前未必先深度思考后调整设计design.md和docs/design详细设计，完成后制定开发计划并同步调整plan.md和docs/plan详细计划信息，然后再进行代码编写，任务完成后需要同步更新plan.md完成标记

### Rust Edition & 依赖

- Rust 2021 edition
- 依赖: `memmap2` 0.9, `miniz_oxide` 0.8, `log` 0.4, `libc` 0.2
- dev-dependency: `criterion` 0.5 (性能基准)

### 命名约定

| 类型 | 规范 | 示例 |
|------|------|------|
| 公开 API | `PascalCase` | `Store`, `DataSet`, `TmslError` |
| 模块内部 | `PascalCase` | `DataSegmentSet`, `IndexSegment`, `BlockCache` |
| FFI 函数 | `snake_case` + `tmsl_` 前缀 | `tmsl_store_open`, `tmsl_dataset_write` |
| 错误变体 | `PascalCase` | `NotFound`, `AlreadyExists`, `SegmentFull` |
| 常量 | `SCREAMING_SNAKE_CASE` | `HEADER_SIZE`, `MAGIC`, `VERSION` |

### 错误处理

- 使用 `TmslError` 枚举, 包含所有错误变体
- 模块内使用 `crate::error::Result<T>` 别名
- `io::Error` 通过 `From` trait 自动转换为 `TmslError::Io`
- FFI 层: 使用 `catch_unwind` + `err_buf` 输出错误信息, 不跨越 FFI 传播 panic

### 并发模型

```
Store: RwLock<HashMap>              (多读少写, 数据集注册表)
DataSet: Arc<Mutex<DataSet<>>>      (读写互斥, 数据集内部操作)
不同 DataSet: 完全并行               (无交叉锁)
```

- 后台线程通过读锁遍历数据集, 写锁获取后 double-check `last_used_at` 防止竞态
- 写操作更新 `last_used_at` 可自动 "唤醒" 即将 idle-close 的数据集

### 文件格式常量

| 常量 | 值 | 说明 |
|------|-----|------|
| `DATA_HEADER_SIZE` | 116 | 数据段文件头大小 (magic + version + meta TLV + state, 9 个 state 字段) |
| `INDEX_HEADER_SIZE` | 52 | 索引段文件头大小 (magic + version + meta TLV + state, 1 个 state 字段) |
| `BLOCK_HEADER_SIZE` | 16 | Block 头部大小 |
| `INDEX_ENTRY_SIZE` | 18 | 索引条目大小 (timestamp:8 + block_offset:8 + in_block:2) |
| `MAGIC` | `b"TMSL"` | 文件魔数 |
| `VERSION` | 1 | 当前文件格式版本 |
| `BLOCK_MAX_SIZE` | 65536 (64KB) | Block 最大Payload大小 |

### unsafe 使用规范

- FFI 边界: 所有 `extern "C"` 函数内使用 `unsafe` 包裹
- 指针操作: 必须在 FFI 层做 null check
- 字符串转换: `CStr::from_ptr` + `to_str()` 必须检查有效性
- 内存所有权: `tmsl_iter_next` 返回的数据由 `libc::malloc` 分配, C 侧必须调用 `tmsl_iter_free_data` 释放

### 代码注释

- 重点功能可添加适当注释，添加的注释应该简洁明了，如无必要不要添加注释
- 不允许在代码中添加冗长的解释性注释
- 注释统一使用英文

## 设计文档

设计文档已拆解为 14 个专题, 位于 [`docs/design/`](docs/design/) 目录:

| 入口 | 文档 | 核心内容 |
|------|------|---------|
| [design.md](design.md) | [架构概览](docs/design/architecture.md) | 整体架构、目录结构、模块结构 |
| | [元数据格式](docs/design/meta-format.md) | DataSetMeta TLV 格式、序列化 |
| | [数据模型](docs/design/data-model.md) | Record/Block/IndexEntry、FileMetadata 100B |
| | [数据段管理](docs/design/data-segment.md) | DataSegmentSet 路由、DataSegment 生命周期 |
| | [时间索引](docs/design/time-index.md) | TimeIndex、IndexSegment 二分查找 |
| | [数据集操作](docs/design/dataset-operations.md) | DataSet create/open/close/drop/write/query |
| | [Store 与 FFI](docs/design/store-and-ffi.md) | Store 门面、FFI 函数列表、C 示例 |
| | [后台任务与缓存](docs/design/background-and-cache.md) | 统一执行器 (线程/手动双模式)、Flush/Idle-Close/Retention、BlockCache LRU |
| | [内存与并发](docs/design/memory-and-concurrency.md) | mmap 生命周期、Crash 安全 |
| | [压缩策略](docs/design/compression.md) | Block 延迟压缩、miniz_oxide deflate |
| | [设计决策](docs/design/design-decisions.md) | 与 TimeStore(Java) 的差异 |
| | [索引连续存储](docs/design/index-continuous.md) | filler 哨兵、O(1) 查询优化 |
| | [懒分配与扩容](docs/design/lazy-allocation.md) | 初始分配、2 倍扩容、磁盘节省 |
| | [构建配置](docs/design/cargo-and-config.md) | Cargo.toml、构建命令 |

**维护规则**: 新增设计内容时，优先在对应 `docs/design/*.md` 文件中追加，保持各文档职责单一。

## 开发计划

计划状态总览在 [`plan.md`](plan.md)，包含：
- 已完成的 Phase (1-4, 9-11)
- 核心完成但有待完成的 Phase (5, 6, 12)
- 待完成的 Phase (7, 8)

详细计划文档在 [`docs/plan/`](docs/plan/) 目录，每个 Phase 独立成文。

### 当前待完成事项

| Phase | 待完成 |
|-------|--------|
| 5 | DataSet open/close/drop 集成测试 |
| 6 | Store create/open/drop FFI 集成测试 |
| 7 | FFI 接口完整实现 + C 链接测试 (全部待完成) |
| 8 | 端到端集成测试 + 性能基准 + 内存安全验证 (全部待完成) |
| 12 | `tmsl_dataset_create` FFI 新增参数 + lazy 相关集成测试 |

## 数据集目录格式

```
{data_dir}/
├── {dataset_name}/
│   ├── {dataset_type}/
│   │   ├── meta                     # 不可变配置 (magic+version+TLV)
│   │   ├── data/                    # 数据段目录
│   │   │   ├── 00000000000000000000  # 起始 offset=0 (20位零填充)
│   │   │   └── 00000000000067108864  # 起始 offset=64MB
│   │   └── index/                   # 索引段目录
│   │       ├── 00000000000000000000  # 起始 timestamp=0
│   │       └── 0000000000001700000000
```

每个 `(dataset_name, dataset_type)` 拥有独立的 `data/` 和 `index/` 目录，物理隔离。

## 数据集生命周期

```
create → open → write/query → close    # 正常生命周期
  │        │
  │        └── 从 meta 读取参数 (不可变)
  └── 写入 meta 文件 (一次性, 之后不可修改)

drop → 删除整个目录 (不可恢复)
```

## 后台任务

通过统一执行器 (`ExecutorState` + `Mutex`) 管理四个周期性任务:

| 任务 | 默认间隔 | 行为 |
|------|----------|------|
| Flush | 10 分钟 | 遍历打开的 segment, 执行 mmap.sync (MS_SYNC) |
| Idle Check | 60 秒 | 扫描 last_used_at, ≥30min → sync + seal pending + unmap + close |
| Cache Eviction | 60 秒 | 扫描缓存池 idle 条目, 超时回收 |
| Retention Reclaim | 每日指定时刻 | 扫描 retention_ms > 0 的 dataset, 删除过期分段 |

**线程启用控制**:
- `StoreConfig.enable_background_thread: bool` (默认 `true`)
- `true` (默认): `Store::open` 自动启动单个后台线程, 动态计算下一次唤醒时间, 无固定轮询
- `false`: 不启动后台线程, 调用方通过 `Store::tick_background_tasks()` 主动驱动

**手动执行 API (与后台线程共存)**:
- `Store::tick_background_tasks() -> TickResult { executed_tasks: usize, next_delay: Duration }`
  - 同步执行一次到期任务检查, 到期则立即执行
  - `enable_background_thread=true` 下也可调用, 与后台线程通过 `Mutex` 串行互斥
  - `enable_background_thread=false` 时由外部主动驱动
- `Store::next_background_delay() -> Duration`
  - 仅计算下一次任务到期延迟, 不执行; 快速读快照

**并发安全**: `executor.state: Mutex<ExecutorState>` 保证后台线程与外部 `tick` 互斥串行, 无死锁风险 (锁顺序: state → datasets → DataSet)。

## 重要注意事项

1. **测试必须单线程**: `cargo test -- --test-threads=1` — 多测试共享 tmp 目录
2. **FFI 内存所有权**: C 侧获取的 buffer 必须通过 FFI 函数释放, 不能直接 free
3. **meta 文件不可变**: 创建后只能通过 open 读取, 任何修改需 drop 后 recreate
4. **Pending Block 恢复**: reopen 时检测到 pending block → 安全密封 (FLAGS=SEALED, 不压缩)
5. **BlockCache**: 读缓存使用 LRU + idle-close 回收, 缓存的是解压后的 Block 数据
6. **连续索引模式**: IndexSegment 支持连续模式 (filler 哨兵 + O(1) 直接计算)
7. **懒分配**: 段文件初始分配最小尺寸, 2 倍扩容, header 保持不变
