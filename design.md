# timslite - Rust 时序数据存储库详细设计

> 目标: Rust 动态库(`cdylib`), 提供 FFI 可调用 C ABI
> 核心: 按数据集名称+类型分段 + 内存映射(mmap) + 时间索引 + Block 延迟压缩 + 懒加载生命周期

---

## 设计文档索引

本设计已拆解为多个专题文档, 位于 [`docs/design/`](docs/design/) 目录, 支持渐进式阅读:

| # | 文档 | 核心内容 | 阅读场景 |
|---|------|---------|---------|
| 1 | [架构概览](docs/design/architecture.md) | 整体架构、目录结构、命名规则、隔离保证、模块结构 | **入口文档**, 了解全局 |
| 2 | [元数据格式](docs/design/meta-format.md) | DataSetMeta TLV 格式、字段定义、序列化/反序列化、向前兼容 | 新增 meta 字段时 |
| 3 | [数据模型](docs/design/data-model.md) | Record(`u32 data_len`)/Block/IndexEntry 结构、可变长度 FileMetadata 布局、类型定义 | 理解存储格式 |
| 4 | [数据段管理](docs/design/data-segment.md) | DataSegmentSet 路由、DataSegment Block 生命周期、Pending 恢复 | 数据写入/读取相关 |
| 5 | [时间索引](docs/design/time-index.md) | TimeIndex 生命周期、IndexSegment 二分查找、18 字节序列化 | 索引查询优化 |
| 6 | [数据集操作](docs/design/dataset-operations.md) | DataSet create/open/close 生命周期、写入/读取/流程详解 | 数据集 API 行为 |
| 7 | [Store 与 FFI](docs/design/store-and-ffi.md) | Store 门面 API、FFI C ABI 函数列表、C 侧调用示例 | 外部集成/跨语言调用 |
| 8 | [后台任务与缓存](docs/design/background-and-cache.md) | 统一执行器 (线程/手动双路径)、Flush/Idle-Close/Retention、外部手动 Execute、BlockCache LRU+Idle 回收 | 性能调优/后台行为/外部事件循环集成 |
| 9 | [内存与并发](docs/design/memory-and-concurrency.md) | mmap 生命周期、并发控制、Crash 安全、Pending 恢复 | 稳定性保障 |
| 10 | [压缩策略](docs/design/compression.md) | Block 级延迟压缩、miniz_oxide deflate、flags 设计 | 压缩相关优化 |
| 11 | [设计决策](docs/design/design-decisions.md) | 关键决策对比表、与 TimeStore(Java) 的差异 | 架构评审/迁移 |
| 12 | [索引连续存储](docs/design/index-continuous.md) | 稀疏 filler 分段、逻辑空洞、base timestamp、O(1) 直接计算优化 | 连续模式需求 |
| 13 | [懒分配与扩容](docs/design/lazy-allocation.md) | 初始分配、2 倍扩容、header 元数据不随扩容变更、磁盘节省分析 | 空间优化需求 |
| 14 | [构建配置](docs/design/cargo-and-config.md) | Cargo.toml 依赖、构建/测试/基准命令、GitHub Actions CI | 项目构建 |
| 15 | [查询迭代器](docs/design/query-iterator.md) | Virtual Iterator 惰性查询、HotBlockCache 读取循环级缓存、FFI 迭代器重构 | 查询性能优化 |
| 16 | [Queue 架构与 API](docs/design/queue-overview.md) | DatasetQueue/Consumer 类型、API 设计、生命周期、并发控制、Dataset 集成 | 队列功能开发 |
| 17 | [Queue 状态文件](docs/design/queue-state-file.md) | 消费组 4KB mmap 状态文件格式、Pending Entry 结构、Crash 恢复、同步策略 | 队列持久化/恢复 |
| 18 | [Journal 变更日志](docs/design/journal.md) | 专用 `.journal/logs` append log、操作日志格式、Store/DataSet hook、专用 read/query/queue API、热迁移/恢复边界 | 变更日志/热迁移/恢复工具 |
| 18.1 | [Journal 专用存储](docs/design/journal-storage.md) | JournalSegment/JournalLog/JournalQueue、sequence 路由、无索引分段存储、crash 可见边界 | Journal 存储优化 |
| 19 | [数据集读操作](docs/design/dataset-read-operations.md) | read/query/query_iter 及新增 read_exist/query_exist/read_length/query_length/query_length_iter 统一描述 | 读操作 API 设计/选型 |
| 20 | [数据集 Inspect](docs/design/dataset-inspect.md) | DataSetInfo (不变配置) + DataSetState (可变状态)、dataset state 文件、Rust/FFI/Python API | 数据集状态监控/运维 |
| 21 | [Dataset Identifier](docs/design/dataset-identifier.md) | Store 级 `max_identifier`、dataset 级 `identifier` 文件、按 id 打开 dataset API、crash 边界 | 外部系统需要稳定数字 dataset id |

---

## 快速导航

### 按功能查找
- **写入数据**: [数据段管理](docs/design/data-segment.md) → [数据集操作·写入流程](docs/design/dataset-operations.md#九写入流程详解) → [压缩策略](docs/design/compression.md)
- **Record 编码**: [数据模型·Block Payload 内部结构](docs/design/data-model.md#block-payload-内部结构-record-编码)
- **纠正写入**: [数据集操作·§9.1 时间戳验证与写入分支](docs/design/dataset-operations.md#91-时间戳验证与写入分支) → [索引连续存储·§23.4](docs/design/index-continuous.md#234-写入行为)
- **乱序写入**: [数据集操作·§9.1 乱序写入机制](docs/design/dataset-operations.md#91-时间戳验证与写入分支) → [索引连续存储·§23.4 情况B](docs/design/index-continuous.md#234-写入行为)
- **删除记录**: [数据集操作·§9.3 删除操作](docs/design/dataset-operations.md#93-删除操作-datasetdelete) → [索引连续存储·§23.5 哨兵值设计](docs/design/index-continuous.md#235-哨兵值设计)
- **读取数据**: [时间索引](docs/design/time-index.md) → [数据集操作·读取流程](docs/design/dataset-operations.md#十读取流程详解) → [数据集读操作](docs/design/dataset-read-operations.md) → [查询迭代器](docs/design/query-iterator.md) → [后台任务与缓存](docs/design/background-and-cache.md)
- **数据保留回收**: [元数据格式·retention_window](docs/design/meta-format.md) → [数据集操作·§11](docs/design/dataset-operations.md#十一数据保留-retention-与回收) → [后台任务·§17.8](docs/design/background-and-cache.md#178-retention-reclaim-数据保留回收)
- **FFI 集成**: [Store 与 FFI](docs/design/store-and-ffi.md)
- **崩溃安全**: [内存与并发](docs/design/memory-and-concurrency.md#崩溃安全)
- **磁盘优化**: [懒分配与扩容](docs/design/lazy-allocation.md)
- **连续时间索引**: [索引连续存储](docs/design/index-continuous.md) (稀疏 filler + 逻辑空洞)
- **队列消费**: [Queue 架构与 API](docs/design/queue-overview.md) → [Queue 状态文件](docs/design/queue-state-file.md)
- **变更日志/热迁移**: [Journal 变更日志](docs/design/journal.md) → [Journal 专用存储](docs/design/journal-storage.md)

### 按模块查找
| 模块 | 对应文档 |
|------|---------|
| `Store` + `create_dataset` + `open_dataset` | [Store 与 FFI](docs/design/store-and-ffi.md) |
| `DataSet` | [数据集操作](docs/design/dataset-operations.md) |
| `DataSetMeta` (meta.rs) | [元数据格式](docs/design/meta-format.md) |
| Dataset Identifier | [Dataset Identifier](docs/design/dataset-identifier.md) |
| `DataSegmentSet` + `DataSegment` | [数据段管理](docs/design/data-segment.md) |
| `TimeIndex` + `IndexSegment` | [时间索引](docs/design/time-index.md) |
| `FileMetadata` + `BlockHeader` | [数据模型](docs/design/data-model.md) |
| `QueryIterator` + `HotBlockCache` | [查询迭代器](docs/design/query-iterator.md) |
| `QueryLengthIterator` | [数据集读操作](docs/design/dataset-read-operations.md) |
| `BlockCache` | [后台任务与缓存](docs/design/background-and-cache.md) |
| `BackgroundTasks` | [后台任务与缓存](docs/design/background-and-cache.md#十七后台任务) |
| `DatasetQueue` + `DatasetQueueConsumer` | [Queue 架构与 API](docs/design/queue-overview.md) |
| `ConsumerStateFile` + `PendingEntry` | [Queue 状态文件](docs/design/queue-state-file.md) |
| `JournalManager` + `JournalLog` + `JournalQueue` | [Journal 变更日志](docs/design/journal.md) / [Journal 专用存储](docs/design/journal-storage.md) |

---

## 原始章节映射

原 design.md 包含 24 个章节 (3018 行), 已按主题拆解如下:

| 原章节 | 归属文档 |
|--------|---------|
| 一、整体架构 | [架构概览](docs/design/architecture.md#一整体架构) |
| 二、目录结构 | [架构概览](docs/design/architecture.md#二目录结构) |
| 二点五、DataSetMeta | [元数据格式](docs/design/meta-format.md) |
| 三、核心数据模型 | [数据模型](docs/design/data-model.md) |
| 四、核心类型定义 | [数据模型](docs/design/data-model.md#四核心类型定义) |
| 五、DataSegmentSet | [数据段管理](docs/design/data-segment.md#五datasegmentset-数据段集合) |
| 六、DataSegment | [数据段管理](docs/design/data-segment.md#六datasegment-单个数据段) |
| 七、TimeIndex | [时间索引](docs/design/time-index.md) |
| 八、DataSet | [数据集操作](docs/design/dataset-operations.md#八dataset-数据集) |
| 九、写入流程 | [数据集操作](docs/design/dataset-operations.md#九写入流程详解) |
| 十、读取流程 | [数据集操作](docs/design/dataset-operations.md#十读取流程详解) |
| 十一、Store | [Store 与 FFI](docs/design/store-and-ffi.md#十一store-存储门面) |
| 十二、FFI API | [Store 与 FFI](docs/design/store-and-ffi.md#十二ffi-api) |
| 十三、C 侧示例 | [Store 与 FFI](docs/design/store-and-ffi.md#十三c-侧调用示例) |
| 十四、内存管理 | [内存与并发](docs/design/memory-and-concurrency.md#十四内存管理) |
| 十五、并发控制 | [内存与并发](docs/design/memory-and-concurrency.md#十五并发控制) |
| 十六、压缩 | [压缩策略](docs/design/compression.md) |
| 十七、后台任务 | [后台任务与缓存](docs/design/background-and-cache.md#十七后台任务) |
| 十八、读缓存池 | [后台任务与缓存](docs/design/background-and-cache.md#十八读缓存池) |
| 十九、Cargo.toml | [构建配置](docs/design/cargo-and-config.md) |
| 二十、与 TimeStore 差异 | [设计决策](docs/design/design-decisions.md#二十与-timestore-的差异) |
| 二十一、模块结构 | [架构概览](docs/design/architecture.md#二十一模块结构) |
| 二十二、关键设计决策 | [设计决策](docs/design/design-decisions.md#二十二关键设计决策) |
| 二十三、索引连续存储 | [索引连续存储](docs/design/index-continuous.md) |
| 二十四、懒分配与扩容 | [懒分配与扩容](docs/design/lazy-allocation.md) |

---

> **维护说明**: 新增设计内容时, 优先在对应的 `docs/design/*.md` 文件中追加, 保持各文档职责单一。仅在涉及多个文档的交叉设计时, 在本文档中增加索引条目。
