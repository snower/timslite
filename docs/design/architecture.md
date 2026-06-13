# timslite - 整体架构

> 目标: Rust 动态库(`cdylib`), 提供 FFI 可调用 C ABI
> 核心: 按数据集名称+类型分段 + 内存映射(mmap) + 时间索引 + Block 延迟压缩 + 懒加载生命周期

## 一、整体架构

```
libtimslite (CDylib)
│
├── Store              (门面 - data_dir 级别)
│   │
│   ├── DataSet        (数据集 - (name, type) 级别)
│   │   │
│   │   ├───DataSegment       (单个数据文件, Mmap-backed, 含多个 Block)
│   │   ├───DataSegmentSet    (同类型数据文件集合)
│   │   │
│   │   └───TimeIndex         (当前数据集的专属时间索引)
│   │       │
│   │       └───IndexSegment  (单个索引文件, Mmap-backed)
│   │
│   └── JournalManager (内置 .journal/logs 专用 append log)
└── FFI                (extern "C" API)
```

**核心设计思想**: 多条 record 聚合成 Block → Block 级压缩 → 时间索引指向 (block_offset, in_block_offset)。`block_offset` 是跨所有数据段的数据区逻辑全局 offset; 读文件时需先定位 `segment`, 再使用 `segment.header_len + (block_offset - segment.file_offset)` 得到物理文件偏移。

## 二、目录结构

```
{data_dir}/
├── {dataset_name_1}/
│   ├── {dataset_type_A}/
│   │   ├── meta                                     # 数据集元数据 (magic+version+meta_data_length+TLV)
│   │   ├── data/
│   │   │   ├── 00000000000000000000                  # data segment, 起始offset (20位,0填充)
│   │   │   ├── 00000000000067108864                  # offset = 64MB
│   │   │   └── 000000000000134217728
│   │   └── index/
│   │       ├── 00000000000000000000                  # 起始秒级时间戳 (20位,0填充)
│   │       └── 0000000000001700000000
│   │
│   └── {dataset_type_B}/
│       ├── meta
│       ├── data/
│       │   └── 00000000000000000000
│       └── index/
│           └── 0000000000001700000000
│
├── {dataset_name_2}/
│   └── {dataset_type_C}/
│       ├── meta
│       ├── data/
│       │   └── 00000000000000000000
│       └── index/
│
└── .journal/                                           # 内部保留 journal 根目录
    └── logs/                                           # 专用 journal append log
        ├── meta
        ├── data/
        │   └── 00000000000000000001                    # journal segment, 起始 sequence
        └── queue/
            └── {group_name}
```

### 2.1 命名规则

| 文件类型 | 目录 | 命名格式 | 示例 |
|---------|------|---------|------|
| 数据集元数据 | `{name}/{type}/` | 固定文件名 `meta` | `{name}/{type}/meta` |
| 数据段(DataSegment) | `{name}/{type}/data/` | 20位十进制, 起始字节offset, 零填充 | `00000000000000000000` |
| 索引段(IndexSegment) | `{name}/{type}/index/` | 20位十进制, 起始秒级timestamp, 零填充 | `0000000000001700000000` |

`dataset_name` 和 `dataset_type` 是目录名, 不做转义或编码。合法值必须非空且整体匹配 `^[0-9A-Za-z_-]+$`: 只允许数字、大小写英文字母、`-`、`_`。任何路径分隔符、`.`、空格、控制字符、非 ASCII 字符、Windows 保留路径写法等都不允许。`Store::create_dataset*` / `open_dataset` / `drop_dataset_by_name` 必须在拼接路径前校验; `Store::open` 扫描已有目录时只加载名称合法且包含 `meta` 的数据集目录。

例外: `.journal/logs` 是 Store 内部保留 journal append log, 不再作为普通 `DataSet` 暴露。`enable_journal=true` 时通过 Store 的 journal 专用 read/query/open_queue API 访问; 普通扫描路径应跳过它, 由 `JournalManager` 单独管理。

### 2.2 隔离保证

- 每个 `(dataset_name, dataset_type)` 拥有完全独立的 `data/` 和 `index/` 目录
- 索引文件只包含对应 `(name, type)` 的时间戳→偏移量映射
- 不同数据集名称之间文件物理隔离
- 同一名称不同类型之间文件物理隔离
- `meta` 文件是数据集创建参数的唯一持久化真源, 打开时只校验 meta 自身格式与必需字段, 不与当前 `StoreConfig` 默认值比较

### 2.3 Dataset Identifier Files

Store 根目录新增 `max_identifier`, 每个普通 dataset 目录新增与 `meta` 同级的 `identifier`:

```text
{data_dir}/
├── max_identifier
└── {dataset_name}/
    └── {dataset_type}/
        ├── identifier
        ├── meta
        ├── data/
        └── index/
```

`max_identifier` 和 `identifier` 均为十进制数字字符串。`Store::open` 扫描普通 dataset 时读取 identifier 并建立 `identifier -> (name,type)` 索引; 详细规则见 [Dataset Identifier](dataset-identifier.md)。

## 二十一、模块结构

```
src/
├── lib.rs              # 入口, re-exports: Store, StoreConfig, TmslError, Result
├── store.rs            # Store (门面, 数据集管理, 后台任务启动, 缓存池初始化)
├── dataset.rs          # DataSet (name+type 级别, sync_all/idle_close_all)
├── meta.rs             # DataSetMeta (TLV meta file, read/write/validation)
├── cache.rs            # BlockCache (全局读缓存池, LRU + idle 回收)
├── segment/
│   ├── mod.rs          # DataSegmentSet (data/ 子目录, lazy open/close)
│   └── data.rs         # DataSegment (Block 管理, lifecycle, pending recovery, read_at_index+缓存)
├── block.rs            # BlockHeader (16B, read/write/flags)
├── index/
│   ├── mod.rs          # TimeIndex (index/ 子目录, lazy open/close, query)
│   └── segment.rs      # IndexSegment (18B entries, lifecycle, binary search)
├── query/
│   ├── mod.rs          # 查询模块导出
│   ├── iter.rs         # QueryIterator + source cursor 惰性读取
│   └── hot_block.rs    # 迭代器局部 hot block 结构
├── journal/
│   ├── mod.rs          # JournalManager + facade + DataSetJournalSink impl
│   ├── record.rs       # JournalRecord encoder/decoder
│   ├── segment.rs      # JournalSegment mmap 分段, block append/read/scan
│   ├── log.rs          # JournalLog sequence registry + read/query/append
│   └── queue.rs        # JournalQueue + JournalQueueConsumer
├── header.rs           # 可变长度 FileMetadata, meta/state 分离, 运行时 header_len
├── ffi.rs              # extern "C" (catch_unwind, opaque handles, memory mgmt)
├── error.rs            # TmslError enum + From impls
├── compress.rs         # deflate_compress/decompress + size comparison
├── config.rs           # StoreConfig + StoreConfigBuilder + DataSetConfig (internal)
├── util.rs             # endian helpers, mmap read/write macros
└── bg/
    └── mod.rs          # BackgroundTasks (flush + idle + 缓存回收 + retention, 线程/手动 tick)
```

---

**相关设计文档**:
- [元数据格式](meta-format.md) — DataSetMeta TLV 格式与验证
- [数据模型](data-model.md) — Record, Block, IndexEntry, FileMetadata
- [数据段管理](data-segment.md) — DataSegmentSet + DataSegment 生命周期
- [时间索引](time-index.md) — TimeIndex + IndexSegment 实现
- [数据集操作](dataset-operations.md) — DataSet 生命周期 + 写入/读取流程
- [Store 与 FFI](store-and-ffi.md) — Store 门面 + FFI API
- [后台任务与缓存](background-and-cache.md) — 后台循环 + BlockCache
- [内存与并发](memory-and-concurrency.md) — 内存管理 + 并发控制
- [压缩策略](compression.md) — Block 级延迟压缩
- [设计决策](design-decisions.md) — 关键决策 + 与 TimeStore 差异
- [索引连续存储](index-continuous.md) — 连续模式稀疏 filler + 逻辑空洞
- [懒分配与扩容](lazy-allocation.md) — 分段文件懒分配 + 倍率扩容
- [Journal 变更日志](journal.md) — 专用 `.journal/logs` append log + 操作日志格式 + queue 实时消费
- [Journal 专用存储](journal-storage.md) — JournalSegment/JournalLog/JournalQueue 底层存储
- [数据集 Inspect](dataset-inspect.md) — DataSetInfo (不变配置) + DataSetState (可变状态) 完整字段定义
- [构建配置](cargo-and-config.md) — Cargo.toml 依赖
