# timslite - 整体架构

> 目标: Rust 动态库(`cdylib`), 提供 FFI 可调用 C ABI
> 核心: 按数据集名称+类型分段 + 内存映射(mmap) + 时间索引 + Block 延迟压缩 + 懒加载生命周期

## 一、整体架构

```
libtimslite (CDylib)
│
├── Store              (门面 - data_dir 级别)
│   │
│   └── DataSet        (数据集 - (name, type) 级别)
│       │
│       ├───DataSegment       (单个数据文件, Mmap-backed, 含多个 Block)
│       ├───DataSegmentSet    (同类型数据文件集合)
│       │
│       └───TimeIndex         (当前数据集的专属时间索引)
│           │
│           └───IndexSegment  (单个索引文件, Mmap-backed)
└── FFI                (extern "C" API)
```

**核心设计思想**: 多条 record 聚合成 Block → Block 级压缩 → 时间索引指向 (block_offset, in_block_offset)

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
└── {dataset_name_2}/
    └── {dataset_type_C}/
        ├── meta
        ├── data/
        │   └── 00000000000000000000
        └── index/
```

### 2.1 命名规则

| 文件类型 | 目录 | 命名格式 | 示例 |
|---------|------|---------|------|
| 数据集元数据 | `{name}/{type}/` | 固定文件名 `meta` | `{name}/{type}/meta` |
| 数据段(DataSegment) | `{name}/{type}/data/` | 20位十进制, 起始字节offset, 零填充 | `00000000000000000000` |
| 索引段(IndexSegment) | `{name}/{type}/index/` | 20位十进制, 起始秒级timestamp, 零填充 | `0000000000001700000000` |

### 2.2 隔离保证

- 每个 `(dataset_name, dataset_type)` 拥有完全独立的 `data/` 和 `index/` 目录
- 索引文件只包含对应 `(name, type)` 的时间戳→偏移量映射
- 不同数据集名称之间文件物理隔离
- 同一名称不同类型之间文件物理隔离
- `meta` 文件唯一标识数据集参数, 打开时校验一致性

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
├── header.rs           # DataFileMetadata (116B) + IndexFileMetadata (52B), meta/state 分离
├── ffi.rs              # extern "C" (catch_unwind, opaque handles, memory mgmt)
├── error.rs            # TmslError enum + From impls
├── compress.rs         # deflate_compress/decompress + size comparison
├── config.rs           # StoreConfig + StoreConfigBuilder + DataSetConfig (internal)
├── util.rs             # endian helpers, mmap read/write macros
└── bg/
    └── mod.rs          # BackgroundTasks (flush + idle + 缓存回收, 单线程统一循环)
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
- [索引连续存储](index-continuous.md) — 连续模式 + Filler 机制
- [懒分配与扩容](lazy-allocation.md) — 分段文件懒分配 + 倍率扩容
- [构建配置](cargo-and-config.md) — Cargo.toml 依赖
