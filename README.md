# timslite

> 高性能 Rust 时序数据存储动态库 — mmap-backed, Block 级聚合, 延迟压缩, C ABI FFI

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![Tests](https://img.shields.io/badge/tests-90%20passing-brightgreen.svg)](#)

---

## 特性

- **内存映射存储**: 基于 `memmap2` 的 mmap I/O, 零拷贝读写
- **Block 级聚合**: 多条 record 聚合成 Block (最大 64KB), 减少元数据开销
- **延迟压缩**: pending Block 保持原始格式, 溢出时 seal + deflate 压缩 (miniz_oxide)
- **懒加载生命周期**: segment 按需打开, 空闲 30 分钟后自动 unmap + close, 降低文件句柄占用
- **时间索引**: 每数据集专属时间索引, 二分查找定位, 18 字节/条目
- **显式生命周期**: `create` / `open` / `close` / `drop` 分离, 参数创建后不可变
- **数据保留策略**: 按数据集配置 `retention_ms` 有效期, 每日定时回收过期分段, 查询自动排除过期数据
- **C ABI FFI**: 提供完整的 `extern "C"` 接口, 可被 C/C++/Go/Python 等语言调用
- **后台任务**: 单线程统一循环执行定期 flush (mmap sync)、idle 检查和数据保留回收
- **纯 Rust 依赖**: 无 C 库依赖, 跨平台编译简单

## 项目结构

```
timslite/
├── src/                    # 核心 Rust 源码
│   ├── lib.rs              # 入口, 公共 API 导出
│   ├── config.rs           # StoreConfig 配置构建器
│   ├── error.rs            # TmslError 错误类型
│   ├── store.rs            # Store 门面 (数据集注册表 + 后台任务)
│   ├── dataset.rs          # DataSet 操作 (create/open/write/query/close/drop)
│   ├── ffi.rs              # extern "C" FFI 接口
│   ├── segment/            # DataSegmentSet + DataSegment (mmap 生命周期)
│   ├── index/              # TimeIndex + IndexSegment (二分查找)
│   ├── bg/                 # BackgroundTasks (flush + idle check)
│   └── ...                 # block/cache/compress/header/meta/util
├── include/
│   └── timslite.h          # C 头文件 (FFI 函数声明)
├── tests/                  # 集成测试
├── benches/                # 性能基准测试 (待创建)
├── docs/
│   ├── design/             # 详细设计文档 (14 个专题)
│   └── plan/               # 开发计划文档 (overview + 12 phases)
├── plan.md                 # 计划状态总览 + 待完成清单
├── design.md               # 设计文档索引
└── Cargo.toml
```

## 数据集目录格式

```
{data_dir}/
├── {dataset_name_1}/
│   ├── {dataset_type_A}/
│   │   ├── meta                     # 不可变配置 (magic+version+TLV, 仅创建时写入)
│   │   ├── data/                    # 数据段目录
│   │   │   ├── 00000000000000000000  # 起始 offset=0 (20 位十进制, 零填充)
│   │   │   ├── 00000000000067108864  # 起始 offset=64MB
│   │   │   └── 000000000000134217728
│   │   └── index/                   # 索引段目录
│   │       ├── 00000000000000000000  # 起始 timestamp=0 (20 位十进制)
│   │       └── 0000000000001700000000
│   │
│   └── {dataset_type_B}/
│       ├── meta
│       ├── data/
│       └── index/
│
└── {dataset_name_2}/
    └── {dataset_type_C}/
```

### 隔离保证

- 每个 `(dataset_name, dataset_type)` 拥有完全独立的 `data/` 和 `index/` 目录
- 不同数据集名称/类型之间文件物理隔离
- `meta` 文件记录不可变参数, 打开时校验一致性

## 快速开始

### Rust API

```toml
# Cargo.toml
[dependencies]
timslite = { path = "path/to/timslite" }
```

```rust
use timslite::{Store, StoreConfig, DataSetConfigBuilder};
use std::time::Duration;

// 1. 打开存储
let config = StoreConfig::builder()
    .flush_interval(Duration::from_secs(600))     // 10 分钟 flush
    .idle_timeout(Duration::from_secs(1800))       // 30 分钟 idle-close
    .data_segment_size(64 * 1024 * 1024)           // 64MB
    .index_segment_size(4 * 1024 * 1024)           // 4MB
    .compress_level(6)
    .retention_check_hour(2)                       // 每日凌晨 2 点执行回收
    .build();

let mut store = Store::open("/data/timslite", config)?;

// 2. 创建新数据集 (参数写入 meta 文件, 之后不可修改)
let handle = store.create_dataset_with_config(
    "patient_001", "waveform",
    Some(
        DataSetConfigBuilder::from_store(&config)
            .retention_ms(30 * 86400 * 1000) // 30 天有效期 (ms timestamps)
    ),
)?;

// 3. 写入数据
{
    let ds = store.get_dataset(&handle)?;
    let mut ds = ds.lock().unwrap();
    ds.write(1700000000, &[1, 2, 3, 4])?;
    ds.write(1700000001, &[5, 6, 7, 8])?;
}

// 4. 查询时间范围
{
    let ds = store.get_dataset(&handle)?;
    let mut ds = ds.lock().unwrap();
    let records = ds.query(1700000000, 1700000060)?;
    for (ts, data) in &records {
        println!("ts={}, data={:?}", ts, data);
    }
}

// 5. 关闭存储 (触发 flush + 关闭所有数据集)
store.close()?;
```

### C FFI API

```c
#include "timslite.h"
#include <stdio.h>

int main() {
    char err[512];

    // 1. 打开存储
    void* store = tmsl_store_open("/data/timslite", err, sizeof(err));
    if (!store) { printf("Error: %s\n", err); return 1; }

    // 2. 创建数据集
    void* ds = tmsl_dataset_create(store, "sensor", "temp",
        64ULL * 1024 * 1024,   // data_segment_size = 64MB
        4ULL * 1024 * 1024,    // index_segment_size = 4MB
        6,                     // compress_level
        0,                     // index_continuous (non-continuous)
        0,                     // retention_ms (0=no limit)
        err, sizeof(err));
    if (!ds) { printf("Error: %s\n", err); return 1; }

    // 3. 写入
    unsigned char data[] = {1, 2, 3, 4};
    tmsl_dataset_write(ds, 1700000000, data, 4, err, sizeof(err));

    // 4. 查询
    void* iter = tmsl_dataset_query(ds, 1700000000, 1700000060, err, sizeof(err));
    long ts;
    unsigned char* buf;
    size_t len;
    while (tmsl_iter_next(iter, &ts, &buf, &len, err, sizeof(err)) == 0) {
        // 处理 buf[0..len]
        tmsl_iter_free_data(buf);
    }
    tmsl_iter_close(iter);

    // 5. 关闭
    tmsl_dataset_close(ds, err, sizeof(err));
    tmsl_store_close(store, err, sizeof(err));
    return 0;
}
```

### FFI 函数列表

| 函数 | 说明 | 返回值 |
|------|------|--------|
| `tmsl_store_open` | 打开存储实例 | `*mut c_void` (NULL=失败) |
| `tmsl_store_close` | 关闭存储实例 | `0`=成功, `-1`=失败 |
| `tmsl_dataset_create` | 创建新数据集 | `*mut c_void` (NULL=失败) |
| `tmsl_dataset_open` | 打开已有数据集 | `*mut c_void` (NULL=失败) |
| `tmsl_dataset_close` | 关闭数据集 | `0`=成功, `-1`=失败 |
| `tmsl_dataset_drop` | 删除整个数据集 | `0`=成功, `-1`=失败 |
| `tmsl_dataset_flush` | 手动 flush 数据集 | `0`=成功, `-1`=失败 |
| `tmsl_dataset_write` | 写入一条记录 | `0`=成功, `-1`=失败 |
| `tmsl_dataset_delete` | 删除指定时间戳的记录 (索引标哨兵, invalid_record_count++) | `0`=成功, `-1`=失败 |
| `tmsl_dataset_query` | 查询时间范围, 返回迭代器 | `*mut c_void` (NULL=失败) |
| `tmsl_iter_next` | 获取下一条记录 | `0`=成功, `1`=无数据, `-1`=失败 |
| `tmsl_iter_free_data` | 释放 `tmsl_iter_next` 分配的数据 | void |
| `tmsl_iter_close` | 关闭并释放迭代器 | void |

> **内存所有权**: `tmsl_iter_next` 返回的数据由 `libc::malloc` 分配, C 侧必须调用 `tmsl_iter_free_data` 释放。

## 数据集生命周期

```
create → open → write/query → close
  │        │
  │        └── 从 meta 读取参数 (不可变)
  └── 写入 meta 文件 (一次性, 之后不可修改)

drop → 删除整个目录 (不可恢复)
```

| 操作 | 参数 | 行为 |
|------|------|------|
| `create` | 需传入 `data_segment_size`, `index_segment_size`, `compress_level` | 写入 meta 文件, 创建 data/ + index/ 目录; 已存在返回错误 |
| `open` | 仅 `block_max_size` (运行时参数) | 读取 meta 文件加载参数; meta 不存在返回错误 |
| `close` | 无 | flush + seal pending + unmap + close 所有 segment |
| `drop` | 无 | `remove_dir_all` 删除整个目录, 不可恢复 |

## 后台任务

单一线程统一执行四个周期性任务:

| 任务 | 默认间隔 | 行为 |
|------|----------|------|
| **Flush** | 10 分钟 | 遍历所有打开的 segment, 执行 `mmap.flush()` (MS_SYNC); 不密封/不压缩 |
| **Idle Check** | 60 秒 | 扫描 `last_used_at`, ≥30 分钟 → sync + 密封 pending + unmap + close |
| **Cache Eviction** | 60 秒 | 扫描缓存池, 超 idle 阈值的 entry → 回收 + 释放内存 → LRU 检查 |
| **Retention Reclaim** | 每日 0 点 | 扫描 retention_ms > 0 的 dataset, 删除过期分段文件 |

**设计优势**: 动态计算下一次唤醒时间 (`min(next_flush, next_idle, next_cache, next_retention) - now`), 无固定轮询浪费, 单一 shutdown channel 简化资源管理。

## 数据保留策略

### retention_ms (数据集级)

每个数据集可在创建时指定 `retention_ms` (数据有效期, 单位与 timestamp 一致, 0=不限):

```
过期阈值 = latest_written_timestamp.saturating_sub(retention_ms)
```

**回收规则**:
- **数据分段**: `closed_segments[].max_timestamp < 过期阈值` → 删除文件
- **索引分段**: `last_entry_timestamp() < 过期阈值` → 删除文件
- **查询约束**: `query` 自动钳制 start_ts 到有效期范围

### retention_check_hour (Store 级)

Store 配置 `retention_check_hour` (0-23, 默认 0=午夜), 每日在该时间点执行回收任务:

```rust
let config = StoreConfig::builder()
    .retention_check_hour(2)   // 每日凌晨 2 点执行回收
    .build();
```

**约束**: 回收期间打开的文件 (读索引最后一个 ts) 检查完成后立即释放, 不依赖 idle-close。

## Block 设计

### Block Layout (磁盘)

```
┌─────────────────────────────────────────┐
│ BlockHeader (16 bytes)                  │
│  - block_payload_size: u32              │
│  - flags: u16 (compressed, sealed, ...) │
│  - record_count: u16                    │
│  - uncompressed_size: u32               │
│  - reserved: u32                        │
├─────────────────────────────────────────┤
│ Block Payload (compressed 或 raw)       │
│  [data_len:2][ts:8][data:N] (record 1)  │
│  [data_len:2][ts:8][data:N] (record 2)  │
│  ...                                    │
└─────────────────────────────────────────┘
```

### 压缩策略

- **Pending Block**: 写入时保持原始格式, 不压缩
- **Seal 时机**: Block 溢出 (>64KB) 时, 或 idle-close 时
- **压缩判断**: 压缩后体积 < 原始体积 → 写入压缩数据 + set `COMPRESSED` flag; 否则保留原始数据
- **超大 record**: 单条 record > 64KB → 独占 Block, 立即 seal + 压缩

### IndexEntry (18 字节)

```
┌────────────────────┬────────────────────┬──────────────┐
│ timestamp: i64 (8) │ block_offset: u64  │ in_block: u16│
└────────────────────┴────────────────────┴──────────────┘
```

- `block_offset`: Block 在数据段中的绝对偏移 (相对 DATA_HEADER_SIZE)
- `in_block_offset`: record 在 Block Payload 中的相对偏移

## 文件格式

### 数据段文件头 (116 字节)

```
┌──────────────────────────────────────────────────────────┐
│ 固定前缀 (9 bytes): magic(4) + version(2) + type(1) +    │
│                     meta_length(2)                        │
├──────────────────────────────────────────────────────────┤
│ Meta 不可变 TLV 区 (33 bytes): created_at, file_offset,   │
│   file_size, compress_level                              │
├──────────────────────────────────────────────────────────┤
│ state_length: u16 (2 bytes)                              │
├──────────────────────────────────────────────────────────┤
│ State 可变区 (72 bytes): 9×8 bytes                       │
│   min_timestamp, max_timestamp, wrote_position,          │
│   record_count, total_uncompressed_size,                 │
│   pending_block_offset, pending_wrote_position,          │
│   pending_record_count, invalid_record_count             │
└──────────────────────────────────────────────────────────┘
```

### 索引段文件头 (52 字节)

```
┌──────────────────────────────────────────────────────────┐
│ 固定前缀 (9 bytes): magic(4) + version(2) + type(1) +    │
│                     meta_length(2)                        │
├──────────────────────────────────────────────────────────┤
│ Meta 不可变 TLV 区 (33 bytes): created_at, file_offset,   │
│   file_size, compress_level                              │
├──────────────────────────────────────────────────────────┤
│ state_length: u16 (2 bytes)                              │
├──────────────────────────────────────────────────────────┤
│ State 可变区 (8 bytes): 1×8 bytes                        │
│   wrote_position                                         │
└──────────────────────────────────────────────────────────┘
```

> **DATA_HEADER_SIZE = 116 bytes, INDEX_HEADER_SIZE = 52 bytes**. Meta/State 分离, 支持向前兼容 (未知 TLV type 通过 length 跳过)。数据段额外维护 min/max_timestamp 用于段级范围过滤。

### 数据集元数据 (meta 文件)

每个数据集目录下固定存在 `meta` 文件, 记录**不可变**配置参数。

```
┌────────────────────────────────────────────────────┐
│ magic: 4 bytes = "TMSM"                            │
│ version: u16 = 1                                   │
│ meta_data_length: u16                              │
├────────────────────────────────────────────────────┤
│ TLV values:                                        │
│   0x01: data_segment_size (u64 LE, 8 bytes)        │
│   0x02: index_segment_size (u64 LE, 8 bytes)       │
│   0x03: compress_level (u8, 1 byte)                │
│   0x04: create_time (i64 LE, unix ms, 8 bytes)     │
│   0x05: index_continuous (u8, 1 byte)              │
│   0x06: initial_data_segment_size (u64 LE, 8B)     │
│   0x07: initial_index_segment_size (u64 LE, 8B)    │
│   0x08: retention_ms (u64 LE, 8 bytes, 0=不限)     │
└────────────────────────────────────────────────────┘
```

## 构建

### 依赖

- Rust 2021 edition
- `memmap2` 0.9 — 内存映射
- `miniz_oxide` 0.8 — 纯 Rust deflate 压缩
- `log` 0.4 — 日志
- `libc` 0.2 — C 标准库绑定 (malloc/free)

### 编译动态库

```bash
# Debug
cargo build

# Release
cargo build --release
# 输出: target/release/libtimslite.so (Linux) / timslite.dll (Windows) / libtimslite.dylib (macOS)
```

### 运行测试

```bash
cargo test -- --test-threads=1
# 90 tests passing (81 unit + 9 integration)
```

### Clippy

```bash
cargo clippy -- -D warnings
```

### 格式化

```bash
cargo fmt -- --check
```

## 文档

- **[设计文档索引](design.md)** — 14 个专题设计文档 (`docs/design/`)，覆盖架构、数据模型、FFI、并发、压缩等
- **[开发计划](plan.md)** — 计划状态总览 + 待完成清单，详细 phase 文档在 `docs/plan/`

## 并发控制

```
Store: RwLock<HashMap>              (多读少写, 数据集注册表)
DataSet: Arc<Mutex<DataSet>>        (读写互斥, 数据集内部操作)
不同 DataSet: 完全并行               (无交叉锁)
```

- 后台线程通过读锁遍历, 写锁获取后 double-check `last_used_at` 防止竞态
- 前台写操作更新 `last_used_at` 可自动"唤醒"即将 idle-close 的数据集

## 崩溃安全

- mmap 写入已有 OS page cache 保护, crash 时最多损失 10 分钟 (flush 间隔) 内未 sync 的数据
- reopen 时检测 pending block 并安全密封 (FLAGS=SEALED, 不压缩), 不会损坏已有数据
- meta 文件创建时一次性写入, 不存在部分写入问题
- 索引和数据段独立文件, 单个文件损坏不影响其他段

## 与 TimeStore (Java) 的差异

| 对比项 | TimeStore (Java) | timslite (Rust) |
|--------|------------------|-----------------|
| 存储单元 | 单条 record | Block (多条聚合, ≤64KB) |
| 压缩粒度 | record | Block |
| 压缩时机 | 立即 | 延迟 (pending→sealed, 溢出时) |
| 内存映射 | MappedByteBuffer | memmap2::MmapMut, 懒加载/超时关闭(30min) |
| 元数据 | Protobuf | 100字节 header (meta/state 分离) |
| 索引目录 | 同级子目录 | `data/` + `index/` 独立子目录 |
| 后台线程 | 多个 | 单一线程统一循环 |

## License

MIT
