# timslite

> 高性能 Rust 时序数据存储动态库 — mmap-backed, Block 级聚合, 延迟压缩, C ABI FFI

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![Tests](https://img.shields.io/badge/tests-67%20passing-brightgreen.svg)](#)

---

## 特性

- **内存映射存储**: 基于 `memmap2` 的 mmap I/O, 零拷贝读写
- **Block 级聚合**: 多条 record 聚合成 Block (最大 64KB), 减少元数据开销
- **延迟压缩**: pending Block 保持原始格式, 溢出时 seal + deflate 压缩 (miniz_oxide)
- **懒加载生命周期**: segment 按需打开, 空闲 30 分钟后自动 unmap + close, 降低文件句柄占用
- **时间索引**: 每数据集专属时间索引, 二分查找定位, 18 字节/条目
- **显式生命周期**: `create` / `open` / `close` / `drop` 分离, 参数创建后不可变
- **C ABI FFI**: 提供完整的 `extern "C"` 接口, 可被 C/C++/Go/Python 等语言调用
- **后台任务**: 单线程统一循环执行定期 flush (mmap sync) 和 idle 检查
- **纯 Rust 依赖**: 无 C 库依赖, 跨平台编译简单

## 目录结构

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
use timslite::{Store, StoreConfig};
use std::time::Duration;

// 1. 打开存储
let config = StoreConfig::builder()
    .flush_interval(Duration::from_secs(600))     // 10 分钟 flush
    .idle_timeout(Duration::from_secs(1800))       // 30 分钟 idle-close
    .data_segment_size(64 * 1024 * 1024)           // 64MB
    .index_segment_size(4 * 1024 * 1024)           // 4MB
    .compress_level(6)
    .build();

let mut store = Store::open("/data/timslite", config)?;

// 2. 创建新数据集 (参数写入 meta 文件, 之后不可修改)
let handle = store.create_dataset(
    "patient_001", "waveform",
    64 * 1024 * 1024,   // data_segment_size
    4 * 1024 * 1024,    // index_segment_size
    6,                  // compress_level (1-9)
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

单一线程统一执行两个周期性任务:

| 任务 | 默认间隔 | 行为 |
|------|----------|------|
| **Flush** | 10 分钟 | 遍历所有打开的 segment, 执行 `mmap.flush()` (MS_SYNC); 不密封/不压缩 |
| **Idle Check** | 60 秒 | 扫描 `last_used_at`, ≥30 分钟 → sync + 密封 pending + unmap + close |

**设计优势**: 动态计算下一次唤醒时间 (`min(next_flush, next_idle) - now`), 无固定轮询浪费, 单一 shutdown channel 简化资源管理。

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

- `block_offset`: Block 在数据段中的绝对偏移 (相对 HEADER_SIZE)
- `in_block_offset`: record 在 Block Payload 中的相对偏移

## 文件格式

### 文件头 (100 字节)

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
│ State 可变区 (56 bytes): 7×8 bytes                       │
│   wrote_position, record_count, total_uncompressed_size, │
│   invalid_record_count, pending_block_offset,            │
│   pending_wrote_position, pending_record_count           │
└──────────────────────────────────────────────────────────┘
```

> **HEADER_SIZE = 100 bytes**. Meta/State 分离, 支持向前兼容 (未知 TLV type 通过 length 跳过)。

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
# 67 tests passing (58 unit + 9 integration)
```

### Clippy

```bash
cargo clippy -- -D warnings
```

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
