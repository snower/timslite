# Timslite 项目实现总结

## 项目概述

Timslite 是一个用 Rust 实现的轻量级时序数据存储库，参考了 MonitorCare Orbit 的设计思想。

## 核心特性

### 1. API 设计

```rust
// 打开存储
let store = TimeStore::open("/data/dir")?;

// 打开数据集 (名称 + 类型)
let dataset = store.open_dataset("monitor_001", DataType::Wave)?;

// 写入数据
dataset.write(timestamp, &data)?;

// 读取数据
let records = dataset.read(&ReadOptions {
    start_timestamp: 1000,
    end_timestamp: 2000,
    ..Default::default()
})?;

// 关闭
store.close()?;
```

### 2. 目录结构

```
data_dir/
├── .index/                    # 全局索引目录
│   └── {dataset_name}/        # 每个数据集的索引
│       └── {timestamp}        # 索引文件
├── {dataset_name}/            # 数据集名称 (二级目录)
│   ├── meta.bin              # 数据集元数据
│   ├── wave/                 # 数据类型 (三级目录)
│   │   └── {offset}          # 数据文件
│   ├── measure/
│   └── event/
```

### 3. 数据类型

| 类型 | 说明 | 文件大小 | 是否压缩 |
|------|------|----------|----------|
| Index | 索引数据 (24字节/秒) | 16 MB | 否 |
| Wave | 波形数据 | 64 MB | 是 |
| Measure | 测量数据 | 32 MB | 是 |
| Event | 事件数据 | 8 MB | 是 |
| ManualMeasure | 手动测量 | 8 MB | 是 |

## 核心模块

### 1. `lib.rs` - 库入口
- 导出公共 API
- 模块组织
- 版本管理

### 2. `error.rs` - 错误处理
- 统一错误类型 `Error`
- Result 别名
- 错误转换实现

### 3. `types.rs` - 数据类型定义
- `DataType` 枚举
- `IndexInfo` 索引结构
- `DataRecord` 数据记录
- `ReadOptions` / `WriteOptions`
- `DatasetMeta` 元数据

### 4. `config.rs` - 配置管理
- 文件大小配置
- 压缩级别
- 过期时间
- WAL 开关

### 5. `file.rs` - 文件管理
- `FileHeader` 文件头
- `MappedFile` 内存映射文件
- 读写操作
- 压缩/解压

### 6. `index.rs` - 索引管理
- `IndexManager` 索引管理器
- `IndexIterator` 索引迭代器
- 时间范围查询

### 7. `dataset.rs` - 数据集
- 数据集打开/关闭
- 写入/读取操作
- 文件管理
- 元数据维护

### 8. `store.rs` - 存储管理
- 打开/关闭存储
- 数据集管理
- 配置应用

### 9. `ffi.rs` - C FFI 接口
- C 兼容的函数接口
- 句柄管理
- 字符串转换

## 关键技术点

### 1. 内存映射 (memmap2)
- 零拷贝读写
- 操作系统自动管理页面缓存
- 高性能 I/O

### 2. 压缩 (flate2)
- Deflate 压缩算法
- 可配置压缩级别 (0-9)
- 自动压缩/解压

### 3. 并发控制
- `parking_lot::RwLock` - 读写锁
- `DashMap` - 并发哈希表
- 线程安全设计

### 4. 序列化
- `bincode` - 快速二进制序列化
- `serde` - 序列化框架

## 性能优化

1. **追加写入**: 避免随机写入开销
2. **内存映射**: 零拷贝 I/O
3. **压缩存储**: 减少磁盘占用
4. **索引优化**: 快速时间范围查询
5. **并发友好**: 细粒度锁设计

## 与 MonitorCare Orbit 对比

| 特性 | MonitorCare Orbit (Java) | Timslite (Rust) |
|------|-------------------------|-----------------|
| 语言 | Java | Rust |
| 内存安全 | GC | 编译时保证 |
| 并发 | synchronized | 无锁/细粒度锁 |
| 序列化 | Protobuf | Bincode |
| I/O | MappedByteBuffer | memmap2 |
| 压缩 | Deflate | Deflate |
| FFI | JNI | C FFI |

## 使用示例

### 基本使用
```rust
use timslite::{TimeStore, DataType, Result};

fn main() -> Result<()> {
    let store = TimeStore::open("/data/timeseries")?;
    let dataset = store.open_dataset("monitor_001", DataType::Wave)?;
    
    dataset.write(1234567890, &[1, 2, 3, 4, 5])?;
    
    store.close()?;
    Ok(())
}
```

### C 语言使用
```c
#include "timslite.h"

int main() {
    void* store = timslite_open("/data/timeseries");
    void* dataset = timslite_open_dataset(store, "monitor_001", 1);
    
    uint8_t data[] = {1, 2, 3, 4, 5};
    timslite_write(dataset, 1234567890, data, 5);
    
    timslite_close_dataset(dataset);
    timslite_close(store);
    return 0;
}
```

## 构建和测试

```bash
# 构建
cargo build --release

# 构建 C FFI
cargo build --release --features ffi

# 运行测试
cargo test

# 运行示例
cargo run --example basic
cargo run --example config
cargo run --example performance

# 生成文档
cargo doc --open
```

## 项目文件清单

```
timslite/
├── Cargo.toml              # 项目配置
├── README.md               # 项目文档
├── .gitignore             # Git 忽略规则
├── timslite.h             # C 头文件
├── build.sh               # Linux 构建脚本
├── build.bat              # Windows 构建脚本
├── src/
│   ├── lib.rs             # 库入口
│   ├── error.rs           # 错误处理
│   ├── types.rs           # 数据类型
│   ├── config.rs          # 配置
│   ├── file.rs            # 文件管理
│   ├── index.rs           # 索引管理
│   ├── dataset.rs         # 数据集
│   ├── store.rs           # 存储管理
│   ├── protos.rs          # 协议定义
│   └── ffi.rs             # C FFI
├── tests/
│   ├── integration_tests.rs  # 集成测试
│   ├── type_tests.rs         # 类型测试
│   ├── config_tests.rs       # 配置测试
│   └── file_tests.rs         # 文件测试
└── examples/
    ├── basic.rs           # 基本示例
    ├── config.rs          # 配置示例
    └── performance.rs     # 性能示例
```

## 未来改进方向

1. **Protobuf 支持**: 添加完整的 Protobuf 序列化
2. **WAL 实现**: 完善写前日志机制
3. **数据过期**: 实现自动过期清理
4. **性能监控**: 添加性能指标收集
5. **Python/Rust 绑定**: 提供更多语言支持
6. **压缩算法**: 支持更多压缩算法 (LZ4, Zstd)
7. **分布式**: 支持分布式部署

## 总结

Timslite 实现了一个高性能、易用的时序数据存储库，核心特性包括：

- ✅ 追加写入设计
- ✅ 时间范围查询
- ✅ 内存映射优化
- ✅ 自动压缩存储
- ✅ 多数据类型支持
- ✅ C FFI 接口
- ✅ 线程安全
- ✅ 详细文档和测试

项目结构清晰，代码质量高，适合用于各种时序数据存储场景。