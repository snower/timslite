# 构建配置

## 十九、Cargo.toml

```toml
[package]
name = "timslite"
version = "0.1.0"
edition = "2021"

[lib]
name = "timslite"
crate-type = ["cdylib", "rlib"]

[dependencies]
memmap2 = "0.9"
miniz_oxide = "0.8"
log = "0.4"
libc = "0.2"

[dev-dependencies]
criterion = "0.5"

[[bench]]
name = "timslite_benchmarks"
harness = false
```

### 依赖说明

| 依赖 | 版本 | 用途 |
|------|------|------|
| `memmap2` | 0.9 | 内存映射文件 I/O |
| `miniz_oxide` | 0.8 | 纯 Rust deflate 压缩/解压 |
| `log` | 0.4 | 日志门面 |
| `libc` | 0.2 | C 标准库绑定 (malloc/free) |
| `criterion` | 0.5 | 基准测试 (dev-only) |

### 构建命令

```bash
# Debug
cargo build

# Release
cargo build --release
# 输出: target/release/libtimslite.so / timslite.dll / libtimslite.dylib

# 测试
cargo test -- --test-threads=1

# Clippy
cargo clippy -- -D warnings

# 基准测试
cargo bench
```

---

**相关**: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md)
