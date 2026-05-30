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

## 二十、GitHub Actions CI

### Workflow 触发

- **push**: 推送到任意分支时自动执行
- **pull_request**: PR 到 `master`/`main` 分支时自动执行

### 测试矩阵

| 层 | 命令 | 说明 |
|----|------|------|
| Rust 单元测试 | `cargo test --lib -- --test-threads=1` | `src/` 内 `#[cfg(test)]` 模块 |
| Rust 集成测试 | `cargo test --test integration_test -- --test-threads=1` | `tests/integration_test.rs` |
| Clippy 检查 | `cargo clippy --all-targets -- -D warnings` | 零警告 |
| 格式检查 | `cargo fmt -- --check` | 零差异 |
| Python 包装测试 | `pytest wrapper/python/tests/ -v` | PyO3 + maturin 构建后执行 |

### Python 测试构建流程

1. 安装 Python 3.9+
2. `pip install maturin pytest`
3. `maturin develop` (在 `wrapper/python/` 目录)
4. `pytest tests/ -v`

---

**相关**: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md)
