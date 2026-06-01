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
```

### 依赖说明

| 依赖 | 版本 | 用途 |
|------|------|------|
| `memmap2` | 0.9 | 内存映射文件 I/O |
| `miniz_oxide` | 0.8 | 纯 Rust deflate 压缩/解压 |
| `log` | 0.4 | 日志门面 |
| `libc` | 0.2 | C 标准库绑定 (malloc/free) |
| `criterion` | 0.5 | 基准测试依赖 (dev-only; 当前尚未定义 bench target) |

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
cargo clippy --all-targets -- -D warnings

# 格式检查
cargo fmt -- --check
```

当前 `Cargo.toml` 尚未声明 `[[bench]]` target, 仓库也尚未创建 `benches/` 目录。`criterion` 仅作为后续性能基准的 dev-dependency 保留; 在新增基准前, 不应把 `cargo bench` 作为必过验证命令。

---

## 二十、CI 状态与建议命令

当前仓库尚未包含 `.github/workflows/` 配置。以下命令是推荐 CI/合并前验证集合, 不是已存在 workflow 的声明。

### 建议验证矩阵

| 层 | 命令 | 说明 |
|----|------|------|
| Rust 单元测试 | `cargo test --lib -- --test-threads=1` | `src/` 内 `#[cfg(test)]` 模块 |
| Rust 集成测试 | `cargo test --test integration_test -- --test-threads=1` | `tests/integration_test.rs` |
| Clippy 检查 | `cargo clippy --all-targets -- -D warnings` | 零警告 |
| 格式检查 | `cargo fmt -- --check` | 零差异 |
| Python 包装测试 | `pytest wrapper/python/tests/ -v` | PyO3 + maturin 构建后执行 |
| 基准测试 | 待补充 `benches/` + `[[bench]]` 后启用 | 当前不作为 CI 要求 |

### Python 测试构建流程

1. 安装 Python 3.9+
2. `pip install maturin pytest`
3. `maturin develop` (在 `wrapper/python/` 目录)
4. `pytest tests/ -v`

---

**相关**: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md)
