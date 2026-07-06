# 构建配置

## 十九、Cargo.toml

当前 crate:

```toml
[package]
name = "timslite"
version = "0.1.3"
edition = "2021"

[lib]
name = "timslite"
crate-type = ["cdylib", "rlib"]

[dependencies]
memmap2 = "0.9"
miniz_oxide = "0.8"
log = "0.4"
libc = "0.2"
zstd = "0.13"

[dev-dependencies]
criterion = "0.5"
proptest = "1"
```

依赖说明:

| 依赖 | 用途 |
|---|---|
| `memmap2` | mmap-backed data/index/queue state/dataset state 文件 |
| `miniz_oxide` | deflate 压缩/解压支持 |
| `zstd` | 默认 zstd 压缩/解压支持 |
| `log` | 日志门面 |
| `libc` | C ABI 内存分配/释放边界 |
| `criterion` | 性能基准 dev-dependency |
| `proptest` | 属性测试 dev-dependency |

## 二十、仓库结构与验证状态

当前仓库已经包含:

- `.github/workflows/ci.yml`: GitHub Actions CI。
- `benches/`: benchmark 目录已存在, 但当前没有 benchmark 源文件, `Cargo.toml` 也未声明 `[[bench]]` target。
- `src/`, `tests/`, `include/`, `wrapper/python/`, `docs/design/`, `docs/plan/`, `docs/review/`。

因此当前不应再描述为“缺少 `.github/workflows/` 或 `benches/` 目录”。同时, 在新增可运行 benchmark 前, `cargo bench` 不作为必过验证命令。

## 二十一、推荐本地验证

```bash
cargo build
cargo build --release
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
```

本仓库文件系统测试共享临时路径, 完整测试必须单线程运行。

修改 Python wrapper 时, 在本地环境支持的前提下还需要执行:

```bash
cd wrapper/python
maturin develop
python -m pytest tests/ -v
```

## 二十二、CI

当前 `.github/workflows/ci.yml` 执行:

- `cargo fmt -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --lib -- --test-threads=1`
- `cargo test --tests -- --test-threads=1`
- Python 3.9 到 3.13 的 wrapper build + pytest

CI 与本地完整验证的关系:

- CI 拆分 lib/tests 以便定位失败。
- 本地交付前仍推荐执行 `cargo test -- --test-threads=1`, 覆盖 lib + integration tests。
- Benchmark 属于后续性能验证项, 不属于当前 CI 必过项。

---

相关: [架构概览](architecture.md) | [Store 与 FFI](store-and-ffi.md)
