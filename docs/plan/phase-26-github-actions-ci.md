# Phase 26: GitHub Actions CI/CD

> 目标: 添加 GitHub Actions 工作流, push 代码自动执行 Rust 单元测试、集成测试、Python 包装测试, 确保每次提交质量。

## 1. CI 工作流结构

```yaml
jobs:
  rust-tests:
    steps:
      - checkout
      - rust-toolchain (clippy, rustfmt)
      - cache
      - cargo fmt -- --check
      - cargo clippy --all-targets -- -D warnings
      - cargo test --lib -- --test-threads=1
      - cargo test --test integration_test -- --test-threads=1

  python-tests:
    needs: rust-tests
    strategy:
      matrix:
        python-version: ["3.9", "3.10", "3.11", "3.12", "3.13"]
    steps:
      - checkout
      - rust-toolchain
      - setup-python
      - cache
      - create .venv + pip install maturin pytest
      - maturin develop --release
      - pytest tests/ -v
```

## 2. 触发条件

- `push`: 任意分支
- `pull_request`: master/main

## 3. 测试覆盖

| 层 | 命令 |
|----|------|
| Rust 单元测试 | `cargo test --lib -- --test-threads=1` |
| Rust 集成测试 | `cargo test --test integration_test -- --test-threads=1` |
| Clippy | `cargo clippy --all-targets -- -D warnings` |
| 格式检查 | `cargo fmt -- --check` |
| Python 包装测试 | maturin develop + pytest wrapper/python/tests/ -v |

## 4. 验收

- [x] workflow YAML 语法正确 (GitHub Actions schema 合规)
- [x] 所有测试层覆盖 (Rust lib + integration + Clippy + fmt + Python pytest)
- [x] Python 测试正确构建 (maturin develop 在 wrapper/python/ 目录, venv 前置)

## 5. 设计文档更新

- [x] `docs/design/cargo-and-config.md`: 新增 §二十 GitHub Actions CI 章节
- [x] `design.md`: 构建配置条目更新, 添加 CI 标注
