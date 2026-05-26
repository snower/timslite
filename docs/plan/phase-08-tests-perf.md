# Phase 8: 集成测试 + 性能调优

**目标**: 完整集成测试套件, 性能达标, 内存安全验证

---

## 8.1 端到端集成测试

- 完整生命周期: create → write → query → close → open → query → drop
- 多数据集并行: 同时操作多个 (name, type) 组合, 验证数据完全隔离
- 异常情况: 不存在数据集 open, 已存在数据集 create, 参数验证

## 8.2 单元测试补全

- 覆盖所有模块的边界条件
- error.rs 所有错误类型展示
- util.rs 所有字节转换函数
- header.rs meta/state roundtrip
- block.rs flags 测试
- compress.rs 压缩/解压 roundtrip + should_use_compressed

## 8.3 性能基准测试 (benches/)

- 写入吞吐: 100K records/sec 目标
- 查询延迟: 1M 条目中查询 10K 条目的延迟
- 内存占用: 打开 N 个数据集后的内存使用 (idle-close 效果)
- 压缩效果: 不同类型数据的压缩率

## 8.4 内存安全验证

- valgrind 或等效工具扫描
- 确认无内存泄漏 (mmap 生命周期正确, cache 回收正常)
- FFI 内存所有权: malloc/free 配对验证

## 8.5 文档

- crate 级文档 (`//!`)
- 所有 public API 的 doc comments (`///`)
- README.md: 快速开始, FFI 示例, 目录结构说明

## 验收标准

- [x] `cargo test` 覆盖率 ≥ 80%
- [x] 所有集成测试 pass (含 create/open/drop 生命周期测试)
- [x] 无内存泄漏 (valgrind clean 或等效)
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo doc` 无 warning
- [x] README.md 完整

---

**导航**: [← Phase 7](phase-07-ffi.md) | [→ Phase 9](phase-09-blockcache.md)
