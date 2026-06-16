# Phase 38: zstd Frame Checksum

> 目标: 新写出的 zstd 压缩 block 开启 zstd frame content checksum, 提升 compressed block 读取时的损坏检测能力。
>
> 状态: 完成。zstd compression helper 已改为写出带 content checksum 的 zstd frame, 并通过测试验证。

## 38.0 设计文档

- [x] [压缩策略](../design/compression.md)

## 38.1 格式契约

- [x] `compress_type = 0` 的新 zstd frame 必须开启 content checksum flag。
- [x] checksum 是 zstd frame 内部属性, 不新增 timslite block header、segment header 或 dataset meta 字段。
- [x] Data segment 与 Journal segment 统一通过 shared zstd compression helper 获得 checksum 行为。
- [x] 旧 zstd frame 即使没有 checksum flag 也必须继续可读。
- [x] deflate (`compress_type = 1`) 行为保持不变。

## 38.2 实现任务

- [x] 更新 `zstd_compress()` 使用 zstd encoder 并开启 checksum。
- [x] 保持 `zstd_decompress()` 使用标准 decoder。
- [x] 保持 `compress()` / `decompress()` 的 `compress_type` 分发接口不变。
- [x] 确认 DataSegment 和 JournalSegment 无需额外改动。

## 38.3 测试计划

- [x] zstd compressed frame header 显示 checksum flag 已开启。
- [x] zstd roundtrip 仍可解压为原始数据。
- [x] deflate roundtrip 与 compressed bytes 不受 zstd checksum 改动影响。
- [x] `compress(..., COMPRESS_TYPE_ZSTD)` 走 checksum-enabled zstd path。

## 38.4 验证命令

```bash
cargo fmt -- --check
cargo test compress -- --test-threads=1
cargo test -- --test-threads=1
cargo check
git diff --check
```

---

## 任务清单

> 以下为 `plan.md` 中 Phase 38 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — `docs/design/compression.md`
- [x] 实现 — zstd encoder 开启 content checksum, shared compression helper 统一生效
- [x] 测试 — zstd frame header checksum flag、zstd roundtrip、deflate 不受影响
- [x] 验证 — `cargo fmt -- --check`, `cargo test compress -- --test-threads=1`, `cargo test -- --test-threads=1`, `cargo check`, `git diff --check`
