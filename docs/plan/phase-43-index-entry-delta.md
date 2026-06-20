# Phase 43: Index Entry Timestamp Delta

## 目标

将 index segment 的 on-disk `IndexEntry` 从 18 字节改为 14 字节:

- `timestamp_delta: u32 LE`，表示 `timestamp - segment.start_timestamp`
- `block_offset: u64 LE`
- `in_block_offset: u16 LE`

内存态、Rust API、FFI、Python wrapper 和 JournalRecord 的 `JournalIndexInfo` 继续使用完整 `timestamp: i64` 语义。

## 范围

- 破坏性 index entry 磁盘格式调整；当前项目尚未首次 release，文件 header version 仍保持 1。
- 新写入 index segment header version 仍为 1。
- 不保留旧 18 字节 index entry 读取逻辑；同一 data_dir 内的旧开发期 index 文件需要删除或重建。
- 连续模式 `segment_capacity` 改为 `floor((index_segment_size - 128) / 14)`。
- 非连续模式在最新 index segment 未满但 timestamp delta 超过 `u32::MAX` 时创建新 segment。

## Checklist

- [x] `IndexEntry` on-disk size 改为 14 字节。
- [x] `IndexSegment` 写入、覆盖、二分查找、直接查找、range query 和 last timestamp 恢复支持 delta decode。
- [x] `TimeIndex` 非连续写入按 u32 delta 范围切新 index segment。
- [x] `IndexFileMetadata` 继续写 index version 1，不因本次开发期格式调整提升版本号。
- [x] `DataFileMetadata` 保持 data segment v1，不随 index 格式升级变化。
- [x] 保持 `JournalIndexInfo` 18 字节格式不变。
- [x] 更新 `design.md` 和 `docs/design/*` 中的 index entry 格式说明。
- [x] 增加 delta 格式、delta 越界、非连续切段和 index header version 保持 1 的测试。

## 验证

- `cargo test delta -- --test-threads=1`
- `cargo test index:: -- --test-threads=1`
- `cargo test test_index_header_version_remains_v1 -- --test-threads=1`
- `cargo fmt -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test -- --test-threads=1`
- `git diff --check`
