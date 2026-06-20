# Phase 43: Index Entry Timestamp Delta

## 目标

将 index segment 的 on-disk `IndexEntry` 从 18 字节改为 14 字节:

- `timestamp_delta: u32 LE`，表示 `timestamp - segment.start_timestamp`
- `block_offset: u64 LE`
- `in_block_offset: u16 LE`

内存态、Rust API、FFI、Python wrapper 和 JournalRecord 的 `JournalIndexInfo` 继续使用完整 `timestamp: i64` 语义。

## 范围

- 破坏性 index v2 磁盘格式升级，不保留 v1 index segment 读取逻辑。
- 新写入 index segment header version 为 2。
- 打开旧 v1 index segment 时返回错误，避免按 14 字节布局静默误读。
- 连续模式 `segment_capacity` 改为 `floor((index_segment_size - 128) / 14)`。
- 非连续模式在最新 index segment 未满但 timestamp delta 超过 `u32::MAX` 时创建新 segment。

## Checklist

- [x] `IndexEntry` on-disk size 改为 14 字节。
- [x] `IndexSegment` 写入、覆盖、二分查找、直接查找、range query 和 last timestamp 恢复支持 delta decode。
- [x] `TimeIndex` 非连续写入按 u32 delta 范围切新 index segment。
- [x] `IndexFileMetadata` 写 index version 2，并拒绝旧 v1 index 文件。
- [x] `DataFileMetadata` 保持 data segment v1，不随 index 格式升级变化。
- [x] 保持 `JournalIndexInfo` 18 字节格式不变。
- [x] 更新 `design.md` 和 `docs/design/*` 中的 index entry 格式说明。
- [x] 增加 delta 格式、delta 越界、非连续切段和旧版本拒绝测试。

## 验证

- `cargo test delta -- --test-threads=1`
- `cargo test index:: -- --test-threads=1`
- `cargo test test_index_header_rejects_v1_version -- --test-threads=1`
- `cargo fmt -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test -- --test-threads=1`
- `git diff --check`
