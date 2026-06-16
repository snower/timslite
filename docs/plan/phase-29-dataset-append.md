# Phase 29: Dataset Append API

> Goal: add a `DataSet::append` API that can create a new latest record or append bytes to the current latest tail record, with journal event `0x13` and a 4MiB single-record limit.
> Status: completed.

## 29.0 Design Documents

- [x] [Data model](../design/data-model.md)
- [x] [Data segment](../design/data-segment.md)
- [x] [Dataset operations](../design/dataset-operations.md)
- [x] [Journal change log](../design/journal.md)
- [x] [Store and FFI API](../design/store-and-ffi.md)

## 29.1 Scope

- [x] Add Rust dataset append APIs: `DataSet::append`, `DataSet::append_with_cache`, and internal `AppendOutcome`.
- [x] Add Store/handle append route so public calls go through cache invalidation and journal hooks.
- [x] Add FFI `tmsl_dataset_append`.
- [x] Add journal record kind `0x13` with `index_info` and `append_info`.
- [x] Enforce 4MiB maximum pure data length for a single record in both `write` and `append`.
- [x] Keep existing latest-record append as in-place tail growth only; no ratio-threshold migration to a single-record block.

## 29.2 Non-Goals

- [-] No append to old timestamps (`latest_written_timestamp = Some(latest)` and `timestamp < latest`).
- [-] No append to compressed, sealed, historical, or non-tail records.
- [-] No transaction/WAL semantics for append beyond the existing journal change-log behavior.
- [-] No migration/compaction of the old physical record for append; existing latest-record append is in-place only.

## 29.3 Behavior Matrix

| Case | Behavior |
|------|----------|
| `latest_written_timestamp = Some(latest)` and `timestamp < latest` | Return error. |
| `latest_written_timestamp is None` or `timestamp > latest_written_timestamp.unwrap()` | Create a new record through the normal forward write path, update latest, return `AppendOutcome(data_offset=0, data_len=input.len())`, write journal `0x13`. |
| `latest_written_timestamp == Some(timestamp)`, latest entry missing/deleted/filler | Return error. |
| `latest_written_timestamp == Some(timestamp)`, target block compressed/sealed | Return error. |
| `latest_written_timestamp == Some(timestamp)`, target record is not the block and segment tail | Return error. |
| `latest_written_timestamp == Some(timestamp)`, final data length > 4MiB | Return error. |
| `latest_written_timestamp == Some(timestamp)`, tail raw record and final encoded record fits current pending block | Append bytes in place, update record/block/segment size fields, keep index unchanged, journal `0x13`. |
| `latest_written_timestamp == Some(timestamp)`, final encoded record cannot fit current pending block | Return error; append does not migrate to a single-record block. |

## 29.4 Implementation Tasks

### 29.4.1 Record Size Limits

- [x] Add a shared constant for the 4MiB single-record data limit.
- [x] Remove ratio-based append migration; append uses only 4MiB logical record validation plus ordinary pending block capacity validation.
- [x] Reject `write` inputs where `data.len() > 4MiB`.
- [x] Reject append attempts where `old_data_len + append_len > 4MiB`.
- [x] Use checked arithmetic for all `old_data_len + append_len`, `12 + data_len`, and size-field updates.

### 29.4.2 DataSegment Tail Append

- [x] Add a segment-level helper to validate and append to the last pending raw record.
- [x] Verify the target block is the segment tail and has `flags=0`.
- [x] Verify `payload_size == uncompressed_size` for pending raw blocks.
- [x] Verify the target record ends at `block_payload_size`.
- [x] Verify the target record end also matches segment `wrote_position`.
- [x] Update mmap bytes: `record.data_len`, appended data bytes, `BlockHeader.block_payload_size`, `BlockHeader.uncompressed_size`, data segment `wrote_position`, `pending_wrote_position`, and `total_uncompressed_size`.
- [x] Keep record count and timestamp range unchanged for in-place append.

### 29.4.3 Removed Migration Path

- [x] Do not read/build `old_data + append_data` for ratio-threshold migration.
- [x] Do not create a single-record block for existing latest-record append.
- [x] Do not update the timestamp index entry for existing latest-record append; the record start position remains stable.
- [x] Do not increment `invalid_record_count` or invalidate cache for append-only growth.
- [x] Return `AppendOutcome` with `data_offset=old_data_len` and `data_len=append_data.len()`.

### 29.4.4 DataSet And Store API

- [x] Add `DataSet::append` and `append_with_cache`.
- [x] Add Store append route using existing dataset handle validation.
- [x] Keep direct DataSet append non-journal by default, matching write/delete behavior.
- [x] Ensure `last_used_at` updates consistently with existing write paths.
- [x] Preserve retention semantics: expired old timestamps are not appendable; forward append may advance latest.

### 29.4.5 FFI And Wrappers

- [x] Add `tmsl_dataset_append` to `src/ffi.rs`.
- [x] Add declaration to `include/timslite.h`.
- [x] Follow existing pointer/null/data length/error-buffer rules.
- [x] If Python wrapper exposes dataset write/delete APIs, add matching `append`.

### 29.4.6 Journal 0x13

- [x] Add `JournalRecordKind::DataAppend` or equivalent.
- [x] Add encoder/decoder for `0x13`.
- [x] Validate `index_info` length is exactly 18 bytes.
- [x] Validate `append_info` length is exactly 8 bytes.
- [x] Add `JournalManager::append_data_append`.
- [x] Store append success writes `0x13`; append failures write no journal.
- [x] Queue notification works through the existing journal append path.

## 29.5 Tests

- [x] `append(timestamp > latest)` creates a new record and advances latest.
- [x] `append(timestamp < latest)` returns error and does not write journal.
- [x] `append(timestamp == latest)` appends bytes in place when the record is the latest uncompressed segment tail.
- [x] Empty append is a no-op and writes no journal.
- [x] In-place append updates `data_len`, `block_payload_size`, `uncompressed_size`, `wrote_position`, `pending_wrote_position`, and `total_uncompressed_size`.
- [x] Append to compressed latest block returns error.
- [x] Append to a latest timestamp whose index entry is deleted/filler returns error.
- [x] Append to a latest record that is not segment tail returns error.
- [x] Large existing latest-record append remains in-place when the pending block still has capacity.
- [x] Append that exceeds the current pending block capacity returns error and does not update index or `invalid_record_count`.
- [x] `write` rejects records larger than 4MiB.
- [x] `append` rejects final logical record data larger than 4MiB.
- [x] Journal `0x13` encodes/decodes `index_info` and `append_info`.
- [x] Store/FFI append success writes `0x13`, including the `timestamp > latest` create-new-record case.
- [x] Disabled journal makes append journal hook a no-op.
- [x] Full test suite passes with `cargo test -- --test-threads=1`.
- [x] `cargo fmt -- --check`.
- [x] `cargo clippy --all-targets -- -D warnings`.

## 29.6 Execution Order

1. [x] Add failing tests for record size limits and append behavior.
2. [x] Implement shared constants and write limit.
3. [x] Implement segment tail append helper.
4. [x] Implement DataSet append branching without ratio-threshold migration.
5. [x] Implement Store and FFI append routes.
6. [x] Implement journal `0x13` codec and hook.
7. [x] Add wrapper support if applicable.
8. [x] Run focused tests, then full suite/fmt/clippy.
9. [x] Update this plan and `plan.md` completion markers.

## 29.8 Implementation Record

2026-06-04: implemented dataset append API, tail raw record append, 4MiB record limit for write/append, Store/FFI append route, journal `0x13`, Python wrapper append, and tests. Verification passed: `cargo test append -- --test-threads=1`, `cargo test -- --test-threads=1`, `cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --manifest-path wrapper/python/Cargo.toml`, `cargo clippy --manifest-path wrapper/python/Cargo.toml --all-targets -- -D warnings`, and Python pytest via a locally built maturin wheel (`57 passed`).

## 29.7 Open Implementation Notes

- Empty append is defined as a no-op that does not write data and does not write journal.
- Append no longer has ratio-based migration. Existing latest-record append is either in-place tail growth or an error.
- Compressed latest block always returns error.

---

## 任务清单

> 以下为 `plan.md` 中 Phase 29 的完成任务详情, 已合并到此文档。

- [x] 设计文档 — append 行为、4MiB 上限、无比例迁移阈值、journal `0x13`
- [x] 测试 — append 行为矩阵、原地增长、错误路径、journal 编解码与 Store/FFI hook
- [x] 实现 — DataSegment tail append、DataSet append、Store/FFI API、journal `0x13`
- [x] 验证 — `cargo test -- --test-threads=1`, `cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings`
