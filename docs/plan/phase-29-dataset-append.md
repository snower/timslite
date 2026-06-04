# Phase 29: Dataset Append API

> Goal: add a `DataSet::append` API that can create a new latest record or append bytes to the current latest tail record, with journal event `0x13` and a 4MiB single-record limit.
> Status: design completed, implementation pending.

## 29.0 Design Documents

- [x] [Data model](../design/data-model.md)
- [x] [Data segment](../design/data-segment.md)
- [x] [Dataset operations](../design/dataset-operations.md)
- [x] [Journal change log](../design/journal.md)
- [x] [Store and FFI API](../design/store-and-ffi.md)

## 29.1 Scope

- [ ] Add Rust dataset append APIs: `DataSet::append`, `DataSet::append_with_cache`, and internal `AppendOutcome`.
- [ ] Add Store/handle append route so public calls go through cache invalidation and journal hooks.
- [ ] Add FFI `tmsl_dataset_append`.
- [ ] Add journal record kind `0x13` with `index_info` and `append_info`.
- [ ] Enforce 4MiB maximum pure data length for a single record in both `write` and `append`.
- [ ] Migrate appended latest records to a single-record block when final encoded record size exceeds `BLOCK_MAX_SIZE * 70 / 100`.

## 29.2 Non-Goals

- [-] No append to old timestamps (`timestamp < latest_written_timestamp`).
- [-] No append to compressed, sealed, historical, or non-tail records.
- [-] No transaction/WAL semantics for append beyond the existing journal change-log behavior.
- [-] No compaction of the old physical record after migration; old data is counted through `invalid_record_count`.

## 29.3 Behavior Matrix

| Case | Behavior |
|------|----------|
| `timestamp < latest_written_timestamp` | Return error. |
| `timestamp > latest_written_timestamp` | Create a new record through the normal forward write path, update latest, return `AppendOutcome(data_offset=0, data_len=input.len())`, write journal `0x13`. |
| `timestamp == latest_written_timestamp`, latest entry missing/deleted/filler | Return error. |
| `timestamp == latest_written_timestamp`, target block compressed/sealed | Return error. |
| `timestamp == latest_written_timestamp`, target record is not the block and segment tail | Return error. |
| `timestamp == latest_written_timestamp`, final data length > 4MiB | Return error. |
| `timestamp == latest_written_timestamp`, final encoded record size > 70% of `BLOCK_MAX_SIZE` | Migrate whole logical record to a single-record block, update index, invalidate old cache key, increment old segment `invalid_record_count`, journal `0x13`. |
| `timestamp == latest_written_timestamp`, tail raw record below threshold | Append bytes in place, update record/block/segment size fields, keep index unchanged, journal `0x13`. |

## 29.4 Implementation Tasks

### 29.4.1 Record Size Limits

- [ ] Add a shared constant for the 4MiB single-record data limit.
- [ ] Add a shared constant or helper for the append migration threshold: `BLOCK_MAX_SIZE * 70 / 100`.
- [ ] Reject `write` inputs where `data.len() > 4MiB`.
- [ ] Reject append attempts where `old_data_len + append_len > 4MiB`.
- [ ] Use checked arithmetic for all `old_data_len + append_len`, `12 + data_len`, and size-field updates.

### 29.4.2 DataSegment Tail Append

- [ ] Add a segment-level helper to validate and append to the last pending raw record.
- [ ] Verify the target block is the segment tail and has `flags=0`.
- [ ] Verify `payload_size == uncompressed_size` for pending raw blocks.
- [ ] Verify the target record ends at `block_payload_size`.
- [ ] Verify the target record end also matches segment `wrote_position`.
- [ ] Update mmap bytes: `record.data_len`, appended data bytes, `BlockHeader.block_payload_size`, `BlockHeader.uncompressed_size`, data segment `wrote_position`, `pending_wrote_position`, and `total_uncompressed_size`.
- [ ] Keep record count and timestamp range unchanged for in-place append.

### 29.4.3 Migration Path

- [ ] Read old logical record data from the latest tail record.
- [ ] Build `old_data + append_data`.
- [ ] Create a single-record block for the full logical record.
- [ ] Update the timestamp index entry to the migrated block location.
- [ ] Increment `invalid_record_count` for the old data segment.
- [ ] Invalidate the old global cache key defensively.
- [ ] Return `AppendOutcome` with `data_offset=old_data_len`, `data_len=append_data.len()`, and `migrated=true`.

### 29.4.4 DataSet And Store API

- [ ] Add `DataSet::append` and `append_with_cache`.
- [ ] Add Store append route using existing dataset handle validation.
- [ ] Keep direct DataSet append non-journal by default, matching write/delete behavior.
- [ ] Ensure `last_used_at` updates consistently with existing write paths.
- [ ] Preserve retention semantics: expired old timestamps are not appendable; forward append may advance latest.

### 29.4.5 FFI And Wrappers

- [ ] Add `tmsl_dataset_append` to `src/ffi.rs`.
- [ ] Add declaration to `include/timslite.h`.
- [ ] Follow existing pointer/null/data length/error-buffer rules.
- [ ] If Python wrapper exposes dataset write/delete APIs, add matching `append`.

### 29.4.6 Journal 0x13

- [ ] Add `JournalRecordKind::DataAppend` or equivalent.
- [ ] Add encoder/decoder for `0x13`.
- [ ] Validate `index_info` length is exactly 18 bytes.
- [ ] Validate `append_info` length is exactly 8 bytes.
- [ ] Add `JournalManager::append_data_append`.
- [ ] Store append success writes `0x13`; append failures write no journal.
- [ ] Queue notification works through the existing journal append path.

## 29.5 Tests

- [ ] `append(timestamp > latest)` creates a new record and advances latest.
- [ ] `append(timestamp < latest)` returns error and does not write journal.
- [ ] `append(timestamp == latest)` appends bytes in place when the record is the latest uncompressed segment tail.
- [ ] Empty append is a no-op and writes no journal.
- [ ] In-place append updates `data_len`, `block_payload_size`, `uncompressed_size`, `wrote_position`, `pending_wrote_position`, and `total_uncompressed_size`.
- [ ] Append to compressed latest block returns error.
- [ ] Append to a latest timestamp whose index entry is deleted/filler returns error.
- [ ] Append to a latest record that is not segment tail returns error.
- [ ] Append crossing the 70% threshold migrates to a single-record block and updates index.
- [ ] Migrated append invalidates the old cache key and increments `invalid_record_count`.
- [ ] `write` rejects records larger than 4MiB.
- [ ] `append` rejects final logical record data larger than 4MiB.
- [ ] Journal `0x13` encodes/decodes `index_info` and `append_info`.
- [ ] Store/FFI append success writes `0x13`, including the `timestamp > latest` create-new-record case.
- [ ] Disabled journal makes append journal hook a no-op.
- [ ] Full test suite passes with `cargo test -- --test-threads=1`.
- [ ] `cargo fmt -- --check`.
- [ ] `cargo clippy --all-targets -- -D warnings`.

## 29.6 Execution Order

1. [ ] Add failing tests for record size limits and append behavior.
2. [ ] Implement shared constants and write limit.
3. [ ] Implement segment tail append helper.
4. [ ] Implement DataSet append branching and migration.
5. [ ] Implement Store and FFI append routes.
6. [ ] Implement journal `0x13` codec and hook.
7. [ ] Add wrapper support if applicable.
8. [ ] Run focused tests, then full suite/fmt/clippy.
9. [ ] Update this plan and `plan.md` completion markers.

## 29.7 Open Implementation Notes

- Empty append is defined as a no-op that does not write data and does not write journal.
- The migration threshold is based on encoded record size (`12 + data_len`) because ordinary block capacity is measured in payload bytes.
- Compressed latest block always returns error before migration logic; migration is only for a still-mutable latest tail record.
