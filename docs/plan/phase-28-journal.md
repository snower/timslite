# Phase 28: Journal Change Log

> Goal: implement a configurable built-in `.journal/logs` dataset to record dataset create/drop and data write/delete operations, while supporting normal read/query access and queue-based real-time consumption.
> Status: completed, with several optional/deep validation items tracked separately.

## 28.0 Design Documents

- [x] [Journal change log](../design/journal.md)
- [x] [Store and FFI API](../design/store-and-ffi.md)
- [x] [Queue module overview and API](../design/queue-overview.md)
- [x] [Dataset operations](../design/dataset-operations.md)
- [x] [Data model](../design/data-model.md)

## 28.1 Scope And Non-Goals

### Implemented Scope

- [x] Added `src/journal/mod.rs` with journal record codec, metadata snapshot, monotonic journal timestamp, and `JournalManager`.
- [x] Added `StoreConfig.enable_journal: bool`, default `true`, with Rust builder, FFI struct/header, and Python wrapper support.
- [x] `Store::open` opens or creates the built-in `.journal/logs` dataset when journal is enabled.
- [x] Successful normal dataset `create/drop/write/delete` appends journal records `0x01/0x02/0x11/0x12`.
- [x] `.journal/logs` supports controlled read-only open and `read/query/query_iter/latest_timestamp/open_queue`.
- [x] `open_journal_queue()` and `.journal/logs` queue poll support real-time consumption.
- [x] External create/write/delete/drop of `.journal/logs` is rejected.
- [x] External `push` to journal queue is rejected.
- [x] Rust, FFI header, and Python wrapper configuration are synchronized.

### Non-Goals

- [-] No WAL, transaction, commit marker, or two-phase commit.
- [-] No guarantee that journal mmap pages are persisted before or after business dataset pages.
- [-] No journal retention/checkpoint/compaction in this phase.
- [-] Journal does not replace full scan, backup, or strict crash recovery.
- [x] `.journal/logs` internal operations are not recorded recursively.

## 28.2 Core Record Format And Codec

### Implementation

- [x] Created `src/journal/mod.rs`.
- [x] Exported `JournalRecord`, `JournalRecordKind`, `JournalIndexInfo`, `JOURNAL_DATASET_NAME`, and `JOURNAL_DATASET_TYPE`.
- [x] Defined log types `0x01`, `0x02`, `0x11`, and `0x12`.
- [x] Implemented outer payload format: `log_type:u8 + length:u16 LE + TLV bytes`.
- [x] Implemented TLV format: `type:u8 + length:u16 LE + value`.
- [x] Implemented create/drop/data-write/data-delete record constructors and encoder.
- [x] Implemented decoder with outer length validation.
- [x] Unknown log type returns `JournalRecordKind::Unknown(u8)`.
- [x] Unknown TLV types are skipped.
- [x] Required TLV fields and fixed 18-byte `index_info` are validated.
- [x] Length overflow returns `TmslError::InvalidData`.
- [x] On-disk integers use Little Endian.
- [x] `index_info.timestamp` uses `i64 LE`.
- [x] `index_info.block_offset` uses `u64 LE`.
- [x] `index_info.in_block_offset` uses `u16 LE`.

### Tests

- [x] `test_journal_encode_decode_data_write`.
- [x] `test_journal_decode_rejects_truncated_outer_length`.
- [x] Create/drop/data-delete codec paths are covered by integration test `t28_4_create_write_delete_drop_are_recorded`.
- [ ] Add dedicated unit test: `test_journal_encode_decode_create`.
- [ ] Add dedicated unit test: `test_journal_encode_decode_drop`.
- [ ] Add dedicated unit test: `test_journal_encode_decode_data_delete`.
- [ ] Add dedicated unit test: `test_journal_decode_rejects_truncated_tlv`.
- [ ] Add dedicated unit test: `test_journal_decode_skips_unknown_tlv`.
- [ ] Add dedicated unit test: `test_journal_encode_rejects_oversized_tlv_value`.

## 28.3 StoreConfig And FFI Config

### Implementation

- [x] `StoreConfig` has `enable_journal: bool`.
- [x] `StoreConfig::default()` sets `enable_journal=true`.
- [x] `StoreConfigBuilder` has `enable_journal: Option<bool>`.
- [x] `StoreConfigBuilder::enable_journal(enable: bool)` is implemented.
- [x] `DataSetConfig::from_store()` does not inherit journal state because journal is Store-level.
- [x] `TmslStoreConfigFFI` has `enable_journal: u8`.
- [x] `TMSL_STORE_CONFIG_FFI_VERSION` was bumped.
- [x] `store_config_to_ffi()` writes `enable_journal`.
- [x] `store_config_from_ffi()` reads `enable_journal`.
- [-] Old FFI config versions are not accepted; this phase does not preserve old-version config compatibility.
- [x] `include/timslite.h` is synchronized.
- [x] Python wrapper exposes `enable_journal`.

### Tests

- [x] Rust default config enables journal.
- [x] Rust builder can disable journal.
- [x] FFI config struct/header compile and clippy checks pass.
- [x] Python wrapper config tests cover default and explicit `enable_journal`.
- [ ] Add dedicated FFI test for `tmsl_store_config_default.enable_journal`.
- [ ] Add dedicated FFI test for disabled `enable_journal`.

## 28.4 JournalManager Lifecycle

### Implementation

- [x] Implemented `JournalManager::Enabled` and `JournalManager::Disabled`.
- [x] Implemented `open_or_create(data_dir, config)`.
- [x] Implemented `is_enabled()`.
- [x] Implemented read-only dataset access for `.journal/logs`.
- [x] Implemented `open_queue()`.
- [x] Implemented `flush()` and `close()` integration.
- [x] Journal path is fixed as `{data_dir}/.journal/logs`.
- [x] Journal dataset uses `index_continuous=false`.
- [x] Journal dataset uses `retention_ms=0`.
- [x] Journal segment sizes and compression inherit Store defaults.
- [x] Creating `.journal/logs` does not append a recursive `0x01` journal record.
- [x] Existing journal dataset is opened from meta when enabled.
- [x] `enable_journal=false` ignores existing `.journal/logs`.
- [x] Store scanning skips `.journal`.

### Tests

- [x] `t28_2_store_open_creates_journal_by_default`.
- [x] `t28_3_disabled_journal_does_not_open_public_handle`.
- [x] Recursive internal create is covered by `t28_4_create_write_delete_drop_are_recorded` expected record sequence.
- [ ] Add dedicated test for disabled open ignoring an already existing journal directory.
- [ ] Add dedicated test proving `.journal` is not exposed as a normal writable dataset.

## 28.5 Dataset Create/Drop Journal Records

### Implementation

- [x] Metadata snapshot reads `{dataset}/meta`.
- [x] Metadata snapshot extracts bytes after the meta header.
- [x] Successful public create appends `0x01`.
- [x] Successful public drop appends `0x02`.
- [x] Public create/drop of `.journal/logs` is rejected.
- [x] Journal append failure does not roll back the already completed primary operation.
- [x] Drop does not keep the datasets registry write lock while appending journal.

### Tests

- [x] `t28_4_create_write_delete_drop_are_recorded` covers create/drop records and metadata payload presence.
- [x] `t28_5_journal_dataset_is_readonly_and_queue_push_is_rejected` covers public drop rejection for journal handle.
- [ ] Add dedicated test for failed create/drop not writing journal.
- [ ] Add dedicated test comparing metadata snapshot bytes exactly with the original meta TLV bytes.
- [ ] Add dedicated test for public create of `.journal/logs` rejection.

## 28.6 Data Write/Delete Journal Records

### Implementation

- [x] Added `WriteOutcome` and `WriteBranch`.
- [x] Added `DeleteOutcome`.
- [x] `write_with_cache_outcome` returns the final `IndexEntry`.
- [x] `delete_with_cache_outcome` returns the old real `IndexEntry`.
- [x] Existing `DataSet::write` and `DataSet::delete_with_cache` compatibility paths remain available.
- [x] Store write path appends `0x11`.
- [x] Store delete path appends `0x12`.
- [x] FFI write/delete paths go through Store hooks.
- [x] `enable_journal=false` makes hooks no-op.
- [x] Journal timestamp is `max(now_ms, last + 1)` and remains strictly increasing.
- [x] Append success notifies journal queue consumers.

### Tests

- [x] `t28_4_create_write_delete_drop_are_recorded` covers normal write, correction write, delete, and decoded index timestamp.
- [x] `t28_6_journal_queue_polls_realtime_records` covers consumer wakeup for append.
- [x] `t28_7_journal_read_query_iter_latest_and_ack_work` covers latest/read/query_iter/ack.
- [ ] Add dedicated out-of-order write journal test.
- [ ] Add dedicated failed delete no-journal test.
- [ ] Add dedicated disabled journal write/delete no-record test.
- [ ] Add dedicated monotonic journal timestamp stress test.

## 28.7 Read-Only Journal Dataset And Queue

### Implementation

- [x] Store tracks read-only journal handles.
- [x] `.journal/logs` public open is allowed only when journal is enabled.
- [x] Allowed operations: close, read, query, query_iter, latest_timestamp, open_queue.
- [x] Rejected operations: write, delete, drop, external queue push.
- [x] `Store::open_journal_queue()` is implemented.
- [x] `Store::open_queue(handle)` returns the journal queue for journal read-only handles.
- [x] `DatasetQueue` supports a read-only producer mode.
- [x] Normal queue `push()` remains writable.
- [x] Journal producer is internal to `JournalManager.append_*`.
- [x] Append reuses queue notify to wake consumers.

### Tests

- [x] `t28_5_journal_dataset_is_readonly_and_queue_push_is_rejected`.
- [x] `t28_6_journal_queue_polls_realtime_records`.
- [x] `t28_7_journal_read_query_iter_latest_and_ack_work`.
- [ ] Add dedicated test for polling existing historical journal records.
- [ ] Add dedicated checkpoint persistence test across reopen.

## 28.8 FFI And Python Wrapper Integration

### Implementation

- [x] Existing dataset FFI can open `.journal/logs` as a read-only handle.
- [x] FFI `read/query/latest_timestamp` work through the same dataset handle path.
- [x] FFI `write/delete/drop` reject journal read-only handles through Store checks.
- [x] Existing queue FFI works for journal queue paths where opened through a journal dataset handle.
- [x] `tmsl_queue_push` rejects journal queue through queue read-only producer mode.
- [-] No new convenience FFI function `tmsl_journal_queue_open` was added.
- [x] Python `StoreConfig` exposes `enable_journal`.
- [x] Python `open_dataset(".journal", "logs")` returns a read-only dataset wrapper.
- [x] Python `open_queue()` on a read-only journal dataset opens the journal queue.
- [x] Python wrapper rejects write/delete on read-only journal datasets.

### Tests

- [x] Python test suite passes after building/installing the wheel into a temporary target directory.
- [x] Python config tests cover `enable_journal`.
- [x] Python basic tests cover read-only journal write rejection.
- [ ] Add dedicated C ABI test for opening journal and reading latest.
- [ ] Add dedicated C ABI test for journal write rejection.
- [ ] Add dedicated C ABI test for journal queue poll.
- [ ] Add dedicated C ABI test for disabled journal open returning error.
- [ ] Add dedicated Python journal query decode test.
- [ ] Add dedicated Python journal queue poll test.

## 28.9 Background Tasks, Cache, And Retention

### Implementation

- [x] Journal dataset participates in explicit flush/close through `JournalManager`.
- [x] Journal dataset has `retention_ms=0`.
- [x] Normal Store dataset scanning skips `.journal`, preventing normal retention/drop handling as a business dataset.
- [x] Journal read/query can use the global `BlockCache`.
- [x] Journal append writes pending raw blocks and does not enter global cache, following existing cache rules.
- [x] Journal queue state uses existing queue state behavior.

### Tests

- [x] Full Rust test suite passes with journal enabled by default.
- [x] Existing background task tests pass with journal integrated.
- [ ] Add dedicated background flush includes journal test.
- [ ] Add dedicated manual tick flushes journal queue state test.
- [ ] Add dedicated retention does not reclaim journal test.
- [ ] Add dedicated journal queue blocks idle close test if idle-close behavior is expanded.

## 28.10 Lock Order And Failure Semantics

### Implementation

- [x] Journal append does not call Store public create/open/drop/write/delete APIs.
- [x] Journal append does not acquire a normal dataset mutex after holding the journal dataset mutex.
- [x] Drop appends journal after registry removal and without holding the registry write lock.
- [x] Primary operation is not rolled back if journal append fails.
- [x] API may return the journal append error after the primary operation has already taken effect.
- [x] Journal record append happens after the business payload/index operation succeeds.
- [x] Journal is a change log and does not provide crash-recovery completeness.

### Tests

- [x] Full Rust test suite and clippy pass without deadlocks.
- [ ] Add dedicated journal append failure after create test.
- [ ] Add dedicated drop append after registry unlock concurrency test.
- [ ] Add dedicated concurrent multi-dataset journal order test.

## 28.11 File List

### Added Files

- [x] `src/journal/mod.rs`
- [x] `tests/journal_integration_test.rs`

### Modified Files

- [x] `src/lib.rs`
- [x] `src/config.rs`
- [x] `src/dataset.rs`
- [x] `src/store.rs`
- [x] `src/queue/mod.rs`
- [x] `src/ffi.rs`
- [x] `include/timslite.h`
- [x] `tests/integration_test.rs`
- [x] `wrapper/python/src/config.rs`
- [x] `wrapper/python/src/dataset.rs`
- [x] `wrapper/python/src/store.rs`
- [x] `wrapper/python/tests/test_basic.py`
- [x] `wrapper/python/tests/test_config.py`
- [x] `docs/review/design-review-todo.md`
- [x] `plan.md`
- [x] `docs/plan/overview.md`

## 28.12 Execution Order

1. [x] Codec TDD and implementation.
2. [x] Config/FFI TDD and implementation.
3. [x] JournalManager lifecycle.
4. [x] Create/drop hook.
5. [x] Write/delete outcome and hook.
6. [x] Read-only journal dataset.
7. [x] Journal queue.
8. [x] FFI/Python config and read-only integration.
9. [x] Background/cache/retention compatibility checks.
10. [x] Review todo and plan status updates.

## 28.13 Verification Commands

```powershell
cargo fmt -- --check
cargo test journal -- --test-threads=1
cargo test -- --test-threads=1
cargo clippy --all-targets -- -D warnings
cargo test --manifest-path wrapper/python/Cargo.toml -- --test-threads=1
cargo clippy --manifest-path wrapper/python/Cargo.toml --all-targets -- -D warnings
python -m maturin build --manifest-path wrapper/python/Cargo.toml --out wrapper/python/target/wheels
python -m pip install --force-reinstall --target wrapper/python/.pytest-target wrapper/python/target/wheels/timslite-0.1.0-cp313-cp313-win_amd64.whl
$env:PYTHONPATH=(Resolve-Path wrapper/python/.pytest-target).Path; python -m pytest wrapper/python/tests -q -p no:cacheprovider
git diff --check
```

## 28.14 Acceptance Overview

- [x] `.journal/logs` is enabled by default and can be disabled with `enable_journal=false`.
- [x] All four record kinds `0x01/0x02/0x11/0x12` can be encoded, written, read, and decoded.
- [x] Successful create/drop/write/delete paths write journal records.
- [x] Failed primary operations do not write journal records.
- [x] `.journal/logs` supports read/query/query_iter/latest_timestamp.
- [x] Journal queue supports real-time poll and ack.
- [x] External journal queue push is rejected.
- [x] FFI header and Rust config structure are synchronized.
- [x] Full Rust tests, clippy, fmt, wrapper Rust tests, wrapper clippy, and Python pytest pass.

## 28.15 Remaining Follow-Ups

- [ ] Add dedicated C ABI journal read/write-reject/queue/disabled tests.
- [ ] Add dedicated Python journal query decode and queue poll tests.
- [ ] Add dedicated journal append failure semantics tests.
- [ ] Add dedicated concurrent journal ordering test.
- [ ] Add dedicated background/retention journal tests.
- [ ] Consider adding optional convenience FFI `tmsl_journal_queue_open` in a later phase if the public API needs it.

## 28.16 Implementation Record

2026-06-03: implemented Journal phase. Added `src/journal/mod.rs`, `StoreConfig.enable_journal`, built-in read-only `.journal/logs`, create/drop/write/delete hooks, real-time journal queue polling, Rust FFI header/config synchronization, and Python wrapper support.

2026-06-04: completed Python pytest verification by building a wheel with maturin, installing it into a temporary workspace target directory, setting `PYTHONPATH`, and running `python -m pytest wrapper/python/tests -q -p no:cacheprovider`. Result: 56 passed.
