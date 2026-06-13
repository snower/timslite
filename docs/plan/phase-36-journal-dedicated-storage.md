# Journal Dedicated Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace the current dataset-backed journal with a dedicated index-free append log that stores journal records in sequence-addressed data segments and exposes dedicated journal read/query/queue APIs.

**Architecture:** Implement `JournalSegment`, `JournalLog`, and `JournalQueue` under `src/journal/`. The record codec remains responsible only for TLV encode/decode; storage no longer depends on `DataSet`, `TimeIndex`, or `DatasetQueue`.

**Tech Stack:** Rust 2021, memmap2, existing `BlockHeader`, compression helpers, variable header helpers, existing queue `ConsumerStateFile`, FFI C ABI, PyO3 wrapper.

---

## Scope

In scope:

- Replace `.journal/logs` storage from standard `DataSet` to dedicated append log.
- Remove journal `index/` directory creation and use.
- Keep journal sequence starting at `1`.
- Add `next_sequence` terminology and off-by-one-safe recovery.
- Support single-record block for maximum TLV payload.
- Implement dedicated `JournalQueue`.
- Add dedicated Rust/FFI/Python journal read/query/queue APIs.

Out of scope:

- Strict WAL or transaction semantics.
- Self-contained business payload logging.
- Journal retention/checkpoint.
- Backward compatibility with the dataset-backed journal layout. The project is still in first development and may break on-disk format.

## Files

- Modify: `src/journal/mod.rs` — keep `JournalManager` facade and `DataSetJournalSink` impl.
- Create: `src/journal/record.rs` — move current `JournalRecord` codec here.
- Create: `src/journal/segment.rs` — mmap journal segment, block append/read/scan/recovery.
- Create: `src/journal/log.rs` — BTreeMap segment registry, sequence routing, append/read/query.
- Create: `src/journal/queue.rs` — JournalQueue and JournalQueueConsumer.
- Modify: `src/bg/mod.rs` — call `JournalManager::flush_dirty()` directly during flush task.
- Modify: `src/store.rs` — replace public journal DataSet handle path with dedicated Store APIs.
- Modify: `src/ffi.rs` and `include/timslite.h` — add dedicated journal C ABI.
- Modify: `wrapper/python/src/store.rs` and wrapper tests — expose Python journal APIs.
- Modify tests: `tests/journal_test.rs`, `tests/queue_test.rs`, `tests/negative_test.rs`, and new focused tests as needed.

## Task 1: Move Journal Record Codec

**Files:**
- Create: `src/journal/record.rs`
- Modify: `src/journal/mod.rs`
- Test: `src/journal/record.rs`

- [x] **Step 1: Move codec types**

Move the existing journal record definitions into `record.rs`:

```rust
pub enum JournalRecordKind {
    CreateDataset,
    DropDataset,
    DataWrite,
    DataDelete,
    DataAppend,
}

pub struct JournalRecord {
    pub kind: JournalRecordKind,
    pub name: String,
    pub dataset_type: String,
    pub metadata: Option<Vec<u8>>,
    pub index_info: Option<IndexEntry>,
    pub append_info: Option<(u32, u32)>,
}
```

- [x] **Step 2: Keep public crate-level exports**

In `src/journal/mod.rs`:

```rust
mod record;

pub(crate) use record::{
    meta_values_from_file, validate_create_drop_record_inputs, JournalRecord, JournalRecordKind,
};
```

- [x] **Step 3: Run codec tests**

Run:

```bash
cargo test journal::record -- --test-threads=1
```

Expected: existing encode/decode tests still pass.

## Task 2: Implement JournalSegment

**Files:**
- Create: `src/journal/segment.rs`
- Test: `src/journal/segment.rs`

- [x] **Step 1: Write failing segment append/read tests**

Tests must cover:

```rust
#[test]
fn journal_segment_appends_sequence_records_and_reads_by_sequence() {}

#[test]
fn journal_segment_uses_single_record_block_for_max_tlv_payload() {}

#[test]
fn journal_segment_recovery_truncates_half_written_tail() {}
```

- [x] **Step 2: Define segment state**

```rust
pub(crate) struct JournalSegment {
    pub base_sequence: i64,
    pub path: PathBuf,
    pub file_size: u64,
    pub max_file_size: u64,
    pub header_size: u64,
    pub wrote_position: u64,
    pub record_count: u64,
    pub total_uncompressed_size: u64,
    pub pending_block_offset: Option<u64>,
    pub pending_wrote_position: u64,
    pub pending_record_count: u64,
    pub is_flushed: bool,
    pub lifecycle: SegmentLifecycle,
    mmap: Option<MmapMut>,
}
```

- [x] **Step 3: Implement append**

Required signature:

```rust
impl JournalSegment {
    pub(crate) fn append_record(&mut self, sequence: i64, payload: &[u8]) -> Result<()>;
}
```

Rules:

- Write record as `data_len:u32 + sequence:i64 + payload`.
- Use pending raw block while it fits.
- Seal/compress pending block before creating a new block.
- Use `SINGLE_RECORD` block when encoded record does not fit normal block capacity.
- Update block header before updating segment state.

- [x] **Step 4: Implement read**

Required signature:

```rust
impl JournalSegment {
    pub(crate) fn read(&mut self, sequence: i64) -> Result<Option<Vec<u8>>>;
}
```

Read must scan block headers by `record_count`, only reading/decompressing the target block.

- [x] **Step 5: Implement recovery scan**

Required signature:

```rust
impl JournalSegment {
    pub(crate) fn recover_visible_state(&mut self) -> Result<()>;
}
```

Recovery keeps only the last complete block/record prefix and repairs in-memory state.

- [x] **Step 6: Verify segment tests**

Run:

```bash
cargo test journal::segment -- --test-threads=1
```

Expected: all segment tests pass.

## Task 3: Implement JournalLog

**Files:**
- Create: `src/journal/log.rs`
- Modify: `src/journal/mod.rs`
- Test: `src/journal/log.rs`

- [x] **Step 1: Write failing log routing tests**

Tests must cover:

```rust
#[test]
fn journal_log_first_sequence_is_one() {}

#[test]
fn journal_log_recovers_next_sequence_from_latest_segment() {}

#[test]
fn journal_log_reads_across_segments_without_index() {}
```

- [x] **Step 2: Define JournalLog**

```rust
pub(crate) struct JournalLog {
    base_dir: PathBuf,
    data_dir: PathBuf,
    segments: BTreeMap<i64, JournalSegment>,
    next_sequence: i64,
    segment_size: u64,
    initial_segment_size: u64,
    compress_type: u8,
    compress_level: u8,
}
```

- [x] **Step 3: Implement open_or_create**

Required signature:

```rust
impl JournalLog {
    pub(crate) fn open_or_create(base_dir: PathBuf, config: &StoreConfig) -> Result<Self>;
}
```

It must create `{base_dir}/meta` and `{base_dir}/data/`, never `{base_dir}/index/`.

- [x] **Step 4: Implement append/read/query**

Required signatures:

```rust
impl JournalLog {
    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<i64>;
    pub(crate) fn read(&mut self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub(crate) fn query(&mut self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub(crate) fn latest_sequence(&self) -> Option<i64>;
    pub(crate) fn next_sequence(&self) -> i64;
}
```

Append must allocate `sequence = next_sequence` before selecting the segment. If the latest segment cannot fit the record, flush that completed segment and create a new segment with `base_sequence = sequence` before writing. If the segment becomes full after the append, flush it but do not pre-create the next segment until the next append.

- [x] **Step 5: Verify log tests**

Run:

```bash
cargo test journal::log -- --test-threads=1
```

Expected: all log tests pass.

## Task 4: Implement JournalQueue

**Files:**
- Create: `src/journal/queue.rs`
- Modify: `src/queue/mod.rs` only if `ConsumerStateFile` visibility needs widening.
- Test: `src/journal/queue.rs`

- [x] **Step 1: Write failing queue tests**

Tests must cover:

```rust
#[test]
fn journal_queue_new_consumer_starts_after_current_latest() {}

#[test]
fn journal_queue_polls_realtime_sequence_one_based() {}

#[test]
fn journal_queue_ack_persists_processed_sequence() {}
```

- [x] **Step 2: Define JournalQueue types**

```rust
pub struct JournalQueue {
    log: Arc<Mutex<JournalLog>>,
    inner: Arc<Mutex<JournalQueueInner>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
}

pub struct JournalQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,
    log: Arc<Mutex<JournalLog>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
    closed: Arc<AtomicBool>,
}
```

- [x] **Step 3: Implement poll**

Poll algorithm:

```rust
let next = state.next_poll_ts(); // 0 -> 1
if next < log.next_sequence() && !state.is_in_pending(next) {
    let row = log.read(next)?;
    state.add_pending(PendingEntry { timestamp: next, start_time: now, status: PENDING_STATUS_UNACKED })?;
    return Ok(row);
}
```

No index query and no filler skip logic.

- [x] **Step 4: Wire notify**

`JournalLog::append` or `JournalManager::append_*` must call JournalQueue notify after successful append.

- [x] **Step 5: Verify queue tests**

Run:

```bash
cargo test journal::queue -- --test-threads=1
```

Expected: all JournalQueue tests pass.

## Task 5: Replace JournalManager Storage

**Files:**
- Modify: `src/journal/mod.rs`
- Modify: `src/store.rs`
- Test: `tests/journal_test.rs`

- [x] **Step 1: Write failing integration tests**

Tests must cover:

```rust
#[test]
fn journal_storage_creates_no_index_directory() {}

#[test]
fn open_dataset_journal_logs_no_longer_returns_dataset_handle() {}

#[test]
fn journal_read_query_use_dedicated_api() {}
```

- [x] **Step 2: Replace manager state**

```rust
pub(crate) enum JournalManager {
    Enabled {
        log: Arc<Mutex<JournalLog>>,
        queue: Mutex<Option<JournalQueue>>,
    },
    Disabled,
}
```

- [x] **Step 3: Implement dedicated read/query**

```rust
impl JournalManager {
    pub(crate) fn latest_sequence(&self) -> Result<Option<i64>>;
    pub(crate) fn read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub(crate) fn query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
}
```

- [x] **Step 4: Remove public journal DataSet path**

`Store::open_dataset(".journal", "logs")` must no longer return a read-only `DataSetHandle`.

- [x] **Step 5: Verify integration tests**

Run:

```bash
cargo test journal -- --test-threads=1
```

Expected: journal integration tests pass with no journal index directory.

## Task 6: Background Flush Integration

**Files:**
- Modify: `src/bg/mod.rs`
- Modify: `src/store.rs`
- Test: `tests/background_test.rs`

- [x] **Step 1: Write failing flush tests**

Tests must cover:

```rust
#[test]
fn background_flush_calls_journal_manager_without_dirty_queue_entry() {}

#[test]
fn manual_tick_flushes_dirty_journal_queue_state() {}
```

- [x] **Step 2: Add JournalManager flush API**

```rust
impl JournalManager {
    pub(crate) fn flush_dirty(&self) -> Result<()>;
}
```

- [x] **Step 3: Call it from background flush task**

After draining ordinary dataset flush targets, call `journal.flush_dirty()`.

- [x] **Step 4: Verify background tests**

Run:

```bash
cargo test background journal -- --test-threads=1
```

Expected: targeted background/journal tests pass.

## Task 7: Rust Public API

**Files:**
- Modify: `src/store.rs`
- Modify: `src/lib.rs`
- Test: `tests/journal_test.rs`

- [x] **Step 1: Add Store APIs**

```rust
impl Store {
    pub fn journal_latest_sequence(&self) -> Result<Option<i64>>;
    pub fn journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn journal_query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub fn open_journal_queue(&mut self) -> Result<JournalQueue>;
}
```

- [x] **Step 2: Add disabled journal tests**

`enable_journal=false` must make each API return `NotFound`.

- [x] **Step 3: Verify Rust API tests**

Run:

```bash
cargo test journal_read journal_query open_journal_queue -- --test-threads=1
```

Expected: dedicated journal API tests pass.

## Task 8: C ABI And Header

**Files:**
- Modify: `src/ffi.rs`
- Modify: `include/timslite.h`
- Test: `src/ffi.rs`

- [x] **Step 1: Add FFI tests first**

Tests must cover:

```rust
#[test]
fn ffi_journal_read_and_query_use_store_handle() {}

#[test]
fn ffi_journal_queue_poll_ack() {}
```

- [x] **Step 2: Add C ABI functions**

Add declarations and implementations for:

```c
int tmsl_journal_latest_sequence(void* store, int64_t* out_sequence, char* err_buf, size_t err_buf_len);
int tmsl_journal_read(void* store, int64_t sequence, int64_t* out_sequence,
                      unsigned char** out_data, size_t* out_data_len,
                      char* err_buf, size_t err_buf_len);
void* tmsl_journal_query(void* store, int64_t start_sequence, int64_t end_sequence,
                         char* err_buf, size_t err_buf_len);
int tmsl_journal_iter_next(void* iter, int64_t* out_sequence,
                           unsigned char** out_data, size_t* out_data_len,
                           char* err_buf, size_t err_buf_len);
void tmsl_journal_iter_close(void* iter);
size_t tmsl_journal_queue_open(void* store, char* err_buf, size_t err_buf_len);
int tmsl_journal_queue_close(size_t queue_handle, char* err_buf, size_t err_buf_len);
size_t tmsl_journal_queue_consumer_open(size_t queue_handle, const char* group_name,
                                        char* err_buf, size_t err_buf_len);
int tmsl_journal_queue_poll(size_t consumer_handle, int64_t timeout_ms,
                            int64_t* out_sequence,
                            unsigned char** out_data, size_t* out_data_len,
                            char* err_buf, size_t err_buf_len);
int tmsl_journal_queue_ack(size_t consumer_handle, int64_t sequence,
                           char* err_buf, size_t err_buf_len);
```

- [x] **Step 3: Verify FFI tests**

Run:

```bash
cargo test ffi::tests::test_journal -- --test-threads=1
```

Expected: dedicated journal FFI tests pass.

## Task 9: Python Wrapper

**Files:**
- Modify: `wrapper/python/src/store.rs`
- Modify: `wrapper/python/src/lib.rs`
- Test: `wrapper/python/tests/test_journal.py`

- [x] **Step 1: Add Python tests first**

Tests must cover:

```python
def test_journal_dedicated_read_query(tmpdir): ...
def test_journal_queue_poll_ack(tmpdir): ...
def test_open_dataset_journal_logs_rejected(tmpdir): ...
```

- [x] **Step 2: Add wrapper methods**

```rust
impl PyStore {
    fn journal_latest_sequence(&self) -> PyResult<Option<i64>>;
    fn journal_read(&self, sequence: i64) -> PyResult<Option<(i64, Vec<u8>)>>;
    fn journal_query(&self, start: i64, end: i64) -> PyResult<Vec<(i64, Vec<u8>)>>;
    fn open_journal_queue(&mut self) -> PyResult<PyJournalQueue>;
}
```

- [x] **Step 3: Verify Python wrapper**

Run:

```bash
cargo test --manifest-path wrapper/python/Cargo.toml
cd wrapper/python
maturin develop
python -m pytest tests/ -q
```

Expected: Rust wrapper builds and Python tests pass.

## Task 10: Final Validation

**Files:**
- No new files.

- [x] **Step 1: Run full Rust validation**

Run:

```bash
cargo fmt -- --check
cargo check
cargo test -- --test-threads=1
cargo clippy --all-targets -- -D warnings
git diff --check
```

Expected: all commands pass.

- [x] **Step 2: Verify on-disk layout manually in tests or debug assertion**

Expected journal layout:

```text
.journal/logs/meta
.journal/logs/data/00000000000000000001
.journal/logs/queue/{group}
```

Expected absent path:

```text
.journal/logs/index/
```

- [x] **Step 3: Update completion status**

After implementation and verification, update:

- `docs/plan/phase-36-journal-dedicated-storage.md`
- `docs/plan/overview.md`
- `plan.md`