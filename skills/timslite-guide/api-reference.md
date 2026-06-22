# timslite API Reference

> Complete API signatures, parameters, return types, and semantics.
> This reference covers the Rust public API. Python and C FFI equivalents are noted where relevant.

---

## 1. Store

The `Store` is the top-level facade. All dataset lifecycle, queue lifecycle, and journal operations go through `Store`.

### 1.1 Lifecycle

#### `Store::open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>`

Opens or connects to a store at `data_dir`. Acquires a `.lock` file for exclusive write access. If the lock is already held and `config.read_only` is `None` (auto), falls back to read-only mode. If `config.read_only == Some(false)`, returns an error when the lock cannot be acquired.

**Parameters**:
- `data_dir`: Path to the data directory (created if not exists)
- `config`: Store configuration

**Returns**: `Ok(Store)` on success, `Err(TmslError)` on failure.

**Side effects**: Creates `data_dir` if missing. Creates `.lock` file. Initializes `BlockCache`, `JournalManager`, and `BackgroundTasks` (if enabled). Scans existing datasets.

#### `Store::close(self) -> Result<()>`

Stops the background thread (if running), flushes all dirty segments, closes all open datasets, closes the journal, and releases the store lock.

**Fails if**: Any dataset handle, iterator, queue, or consumer created from this store is still open.

#### `Store::is_read_only(&self) -> bool`

Returns whether this store resolved to read-only mode at open time. Read-only stores cannot create/drop datasets, write data, or open queues.

### 1.2 Dataset Management

#### `Store::create_dataset(&mut self, name: &str, dataset_type: &str, data_segment_size: u64, index_segment_size: u64, compress_level: u8, index_continuous: u8, retention_window: u64) -> Result<DataSetHandle>`

Creates a new dataset with explicit parameters.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `dataset_type`: Dataset type, same naming rules
- `data_segment_size`: Max data segment file size in bytes (must be > 0)
- `index_segment_size`: Max index segment file size in bytes (must be > 0)
- `compress_level`: Compression level 0-9 (clamped to 9 if > 9)
- `index_continuous`: 0 = sparse mode, 1 = continuous mode
- `retention_window`: Retention window in timestamp units (0 = no limit)

**Returns**: `DataSetHandle` (opaque numeric handle for opening the dataset).

**Errors**:
- `AlreadyExists` if dataset already exists
- `InvalidData` if name/type invalid or parameters out of range
- Rejects the reserved name `.journal`

#### `Store::create_dataset_with_config(&mut self, name: &str, dataset_type: &str, config_builder: Option<DataSetConfigBuilder>) -> Result<DataSetHandle>`

Creates a dataset with a full `DataSetConfigBuilder` for complete control. Pass `None` to use store defaults.

#### `Store::open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSetHandle>`

Opens an existing dataset. Parameters are read from the `meta` file, not from arguments.

**Errors**: `NotFound` if dataset doesn't exist.

#### `Store::open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSetHandle>`

Opens a dataset by its numeric identifier (stored in the `identifier` file). Useful when you only have the ID (e.g., from journal records).

#### `Store::drop_dataset(&mut self, name: &str, dataset_type: &str) -> Result<()>`

Deletes a dataset and all associated files (data, index, meta, state, queue). The dataset can be recreated afterwards.

#### `Store::get_dataset(&self, handle: &DataSetHandle) -> Result<Arc<DataSet>>`

Returns a shared (`Arc`) reference to the dataset for read/write operations. The returned `Arc<DataSet>` uses `&self` methods internally (mutex-protected), so multiple threads can safely use it concurrently.

#### `Store::list_datasets(&self) -> Result<Vec<(String, String)>>`

Lists all `(name, type)` pairs in the store, excluding the reserved `.journal` path.

#### `Store::inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult>`

Returns the dataset's immutable config (`DataSetInfo`) and mutable state (`DataSetState`). See §3 for struct details.

### 1.3 Queue Operations

#### `Store::open_queue(&mut self, handle: DataSetHandle) -> Result<DatasetQueue>`

Opens the queue for a dataset. A dataset can have only one queue open at a time. The queue is backed by the dataset's `queue/` directory.

#### `Store::close_queue(&mut self, queue: DatasetQueue) -> Result<()>`

Closes a queue, syncing all consumer state files and waking waiters.

#### `Store::queue_push(&self, queue: &DatasetQueue, data: &[u8]) -> Result<i64>`

Pushes data to the queue. Auto-assigns the next timestamp (`latest_written_timestamp + 1`). Returns the assigned timestamp.

**Note**: The consumer must be opened before pushing if you want to consume from the beginning. New consumers initialize from the current `latest_written_timestamp`.

#### `Store::queue_poll(&self, consumer: &DatasetQueueConsumer, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>`

Polls for the next unacked record. Blocks up to `timeout` if no data is available. Returns `Ok(Some((timestamp, data)))` if data is available, `Ok(None)` on timeout.

#### `Store::queue_ack(&self, consumer: &DatasetQueueConsumer, timestamp: i64) -> Result<()>`

Acknowledges a previously polled record. Only updates the current consumer group's state file.

#### `Store::open_consumer(&self, queue: &DatasetQueue, group_name: &str) -> Result<DatasetQueueConsumer>`

Opens a consumer group with default config (`running_expired_seconds=900`, `max_retry_count=3`). If the group already exists, reopens with the existing state.

#### `Store::open_consumer_with_config(&self, queue: &DatasetQueue, group_name: &str, config: QueueConsumerConfig) -> Result<DatasetQueueConsumer>`

Opens a consumer group with explicit retry/visibility config. If the group already exists, the config must match the previously opened config.

#### `Store::drop_consumer(&mut self, queue: &DatasetQueue, group_name: &str) -> Result<()>`

Drops a consumer group and deletes its state file.

### 1.4 Journal Operations

#### `Store::journal_latest_sequence(&self) -> Result<Option<i64>>`

Returns the latest journal sequence number. Returns `None` if journal is disabled or empty. The first sequence is `1`.

#### `Store::journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>`

Reads a journal record by sequence. Returns `Ok(None)` if the sequence doesn't exist or journal is disabled.

#### `Store::journal_query(&self, start_seq: i64, end_seq: i64) -> Result<Vec<(i64, Vec<u8>)>>`

Range query on journal records. Returns `Vec<(sequence, raw_record_bytes)>`.

#### `Store::open_journal_queue(&mut self) -> Result<JournalQueue>`

Opens the journal queue for consuming change records. Each successfully written journal record (create/drop/write/delete/append) is delivered as an independent queue entry.

**Not supported in read-only mode.**

#### `Store::read_journal_source_record(&self, dataset_identifier: u64, index_info: JournalIndexInfo) -> Result<Option<(i64, Vec<u8>)>>`

Dereferences a journal record to its source business data. Used by journal consumers to fetch the actual payload referenced by a journal entry. Validates the source dataset still exists and the referenced entry is valid.

### 1.5 Background Tasks

#### `Store::tick_background_tasks(&mut self) -> Result<TickResult>`

Executes one tick of background tasks (flush, idle-close, cache eviction, retention reclaim). Safe to call even when the background thread is enabled.

**Returns**: `TickResult { executed_tasks: usize, next_delay: Duration }`

**Required when**: `StoreConfig.enable_background_thread == false`. In this case, the caller must call this periodically to drive maintenance tasks.

#### `Store::next_background_delay(&self) -> Result<Duration>`

Queries the delay until the next background task is due, without executing anything. Useful for event loop integration.

---

## 2. DataSet

`DataSet` is obtained via `Store::get_dataset(&handle)`. It's an `Arc<DataSet>` — all methods take `&self` and use internal mutex synchronization, so it's safe for concurrent use.

### 2.1 Write Operations

#### `ds.write(&self, timestamp: i64, data: &[u8]) -> Result<()>`

Writes a record at the given timestamp.

**Rules**:
- `timestamp` must be `>= latest_written_timestamp` (for sparse mode)
- `data.len()` must be `<= 4 MiB` (4,194,304 bytes)
- If `timestamp == latest_written_timestamp`, triggers correction write (in-place modify if latest record is in a pending raw block)
- If `timestamp > latest_written_timestamp`, creates a new record
- In sparse mode, `timestamp < latest_written_timestamp` with existing index entry → out-of-order rewrite; without existing entry → error
- In continuous mode, `time_step = 1`; timestamps fill sequentially with filler entries for gaps

**Updates `latest_written_timestamp`** to `max(latest, timestamp)` on success.

#### `ds.append(&self, timestamp: i64, data: &[u8]) -> Result<()>`

Appends data with specific semantics based on timestamp:

- `timestamp < latest_written_timestamp` → error (no backward append)
- `timestamp > latest_written_timestamp` → forward append (creates new record)
- `timestamp == latest_written_timestamp` → in-place append to the latest tail record (only if it's in an uncompressed pending block)

**Rules**:
- Empty `data` is a no-op (after timestamp/retention checks)
- `old_data_len + append_len <= 4 MiB`
- Appended data must fit within the current pending block's capacity
- Does NOT re-notify the queue when appending to an existing record
- Does notify the queue when creating a new timestamp

#### `ds.delete(&self, timestamp: i64) -> Result<()>`

Deletes a record by timestamp. Does NOT retroactively update `latest_written_timestamp`.

**Rules**:
- Returns `Ok(())` even if the record was already deleted or never existed
- Refuses to delete expired timestamps (retention)
- Invalidates the affected cache key

### 2.2 Read Operations

#### `ds.read(&self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>`

Reads a single complete record by exact timestamp.

**Returns**:
- `Ok(Some((timestamp, data)))` if the record exists and is valid
- `Ok(None)` if the record doesn't exist, is a filler, is deleted, or is expired

**Note**: `read(-1)` reads the record at timestamp `-1`, NOT the latest. Use `read_latest()` for the latest.

#### `ds.read_latest(&self) -> Result<Option<(i64, Vec<u8>)>>`

Reads the record at `latest_written_timestamp`. Returns `Ok(None)` if never written, deleted, or expired. Does NOT fall back to earlier records.

#### `ds.read_exist(&self, timestamp: i64) -> Result<bool>`

Fast existence check — index lookup only, no data segment I/O. Returns `true` if a visible record exists at this timestamp.

#### `ds.read_length(&self, timestamp: i64) -> Result<Option<u32>>`

Reads only the data length of a record (reads the 12-byte record header, not the full data). Faster than `read()` when you only need the size.

#### `ds.query(&self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>`

Range query returning all valid records in `[start_ts, end_ts]` (inclusive). Eager — loads all data into memory. Internally calls `query_iter().collect_all()`.

**Returns**: `Vec<(timestamp, data)>` sorted by timestamp.

#### `ds.query_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryIterator>`

Lazy range query iterator. Records are read on-demand via `next()`. Uses a query-level `HotBlockCache` for block reuse. Preferred for large ranges.

**Returns**: `QueryIterator` implementing `Iterator<Item = Result<(i64, Vec<u8>)>>`.

#### `ds.query_exist(&self, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>`

Fast range existence bitmap. Returns a bitmap where bit `i` is set if a record exists at `start_ts + i`. No data segment I/O.

#### `ds.query_length(&self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>`

Range query returning `(timestamp, data_length)` pairs. Reads only record headers (12 bytes each), not full data.

#### `ds.query_length_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator>`

Lazy iterator over `(timestamp, data_length)` pairs. Memory-efficient for large ranges.

### 2.3 State & Info

#### `ds.latest_timestamp(&self) -> Result<Option<i64>>`

Returns `latest_written_timestamp` without reading any data. `None` if never written.

#### `ds.flush(&self) -> Result<()>`

Flushes dirty data/index segments and queue state to disk (mmap `MS_SYNC`). Does NOT seal pending blocks or compress.

#### `ds.inspect(&self) -> Result<DataSetInspectResult>`

Returns config and state. See §3 for struct details.

#### `ds.identifier(&self) -> u64`

Returns the numeric dataset identifier assigned by the Store. Low-level non-Store-managed datasets return `0`.

### 2.4 Performance Tier Summary

| Operation | I/O | Speed |
|-----------|-----|-------|
| `read_exist` | Index only | Fastest |
| `query_exist` | Index only | Fast |
| `read_length`, `query_length`, `query_length_iter` | Record header (12B) | Medium |
| `read`, `query`, `query_iter` | Full data | Slowest |

---

## 3. Inspect Structs

### 3.1 DataSetInfo (immutable config)

```rust
pub struct DataSetInfo {
    // Identity
    pub name: String,
    pub dataset_type: String,
    pub base_dir: String,
    pub identifier: u64,

    // Storage config
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,

    // Compression config
    pub compress_type: u8,       // 0=zstd, 1=deflate
    pub compress_level: u8,      // 0-9

    // Index config
    pub index_continuous: u8,    // 0=sparse, 1=continuous

    // Retention
    pub retention_window: u64,   // 0 = no limit

    // Journal
    pub enable_journal: bool,    // immutable after creation

    // Metadata
    pub create_time: i64,        // Unix milliseconds
}
```

### 3.2 DataSetState (mutable runtime state)

```rust
pub struct DataSetState {
    // Write state
    pub latest_written_timestamp: Option<i64>,

    // Data segments
    pub open_data_segments: u32,
    pub data_segments: u32,       // total (open + closed)
    pub total_record_count: u64,
    pub total_data_size: u64,     // used bytes, excluding header
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,

    // Index segments
    pub open_index_segments: u32,
    pub index_segments: u32,      // total

    // Timestamp range
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
}
```

**Field semantics**:
- `data_segments` / `index_segments`: total count, NOT open + closed. API doesn't expose closed count.
- `total_*` fields cover the entire dataset (archived state file + active tail).
- `latest_written_timestamp`: max timestamp ever successfully written. Delete does NOT retroactively update this.
- `min_timestamp` / `max_timestamp`: based on index visible range.

### 3.3 DataSetInspectResult

```rust
pub struct DataSetInspectResult {
    pub info: DataSetInfo,
    pub state: DataSetState,
}
```

### 3.4 TickResult

```rust
pub struct TickResult {
    pub executed_tasks: usize,   // 0..=4
    pub next_delay: Duration,    // saturating, >= 0
}
```

---

## 4. Configuration

### 4.1 StoreConfig

```rust
pub struct StoreConfig {
    pub flush_interval: Duration,              // default 15s
    pub idle_timeout: Duration,                // default 1800s (30min)
    pub data_segment_size: u64,                // default 64 MiB
    pub index_segment_size: u64,               // default 4 MiB
    pub initial_data_segment_size: u64,        // default 256 KiB
    pub initial_index_segment_size: u64,       // default 4 KiB
    pub compress_level: u8,                    // default 6 (0-9)
    pub compress_type: u8,                     // default 0 (zstd)
    pub cache_max_memory: usize,               // default 256 MiB (0=disabled)
    pub cache_idle_timeout: Duration,          // default 1800s
    pub retention_check_hour: u8,              // default 0 (UTC 00:00)
    pub enable_background_thread: bool,        // default true
    pub enable_journal: bool,                  // default true
    pub read_only: Option<bool>,               // default None (auto)
}
```

#### Builder pattern

```rust
let config = StoreConfig::builder()
    .flush_interval(Duration::from_secs(30))
    .idle_timeout(Duration::from_secs(60))
    .compress_level(9)
    .cache_max_memory(512 * 1024 * 1024)
    .enable_background_thread(false)
    .enable_journal(false)
    .build();
```

**Read-only mode**:
- `None` (auto): writable if `.lock` can be acquired, otherwise read-only
- `Some(false)`: require writable, fail if `.lock` is already locked
- `Some(true)`: force read-only, do not check or take `.lock`

### 4.2 DataSetConfig / DataSetConfigBuilder

`DataSetConfig` fields are crate-internal. Use `DataSetConfigBuilder` to construct:

```rust
let builder = DataSetConfigBuilder::new()
    .data_segment_size(128 * 1024 * 1024)
    .index_segment_size(8 * 1024 * 1024)
    .compress_level(9)
    .compress_type(0)  // zstd
    .index_continuous(1)  // continuous mode
    .retention_window(86400)  // 1 day (in timestamp units)
    .enable_journal(true);

store.create_dataset_with_config("my_ds", "events", Some(builder))?;
```

**Validation rules**:
- `data_segment_size > 0`, `index_segment_size > 0`
- `initial_data_segment_size <= data_segment_size`
- `initial_index_segment_size <= index_segment_size`
- `compress_level <= 9`
- `compress_type`: 0 (zstd) or 1 (deflate)
- `index_continuous`: 0 or 1
- `retention_window <= i64::MAX`

### 4.3 QueueConsumerConfig

```rust
pub struct QueueConsumerConfig {
    pub running_expired_seconds: u16,  // 0=never expire while running, default 900, max 65535
    pub max_retry_count: u8,           // 0=unlimited, default 3, max 255
}
```

- `running_expired_seconds`: pending entries older than this (while the consumer is running) become eligible for re-delivery
- `max_retry_count`: after this many retries, a pending entry is dropped (0 = never drop)

---

## 5. Queue Types

### 5.1 DatasetQueue

Obtained via `Store::open_queue(handle)`. Clone-safe (internally `Arc`-shared). Call `Store::close_queue()` to close.

**Key behavior**:
- `push(data)` auto-assigns `timestamp = latest_written_timestamp + 1`
- `poll(timeout)` returns the next unacked record for this consumer group
- `ack(timestamp)` marks a record as processed
- Multiple consumer groups are independent; each maintains its own progress
- Multiple consumer instances in the same group share progress (mutual exclusion via state file lock)

### 5.2 DatasetQueueConsumer

Obtained via `Store::open_consumer(&queue, group_name)`.

**Poll semantics**:
- Polls from `processed_ts + 1` forward
- Skips filler/gap entries (not delivered, not pending, not acked)
- On direct read miss, jumps via index to the next real unacked record
- Returns `Ok(None)` on timeout
- Returns `Err(QueueClosed)` if queue is closed during poll

### 5.3 JournalQueue

Obtained via `Store::open_journal_queue()`. Uses the same `ConsumerStateFile` format and at-least-once ack semantics as `DatasetQueue`, but the data source is `JournalLog::read(sequence)`.

**Poll returns**: `(journal_sequence, raw_record_bytes)` where the raw bytes are the encoded journal record (type byte + TLV fields).

### 5.4 JournalQueueConsumer

Same interface as `DatasetQueueConsumer` (`poll`, `ack`, `poll_callback`).

### 5.5 Queue Consumer poll_callback

```rust
pub fn poll_callback(&self, callback: Option<QueuePollCallback>) -> Result<()>
```

Registers or clears a lightweight wake callback invoked after queue data notifications. The callback is best-effort and must not do data processing — it's a notification hook, not a data handler. Pass `None` to clear.

**Python**: Not directly exposed; use `poll(timeout_ms)` in a loop instead.

### 5.6 DataSetHandle

`DataSetHandle` is a lightweight opaque wrapper around a `u64` identifier. It's `Copy` and `Clone`, so you can freely pass it by value. Use it with `Store::get_dataset(&handle)` to obtain an `Arc<DataSet>` for operations.

```rust
pub struct DataSetHandle(pub u64);  // Copy, Clone
```

### 5.7 JournalIndexInfo

`JournalIndexInfo` is a reference pointer stored in journal records for `0x11` (write) and `0x13` (append) events. It contains the information needed to locate the source business data:

```rust
pub struct JournalIndexInfo {
    // Locates the record within the source dataset's index
    // Used by Store::read_journal_source_record() to fetch the actual payload
}
```

**Usage**: You don't construct `JournalIndexInfo` directly. It's returned when parsing journal records and passed to `Store::read_journal_source_record(dataset_identifier, index_info)` for dereferencing.

---

## 6. Journal Record Types

Journal records use a type byte followed by TLV-encoded fields:

| Type | Name | Description |
|------|------|-------------|
| `0x01` | create dataset | Dataset created (name, type, meta snapshot) |
| `0x02` | drop dataset | Dataset dropped (name, type) |
| `0x11` | dataset write | Record written (dataset identifier, index_info) |
| `0x12` | dataset delete | Record deleted (dataset identifier, timestamp) |
| `0x13` | dataset append | Record appended (dataset identifier, index_info) |

**Journal contract**:
- Journal is an **auxiliary** change log, NOT a WAL or transaction log
- Journal sequence starts from `1`, increments by `1` per record
- `0` is reserved as queue state's initial processed position
- Journal does NOT carry business payload — `0x11`/`0x13` reference source data via `index_info`
- Consumers must use `Store::read_journal_source_record()` to fetch the actual payload
- Journal append failure does NOT roll back the completed main operation
- Journal is controlled by `StoreConfig.enable_journal && DataSetConfig.enable_journal` (both must be true)

---

## 7. Error Types

```rust
pub enum TmslError {
    IoError(io::Error),
    NotFound(String),
    AlreadyExists(String),
    InvalidData(String),
    SegmentFull(String),
    MmapError(String),
    CompressionError(String),
    DecompressionError(String),
    Expired(String),
    QueueAlreadyOpen(String),
    QueueNotOpen(String),
    ConsumerGroupNotFound(String),
    ConsumerGroupExists(String),
    QueueClosed(String),
    PendingFull(String),
}
```

**Python equivalents**: `TmslError` (base), `TmslNotFoundError`, `TmslAlreadyExistsError`, `TmslInvalidDataError`, `TmslIoError`, `TmslSegmentFullError`, `TmslMmapError`, `TmslCompressionError`, `TmslDecompressionError`, `TmslExpiredError`, `TmslQueueAlreadyOpenError`, `TmslQueueNotOpenError`, `TmslConsumerGroupNotFoundError`, `TmslConsumerGroupExistsError`, `TmslQueueClosedError`, `TmslPendingFullError`.

---

## 8. FFI (C ABI)

The C header is at `include/timslite.h`. The library compiles as `cdylib` (`libtimslite.so` / `libtimslite.dylib` / `timslite.dll`).

### 8.1 Core FFI Functions

| Function | Purpose |
|----------|---------|
| `tmsl_store_config_default` | Fill config with defaults |
| `tmsl_store_open` | Open store with default config |
| `tmsl_store_open_with_config` | Open store with explicit config |
| `tmsl_store_close` | Close store |
| `tmsl_store_tick_background_tasks` | Manual background tick |
| `tmsl_store_next_background_delay` | Query next task delay |
| `tmsl_dataset_create` | Create dataset |
| `tmsl_dataset_create_with_config` | Create with full config |
| `tmsl_dataset_open` | Open dataset |
| `tmsl_dataset_open_by_identifier` | Open by numeric ID |
| `tmsl_dataset_drop` | Drop dataset |
| `tmsl_dataset_write` | Write a record |
| `tmsl_dataset_append` | Append to a record |
| `tmsl_dataset_delete` | Delete a record |
| `tmsl_dataset_read` | Read a record |
| `tmsl_dataset_read_latest` | Read latest record |
| `tmsl_dataset_read_exist` | Check existence |
| `tmsl_dataset_read_length` | Read record length |
| `tmsl_dataset_query` | Range query (returns iterator) |
| `tmsl_dataset_query_length` | Range length query (returns iterator) |
| `tmsl_iter_next` | Get next item from iterator |
| `tmsl_iter_free` | Free iterator |
| `tmsl_dataset_flush` | Flush dataset |
| `tmsl_dataset_inspect` | Get info + state |
| `tmsl_dataset_list` | List datasets |
| `tmsl_queue_open` | Open queue |
| `tmsl_queue_close` | Close queue |
| `tmsl_queue_push` | Push data |
| `tmsl_queue_poll` | Poll for data |
| `tmsl_queue_ack` | Ack record |
| `tmsl_consumer_open` | Open consumer group |
| `tmsl_consumer_open_with_config` | Open with config |
| `tmsl_consumer_drop` | Drop consumer group |
| `tmsl_journal_latest_sequence` | Get latest journal seq |
| `tmsl_journal_read` | Read journal record |
| `tmsl_journal_query` | Query journal range |
| `tmsl_journal_queue_open` | Open journal queue |

### 8.2 FFI Config Structs

```c
typedef struct TmslStoreConfigFFI {
    uint32_t version;
    uint64_t flush_interval_ms;
    uint64_t idle_timeout_ms;
    uint64_t data_segment_size;
    uint64_t index_segment_size;
    uint64_t initial_data_segment_size;
    uint64_t initial_index_segment_size;
    uint64_t cache_max_memory;
    uint64_t cache_idle_timeout_ms;
    uint8_t  compress_level;
    uint8_t  compress_type;        /* 0=zstd, 1=deflate */
    uint8_t  retention_check_hour; /* UTC 0-23 */
    uint8_t  enable_background_thread;
    uint8_t  enable_journal;
    uint8_t  read_only_mode;       /* 0=auto, 1=require writable, 2=force RO */
} TmslStoreConfigFFI;

typedef struct TmslDatasetConfigFFI {
    uint32_t version;
    uint64_t data_segment_size;
    uint64_t index_segment_size;
    uint64_t initial_data_segment_size;
    uint64_t initial_index_segment_size;
    uint64_t retention_window;
    uint8_t  compress_level;
    uint8_t  compress_type;
    uint8_t  index_continuous;
    uint8_t  enable_journal;
} TmslDatasetConfigFFI;

typedef struct TmslQueueConsumerConfigFFI {
    uint32_t version;
    uint32_t running_expired_seconds; /* default 900, max 65535 */
    uint32_t max_retry_count;         /* default 3, max 255 */
} TmslQueueConsumerConfigFFI;
```

### 8.3 FFI Patterns

All FFI functions follow these conventions:
- Return `void*` (opaque pointer) or `int` (0=success, -1=error)
- Error messages are written to a caller-provided `err_buf` of length `err_buf_len`
- Iterators are created by query functions, advanced by `tmsl_iter_next`, freed by `tmsl_iter_free`
- String outputs use `(char** out_str, size_t* out_len)` pattern; caller must free with `tmsl_free_string`

---

## 9. Python Wrapper

### 9.1 Module Exports

```python
from timslite import (
    Store, StoreConfig,
    Dataset, QueryIterator,
    DatasetQueue, DatasetQueueConsumer,
    JournalQueue, JournalQueueConsumer,
    # Errors
    TmslError, TmslIoError, TmslNotFoundError, TmslAlreadyExistsError,
    TmslInvalidDataError, TmslSegmentFullError, TmslMmapError,
    TmslCompressionError, TmslDecompressionError, TmslExpiredError,
    TmslQueueAlreadyOpenError, TmslQueueNotOpenError,
    TmslConsumerGroupNotFoundError, TmslConsumerGroupExistsError,
    TmslQueueClosedError, TmslPendingFullError,
)
```

### 9.2 Python Store API

```python
# Open/close (context manager supported)
store = timslite.Store.open(path)
store = timslite.Store.open(path, config)
store.close()
with timslite.Store.open(path) as store: ...

# Dataset management
store.create_dataset(name, dataset_type)
store.create_dataset(name, dataset_type, data_segment_size=..., ...)
ds = store.open_dataset(name, dataset_type)
ds = store.open_dataset_by_identifier(identifier)
store.drop_dataset(name, dataset_type)
store.list_datasets()  # -> list of (name, type)
store.inspect_dataset(name, dataset_type)  # -> DataSetInspectResult

# Background
store.tick_background_tasks()  # -> (executed, delay_ms)
store.next_background_delay()  # -> delay_ms
```

### 9.3 Python Dataset API

```python
ds = store.open_dataset(name, dataset_type)

# Identity
ds.identifier  # -> int (numeric dataset ID)
ds.id          # -> int (alias for identifier, used by open_queue)

# Write
ds.write(timestamp, data_bytes)
ds.append(timestamp, data_bytes)
ds.delete(timestamp)

# Read
ds.read(timestamp)          # -> (ts, data) or None
ds.read_latest()            # -> (ts, data) or None
ds.read_exist(timestamp)    # -> bool
ds.read_length(timestamp)   # -> int or None
ds.latest_timestamp         # -> int or None

# Query
# NOTE: Python query() returns a LAZY QueryIterator (unlike Rust query() which returns Vec).
# Use query_all() for an eager list, or iterate query() lazily.
ds.query(start, end)        # -> QueryIterator (lazy, iterate with for)
ds.query_all(start, end)    # -> list of (ts, data) (eager)
ds.query_exist(start, end)  # -> bytes (bitmap)
ds.query_length(start, end) # -> list of (ts, length)

# Maintenance
ds.flush()
ds.inspect()  # -> DataSetInspectResult
```

**Cross-language note**: In Rust, `query()` returns `Vec<(i64, Vec<u8>)>` (eager). In Python, `query()` returns a lazy `QueryIterator` — use `query_all()` for an eager list or iterate with `for ts, data in ds.query(...)`.

### 9.4 Python Queue API

```python
ds = store.open_dataset(name, dataset_type)
q = store.open_queue(ds.id)
c = q.open_consumer("group-1")
c = q.open_consumer("group-1", running_expired_seconds=60, max_retry_count=3)

ts = q.push(data_bytes)
result = c.poll(timeout_ms)  # -> (ts, data) or None
c.ack(ts)

q.close()
```

### 9.5 Python Journal API

```python
latest = store.journal_latest_sequence()  # -> int or None
record = store.journal_read(sequence)     # -> (seq, payload) or None
rows = store.journal_query(start, end)    # -> list of (seq, payload)

jq = store.open_journal_queue()
c = jq.open_consumer("journal-worker")
result = c.poll(timeout_ms)  # -> (seq, payload) or None
c.ack(seq)
jq.close()
```

### 9.6 Installation

```bash
cd wrapper/python
maturin develop          # Development build
maturin develop --release  # Release build
```

---

## 10. Index Modes

### Sparse Mode (`index_continuous = 0`, default)

- Index entries appended in write order
- No filler entries for missing timestamps
- `write(ts)` with `ts < latest` and existing index entry → out-of-order rewrite
- `write(ts)` with `ts < latest` and no index entry → **error**
- `write(ts)` with `ts == latest` → correction write (in-place modify if pending)

### Continuous Mode (`index_continuous = 1`)

- Fixed `time_step = 1` (adjacent integers in your timestamp unit)
- First real write sets `base_timestamp`
- Missing timestamps within a materialized segment are filler entries
- Large gaps: only the tail of the previous segment and head of the new segment are materialized; middle segments are NOT created (logical holes)
- `write(ts)` fills the appropriate position, creating filler prefixes as needed
- O(1) timestamp-to-position calculation within a segment

**Choose continuous mode when**: Your timestamps are dense sequential integers (e.g., per-second sensor readings with few gaps).

**Choose sparse mode when**: Your timestamps are irregular, event-driven, or have large gaps.
