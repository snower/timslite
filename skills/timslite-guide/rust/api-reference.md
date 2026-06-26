# Rust API Reference

> Complete Rust API signatures, parameters, return types, and semantics.

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

#### `Store::create_dataset_with_config(&mut self, name: &str, dataset_type: &str, config: Option<DataSetConfigBuilder>) -> Result<DataSetHandle>`

Creates a dataset with optional custom configuration. Pass `None` to use store defaults.

#### `Store::open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>`

Opens an existing dataset and returns a `DataSetHandle`. The dataset's configuration is read from its meta file.

**Errors**: `NotFound` if dataset doesn't exist.

#### `Store::open_dataset_by_identifier(&self, identifier: u64) -> Result<DataSetHandle>`

Opens a dataset by its numeric identifier (assigned at creation time).

#### `Store::drop_dataset(&mut self, name: &str, dataset_type: &str) -> Result<()>`

Deletes a dataset and all its files (data segments, index segments, meta, state, queue). Irreversible.

#### `Store::get_dataset(&self, handle: &DataSetHandle) -> Result<Arc<DataSet>>`

Returns a clone-safe `Arc<DataSet>` for read/write operations. Multiple calls with the same handle return the same `Arc`.

#### `Store::list_datasets(&self) -> Result<Vec<(String, String)>>`

Lists all dataset `(name, type)` pairs in the store.

#### `Store::inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult>`

Returns `DataSetInspectResult { info: DataSetInfo, state: DataSetState }` with dataset metadata and runtime statistics.

---

## 2. DataSet

### 2.1 Write Operations

#### `ds.write(&self, timestamp: i64, data: &[u8]) -> Result<()>`

Writes a record at the given timestamp.

**Rules**:
- `timestamp >= latest_written_timestamp` → new write (or correction if equal)
- `timestamp < latest_written_timestamp` and index entry exists → out-of-order rewrite (sparse mode only)
- `timestamp < latest_written_timestamp` and no index entry → error
- `data.len() <= 4 MiB` (otherwise `InvalidData`)

**Behavior**:
- If the current pending block has space, the record is appended
- If the block overflows, it is sealed (compressed) and a new block is started
- Large records (>64KB encoded) get their own single-record block
- Cache invalidation occurs for affected blocks

#### `ds.append(&self, timestamp: i64, data: &[u8]) -> Result<()>`

Forward append or in-place tail append.

**Rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + append_len <= 4 MiB`

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

Returns the `latest_written_timestamp` for this dataset. Returns `Ok(None)` if never written.

#### `ds.flush(&self) -> Result<()>`

Flushes all dirty segments for this dataset to disk.

#### `ds.inspect(&self) -> Result<DataSetInspectResult>`

Returns `DataSetInspectResult { info: DataSetInfo, state: DataSetState }` with dataset metadata and runtime statistics.

---

## 3. QueryIterator / QueryLengthIterator

Lazy iterators returned by `query_iter()` and `query_length_iter()`.

```rust
let mut iter = ds.query_iter(1, 1000)?;
while let Some(result) = iter.next() {
    let (ts, data) = result?;
    // process record
}
```

**Behavior**:
- Reads records on-demand from data segments
- Uses a query-level `HotBlockCache` for block reuse within the query
- Implements `Iterator<Item = Result<(i64, Vec<u8>)>>`
- Automatically releases resources when dropped

---

## 4. Configuration

### 4.1 StoreConfig / StoreConfigBuilder

```rust
use timslite::{StoreConfig, StoreConfigBuilder};

// Using struct directly
let config = StoreConfig {
    flush_interval: 15,
    idle_timeout: 1800,
    data_segment_size: 64 * 1024 * 1024,
    index_segment_size: 4 * 1024 * 1024,
    compress_level: 6,
    cache_max_memory: 256 * 1024 * 1024,
    enable_background_thread: true,
    enable_journal: true,
    read_only: None,
    ..Default::default()
};

// Using builder
let config = StoreConfigBuilder::new()
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
- On direct read miss → skip
- On data read miss → mark processed, retry next
- Pending entries (already polled, not yet acked) are re-delivered after `running_expired_seconds`
- After `max_retry_count` retries, a pending entry is dropped (0 = never drop)

---

## 6. Journal

### 6.1 Journal Queue

Obtained via `Store::open_journal_queue()`.

**Behavior**:
- Each successful `0x11` (write), `0x12` (delete), or `0x13` (append) operation creates a journal record
- Journal records are consumed via the queue interface
- Use `Store::read_journal_source_record(dataset_identifier, index_info)` to dereference journal records to source data

### 6.2 Journal Consumption

```rust
let jq = store.open_journal_queue()?;
let consumer = jq.open_consumer("sync_worker")?;

while let Some((seq, payload)) = consumer.poll(Duration::from_millis(100))? {
    // process journal record
    consumer.ack(seq)?;
}
```

---

## 7. Background Tasks

### 7.1 Manual Tick

```rust
let (executed, delay_ms) = store.tick_background_tasks()?;
println!("Executed {} tasks, next in {}ms", executed, delay_ms);

let delay = store.next_background_delay()?;
println!("Next task due in {}ms", delay);
```

### 7.2 Automatic Thread

When `enable_background_thread = true` (default), a background thread automatically runs:
- Flush (every `flush_interval` seconds)
- Idle-close (segments idle for `idle_timeout` seconds)
- Cache eviction (entries idle for `cache_idle_timeout` seconds)
- Retention reclaim (daily at `retention_check_hour` UTC)

---

## 8. Error Types

```rust
pub enum TmslError {
    AlreadyExists,
    NotFound,
    InvalidData(String),
    SegmentFull,
    ReadOnly,
    Io(std::io::Error),
    // ... other variants
}
```

All operations return `Result<T, TmslError>`. Use pattern matching or `?` operator for error handling.

---

## 9. Index Modes

### Sparse Mode (`index_continuous = 0`)

- Binary search on `(timestamp, block_offset)` pairs
- Supports arbitrary timestamp values
- Out-of-order writes allowed (if timestamp already exists)
- Choose when: timestamps are irregular, event-driven, or have large gaps

### Continuous Mode (`index_continuous = 1`)

- Mathematical formula: `position = (timestamp - base_timestamp) / time_step`
- Timestamps must be dense sequential integers
- `write(ts)` fills the appropriate position, creating filler prefixes as needed
- O(1) timestamp-to-position calculation within a segment
- Choose when: timestamps are dense sequential integers (e.g., per-second sensor readings with few gaps)