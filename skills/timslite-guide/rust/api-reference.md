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

#### `Store::is_read_only(&self) -> bool`

Returns whether this store resolved to read-only mode at open time. Read-only stores cannot create/drop datasets, write data, or open queues.

### 1.2 Dataset Management

#### `Store::create_dataset(&mut self, name: &str, dataset_type: &str, data_segment_size: u64, index_segment_size: u64, compress_level: u8, index_continuous: u8, retention_window: u64) -> Result<DataSet>`

Creates a new dataset with explicit parameters.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `dataset_type`: Dataset type, same naming rules
- `data_segment_size`: Max data segment file size in bytes (must be > 0)
- `index_segment_size`: Max index segment file size in bytes (must be > 0)
- `compress_level`: Compression level 0-9 (clamped to 9 if > 9)
- `index_continuous`: 0 = sparse mode, 1 = continuous mode
- `retention_window`: Retention window in timestamp units (0 = no limit)

**Returns**: `DataSet` instance for direct read/write operations.

**Errors**:
- `AlreadyExists` if dataset already exists
- `InvalidData` if name/type invalid or parameters out of range
- Rejects the reserved name `.journal`

#### `Store::create_dataset_with_config(&mut self, name: &str, dataset_type: &str, config: Option<DataSetConfigBuilder>) -> Result<DataSet>`

Creates a dataset with optional custom configuration. If `config` is `None`, uses store defaults.

**Parameters**:
- `name`: Dataset name
- `dataset_type`: Dataset type
- `config`: Optional `DataSetConfigBuilder` (None = use store defaults)

**Returns**: `DataSet` instance.

#### `Store::open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSet>`

Opens an existing dataset by name and type. Returns the cached handle if already open.

**Parameters**:
- `name`: Dataset name
- `dataset_type`: Dataset type

**Returns**: `DataSet` instance.

**Errors**:
- `NotFound` if dataset does not exist
- Rejects the reserved name `.journal`

#### `Store::open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSet>`

Opens an existing dataset by its Store-assigned numeric identifier.

**Parameters**:
- `identifier`: Dataset identifier (must be > 0)

**Returns**: `DataSet` instance.

#### `Store::drop_dataset(&mut self, name: &str, dataset_type: &str) -> Result<()>`

Deletes an entire dataset directory (destructive, not recoverable).

**Parameters**:
- `name`: Dataset name
- `dataset_type`: Dataset type

**Errors**:
- `NotFound` if dataset does not exist
- Rejects the reserved name `.journal`

#### `Store::inspect_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult>`

Inspects a dataset by opening it (if needed) and returning config + state.

**Parameters**:
- `name`: Dataset name
- `dataset_type`: Dataset type

**Returns**: `DataSetInspectResult` with `info` (DataSetInfo) and `state` (DataSetState).

### 1.3 Listing

#### `Store::get_dataset_names(&self) -> Result<Vec<String>>`

Returns all unique dataset names in the store (sorted).

#### `Store::get_dataset_types(&self, name: &str) -> Result<Vec<String>>`

Returns all dataset types for a given name (sorted).

### 1.4 Journal

#### `Store::journal_latest_sequence(&self) -> Result<Option<i64>>`

Returns the latest journal sequence, or `None` when the journal is empty.

#### `Store::journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>`

Reads one encoded journal record by sequence.

**Parameters**:
- `sequence`: Journal sequence number

**Returns**: `Some((sequence, payload))` if found, `None` if not found.

#### `Store::journal_query(&self, start_sequence: i64, end_sequence: i64) -> Result<Vec<(i64, Vec<u8>)>>`

Queries encoded journal records by inclusive sequence range.

**Parameters**:
- `start_sequence`: Start of range (inclusive)
- `end_sequence`: End of range (inclusive)

**Returns**: `Vec<(sequence, payload)>` for matching records.

#### `Store::open_journal_queue(&mut self) -> Result<JournalQueue>`

Opens the built-in journal queue for reliable consumption.

**Errors**:
- `InvalidData` if store is read-only

#### `Store::read_journal_source_record(&mut self, dataset_identifier: u64, index_info: JournalIndexInfo) -> Result<(i64, Vec<u8>)>`

Reads the source dataset record referenced by a journal write/delete/append record.

**Parameters**:
- `dataset_identifier`: Dataset identifier from the journal record
- `index_info`: Index info from the journal record (timestamp, block_offset, in_block_offset)

**Returns**: `(timestamp, data)` of the source record.

### 1.5 Background Tasks

#### `Store::tick_background_tasks(&self) -> Result<TickResult>`

Executes one tick of all background tasks synchronously. Checks if flush, idle-close, cache eviction, or retention reclaim are due and runs them immediately.

**Returns**: `TickResult` with `executed_tasks` count and `next_delay_ms` until next task.

**Errors**:
- `InvalidData` if store is read-only

#### `Store::next_background_delay(&self) -> Result<Duration>`

Returns the duration until the next background task is due. Reads a snapshot without running any tasks.

**Returns**: `Duration` until next task.

### 1.6 Accessors

#### `Store::block_cache(&self) -> &Arc<BlockCache>`

Returns a reference to the global block cache.

#### `Store::config(&self) -> &StoreConfig`

Returns a reference to the store config.

---

## 2. DataSet

`DataSet` handles per-dataset read/write operations. It is `Clone` (backed by `Arc<Mutex<DataSetInner>>`).

### 2.1 Read Operations

#### `DataSet::read(&mut self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>`

Reads a record by timestamp. Returns `None` if not found, deleted (filler), or expired.

**Parameters**:
- `timestamp`: Exact timestamp to read (`-1` reads timestamp `-1`, not latest)

**Returns**: `Some((timestamp, data))` if found, `None` otherwise.

#### `DataSet::read_latest(&mut self) -> Result<Option<(i64, Vec<u8>)>>`

Reads the record at `latest_written_timestamp` without searching backward.

**Returns**: `Some((timestamp, data))` if found, `None` if empty or deleted.

#### `DataSet::read_exist(&mut self, timestamp: i64) -> Result<bool>`

Checks if a visible record exists at the given timestamp.

**Parameters**:
- `timestamp`: Exact timestamp to check

**Returns**: `true` if record exists and is not filler/expired.

#### `DataSet::read_length(&mut self, timestamp: i64) -> Result<Option<u32>>`

Reads the logical data length for a timestamp without reading the data.

**Parameters**:
- `timestamp`: Exact timestamp to check

**Returns**: `Some(data_len)` if record exists, `None` if not found, filler, or expired.

### 2.2 Query Operations

#### `DataSet::query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>>`

Queries records in the time range `[start_ts, end_ts]` (eager - loads all into memory). Filler entries are skipped.

**Parameters**:
- `start_ts`: Start of range (inclusive)
- `end_ts`: End of range (inclusive)

**Returns**: `Vec<(timestamp, data)>` for visible records.

#### `DataSet::query_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryIterator>`

Returns a lazy iterator for records in `[start_ts, end_ts]`. Better for large ranges.

**Parameters**:
- `start_ts`: Start of range (inclusive)
- `end_ts`: End of range (inclusive)

**Returns**: `QueryIterator` yielding `Result<(i64, Vec<u8>)>`.

#### `DataSet::query_length(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>`

Queries data lengths for timestamps in `[start_ts, end_ts]` (eager). Skips filler entries.

**Parameters**:
- `start_ts`: Start of range (inclusive)
- `end_ts`: End of range (inclusive)

**Returns**: `Vec<(timestamp, data_len)>` for visible records.

#### `DataSet::query_length_iter(&self, start_ts: i64, end_ts: i64) -> Result<QueryLengthIterator>`

Returns a lazy iterator for data lengths in `[start_ts, end_ts]`.

**Parameters**:
- `start_ts`: Start of range (inclusive)
- `end_ts`: End of range (inclusive)

**Returns**: `QueryLengthIterator` yielding `Result<(i64, u32)>`.

### 2.3 Write Operations

#### `DataSet::write(&mut self, timestamp: i64, data: &[u8]) -> Result<()>`

Writes a record to the dataset. Dispatch behavior depends on timestamp relative to `latest_written_timestamp`:
- `timestamp > latest_written_timestamp`: Normal write (append)
- `timestamp == latest_written_timestamp`: Correction write (in-place overwrite if uncompressed, else out-of-order)
- `timestamp < latest_written_timestamp`: Out-of-order write (append + index update)

**Parameters**:
- `timestamp`: Record timestamp (must be > 0)
- `data`: Record data (max 4 MiB)

**Errors**:
- `InvalidData` if read-only, timestamp <= 0, or data > 4 MiB
- `SegmentFull` if segment cannot fit the record

#### `DataSet::correct(&mut self, timestamp: i64, data: &[u8]) -> Result<()>`

Corrects an existing record's data in place.

**Parameters**:
- `timestamp`: Existing timestamp to correct
- `data`: New data (max 4 MiB)

**Errors**:
- `NotFound` if no index entry exists at `timestamp`
- Falls back to out-of-order write if block is sealed/compressed

#### `DataSet::append(&mut self, timestamp: i64, data: &[u8]) -> Result<()>`

Appends data to an existing record. Only works on uncompressed tail records.

**Parameters**:
- `timestamp`: Must be `>= latest_written_timestamp`
- `data`: Data to append

**Behavior**:
- `timestamp == latest_written_timestamp`: Appends to existing tail record (only if uncompressed)
- `timestamp > latest_written_timestamp`: Creates new record (forward append)

#### `DataSet::delete(&mut self, timestamp: i64) -> Result<()>`

Deletes a record by marking its index entry as filler.

**Parameters**:
- `timestamp`: Existing timestamp to delete

**Errors**:
- `NotFound` if dataset is empty, no entry at timestamp, or entry is already filler
- `Expired` if timestamp is outside retention window

### 2.4 Lifecycle

#### `DataSet::flush(&self) -> Result<()>`

Flushes all dirty segments (data and index) to disk via mmap sync.

#### `DataSet::close(&self) -> Result<()>`

Closes all segments, invalidates cache entries, and marks the dataset as closed.

#### `DataSet::touch(&self) -> Result<()>`

Updates the `last_used_at` timestamp to prevent idle-close.

### 2.5 Queue Operations

#### `DataSet::open_queue(&self) -> Result<DatasetQueue>`

Opens the queue subsystem for this dataset. Only one queue can be open per dataset.

**Errors**:
- `QueueAlreadyOpen` if queue is already open
- `InvalidData` if dataset is read-only

#### `DataSet::close_queue(&self) -> Result<()>`

Closes the queue subsystem and syncs consumer state files.

### 2.6 Inspection

#### `DataSet::inspect(&self) -> Result<DataSetInspectResult>`

Returns immutable config and mutable runtime state.

**Returns**: `DataSetInspectResult` with `info` and `state`.

### 2.7 Accessors

#### `DataSet::base_dir(&self) -> PathBuf`

Returns the dataset's base directory path.

#### `DataSet::last_used_at(&self) -> Instant`

Returns the last used time (for idle-close detection).

#### `DataSet::retention_window(&self) -> u64`

Returns the retention window (0 = no limit).

#### `DataSet::enable_journal(&self) -> bool`

Returns whether this dataset records journal entries.

#### `DataSet::latest_written_timestamp(&self) -> Option<i64>`

Returns the highest written timestamp (`None` if empty). Not the latest valid record—deletion doesn't roll back.

---

## 3. Iterators

### 3.1 QueryIterator

```rust
let iter = ds.query_iter(start_ts, end_ts)?;
```

**Methods**:
- `next() -> Option<Result<(i64, Vec<u8>)>>` — Yield next record
- `reverse(&mut self)` — Reverse iteration direction (must call before first `next()`)
- `skip(&mut self, count: usize)` — Skip N records (must call before first `next()`)
- `collect_all(&mut self) -> Result<Vec<(i64, Vec<u8>)>>` — Collect all remaining records

**Behavior**:
- Skips filler entries automatically
- Closes automatically when iteration completes or is dropped
- Uses a hot block cache for efficient sequential reads

### 3.2 QueryLengthIterator

```rust
let iter = ds.query_length_iter(start_ts, end_ts)?;
```

**Methods**:
- `next() -> Option<Result<(i64, u32)>>` — Yield next (timestamp, length)
- `reverse(&mut self)` — Reverse direction
- `skip(&mut self, count: usize)` — Skip N entries
- `collect_all(&mut self) -> Result<Vec<(i64, u32)>>` — Collect all remaining

---

## 4. Queue

### 4.1 DatasetQueue

```rust
let queue = ds.open_queue()?;
```

**Methods**:
- `open_consumer(&self, group_name: &str, config: QueueConsumerConfig) -> Result<DatasetQueueConsumer>` — Open a consumer group
- `push(&self, data: &[u8]) -> Result<i64>` — Push data (auto-assigns next timestamp)
- `close(&self) -> Result<()>` — Close the queue

### 4.2 DatasetQueueConsumer

```rust
let mut consumer = queue.open_consumer("group", config)?;
```

**Methods**:
- `poll(&mut self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>` — Poll for data (blocks up to timeout)
- `ack(&mut self, timestamp: i64) -> Result<()>` — Acknowledge processing
- `flush(&self) -> Result<()>` — Sync consumer state to disk
- `inspect(&self) -> Result<DatasetQueueConsumerInspectResult>` — Inspect consumer state
- `close(&mut self) -> Result<()>` — Close the consumer

### 4.3 QueueConsumerConfig

```rust
let config = QueueConsumerConfig {
    running_expired_secs: 60,  // seconds before stuck task is retried
    max_retry_count: 3,        // max retries before parked
};
```

### 4.4 Inspect Result

```rust
let inspect = consumer.inspect()?;
inspect.state.processed_ts        // Option<i64> — last acked timestamp
inspect.state.pending_entries     // Vec<PendingEntry> — in-flight entries
inspect.info.group_name           // &str
```

---

## 5. Journal

### 5.1 JournalQueue

```rust
let jq = store.open_journal_queue()?;
```

**Methods**:
- `open_consumer(&self) -> Result<JournalQueueConsumer>` — Open a consumer
- `close(&self) -> Result<()>` — Close the queue

### 5.2 JournalQueueConsumer

```rust
let mut jc = jq.open_consumer()?;
```

**Methods**:
- `poll(&mut self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>` — Poll for journal entry (sequence, payload)
- `ack(&mut self, sequence: i64) -> Result<()>` — Acknowledge entry
- `get(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>` — Get specific entry without advancing cursor
- `close(&mut self) -> Result<()>` — Close the consumer

---

## 6. Configuration

### 6.1 StoreConfig

```rust
use std::time::Duration;
use timslite::StoreConfig;

let config = StoreConfig::builder()
    .flush_interval(Duration::from_secs(15))     // mmap sync interval
    .idle_timeout(Duration::from_secs(1800))     // segment idle-close timeout
    .data_segment_size(64 * 1024 * 1024)        // 64 MiB
    .index_segment_size(16 * 1024 * 1024)       // 16 MiB
    .initial_data_segment_size(256 * 1024)      // 256 KiB
    .initial_index_segment_size(16 * 1024)      // 16 KiB
    .compress_level(6)                          // 0-9
    .compress_type(0)                           // 0=zstd, 1=deflate
    .cache_max_memory(256 * 1024 * 1024)        // 256 MiB
    .cache_idle_timeout(Duration::from_secs(1800))
    .retention_check_hour(0)                    // UTC hour 0-23
    .enable_background_thread(true)
    .enable_journal(true)
    .read_only(None)                            // None=auto, Some(false)=require writable, Some(true)=force RO
    .build();
```

**Defaults**:
- `flush_interval`: 15s
- `idle_timeout`: 30min
- `data_segment_size`: 64 MiB
- `index_segment_size`: 16 MiB
- `initial_data_segment_size`: 256 KiB
- `initial_index_segment_size`: 16 KiB
- `compress_level`: 6
- `compress_type`: zstd (0)
- `cache_max_memory`: 256 MiB
- `cache_idle_timeout`: 30min
- `retention_check_hour`: 0 (UTC 00:00)
- `enable_background_thread`: true
- `enable_journal`: true
- `read_only`: None (auto)

### 6.2 DataSetConfigBuilder

```rust
use timslite::DataSetConfigBuilder;

// Pre-fill from store defaults
let builder = DataSetConfigBuilder::from_store(&store.config())
    .data_segment_size(128 * 1024 * 1024)
    .compress_level(9)
    .index_continuous(1)
    .retention_window(86400)
    .enable_journal(true);

// Or create from scratch
let builder = DataSetConfigBuilder::new()
    .data_segment_size(64 * 1024 * 1024)
    .index_segment_size(4 * 1024 * 1024)
    .initial_data_segment_size(256 * 1024)
    .initial_index_segment_size(4096)
    .compress_level(6)
    .compress_type(0)
    .index_continuous(0)
    .retention_window(0)
    .enable_journal(false);
```

**Methods**:
- `from_store(config: &StoreConfig) -> Self` — Pre-fill with store defaults
- `new() -> Self` — Create empty builder
- `data_segment_size(size: u64) -> Self`
- `index_segment_size(size: u64) -> Self`
- `initial_data_segment_size(size: u64) -> Self`
- `initial_index_segment_size(size: u64) -> Self`
- `compress_level(level: u8) -> Self` — 0-9 (clamped)
- `compress_type(compress_type: u8) -> Self` — 0=zstd, 1=deflate
- `index_continuous(value: u8) -> Self` — 0=sparse, 1=continuous (clamped)
- `retention_window(units: u64) -> Self` — 0=no limit
- `enable_journal(enable: bool) -> Self`

---

## 7. Inspection Types

### 7.1 DataSetInfo (Immutable Config)

```rust
pub struct DataSetInfo {
    pub name: String,
    pub dataset_type: String,
    pub identifier: u64,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub compress_type: u8,
    pub compress_level: u8,
    pub index_continuous: u8,
    pub retention_window: u64,
    pub enable_journal: bool,
    pub create_time: i64,  // Unix milliseconds
}
```

### 7.2 DataSetState (Mutable Runtime State)

```rust
pub struct DataSetState {
    pub latest_written_timestamp: Option<i64>,
    pub open_data_segments: u32,
    pub data_segments: u32,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub open_index_segments: u32,
    pub index_segments: u32,
    pub pending_index_entries: u32,  // always 0
    pub base_timestamp: Option<i64>,
    pub read_only: bool,
    pub has_block_cache: bool,
    pub has_journal: bool,
    pub has_queue: bool,
    pub queue_consumer_groups: u32,
}
```

### 7.3 DataSetInspectResult

```rust
pub struct DataSetInspectResult {
    pub info: DataSetInfo,
    pub state: DataSetState,
}
```

---

## 8. Background Tasks

### 8.1 TickResult

```rust
pub struct TickResult {
    pub executed_tasks: u32,  // number of tasks executed
    pub next_delay_ms: u64,   // ms until next task is due
}
```

---

## 9. Error Types

```rust
pub enum TmslError {
    Io(io::Error),
    InvalidMagic,
    InvalidVersion(u16),
    MmapError(String),
    CompressionError(String),
    DecompressionError(String),
    InvalidData(String),
    NotFound(String),
    Expired(String),
    AlreadyExists(String),
    SegmentFull,
    QueueAlreadyOpen(String),
    QueueNotOpen(String),
    ConsumerGroupNotFound(String),
    ConsumerGroupExists(String),
    QueueClosed(String),
    PendingFull(String),
}
```

All variants implement `Display` and `Error`.

---

## 10. Constants

```rust
pub const QUEUE_STATE_MAGIC: [u8; 4] = *b"QSTF";
pub const QUEUE_STATE_VERSION: u32 = 1;
pub const QUEUE_STATE_FILE_SIZE: usize = 4096;

pub const JOURNAL_DATASET_NAME: &str = ".journal";
pub const JOURNAL_DATASET_TYPE: &str = "logs";
```
