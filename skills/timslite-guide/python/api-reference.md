# Python API Reference

> Complete Python API signatures, parameters, return types, and semantics.

---

## 1. Module Exports

```python
from timslite import (
    Store, StoreConfig,
    Dataset, QueryIterator, QueryLengthIterator,
    DatasetQueue, DatasetQueueConsumer,
    DatasetQueueConsumerInfo, DatasetQueueConsumerPendingEntry,
    DatasetQueueConsumerState, DatasetQueueConsumerInspectResult,
    JournalQueue, JournalQueueConsumer,
    DataSetInfo, DataSetState, DataSetInspectResult,
    TmslError,
)
```

---

## 2. Store

### 2.1 Lifecycle

#### `Store.open(data_dir: str, config: Optional[StoreConfig] = None) -> Store`

Opens or connects to a store at `data_dir`. Acquires a `.lock` file for exclusive write access. If the lock is already held and `config.read_only` is `None` (auto), falls back to read-only mode.

**Parameters**:
- `data_dir`: Path to the data directory (created if not exists)
- `config`: Optional store configuration (uses defaults if not provided)

**Returns**: `Store` on success, raises `TmslError` on failure.

**Side effects**: Creates `data_dir` if missing. Creates `.lock` file. Initializes `BlockCache`, `JournalManager`, and `BackgroundTasks` (if enabled). Scans existing datasets.

#### `store.close() -> None`

Flushes all dirty segments, closes all open datasets, closes the journal, and releases the store lock.

**Context manager**: Use `with Store.open(...) as store:` for automatic cleanup.

#### `store.is_read_only() -> bool`

Returns whether this store resolved to read-only mode at open time.

### 2.2 Dataset Management

#### `store.create_dataset(name, dataset_type, *, data_segment_size=None, index_segment_size=None, compress_level=None, index_continuous=False, initial_data_segment_size=None, initial_index_segment_size=None, enable_journal=False) -> Dataset`

Creates a new dataset and returns a `Dataset` object. All configuration parameters are optional keyword arguments that override store defaults.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `dataset_type`: Dataset type, same naming rules
- `data_segment_size`: Data segment file size limit in bytes
- `index_segment_size`: Index segment file size limit in bytes
- `compress_level`: Compression level 0-9 (0=none, 9=max)
- `index_continuous`: `False` = sparse mode, `True` = continuous mode
- `initial_data_segment_size`: Initial data segment file size in bytes
- `initial_index_segment_size`: Initial index segment file size in bytes
- `enable_journal`: Whether to write journal entries for this dataset

**Returns**: `Dataset` object for read/write operations.

**Errors**:
- `TmslAlreadyExistsError` if dataset already exists
- `TmslInvalidDataError` if name/type invalid

**Example**:
```python
# Default config
ds = store.create_dataset("sensor", "waveform")

# With custom config
ds = store.create_dataset("sensor", "waveform",
    index_continuous=True,
    compress_level=9,
    retention_window=86400,
)
```

#### `store.open_dataset(name: str, dataset_type: str) -> Dataset`

Opens an existing dataset and returns a `Dataset` object.

**Errors**: `TmslNotFoundError` if dataset doesn't exist.

#### `store.open_dataset_by_identifier(identifier: int) -> Dataset`

Opens a dataset by its numeric identifier (assigned at creation time).

#### `store.drop_dataset(name: str, dataset_type: str) -> None`

Deletes a dataset and all its files (data segments, index segments, meta, state, queue). Irreversible.

#### `store.get_dataset_names() -> List[str]`

Returns all unique dataset names in the store.

#### `store.get_dataset_types(name: str) -> List[str]`

Returns all dataset types for a given dataset name.

#### `store.inspect_dataset(name: str, dataset_type: str) -> DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

### 2.3 Background Tasks

#### `store.tick_background_tasks() -> Tuple[int, int]`

Manually executes pending background tasks. Returns `(executed_tasks, next_delay_ms)`.

#### `store.next_background_delay() -> int`

Returns delay in milliseconds until the next background task is due.

### 2.4 Queue Management

#### `store.open_queue(dataset_id: int) -> DatasetQueue`

Opens the queue subsystem for a dataset. The `dataset_id` is the `ds.id` attribute of an opened `Dataset`.

**Raises**: `TmslQueueAlreadyOpenError` if queue is already open for this dataset.

---

## 3. Dataset

### 3.1 Identity

#### `ds.identifier -> int`

Numeric dataset ID assigned by Store.

#### `ds.id -> int`

Internal dataset ID used for queue operations (via `store.open_queue(ds.id)`).

#### `ds.data_dir -> str`

Base directory path of this dataset.

#### `ds.latest_timestamp -> Optional[int]`

Property. Returns the `latest_written_timestamp` for this dataset, or `None` if the dataset is empty.

### 3.2 Write Operations

#### `ds.write(timestamp: int, data: bytes) -> None`

Writes a record at the given timestamp.

**Rules**:
- `timestamp >= latest_written_timestamp` → new write (or correction if equal)
- `timestamp < latest_written_timestamp` and index entry exists → out-of-order rewrite (sparse mode only)
- `timestamp < latest_written_timestamp` and no index entry → error
- `len(data) <= 4 MiB` (otherwise `TmslInvalidDataError`)

#### `ds.write_now(data: bytes) -> None`

Writes a record using the current Unix timestamp (seconds) as the timestamp.

**Rules**:
- `len(data) <= 4 MiB` (otherwise `TmslInvalidDataError`)

#### `ds.append(timestamp: int, data: bytes) -> None`

Forward append or in-place tail append.

**Rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + len(data) <= 4 MiB`

#### `ds.append_now(data: bytes) -> None`

Appends data to a record using the current Unix timestamp (seconds).

#### `ds.delete(timestamp: int) -> None`

Deletes a record by timestamp. Does NOT retroactively update `latest_written_timestamp`.

**Errors**:
- `TmslNotFoundError` if no record exists at that timestamp
- `TmslInvalidDataError` if dataset is empty or timestamp is expired

### 3.3 Read Operations

#### `ds.read(timestamp: int) -> Optional[Tuple[int, bytes]]`

Reads a single complete record by exact timestamp.

**Returns**:
- `(timestamp, data)` if the record exists and is valid
- `None` if the record doesn't exist, is a filler, is deleted, or is expired

**Note**: `read(-1)` reads the record at timestamp `-1`, NOT the latest. Use `read_latest()` for the latest.

#### `ds.read_latest() -> Optional[Tuple[int, bytes]]`

Reads the record at `latest_written_timestamp`. Returns `None` if never written, deleted, or expired.

#### `ds.read_exist(timestamp: int) -> bool`

Fast existence check — index lookup only, no data segment I/O.

#### `ds.read_length(timestamp: int) -> Optional[int]`

Reads only the data length of a record (reads the 12-byte record header, not the full data). Returns `None` if not found.

### 3.4 Query Operations

#### `ds.query(start_ts: int, end_ts: int) -> QueryIterator`

Lazy range query iterator. Records are read on-demand via iteration.

**Returns**: `QueryIterator` that yields `(timestamp, data)` tuples.

**Note**: Unlike Rust `query()` which returns `Vec`, Python `query()` returns a lazy iterator. Use `query_all()` for an eager list.

#### `ds.query_all(start_ts: int, end_ts: int) -> List[Tuple[int, bytes]]`

Eager range query. Loads all data into memory. Equivalent to `list(ds.query(start_ts, end_ts))`.

#### `ds.query_exist(start_ts: int, end_ts: int) -> bytes`

Fast range existence bitmap. Returns a bitmap where bit `i` is set if a record exists at `start_ts + i`.

#### `ds.query_length(start_ts: int, end_ts: int) -> QueryLengthIterator`

Lazy range query returning a `QueryLengthIterator` that yields `(timestamp, data_length)` tuples. Reads only record headers (12 bytes each).

#### `ds.query_length_all(start_ts: int, end_ts: int) -> List[Tuple[int, int]]`

Eager range query returning `(timestamp, data_length)` pairs. Equivalent to `list(ds.query_length(start_ts, end_ts))`.

### 3.5 Maintenance

#### `ds.flush() -> None`

Flushes all dirty segments for this dataset to disk.

#### `ds.inspect() -> DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

---

## 4. QueryIterator

Lazy iterator returned by `query()`.

```python
for ts, data in ds.query(1, 1000):
    print(f"ts={ts}: {data.decode()}")
```

**Behavior**:
- Reads records on-demand from data segments
- Implements `Iterator[Tuple[int, bytes]]`
- Automatically releases resources when dropped

**Methods**:
- `reverse()` — Reverse the iteration direction
- `skip(count: int)` — Skip the first `count` records
- `collect_all() -> List[Tuple[int, bytes]]` — Collect all remaining records into a list
- `collect_take(count: int) -> List[Tuple[int, bytes]]` — Collect up to `count` records
- `close()` — Explicitly release the iterator resources

---

## 5. QueryLengthIterator

Lazy iterator returned by `query_length()`.

```python
for ts, length in ds.query_length(1, 1000):
    print(f"ts={ts}: {length} bytes")
```

**Behavior**:
- Reads record headers on-demand from data segments
- Implements `Iterator[Tuple[int, int]]`
- Automatically releases resources when dropped

**Methods**:
- `reverse()` — Reverse the iteration direction
- `skip(count: int)` — Skip the first `count` records
- `collect_all() -> List[Tuple[int, int]]` — Collect all remaining entries into a list
- `collect_take(count: int) -> List[Tuple[int, int]]` — Collect up to `count` entries
- `close()` — Explicitly release the iterator resources

---

## 6. Configuration

### 6.1 StoreConfig

```python
config = timslite.StoreConfig(
    flush_interval=15,           # seconds
    idle_timeout=1800,           # 30 minutes
    data_segment_size=64 * 1024 * 1024,  # 64 MiB
    index_segment_size=4 * 1024 * 1024,   # 4 MiB
    initial_data_segment_size=256 * 1024,  # 256 KiB
    initial_index_segment_size=4 * 1024,   # 4 KiB
    compress_level=6,            # 0-9
    cache_max_memory=256 * 1024 * 1024,  # 256 MiB
    cache_idle_timeout=1800,     # 30 minutes
    retention_check_hour=0,      # UTC hour 0-23
    enable_background_thread=True,
    enable_journal=True,
    read_only=None,              # None=auto, True=force RO, False=require writable
)
```

**Note**: `StoreConfig` does not have a `compress_type` parameter. The compression type is selected automatically.

**Default values** (when `StoreConfig()` is called with no arguments):
- `flush_interval`: 15
- `idle_timeout`: 1800
- `data_segment_size`: 67108864 (64 MiB)
- `index_segment_size`: 16777216 (16 MiB)
- `initial_data_segment_size`: 262144 (256 KiB)
- `initial_index_segment_size`: 16384 (16 KiB)
- `compress_level`: 6
- `cache_max_memory`: 268435456 (256 MiB)
- `cache_idle_timeout`: 1800
- `retention_check_hour`: 0
- `enable_background_thread`: True
- `enable_journal`: True
- `read_only`: None

### 6.2 CreateDatasetOptions (via create_dataset kwargs)

Dataset configuration is passed as keyword arguments to `store.create_dataset()`:

```python
ds = store.create_dataset("sensor", "waveform",
    data_segment_size=128 * 1024 * 1024,  # 128 MiB
    index_segment_size=8 * 1024 * 1024,    # 8 MiB
    initial_data_segment_size=512 * 1024,  # 512 KiB
    initial_index_segment_size=4 * 1024,   # 4 KiB
    compress_level=9,                      # 0-9
    index_continuous=True,                 # False=sparse, True=continuous
    enable_journal=True,
)
```

**Note**: There is no `DataSetConfig` class in the Python wrapper. Use `store.create_dataset()` keyword arguments instead.

### 6.3 QueueConsumerOptions (via open_consumer kwargs)

```python
consumer = queue.open_consumer("worker_group",
    running_expired_seconds=900,  # default 900, max 65535
    max_retry_count=3,           # default 3, max 255
)
```

---

## 7. Queue Types

### 7.1 DatasetQueue

Obtained via `store.open_queue(ds.id)`.

**Key behavior**:
- `push(data)` auto-assigns `timestamp = latest_written_timestamp + 1`
- `poll(timeout_ms)` returns the next unacked record for this consumer group
- `ack(timestamp)` marks a record as processed
- Multiple consumer groups are independent; each maintains its own progress

#### `queue.push(data: bytes) -> int`

Push data to the queue. Returns the assigned timestamp.

#### `queue.open_consumer(group_name: str, running_expired_seconds: int = 900, max_retry_count: int = 3) -> DatasetQueueConsumer`

Open a consumer group.

#### `queue.get_consumer_group_names() -> List[str]`

Return current consumer group names.

#### `queue.drop_consumer(group_name: str) -> None`

Drop (close and remove) a consumer group. The consumer group's state file is synced and deleted.

#### `queue.close() -> None`

Close the queue and all associated consumers.

### 7.2 DatasetQueueConsumer

Obtained via `queue.open_consumer("group_name")`.

#### `consumer.poll(timeout_ms: int) -> Optional[Tuple[int, bytes]]`

Poll for the next unacked record. Returns `(timestamp, data)` or `None` on timeout.

**Args**:
- `timeout_ms`: Maximum wait time in milliseconds. Use 0 for non-blocking poll.

#### `consumer.ack(timestamp: int) -> None`

Acknowledge a polled record.

#### `consumer.flush() -> None`

Flush this consumer group's state file.

#### `consumer.close() -> None`

Close this consumer group. All pending records are synced and unacknowledged entries are released for redelivery after reopen.

#### `consumer.inspect() -> DatasetQueueConsumerInspectResult`

Inspect this consumer group's public config and durable state.

#### `consumer.poll_callback(callback: Optional[Callable]) -> None`

Register or clear a lightweight wake callback. The callback is invoked synchronously after data waiters are notified. Pass `None` to clear the callback.

---

## 8. Queue Data Types

### 8.1 DatasetQueueConsumerInfo

Returned by `consumer.inspect().info`.

```python
info.group_name: str           # Consumer group name
info.running_expired_seconds: int  # Pending record expiry (seconds)
info.max_retry_count: int      # Max retry count for redelivery
```

### 8.2 DatasetQueueConsumerPendingEntry

Returned in `consumer.inspect().state.pending_entries`.

```python
entry.timestamp: int           # Record timestamp
entry.start_time: int          # When the record was polled
entry.status: int              # Pending status
entry.retry_count: int         # Current retry count
```

### 8.3 DatasetQueueConsumerState

Returned by `consumer.inspect().state`.

```python
state.processed_ts: int        # Last acknowledged timestamp
state.pending_entries: List[DatasetQueueConsumerPendingEntry]
```

### 8.4 DatasetQueueConsumerInspectResult

Returned by `consumer.inspect()`.

```python
result.info: DatasetQueueConsumerInfo
result.state: DatasetQueueConsumerState
```

---

## 9. Journal

### 9.1 Journal API

#### `store.journal_latest_sequence() -> Optional[int]`

Get the latest journal sequence number.

#### `store.journal_read(sequence: int) -> Optional[Tuple[int, bytes]]`

Read a journal record by sequence.

#### `store.journal_query(start: int, end: int) -> List[Tuple[int, bytes]]`

Range query journal records.

### 9.2 Journal Queue

#### `store.open_journal_queue() -> JournalQueue`

Open a journal queue for consumption.

#### `jq.open_consumer(group_name: str, running_expired_seconds: int = 900, max_retry_count: int = 3) -> JournalQueueConsumer`

Open a consumer group for journal consumption.

#### `jq.close() -> None`

Close the journal queue.

### 9.3 JournalQueueConsumer

#### `consumer.poll(timeout_ms: int) -> Optional[Tuple[int, bytes]]`

Poll for the next journal record. Returns `(sequence, payload)` or `None` on timeout.

#### `consumer.ack(sequence: int) -> None`

Acknowledge a journal record.

#### `consumer.poll_callback(callback: Optional[Callable]) -> None`

Register or clear a lightweight wake callback.

---

## 10. DataSet Inspection

### 10.1 DataSetInspectResult

Returned by `store.inspect_dataset()` and `ds.inspect()`.

```python
result.info: DataSetInfo       # Immutable config
result.state: DataSetState     # Mutable runtime state
```

### 10.2 DataSetInfo

Immutable dataset configuration info.

```python
info.name: str                 # Dataset name
info.dataset_type: str         # Dataset type
info.base_dir: str             # Dataset directory path
info.identifier: int           # Store-assigned numeric dataset identifier
info.data_segment_size: int    # Data segment file size limit (bytes)
info.index_segment_size: int   # Index segment file size limit (bytes)
info.initial_data_segment_size: int  # Initial data segment file size (bytes)
info.initial_index_segment_size: int # Initial index segment file size (bytes)
info.compress_type: int        # Compression algorithm type (0=zstd, 1=deflate)
info.compress_level: int       # Compression level (0-9)
info.index_continuous: int     # Index mode: 0=sparse, 1=continuous
info.retention_window: int     # Data retention window (same unit as timestamp, 0=no limit)
info.enable_journal: bool      # Whether this dataset records journal entries
info.create_time: int          # Dataset creation time (Unix milliseconds)
```

### 10.3 DataSetState

Mutable dataset runtime state.

```python
state.latest_written_timestamp: Optional[int]  # Highest written timestamp
state.open_data_segments: int  # Number of currently open data segments
state.data_segments: int       # Total number of data segments
state.total_record_count: int  # Total record count across all data segments
state.total_data_size: int     # Total used space across all data segments (bytes)
state.total_original_size: int # Total uncompressed size across all data segments (bytes)
state.index_segments: int      # Total number of index segments
```

---

## 11. Error Types

All operations raise `TmslError` (base) on failure. The error hierarchy:

```python
TmslError                      # Base exception for all timslite errors
├── TmslIoError                # I/O error (file not found, permission denied, etc.)
├── TmslNotFoundError          # Dataset, segment, or handle not found
├── TmslAlreadyExistsError     # Dataset already exists
├── TmslInvalidDataError       # Invalid data: bad timestamp, out-of-order, duplicate, corrupt block
├── TmslSegmentFullError       # Segment file is full (expansion needed)
├── TmslMmapError              # Memory-mapping error
├── TmslCompressionError       # Compression failure
├── TmslDecompressionError     # Decompression failure
├── TmslExpiredError           # Timestamp is outside the retention window
├── TmslQueueAlreadyOpenError  # Queue is already open for this dataset
├── TmslQueueNotOpenError      # Queue is not open for this dataset
├── TmslConsumerGroupNotFoundError  # Consumer group not found
├── TmslConsumerGroupExistsError    # Consumer group already exists
├── TmslQueueClosedError       # Queue has been closed
└── TmslPendingFullError       # Pending entries limit reached (max 239)
```

**Example**:
```python
try:
    store.create_dataset("sensor", "waveform")
except timslite.TmslAlreadyExistsError as e:
    print(f"Already exists: {e}")
except timslite.TmslError as e:
    print(f"Error: {e}")
```

---

## 12. Index Modes

### Sparse Mode (`index_continuous=False`)

- Binary search on `(timestamp, block_offset)` pairs
- Supports arbitrary timestamp values
- Out-of-order writes allowed (if timestamp already exists)
- Choose when: timestamps are irregular, event-driven, or have large gaps

### Continuous Mode (`index_continuous=True`)

- Mathematical formula: `position = (timestamp - base_timestamp) / time_step`
- Timestamps must be dense sequential integers
- `write(ts)` fills the appropriate position, creating filler prefixes as needed
- O(1) timestamp-to-position calculation within a segment
- Choose when: timestamps are dense sequential integers (e.g., per-second sensor readings with few gaps)
