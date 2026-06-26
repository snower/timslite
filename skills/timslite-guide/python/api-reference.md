# Python API Reference

> Complete Python API signatures, parameters, return types, and semantics.

---

## 1. Module Exports

```python
from timslite import (
    Store, StoreConfig,
    Dataset, QueryIterator,
    DatasetQueue, DatasetQueueConsumer,
    JournalQueue, JournalQueueConsumer,
    DataSetConfig,
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

#### `store.create_dataset(name: str, dataset_type: str) -> None`

Creates a new dataset with default configuration.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `dataset_type`: Dataset type, same naming rules

**Errors**:
- `AlreadyExists` if dataset already exists
- `InvalidData` if name/type invalid

#### `store.create_dataset_with_config(name: str, dataset_type: str, config: Optional[DataSetConfig] = None) -> None`

Creates a dataset with optional custom configuration. Pass `None` to use store defaults.

#### `store.open_dataset(name: str, dataset_type: str) -> Dataset`

Opens an existing dataset and returns a `Dataset` object.

**Errors**: `NotFound` if dataset doesn't exist.

#### `store.open_dataset_by_identifier(identifier: int) -> Dataset`

Opens a dataset by its numeric identifier (assigned at creation time).

#### `store.drop_dataset(name: str, dataset_type: str) -> None`

Deletes a dataset and all its files (data segments, index segments, meta, state, queue). Irreversible.

#### `store.list_datasets() -> List[Tuple[str, str]]`

Lists all dataset `(name, type)` pairs in the store.

#### `store.inspect_dataset(name: str, dataset_type: str) -> DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

### 2.3 Background Tasks

#### `store.tick_background_tasks() -> Tuple[int, int]`

Manually executes pending background tasks. Returns `(executed_tasks, next_delay_ms)`.

#### `store.next_background_delay() -> int`

Returns delay in milliseconds until the next background task is due.

---

## 3. Dataset

### 3.1 Identity

#### `ds.identifier -> int`

Numeric dataset ID assigned by Store.

#### `ds.id -> int`

Alias for `identifier`, used by `open_queue`.

### 3.2 Write Operations

#### `ds.write(timestamp: int, data: bytes) -> None`

Writes a record at the given timestamp.

**Rules**:
- `timestamp >= latest_written_timestamp` → new write (or correction if equal)
- `timestamp < latest_written_timestamp` and index entry exists → out-of-order rewrite (sparse mode only)
- `timestamp < latest_written_timestamp` and no index entry → error
- `len(data) <= 4 MiB` (otherwise `InvalidData`)

#### `ds.append(timestamp: int, data: bytes) -> None`

Forward append or in-place tail append.

**Rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + len(data) <= 4 MiB`

#### `ds.delete(timestamp: int) -> None`

Deletes a record by timestamp. Does NOT retroactively update `latest_written_timestamp`.

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

Reads only the data length of a record (reads the 12-byte record header, not the full data).

#### `ds.latest_timestamp -> Optional[int]`

Property. Returns the `latest_written_timestamp` for this dataset.

### 3.4 Query Operations

#### `ds.query(start_ts: int, end_ts: int) -> QueryIterator`

Lazy range query iterator. Records are read on-demand via iteration.

**Returns**: `QueryIterator` that yields `(timestamp, data)` tuples.

**Note**: Unlike Rust `query()` which returns `Vec`, Python `query()` returns a lazy iterator. Use `query_all()` for an eager list.

#### `ds.query_all(start_ts: int, end_ts: int) -> List[Tuple[int, bytes]]`

Eager range query. Loads all data into memory. Equivalent to `list(ds.query(start_ts, end_ts))`.

#### `ds.query_exist(start_ts: int, end_ts: int) -> bytes`

Fast range existence bitmap. Returns a bitmap where bit `i` is set if a record exists at `start_ts + i`.

#### `ds.query_length(start_ts: int, end_ts: int) -> List[Tuple[int, int]]`

Range query returning `(timestamp, data_length)` pairs. Reads only record headers (12 bytes each).

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

---

## 5. Configuration

### 5.1 StoreConfig

```python
config = timslite.StoreConfig(
    flush_interval=15,           # seconds
    idle_timeout=1800,           # 30 minutes
    data_segment_size=64 * 1024 * 1024,  # 64 MiB
    index_segment_size=4 * 1024 * 1024,   # 4 MiB
    initial_data_segment_size=256 * 1024,  # 256 KiB
    initial_index_segment_size=4 * 1024,   # 4 KiB
    compress_level=6,            # 0-9
    compress_type=0,             # 0=zstd, 1=deflate
    cache_max_memory=256 * 1024 * 1024,  # 256 MiB
    cache_idle_timeout=1800,     # 30 minutes
    retention_check_hour=0,      # UTC hour 0-23
    enable_background_thread=True,
    enable_journal=True,
    read_only=None,              # None=auto, True=force RO, False=require writable
)
```

### 5.2 DataSetConfig

```python
config = timslite.DataSetConfig(
    data_segment_size=128 * 1024 * 1024,  # 128 MiB
    index_segment_size=8 * 1024 * 1024,    # 8 MiB
    initial_data_segment_size=512 * 1024,  # 512 KiB
    initial_index_segment_size=4 * 1024,   # 4 KiB
    compress_level=9,                      # 0-9
    compress_type=0,                       # 0=zstd, 1=deflate
    index_continuous=1,                    # 0=sparse, 1=continuous
    retention_window=86400,                # 1 day in timestamp units
    enable_journal=True,
)
```

---

## 6. Queue Types

### 6.1 DatasetQueue

Obtained via `store.open_queue(ds.id)`.

**Key behavior**:
- `push(data)` auto-assigns `timestamp = latest_written_timestamp + 1`
- `poll(timeout_ms)` returns the next unacked record for this consumer group
- `ack(timestamp)` marks a record as processed
- Multiple consumer groups are independent; each maintains its own progress

#### `q.push(data: bytes) -> int`

Push data to the queue. Returns the assigned timestamp.

#### `q.open_consumer(group: str, running_expired_seconds: int = 900, max_retry_count: int = 3) -> DatasetQueueConsumer`

Open a consumer group.

#### `q.close() -> None`

Close the queue.

### 6.2 DatasetQueueConsumer

Obtained via `q.open_consumer("group_name")`.

#### `c.poll(timeout_ms: int) -> Optional[Tuple[int, bytes]]`

Poll for the next unacked record. Returns `(timestamp, data)` or `None` on timeout.

#### `c.ack(timestamp: int) -> None`

Acknowledge a polled record.

---

## 7. Journal

### 7.1 Journal API

#### `store.journal_latest_sequence() -> Optional[int]`

Get the latest journal sequence number.

#### `store.journal_read(sequence: int) -> Optional[Tuple[int, bytes]]`

Read a journal record by sequence.

#### `store.journal_query(start: int, end: int) -> List[Tuple[int, bytes]]`

Range query journal records.

### 7.2 Journal Queue

#### `store.open_journal_queue() -> JournalQueue`

Open a journal queue for consumption.

#### `jq.open_consumer(group: str) -> JournalQueueConsumer`

Open a consumer group for journal consumption.

#### `jq.close() -> None`

Close the journal queue.

### 7.3 JournalQueueConsumer

#### `c.poll(timeout_ms: int) -> Optional[Tuple[int, bytes]]`

Poll for the next journal record. Returns `(sequence, payload)` or `None` on timeout.

#### `c.ack(sequence: int) -> None`

Acknowledge a journal record.

---

## 8. Error Types

All operations raise `TmslError` on failure:

```python
try:
    store.create_dataset("sensor", "waveform")
except timslite.TmslError as e:
    print(f"Error: {e}")
```

Common error types:
- `AlreadyExists` — dataset already exists
- `InvalidData` — invalid parameters or data
- `NotFound` — dataset or record not found
- `SegmentFull` — segment capacity exceeded
- `ReadOnly` — write attempted on read-only store

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