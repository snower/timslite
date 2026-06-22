# timslite Python Wrapper — PyO3 Design

> Python bindings for the timslite high-performance time-series data storage library.

## 1. Architecture Overview

### 1.1 Technology Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Core Library | Rust (`libtimslite`) | Existing codebase, no changes needed |
| Binding Layer | PyO3 + maturin | Zero-cost Rust↔Python FFI |
| Python Package | `timslite` (PyPI name) | PEP 517/518 build, wheels for multiple platforms |
| Build System | maturin | Handles compilation, wheel packaging, publish |

### 1.2 Design Philosophy

**Thin wrapper, Pythonic API.** The Rust library owns all data and logic. The Python layer:
- Wraps Rust types via PyO3 `#[pyclass]`
- Converts Rust errors to Python exceptions
- Exposes `query()` as a Python iterator (`__iter__` / `__next__`)
- Uses context managers (`__enter__` / `__exit__`) for lifecycle management
- Simplifies builders — Python kwargs replace `StoreConfigBuilder` / `DataSetConfigBuilder`
- Lock management (`Arc<DataSet>` plus DataSet's internal mutex) is fully hidden from Python users

### 1.3 What NOT to Expose

| Rust Type | Reason |
|-----------|--------|
| `StoreConfigBuilder` | Python kwargs make it unnecessary |
| `DataSetConfigBuilder` | Python kwargs make it unnecessary |
| `DataSetConfig` | Internal detail, kwargs on `create_dataset` suffice |
| `BlockCache` | Auto-managed by Store, no user interaction needed |
| `HotBlockCache` | Internal optimization, no Python API needed |
| `QuerySource`, `SourceIndex` | Internal iterator state |
| `DataSetHandle` | Wrapped as opaque `Dataset` object |
| `DataSetKey` | Internal key, never exposed |

---

## 2. Python API Design

### 2.1 Class Hierarchy

```
timslite
├── Store           # Main entry point, context manager
├── StoreConfig     # Configuration (kwargs-based, no builder)
├── Dataset         # Returned by store.open_dataset(), lock hidden
├── QueryIterator   # Lazy iterator for query results
├── DatasetQueue    # Push/poll/ack queue with consumer groups
├── DatasetQueueConsumer  # Consumer handle for polling
├── QueryResult     # Eager result (list-like wrapper, optional)
└── Exceptions
    ├── TmslError       # Base exception
    ├── TmslIoError
    ├── TmslNotFoundError
    ├── TmslAlreadyExistsError
    ├── TmslInvalidDataError
    ├── TmslSegmentFullError
    ├── TmslMmapError
    ├── TmslCompressionError
    ├── TmslDecompressionError
    ├── TmslExpiredError
    ├── TmslQueueAlreadyOpenError
    ├── TmslQueueNotOpenError
    ├── TmslConsumerGroupNotFoundError
    ├── TmslConsumerGroupExistsError
    ├── TmslQueueClosedError
    └── TmslPendingFullError
```

### 2.2 Module Structure

```
wrapper/python/
├── Cargo.toml                  # maturin/PyO3 crate
├── pyproject.toml              # PEP 517 build config
├── design.md                   # This file
├── src/
│   └── lib.rs                  # PyO3 module root
│   ├── store.rs                # PyStore#[pyclass]
│   ├── config.rs               # PyStoreConfig#[pyclass]
│   ├── dataset.rs              # PyDataset#[pyclass]
│   ├── query.rs                # PyQueryIterator[#pyclass]
│   ├── queue.rs                # PyDatasetQueue + PyDatasetQueueConsumer #[pyclass]
│   └── exceptions.rs           # Python exception types
├── python/
│   └── timslite/
│       └── __init__.py         # Re-exports, pure Python helpers
└── tests/
    ├── test_basic.py           # Smoke tests
    ├── test_lifecycle.py       # Create/open/close/drop
    ├── test_write_query.py     # Write + query patterns
    ├── test_continuous.py      # Continuous mode tests
    ├── test_exceptions.py      # Error handling tests
    └── test_persistence.py     # Reopen after close
```

---

## 3. Detailed Class Specifications

### 3.1 `Store` — `#[pyclass]`

**Lifecycle**: Context manager. `open` → `__enter__`, `close` → `__exit__`.

```python
class Store:
    """Manages time-series datasets under a root directory."""

    @classmethod
    def open(cls, data_dir: str, config: StoreConfig | None = None) -> "Store":
        """Open or create a store at `data_dir`.

        Directories are created automatically if they don't exist.
        Existing datasets are discovered and registered on open.

        Args:
            data_dir: Root directory for the store.
            config: Optional store config. Uses defaults if None.

        Returns:
            A new Store instance.
        """

    def __enter__(self) -> "Store": ...
    def __exit__(self, *args) -> None: ...

    # ─── Dataset Management ───────────────────────────────────────────

    def create_dataset(
        self,
        name: str,
        dataset_type: str,
        *,
        data_segment_size: int = None,
        index_segment_size: int = None,
        compress_level: int = None,
        index_continuous: bool = False,
        initial_data_segment_size: int = None,
        initial_index_segment_size: int = None,
    ) -> None:
        """Create a new dataset.

        Only `name` and `dataset_type` are required. All other parameters
        inherit from StoreConfig defaults unless overridden.

        Args:
            name: Dataset name (e.g. "sensor_001").
            dataset_type: Dataset type (e.g. "waveform", "events").
            data_segment_size: Max data segment file size (bytes).
            index_segment_size: Max index segment file size (bytes).
            compress_level: Compression level 0-9, interpreted by the selected algorithm (default: 6).
            index_continuous: Allow out-of-order / backfill writes.
            initial_data_segment_size: Initial file size (lazy alloc).
            initial_index_segment_size: Initial index file size.

        Raises:
            TmslAlreadyExistsError: Dataset already exists.
        """

    def open_dataset(self, name: str, dataset_type: str) -> "Dataset":
        """Open an existing dataset.

        Reads immutable parameters from the meta file.

        Args:
            name: Dataset name.
            dataset_type: Dataset type.

        Returns:
            A Dataset object for read/write operations.

        Raises:
            TmslNotFoundError: Dataset does not exist.
        """

    def drop_dataset(self, name: str, dataset_type: str) -> None:
        """Delete an entire dataset.

        WARNING: Irreversible. All data is permanently deleted.

        Raises:
            TmslNotFoundError: Dataset does not exist.
        """

    def close(self) -> None:
        """Flush and close all datasets, stop background tasks."""
```

**Rust backing**: `PyStore` wraps `Option<Store>`. `close()` consumes inner value. `__exit__` calls `close()`.

### 3.2 `StoreConfig` — `#[pyclass]`

**Simplified**: No builder. All params are `__init__` kwargs with defaults matching `StoreConfig::default()`.

```python
class StoreConfig:
    """Store-level configuration.

    All parameters are optional and have sensible defaults.
    """

    def __init__(
        self,
        *,
        flush_interval: int = 15,           # seconds
        idle_timeout: int = 1800,            # seconds
        data_segment_size: int = 67108864,   # 64 MiB
        index_segment_size: int = 4194304,   # 4 MiB
        initial_data_segment_size: int = 262144,   # 256 KiB
        initial_index_segment_size: int = 4096,    # 4 KiB
        block_max_size: int = 65536,         # 64 KiB
        compress_level: int = 6,             # 0-9
        cache_max_memory: int = 268435456,   # 256 MiB (0 = disabled)
        cache_idle_timeout: int = 1800,      # seconds
        retention_check_hour: int = 0,       # UTC hour 0..=23
        enable_background_thread: bool = True,
        enable_journal: bool = True,
        read_only: bool | None = None,       # None=auto, False=writable, True=read-only
    ) -> None:
        ...

    @classmethod
    def default(cls) -> "StoreConfig":
        """Return a config with all default values."""
```

**Note**: Durations accept `int` (seconds) for simplicity. Could support `timedelta` later. `read_only=None` follows the Rust auto mode: acquire the Store `.lock` for writable open, or fall back to a read-only Store if another writer holds it.

### 3.3 `Dataset` — `#[pyclass]`

**Lock hidden**: Holds `Arc<DataSet>` internally. Every method calls the public DataSet API, which acquires the internal lock automatically.

```python
class Dataset:
    """A single time-series dataset.

    Returned by Store.open_dataset(). Thread-safe — all operations
    acquire the internal lock automatically.
    """

    def write(self, timestamp: int, data: bytes) -> None:
        """Write a record.

        Args:
            timestamp: signed i64 business timestamp. Negative values and 0
                       are valid; forward writes must not move backward unless
                       they update an existing timestamp.
            data: Payload bytes.

        Raises:
            TmslInvalidDataError: out-of-order missing timestamp, invalid
                                  configuration, or oversized record.
        """

    def query(self, start_ts: int, end_ts: int) -> QueryIterator:
        """Query records in [start_ts, end_ts], returns a lazy iterator.

        The iterator implements the Python iterator protocol (__iter__/__next__).
        Yields (timestamp, bytes) tuples.

        Args:
            start_ts: Inclusive start timestamp.
            end_ts: Inclusive end timestamp.

        Returns:
            A QueryIterator yielding (int, bytes) pairs.
        """

    def query_all(self, start_ts: int, end_ts: int) -> list[tuple[int, bytes]]:
        """Query and collect all results into a list.

        Convenience wrapper: equivalent to list(dataset.query(...)).

        Returns:
            List of (timestamp, data) tuples.
        """

    def flush(self) -> None:
        """Flush pending data to disk."""

    @property
    def data_dir(self) -> str:
        """Base directory of this dataset."""
```

**Rust backing**: `PyDataset` holds `Arc<DataSet>` from `Store::get_dataset()`.
Each method calls `self.dataset.write(...)`, `self.dataset.read(...)`, etc.; the Rust DataSet owns the mutex.

### 3.4 `QueryIterator` — `#[pyclass]`

**Python iterator protocol**: Implements `__iter__` (returns self) and `__next__` (calls `QueryIterator::next_entry`).

```python
class QueryIterator:
    """Lazy iterator over query results.

    Yields (timestamp: int, data: bytes) tuples.
    Filler entries (internal to continuous mode) are automatically skipped.

    Usage:
        for ts, data in dataset.query(100, 200):
            process(ts, data)

    Or convert to list:
        records = list(dataset.query(100, 200))
    """

    def __iter__(self) -> "QueryIterator": ...

    def __next__(self) -> tuple[int, bytes]:
        """Return next (timestamp, data) tuple.

        Raises:
            StopIteration: No more entries.
            TmslError: Internal error during iteration.
        """

    def close(self) -> None:
        """Release iterator resources.

        Normally not needed — resources are released when the iterator
        is garbage collected or fully consumed.
        """
```

**Rust backing**: `PyQueryIterator` stores a snapshot of `IndexEntry` values plus `Arc<DataSet>`.
Since PyO3 classes have `'static` lifetime, the iterator does not borrow Rust segment internals directly. Each `__next__` skips filler entries and calls `DataSet::read_entry_at_index`, so data blocks are still loaded lazily while locking stays inside DataSet.

### 3.5 Exception Hierarchy

All exceptions inherit from a base `TmslError` class.

```python
class TmslError(Exception):
    """Base exception for all timslite errors."""

class TmslIoError(TmslError):
    """I/O error (file not found, permission denied, etc.)."""

class TmslNotFoundError(TmslError):
    """Dataset, segment, or handle not found."""

class TmslAlreadyExistsError(TmslError):
    """Dataset already exists."""

class TmslInvalidDataError(TmslError):
    """Invalid data: bad timestamp, out-of-order, duplicate, corrupt block."""

class TmslSegmentFullError(TmslError):
    """Segment file is full (expansion needed)."""

class TmslMmapError(TmslError):
    """Memory-mapping error."""

class TmslCompressionError(TmslError):
    """Compression failure."""

class TmslDecompressionError(TmslError):
    """Decompression failure."""

class TmslExpiredError(TmslError):
    """Timestamp is outside the retention window."""

class TmslQueueAlreadyOpenError(TmslError):
    """Queue is already open for this dataset."""

class TmslQueueNotOpenError(TmslError):
    """Queue is not open for this dataset."""

class TmslConsumerGroupNotFoundError(TmslError):
    """Consumer group not found."""

class TmslConsumerGroupExistsError(TmslError):
    """Consumer group already exists."""

class TmslQueueClosedError(TmslError):
    """Queue has been closed."""

class TmslPendingFullError(TmslError):
    """Pending entries limit reached (max 239)."""
```

**Rust mapping**: `fn map_error(err: TmslError) -> PyErr` in `exceptions.rs`, matches on variant and creates corresponding Python exception via `pyo3::exceptions::PyException`.

### 3.6 `DatasetQueue` + `DatasetQueueConsumer` — `#[pyclass]`

Queue semantics built on top of Dataset with consumer group support.

**Directory layout per dataset**: `{data_dir}/{name}/{type}/queue/{group_name}`

#### DatasetQueue

```python
class DatasetQueue:
    """Queue handle for a dataset (one per dataset).

    Supports push and consumer group management. Multiple consumers
    in the same group share progress via 4KB mmap state files.

    Obtained via Store.open_queue(dataset_id).
    """

    def push(self, data: bytes) -> int:
        """Push data into the queue.

        Auto-increments the dataset timestamp and notifies all
        waiting consumers across all consumer groups.

        Returns:
            Assigned timestamp.

        Raises:
            TmslQueueClosedError: Queue has been closed.
            TmslInvalidDataError: Write failed.
        """

    def open_consumer(self, group_name: str) -> "DatasetQueueConsumer":
        """Open or create a consumer group and return a consumer handle.

        Multiple consumers in the same group share progress via
        the shared 4KB mmap state file. The first call for a group
        creates the state file; subsequent calls open the existing file.

        Args:
            group_name: Consumer group identifier (e.g. "worker-1").

        Returns:
            DatasetQueueConsumer handle.

        Raises:
            TmslQueueClosedError: Queue has been closed.
        """

    def close(self) -> None:
        """Close the queue and all associated consumers.

        All pending records are synced, consumer state files are
        flushed, and waiting polls are unblocked with QueueClosed.
        """
```

#### DatasetQueueConsumer

```python
class DatasetQueueConsumer:
    """Consumer handle for a specific consumer group.

    Polls for new records with configurable timeout.
    Multiple consumers for the same group share progress via mmap.

    Obtained via DatasetQueue.open_consumer(group_name).
    """

    def poll(self, timeout_ms: int) -> tuple[int, bytes] | None:
        """Poll for the next record.

        Returns the next unacked record as (timestamp, data), or None
        if the timeout expires with no data available.

        Internally: checks for unacked pending entries first,
        then reads from dataset starting at processed_ts + 1.
        Uses Condvar wait/notify for efficient polling.

        Args:
            timeout_ms: Maximum wait time in milliseconds. Use 0 for
                non-blocking poll (returns immediately if no data).

        Returns:
            (timestamp, data) tuple, or None on timeout.

        Raises:
            TmslQueueClosedError: Queue has been closed.
        """

    def ack(self, timestamp: int) -> None:
        """Acknowledge a previously polled record.

        Removes the pending entry and advances the consumer's
        processed timestamp. Only call after successfully processing
        a record returned by poll().

        Args:
            timestamp: The timestamp from the poll() return value.

        Raises:
            TmslConsumerGroupNotFoundError: Consumer group not found.
        """

    def poll_callback(self, callback: Callable[[], None] | None) -> None:
        """Register or clear a lightweight wake callback.

        The callback is invoked synchronously after queue data waiters
        are notified. It is best-effort and must only wake external
        processing; use poll() and ack() for all data handling. Passing
        None clears the callback. Setting a non-None callback while this
        consumer already has one raises TmslError instead of replacing it.
        """
```

**Rust backing**: `PyDatasetQueue` wraps `timslite::DatasetQueue` (Clone-safe, all fields Arc). `PyDatasetQueueConsumer` wraps `timslite::DatasetQueueConsumer`. 

`PyJournalQueueConsumer` exposes the same `poll_callback(callback_or_none)` method for dedicated journal queue wake notifications.

**Store integration**: `PyStore.open_queue(dataset_id)` looks up the `Arc<DataSet>` from its internal dataset registry by ID, then calls `DataSet::open_queue()` and constructs `DatasetQueue` from the resulting components.

**Lock hierarchy**: Store → Dataset → QueueInner → ConsumerStateFile → Condvar.

---

## 4. Rust↔Python Type Mapping

| Rust Type | Python Type | Notes |
|-----------|-------------|-------|
| `String` / `&str` | `str` | UTF-8 only; non-UTF-8 → error |
| `i64` | `int` | Timestamps; Python handles arbitrary precision |
| `u64` | `int` | Sizes; may exceed 2^63 but Python int handles it |
| `u8` | `int` | compress_level, flags |
| `u32` | `int` | block_max_size |
| `usize` | `int` | cache_max_memory |
| `&[u8]` / `Vec<u8>` | `bytes` | Data payloads; zero-copy where possible |
| `Vec<(i64, Vec<u8>)>` | `list[tuple[int, bytes]]` | Query results |
| `Duration` | `int` (seconds) | Accept int, convert to Duration internally |
| `PathBuf` | `str` | Accept str, convert to PathBuf |
| `Result<T, TmslError>` | `T` / raises exception | Error converted via `map_error` |
| `Arc<DataSet>` | `Dataset` (opaque) | DataSet internal lock acquired per operation |

---

## 5. PyO3 Implementation Details

### 5.1 Cargo.toml

```toml
[package]
name = "timslite-python"
version = "0.1.0"
edition = "2021"

[lib]
name = "timslite"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.23", features = ["extension-module"] }
timslite = { path = "../.." }
```

### 5.2 pyproject.toml

```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name = "timslite"
version = "0.1.0"
description = "High-performance time-series data storage"
requires-python = ">=3.9"
license = { text = "MIT" }

[tool.maturin]
features = ["pyo3/extension-module"]
```

### 5.3 Lifetime Management Strategy

**Problem**: `QueryIterator` borrows `&'a mut DataSegmentSet`. PyO3 objects must be `'static`.

**Solution**: Use `Arc<DataSet>` sharing pattern; lazy iterators pre-collect index entries and call DataSet methods for each fetched row.

```rust
struct PyDataset {
    inner: Arc<DataSet>,
}

struct PyQueryIterator {
    // Pre-collect index entries (cheap: timestamps + offsets, not full data)
    entries: Vec<IndexEntry>,
    // Shared reference to dataset for data fetching
    dataset_arc: Arc<DataSet>,
    // Current position
    index: usize,
}

impl PyQueryIterator {
    fn __next__(&mut self) -> PyResult<(i64, Vec<u8>)> {
        // Skip filler entries, fetch real data through DataSet's public API.
        while self.index < self.entries.len() {
            let entry = &self.entries[self.index];
            self.index += 1;
            if entry.block_offset == BLOCK_OFFSET_FILLER {
                continue;
            }
            // Fetch data from segment
            return Ok(self.dataset_arc.read_entry_at_index(entry)?);
        }
        Err(/* StopIteration */)
    }
}
```

This approach:
- Pre-fetches index entries during `query()` call (cheap, no data loaded yet)
- Stores `Arc<DataSet>` for lazy data fetching
- Truly lazy: data blocks are only loaded when `__next__` is called
- Thread-safe: DataSet's internal mutex protects concurrent access

### 5.4 GIL Considerations

- PyO3 automatically holds the GIL during `#[pymethods]` calls
- DataSet locking happens inside the Rust public API; the Python layer does not take a dataset mutex directly
- Background flush thread runs separate from GIL — safe

### 5.5 Memory Ownership

- `tmsl_iter_next` malloc'd data → NOT used in PyO3 wrapper
- PyO3 directly calls `DataSet` methods → `Vec<u8>` → `PyBytes` (zero-copy possible with GIL)
- No manual malloc/free needed — Rust's ownership model handles everything

---

## 6. Usage Examples

### 6.1 Basic Workflow

```python
import timslite

# 1. Open store (context manager)
with timslite.Store.open("/data/timslite") as store:
    # 2. Create dataset (uses store defaults)
    store.create_dataset("sensor_001", "waveform")

    # 3. Open & write
    ds = store.open_dataset("sensor_001", "waveform")
    for i in range(100):
        ds.write(i + 1, f"reading_{i}".encode())

    ds.flush()

    # 4. Query (lazy iterator)
    for ts, data in ds.query(10, 50):
        print(f"ts={ts}, data={data}")

    # 5. Or collect all at once
    records = list(ds.query(10, 50))
    # or: records = ds.query_all(10, 50)
```

### 6.2 Custom Configuration

```python
import timslite

config = timslite.StoreConfig(
    flush_interval=300,       # 5 min flush
    idle_timeout=600,         # 10 min idle-close
    cache_max_memory=512 * 1024 * 1024,  # 512 MiB cache
)

with timslite.Store.open("/data/timslite", config=config) as store:
    store.create_dataset(
        "patient_001", "ecg",
        compress_level=9,           # max compression
        data_segment_size=128 * 1024 * 1024,  # 128 MiB segments
    )
```

### 6.3 Continuous Mode

```python
with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor_x", "events", index_continuous=True)
    ds = store.open_dataset("sensor_x", "events")

    # Out-of-order writes allowed (back-fills gaps)
    ds.write(100, b"first")
    ds.write(150, b"last")
    ds.write(125, b"middle")  # fills gap between 100 and 150
```

### 6.4 Error Handling

```python
import timslite

with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor", "data")

    try:
        store.create_dataset("sensor", "data")
    except timslite.TmslAlreadyExistsError as e:
        print(f"Dataset exists: {e}")

    ds = store.open_dataset("sensor", "data")

    try:
        ds.write(10, b"first")
        ds.write(5, b"bad")
    except timslite.TmslInvalidDataError as e:
        print(f"Bad data: {e}")
```

### 6.5 Persistence

```python
# Block 1: Create and write
with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor", "data")
    ds = store.open_dataset("sensor", "data")
    ds.write(1, b"hello")
    ds.write(2, b"world")

# Block 2: Reopen and read
with timslite.Store.open("/data/timslite") as store:
    ds = store.open_dataset("sensor", "data")
    for ts, data in ds.query(1, 100):
        print(f"ts={ts}, data={data}")
    # Output: ts=1, data=b'hello' / ts=2, data=b'world'
```

---

## 7. Build & Distribution

### 7.1 Local Development

```bash
cd wrapper/python
maturin develop          # Build + install into current venv
maturin develop --release  # Release build
```

### 7.2 Wheel Building

```bash
# Build wheels for current platform
maturin build --release

# Build for all targets (requires cross-compilation setup)
maturin build --release --target x86_64-unknown-linux-gnu
maturin build --release --target aarch64-apple-darwin
maturin build --release --target x86_64-pc-windows-msvc
```

### 7.3 PyPI Publishing

```bash
maturin publish --username __token__ --password $PYPI_TOKEN
```

### 7.4 CI/CD (GitHub Actions) — Outline

```yaml
# .github/workflows/python-release.yml
name: Python Release
on:
  push:
    tags: ['python-v*']
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          command: build
          args: --release --out dist
          target: ${{ matrix.os == 'ubuntu-latest' && 'x86_64-unknown-linux-gnu' || '' }}
      - upload: dist/*.whl
  publish:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: pypa/gh-action-pypi-publish@release/v1
```

---

## 8. Testing Strategy

### 8.1 Test Categories

| Category | File | Focus |
|----------|------|-------|
| Smoke | `test_basic.py` | Import, Store.open, Store.close |
| Lifecycle | `test_lifecycle.py` | create/open/close/drop flows |
| Write/Query | `test_write_query.py` | Write records, query ranges, iterator |
| Continuous | `test_continuous.py` | Out-of-order writes, backfill, gaps |
| Exceptions | `test_exceptions.py` | All error types triggered and caught |
| Persistence | `test_persistence.py` | Reopen after close, data survives |
| Multi-dataset | `test_multi_dataset.py` | Isolation between datasets |
| Config | `test_config.py` | StoreConfig fields, create_dataset kwargs |

### 8.2 Test Example

```python
import pytest
import timslite
import tempfile
import os

class TestBasicLifecycle:
    def test_create_write_query(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with timslite.Store.open(tmpdir) as store:
                store.create_dataset("test", "events")
                ds = store.open_dataset("test", "events")

                for i in range(10):
                    ds.write(i + 1, f"data_{i}".encode())

                results = list(ds.query(3, 7))
                assert len(results) == 5
                assert results[0] == (3, b"data_2")
                assert results[-1] == (7, b"data_6")

    def test_already_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with timslite.Store.open(tmpdir) as store:
                store.create_dataset("test", "events")
                with pytest.raises(timslite.TmslAlreadyExistsError):
                    store.create_dataset("test", "events")

    def test_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with timslite.Store.open(tmpdir) as store:
                with pytest.raises(timslite.TmslNotFoundError):
                    store.open_dataset("nonexistent", "data")
```

### 8.3 Test Execution

```bash
cd wrapper/python
pytest tests/ -v
```

---

## 9. Performance Considerations

### 9.1 Zero-Copy Goals

| Operation | Strategy |
|-----------|----------|
| `write(data: bytes)` | `PyBytes` → `&[u8]` (zero-copy, borrow from PyBytes) |
| `query()` results | `Vec<u8>` → `PyBytes::new(py, &vec)` (copy unavoidable, data crosses boundary) |
| String params | `&str` → Python `str` (UTF-8, zero-copy for ASCII) |

### 9.2 Iterator Performance

- Pre-fetching `Vec<IndexEntry>` is cheap relative to payload data; in memory it keeps the full `i64` timestamp plus data offsets, while index segment files store timestamp deltas in 14-byte entries.
- Data blocks loaded on-demand during `__next__` (lazy, matches Rust `QueryIterator`)
- BlockCache shared from Store — repeated queries benefit from cache hits

### 9.3 GIL Impact

- All operations run under GIL (PyO3 default)
- Background flush thread does NOT hold GIL
- No `allow_threads` needed (single Python thread interacts with Store)

---

## 10. Platform Support

| Platform | Target | Status |
|----------|--------|--------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | Primary |
| macOS x86_64 | `x86_64-apple-darwin` | Primary |
| macOS ARM64 | `aarch64-apple-darwin` | Primary |
| Windows x86_64 | `x86_64-pc-windows-msvc` | Primary |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | Stretch |

**Python versions**: 3.9 – 3.13 (PyO3 supports back to 3.8, but 3.9 minimum for `list[tuple[...]]` type hints without `from __future__`).

---

## 11. Future Extensions (Post-MVP)

| Feature | Description |
|---------|-------------|
| `__getitem__` on Dataset | `dataset[100:200]` syntax for query |
| `pandas` integration | `df = ds.query_pandas(1, 100)` returns DataFrame |
| `numpy` integration | Return data as `np.ndarray` for numeric payloads |
| Async API | `async def write(...)`, `async for ts, data in ds.query_async(...)` |
| Type hints stubs | `.pyi` files for IDE autocomplete |
| Documentation | Sphinx/ReadTheDocs site with API reference |
