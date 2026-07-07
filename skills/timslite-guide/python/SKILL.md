---
name: timslite-guide-python
description: Python guide for timslite time-series storage library - installation, quick start, API reference, and examples
---

# timslite Python Guide

## Installation

```bash
pip install timslite
```

Prebuilt wheels are published for the supported release platforms. If pip cannot find a compatible wheel, it falls back to building the source distribution with maturin. Source builds require a Rust toolchain and native compiler toolchain.

For repository development:

```bash
maturin develop            # Development
maturin develop --release  # Release build
```

## Quick Start

```python
import timslite

with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor", "waveform")
    ds = store.open_dataset("sensor", "waveform")
    ds.write(1, b"reading_1")
    ds.write(2, b"reading_2")

    # Read single record by timestamp
    record = ds.read(1)  # -> (1, b"reading_1") or None
    if record:
        ts, data = record
        print(f"ts={ts}, data={data}")

    # Read the latest record
    record = ds.read_latest()  # -> (2, b"reading_2") or None for empty dataset

    # Query the latest timestamp without a range scan
    print(f"latest: {ds.latest_timestamp}")  # -> 2

    # Range query
    for ts, data in ds.query(1, 100):
        print(f"ts={ts}, data={data}")

    # Delete a record
    ds.delete(1)
```

## Documentation

- **[API Reference](api-reference.md)** — Complete Python API signatures, parameters, return types, and semantics
- **[Examples](examples.md)** — Feature scenarios with copy-paste Python examples

## Key Concepts

### Store and DataSet

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `DataSet` handles per-dataset read/write operations, segment management, and indexing
- Each `(dataset_name, dataset_type)` pair is an independent dataset with its own segments

### Timestamps

- All timestamps are `i64` values (Python `int`)
- In sparse mode: timestamps must be `>= latest_written_timestamp`
- In continuous mode: timestamps must be `>= latest_written_timestamp`
- Use `read_latest()` to get the most recent record
- Use `read(-1)` to read the record at timestamp `-1` (not the latest)

### Blocks and Compression

- Records are aggregated into blocks (max 64KB payload)
- Blocks are lazily compressed on seal (zstd or deflate)
- Large records (>64KB) get their own single-record block
- Max record size: 4 MiB

## Configuration

### StoreConfig

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

### Dataset Configuration

Dataset configuration is passed as keyword arguments to `store.create_dataset()`:

```python
ds = store.create_dataset("sensor", "waveform",
    data_segment_size=128 * 1024 * 1024,  # 128 MiB
    index_segment_size=8 * 1024 * 1024,    # 8 MiB
    compress_level=9,                      # max compression
    index_continuous=True,                 # continuous mode
    retention_window=86400,                # 1 day in timestamp units
    enable_journal=True,
)
```

**Note**: There is no `DataSetConfig` class in the Python wrapper.

## Module Exports

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

## Error Handling

```python
try:
    store.create_dataset("sensor", "waveform")
except timslite.TmslAlreadyExistsError:
    print("Dataset already exists")
except timslite.TmslNotFoundError:
    print("Dataset not found")
except timslite.TmslError as e:
    print(f"Error: {e}")
```

Error hierarchy:
- `TmslError` (base)
  - `TmslIoError`
  - `TmslNotFoundError`
  - `TmslAlreadyExistsError`
  - `TmslInvalidDataError`
  - `TmslSegmentFullError`
  - `TmslMmapError`
  - `TmslCompressionError`
  - `TmslDecompressionError`
  - `TmslExpiredError`
  - `TmslQueueAlreadyOpenError`
  - `TmslQueueNotOpenError`
  - `TmslConsumerGroupNotFoundError`
  - `TmslConsumerGroupExistsError`
  - `TmslQueueClosedError`
  - `TmslPendingFullError`
