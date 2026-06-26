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
import timslite

config = timslite.StoreConfig(
    flush_interval=15,           # seconds
    idle_timeout=1800,           # 30 minutes
    data_segment_size=64 * 1024 * 1024,  # 64 MiB
    index_segment_size=4 * 1024 * 1024,   # 4 MiB
    compress_level=6,            # 0-9
    cache_max_memory=256 * 1024 * 1024,  # 256 MiB
    enable_background_thread=True,
    enable_journal=True,
)
```

### DataSetConfig

```python
import timslite

# Using create_dataset with default config
store.create_dataset("metrics", "cpu")

# Using create_dataset_with_config
config = timslite.DataSetConfig(
    data_segment_size=128 * 1024 * 1024,  # 128 MiB
    index_segment_size=8 * 1024 * 1024,    # 8 MiB
    compress_level=9,                      # max compression
    index_continuous=1,                    # continuous mode
    retention_window=86400,                # 1 day in timestamp units
    enable_journal=True,
)
store.create_dataset_with_config("metrics", "per_second", config)
```

## Common Patterns

### Batch Writes

```python
for i in range(1000):
    data = f'{{"value": {i}}}'.encode()
    ds.write(i + 1, data)
```

### Range Queries

```python
# Lazy query (iterator)
for ts, data in ds.query(100, 200):
    print(f"ts={ts}: {data.decode()}")

# Eager query (list)
records = ds.query_all(100, 200)
for ts, data in records:
    print(f"ts={ts}: {data.decode()}")
```

### Queue Consumption

```python
ds = store.open_dataset("tasks", "jobs")
q = store.open_queue(ds.id)
consumer = q.open_consumer("worker_group")

result = consumer.poll(5000)  # timeout in ms
if result:
    ts, data = result
    print(f"Got task: {data.decode()}")
    consumer.ack(ts)

q.close()
```

### Journal Consumption

```python
jq = store.open_journal_queue()
consumer = jq.open_consumer("sync_worker")

result = consumer.poll(100)  # timeout in ms
if result:
    seq, payload = result
    print(f"Journal seq: {seq}")
    consumer.ack(seq)

jq.close()
```

### Manual Background Tasks

When `enable_background_thread=False`, the store does not spawn an internal background thread. You must call `store.tick_background_tasks()` periodically to drive flush, idle-close, cache eviction, and retention reclaim.

```python
import timslite

cfg = timslite.StoreConfig(enable_background_thread=False)
store = timslite.Store.open("/data/timslite", cfg)

store.create_dataset("sensor", "waveform")
ds = store.open_dataset("sensor", "waveform")
ds.write(1, b"reading_1")

# Manually execute a tick — returns (executed_tasks, next_delay_ms)
executed, delay_ms = store.tick_background_tasks()
print(f"executed={executed}, next in {delay_ms}ms")

# Check the delay without executing anything
delay = store.next_background_delay()
print(f"next task due in {delay}ms")

# In an event loop:
import time
while True:
    executed, delay_ms = store.tick_background_tasks()
    if executed > 0:
        print(f"ran {executed} background tasks")
    time.sleep(delay_ms / 1000.0)

store.close()
```

## Error Handling

All operations raise `TmslError` on failure. Common errors:

- `AlreadyExists` — dataset already exists
- `InvalidData` — invalid parameters or data
- `NotFound` — dataset or record not found
- `SegmentFull` — segment capacity exceeded
- `ReadOnly` — write attempted on read-only store

See [Troubleshooting](../troubleshooting.md) for detailed error solutions.