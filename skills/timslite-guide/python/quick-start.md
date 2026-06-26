# Python Quick Start

## Installation

```bash
pip install timslite
```

Prebuilt wheels are published for the supported release platforms. If pip cannot find a compatible wheel, it falls back to building the source distribution with maturin. Source builds require a Rust toolchain and native compiler toolchain.

## Basic Usage

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

## Using DataSetConfig

For fine-grained control over dataset configuration:

```python
import timslite

with timslite.Store.open("/data/app") as store:
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

## Queue Usage

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("tasks", "jobs")
    ds = store.open_dataset("tasks", "jobs")

    # Open queue for a dataset
    q = store.open_queue(ds.id)

    # Push data (auto-assigns next timestamp)
    ts = q.push(b"task_payload")

    # Open a consumer group
    consumer = q.open_consumer("worker_group")

    # Poll for records (with timeout in ms)
    result = consumer.poll(5000)
    if result:
        ts, data = result
        print(f"Got task: {data.decode()}")
        # Acknowledge processing
        consumer.ack(ts)

    q.close()
```

## Journal Usage

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("events", "user_actions")
    ds = store.open_dataset("events", "user_actions")

    # Every write/delete/append automatically appends to the journal
    ds.write(1, b"user_login")
    ds.write(2, b"page_view")
    ds.delete(1)

    # Read journal records
    latest = store.journal_latest_sequence()  # -> int or None
    print(f"Latest journal seq: {latest}")

    # Consume journal via queue (for downstream sync)
    jq = store.open_journal_queue()
    consumer = jq.open_consumer("sync_worker")

    result = consumer.poll(100)  # timeout in ms
    if result:
        seq, payload = result
        print(f"Consumed journal seq {seq}")
        consumer.ack(seq)

    jq.close()
```

## Manual Background Tasks

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

All operations raise `TmslError` on failure:

```python
import timslite

try:
    store = timslite.Store.open("/data/timslite")
    store.create_dataset("sensor", "waveform")
except timslite.TmslError as e:
    print(f"Error: {e}")
```

## Next Steps

- See [API Reference](api-reference.md) for complete API documentation
- See [Examples](examples.md) for more feature scenarios