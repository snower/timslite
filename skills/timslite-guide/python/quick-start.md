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

## Custom Dataset Configuration

For fine-grained control over dataset configuration, pass keyword arguments to `create_dataset`:

```python
import timslite

with timslite.Store.open("/data/app") as store:
    # Using create_dataset with default config
    store.create_dataset("metrics", "cpu")

    # Using create_dataset with custom config
    ds = store.create_dataset("metrics", "per_second",
        data_segment_size=128 * 1024 * 1024,  # 128 MiB
        index_segment_size=8 * 1024 * 1024,    # 8 MiB
        compress_level=9,                      # max compression
        index_continuous=True,                 # continuous mode
        enable_journal=True,
    )
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

with timslite.Store.open("/data/app", timslite.StoreConfig(enable_journal=True)) as store:
    store.create_dataset("sensor", "temp", enable_journal=True)
    ds = store.open_dataset("sensor", "temp")

    # Write data
    ds.write(1, b"25.3")

    # Read journal entries
    seq = store.journal_latest_sequence()
    if seq:
        entry = store.journal_read(seq)
        print(f"Journal seq={entry[0]}, data={entry[1]}")

    # Range query journal
    entries = store.journal_query(1, 100)
    for seq, data in entries:
        print(f"seq={seq}: {data}")

    # Open journal queue for consumption
    jq = store.open_journal_queue()
    consumer = jq.open_consumer("my_group")
    result = consumer.poll(1000)
    if result:
        seq, payload = result
        print(f"Journal entry: seq={seq}")
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
