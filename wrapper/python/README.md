# timslite-py

Python bindings for the timslite high-performance time-series data storage library.

## Installation

```bash
pip install timslite
```

Prebuilt wheels are published for the supported release platforms. If pip cannot
find a compatible wheel, it falls back to building the source distribution with
maturin. Source builds require a Rust toolchain and native compiler toolchain.
The PyPI source distribution depends on the matching `timslite` crate version
from crates.io.

For repository development:

```bash
maturin develop            # Development
maturin develop --release  # Release build
```

## Usage

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

### Manual Background Tasks

When `enable_background_thread=False`, the store does not spawn an internal
background thread.  You must call `store.tick_background_tasks()` periodically
to drive flush, idle-close, cache eviction, and retention reclaim.

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
