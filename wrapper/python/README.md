# timslite-py

Python bindings for the timslite high-performance time-series data storage library.

## Installation

```bash
maturin develop          # Development
maturin develop --release  # Release build
```

## Usage

```python
import timslite

with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor", "waveform")
    ds = store.open_dataset("sensor", "waveform")
    ds.write(1, b"reading_1")
    for ts, data in ds.query(1, 100):
        print(f"ts={ts}, data={data}")
```
