---
name: timslite-guide-rust
description: Rust guide for timslite time-series storage library - installation, quick start, API reference, and examples
---

# timslite Rust Guide

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
timslite = "0.1"
```

Or use the latest version from crates.io:

```toml
[dependencies]
timslite = "*"
```

## Quick Start

```rust
use timslite::{Store, StoreConfig};

// Open or create a store (acquires exclusive lock for writes)
let mut store = Store::open("/tmp/timslite_demo", StoreConfig::default())?;

// Create a dataset (name, type, data_segment_size, index_segment_size, compress_level, index_continuous, retention_window)
store.create_dataset("sensor", "temp", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)?;

// Open and write data
let handle = store.open_dataset("sensor", "temp")?;
let ds = store.get_dataset(&handle)?;

// Write records (timestamps must be >= latest_written_timestamp)
ds.write(1, b"temperature=23.5")?;
ds.write(2, b"temperature=24.0")?;

// Query a range
for (ts, data) in ds.query(1, 100)? {
    println!("ts={ts}: {}", String::from_utf8_lossy(&data));
}

// Read a specific timestamp
if let Some((ts, data)) = ds.read(1)? {
    println!("Record at {ts}: {}", String::from_utf8_lossy(&data));
}

// Read the latest record
if let Some((ts, data)) = ds.read_latest()? {
    println!("Latest at {ts}: {}", String::from_utf8_lossy(&data));
}

store.close()?;
```

## Documentation

- **[API Reference](api-reference.md)** — Complete Rust API signatures, parameters, return types, and semantics
- **[Examples](examples.md)** — Feature scenarios with copy-paste Rust examples

## Key Concepts

### Store and DataSet

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `DataSet` handles per-dataset read/write operations, segment management, and indexing
- Each `(dataset_name, dataset_type)` pair is an independent dataset with its own segments

### Timestamps

- All timestamps are `i64` values
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

```rust
use timslite::StoreConfig;

let config = StoreConfig {
    flush_interval: 15,           // seconds
    idle_timeout: 1800,           // 30 minutes
    data_segment_size: 64 * 1024 * 1024,  // 64 MiB
    index_segment_size: 4 * 1024 * 1024,   // 4 MiB
    compress_level: 6,            // 0-9
    cache_max_memory: 256 * 1024 * 1024,  // 256 MiB
    enable_background_thread: true,
    enable_journal: true,
    read_only: None,              // auto-detect
    ..Default::default()
};
```

### DataSetConfig

```rust
use timslite::DataSetConfigBuilder;

let builder = DataSetConfigBuilder::new()
    .data_segment_size(128 * 1024 * 1024)  // 128 MiB
    .index_segment_size(8 * 1024 * 1024)    // 8 MiB
    .compress_level(9)                      // max compression
    .index_continuous(1)                    // continuous mode
    .retention_window(86400)                // 1 day in timestamp units
    .enable_journal(true);
```

## Common Patterns

### Batch Writes

```rust
for i in 0..1000i64 {
    let data = format!("{{\"value\": {}}}", i).into_bytes();
    ds.write(i + 1, &data)?;
}
```

### Range Queries

```rust
// Eager query (loads all into memory)
let entries = ds.query(100, 200)?;

// Lazy query (iterator)
let mut iter = ds.query_iter(100, 200)?;
while let Some((ts, data)) = iter.next()? {
    // process record
}
```

### Queue Consumption

```rust
let q = store.open_queue(handle)?;
let consumer = store.open_consumer(&q, "my_group")?;

while let Some((ts, data)) = store.queue_poll(&consumer, Duration::from_secs(5))? {
    // process record
    store.queue_ack(&consumer, ts)?;
}
```

### Journal Consumption

```rust
let jq = store.open_journal_queue()?;
let consumer = jq.open_consumer("sync_worker")?;

while let Some((seq, payload)) = consumer.poll(Duration::from_millis(100))? {
    // process journal record
    consumer.ack(seq)?;
}
```

## Error Handling

All operations return `Result<T, TmslError>`. Common errors:

- `AlreadyExists` — dataset already exists
- `InvalidData` — invalid parameters or data
- `NotFound` — dataset or record not found
- `SegmentFull` — segment capacity exceeded
- `ReadOnly` — write attempted on read-only store

See [Troubleshooting](../troubleshooting.md) for detailed error solutions.