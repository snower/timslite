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

## Quick Start

```rust
use timslite::{Store, StoreConfig};

// Open or create a store
let config = StoreConfig::default();
let mut store = Store::open("/data/timslite", config)?;

// Create a dataset
let ds = store.create_dataset(
    "sensor", "temp",
    64 * 1024 * 1024,  // data_segment_size = 64MB
    4 * 1024 * 1024,   // index_segment_size = 4MB
    6,                 // compress_level (0-9)
    0,                 // index_continuous (0=sparse, 1=continuous)
    0,                 // retention_window (0 = no limit)
)?;

// Write records (timestamps must be >= latest_written_timestamp)
ds.write(1, b"temperature=23.5")?;
ds.write(2, b"temperature=24.0")?;

// Read a specific timestamp
if let Some((ts, data)) = ds.read(1)? {
    println!("ts={ts}: {}", String::from_utf8_lossy(&data));
}

// Query a range (eager - loads all into memory)
let entries = ds.query(1, 100)?;
for (ts, data) in &entries {
    println!("ts={ts}: {}", String::from_utf8_lossy(data));
}

// Query range with lazy iterator (for large ranges)
let iter = ds.query_iter(1, 100)?;
for result in iter {
    let (ts, data) = result?;
    println!("ts={ts}: {}", String::from_utf8_lossy(&data));
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
- `DataSet` is `Clone` (backed by `Arc<Mutex<DataSetInner>>`)

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

### Index Modes

- **Sparse mode** (`index_continuous: 0`): Flexible timestamps, O(log n) lookup
- **Continuous mode** (`index_continuous: 1`): Dense sequential timestamps, O(1) lookup

### Queue System

- Each dataset can have one queue subsystem
- Queue pushes create new records with auto-incremented timestamps
- Consumer groups track independent positions
- Queue state is persisted in mmap files

### Journal

- Built-in `.journal/logs` change log for audit/sync
- Records dataset create/drop/write/delete/append operations
- Journal sequences are consecutive `i64` values starting from 1

## Configuration

### StoreConfig

```rust
use std::time::Duration;
use timslite::StoreConfig;

let config = StoreConfig::builder()
    .flush_interval(Duration::from_secs(15))
    .idle_timeout(Duration::from_secs(1800))
    .data_segment_size(64 * 1024 * 1024)
    .index_segment_size(16 * 1024 * 1024)
    .initial_data_segment_size(256 * 1024)
    .initial_index_segment_size(16 * 1024)
    .compress_level(6)
    .compress_type(0)  // 0=zstd, 1=deflate
    .cache_max_memory(256 * 1024 * 1024)
    .cache_idle_timeout(Duration::from_secs(1800))
    .retention_check_hour(0)
    .enable_background_thread(true)
    .enable_journal(true)
    .read_only(None)  // None=auto, Some(false)=require writable, Some(true)=force RO
    .build();
```

### DataSetConfigBuilder

```rust
use timslite::DataSetConfigBuilder;

let builder = DataSetConfigBuilder::new()
    .data_segment_size(128 * 1024 * 1024)
    .index_segment_size(8 * 1024 * 1024)
    .initial_data_segment_size(512 * 1024)
    .initial_index_segment_size(8 * 1024)
    .compress_level(9)
    .compress_type(0)  // 0=zstd, 1=deflate
    .index_continuous(1)  // 0=sparse, 1=continuous
    .retention_window(86400)  // 0 = no limit, in timestamp units
    .enable_journal(true);

// Create dataset with config builder
store.create_dataset_with_config("metrics", "per_second", Some(builder))?;

// Use store defaults
store.create_dataset_with_config("simple", "events", None)?;
```

## Error Handling

```rust
use timslite::{TmslError, Result};

match store.create_dataset("test", "data", 64*1024*1024, 4*1024*1024, 6, 0, 0) {
    Ok(_) => println!("Created"),
    Err(TmslError::AlreadyExists(msg)) => println!("Already exists: {msg}"),
    Err(TmslError::InvalidData(msg)) => println!("Invalid: {msg}"),
    Err(e) => println!("Error: {e}"),
}
```

Error variants:
- `Io(io::Error)` — I/O error
- `InvalidMagic` — Invalid file magic bytes
- `InvalidVersion(u16)` — Unsupported version
- `MmapError(String)` — Memory-mapping failed
- `CompressionError(String)` — Compression failed
- `DecompressionError(String)` — Decompression failed
- `InvalidData(String)` — Invalid data or parameters
- `NotFound(String)` — Resource not found
- `Expired(String)` — Timestamp outside retention window
- `AlreadyExists(String)` — Resource already exists
- `SegmentFull` — Segment is full
- `QueueAlreadyOpen(String)` — Queue already opened
- `QueueNotOpen(String)` — Queue not opened
- `ConsumerGroupNotFound(String)` — Consumer group not found
- `ConsumerGroupExists(String)` — Consumer group already exists
- `QueueClosed(String)` — Queue closed
- `PendingFull(String)` — Pending entries limit reached

## Module Exports

```rust
use timslite::{
    // Main classes
    Store,
    DataSet,
    DataSetInfo,
    DataSetState,
    DataSetInspectResult,

    // Configuration
    StoreConfig,
    StoreConfigBuilder,
    DataSetConfig,
    DataSetConfigBuilder,

    // Queue
    DatasetQueue,
    DatasetQueueConsumer,
    DatasetQueueConsumerInfo,
    DatasetQueueConsumerInspectResult,
    DatasetQueueConsumerPendingEntry,
    DatasetQueueConsumerState,
    QueueConsumerConfig,
    QueuePollCallback,
    PendingEntry,

    // Journal
    JournalQueue,
    JournalQueueConsumer,
    JournalRecord,
    JournalRecordKind,
    JournalAppendInfo,
    JournalIndexInfo,
    JOURNAL_DATASET_NAME,
    JOURNAL_DATASET_TYPE,

    // Iterators
    QueryIterator,
    QueryLengthIterator,

    // Background tasks
    TickResult,

    // Error
    TmslError,
    Result,

    // Constants
    QUEUE_STATE_MAGIC,
    QUEUE_STATE_VERSION,
    QUEUE_STATE_FILE_SIZE,
};
```
