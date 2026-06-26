# Rust Quick Start

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
timslite = "0.1"
```

## Basic Usage

```rust
use timslite::{Store, StoreConfig};

// 1. Open a store
let config = StoreConfig::default();
let mut store = Store::open("/data/timslite", config)?;

// 2. Create a dataset (explicit parameters)
store.create_dataset(
    "sensor_001", "events",
    64 * 1024 * 1024,  // data_segment_size = 64MB
    4 * 1024 * 1024,   // index_segment_size = 4MB
    6,                 // compress_level (0-9)
    0,                 // index_continuous (0=sparse, 1=continuous)
    0,                 // retention_window (0 = no limit, in timestamp units)
)?;

// 3. Open the dataset (parameters read from meta)
let handle = store.open_dataset("sensor_001", "events")?;

// 4. Write records (timestamp must be >= latest_written_timestamp)
let ds = store.get_dataset(&handle)?;
ds.write(1, b"event_0")?;
ds.write(2, b"event_1")?;

// 5. Read single record
let record = ds.read(1)?;  // Option<(i64, Vec<u8>)>

// 6. Range query
let entries = ds.query(1, 100)?;  // Vec<(i64, Vec<u8>)>

// 7. Close
store.close()?;
```

## Using DataSetConfigBuilder

For fine-grained control over dataset configuration:

```rust
use timslite::{Store, StoreConfig, DataSetConfigBuilder};

let mut store = Store::open("/data/app", StoreConfig::default())?;

let builder = DataSetConfigBuilder::new()
    .data_segment_size(128 * 1024 * 1024)   // 128MB data segments
    .index_segment_size(8 * 1024 * 1024)    // 8MB index segments
    .initial_data_segment_size(512 * 1024)  // 512KB initial (lazy alloc)
    .compress_level(9)                      // max compression
    .compress_type(0)                       // zstd
    .index_continuous(1)                    // continuous mode
    .retention_window(86400)                // 1 day (in timestamp units)
    .enable_journal(true);

store.create_dataset_with_config("metrics", "per_second", Some(builder))?;

// Using store defaults (pass None)
store.create_dataset_with_config("simple", "events", None)?;
```

## Queue Usage

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;
store.create_dataset("tasks", "jobs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let handle = store.open_dataset("tasks", "jobs")?;

// Open queue for a dataset
let q = store.open_queue(handle)?;

// Push data (auto-assigns next timestamp)
let ts = store.queue_push(&q, b"task_payload")?;

// Open a consumer group
let consumer = store.open_consumer(&q, "worker_group")?;

// Poll for records (with timeout)
if let Some((ts, data)) = store.queue_poll(&consumer, Duration::from_secs(5))? {
    println!("Got task at {ts}: {}", String::from_utf8_lossy(&data));
    // Acknowledge processing
    store.queue_ack(&consumer, ts)?;
}

store.close_queue(q)?;
store.close()?;
```

## Journal Usage

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;
store.create_dataset("events", "user_actions", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let handle = store.open_dataset("events", "user_actions")?;
let ds = store.get_dataset(&handle)?;

// Every write/delete/append automatically appends to the journal
ds.write(1, b"user_login")?;
ds.write(2, b"page_view")?;
ds.delete(1)?;

// Read journal records
let latest = store.journal_latest_sequence()?;  // e.g., Some(4)
println!("Latest journal seq: {:?}", latest);

// Consume journal via queue (for downstream sync)
let jq = store.open_journal_queue()?;
let consumer = jq.open_consumer("sync_worker")?;

while let Some((seq, payload)) = consumer.poll(Duration::from_millis(100))? {
    println!("Consumed journal seq {seq}");
    consumer.ack(seq)?;
}

store.close()?;
```

## Error Handling

All operations return `Result<T, TmslError>`. Common patterns:

```rust
use timslite::{Store, StoreConfig, TmslError};

match Store::open("/data/timslite", StoreConfig::default()) {
    Ok(store) => {
        // use store
    }
    Err(TmslError::AlreadyExists) => {
        eprintln!("Store already exists");
    }
    Err(TmslError::InvalidData(msg)) => {
        eprintln!("Invalid data: {}", msg);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

## Next Steps

- See [API Reference](api-reference.md) for complete API documentation
- See [Examples](examples.md) for more feature scenarios