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

// 2. Create a dataset
let ds = store.create_dataset(
    "sensor_001", "events",
    64 * 1024 * 1024,  // data_segment_size = 64MB
    4 * 1024 * 1024,   // index_segment_size = 4MB
    6,                 // compress_level (0-9)
    0,                 // index_continuous (0=sparse, 1=continuous)
    0,                 // retention_window (0 = no limit, in timestamp units)
)?;

// 3. Write records (timestamp must be >= latest_written_timestamp)
ds.write(1, b"event_0")?;
ds.write(2, b"event_1")?;

// 4. Read single record
let record = ds.read(1)?;  // Option<(i64, Vec<u8>)>

// 5. Range query (eager - loads all into memory)
let entries = ds.query(1, 100)?;  // Vec<(i64, Vec<u8>)>

// 6. Range query with lazy iterator (for large ranges)
let iter = ds.query_iter(1, 100)?;

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
    .initial_index_segment_size(8 * 1024)   // 8KB initial (lazy alloc)
    .compress_level(9)                      // max compression
    .compress_type(0)                       // 0=zstd, 1=deflate
    .index_continuous(1)                    // continuous mode
    .retention_window(86400)                // 1 day (in timestamp units)
    .enable_journal(true);

store.create_dataset_with_config("metrics", "per_second", Some(builder))?;

// Using store defaults (pass None)
store.create_dataset_with_config("simple", "events", None)?;
```

## Read Operations

```rust
use timslite::Store;

let mut store = Store::open("/data/app", StoreConfig::default())?;
let ds = store.open_dataset("metrics", "per_second")?;

// Read specific timestamp
if let Some((ts, data)) = ds.read(123)? {
    println!("ts={ts}: {} bytes", data.len());
}

// Read latest record
if let Some((ts, data)) = ds.read_latest()? {
    println!("Latest at ts={ts}: {} bytes", data.len());
}

// Check if record exists
let exists = ds.read_exist(123)?;  // bool

// Read record length without reading data
if let Some(len) = ds.read_length(123)? {
    println!("Record length: {len} bytes");
}

// Query with iterator (lazy - for large ranges)
let iter = ds.query_iter(1, 1000)?;
for result in iter {
    let (ts, data) = result?;
    println!("ts={ts}: {} bytes", data.len());
}

// Query lengths only (no data transfer)
let lengths = ds.query_length(1, 100)?;  // Vec<(i64, u32)>
for (ts, len) in &lengths {
    println!("ts={ts}: {len} bytes");
}

// Query lengths with lazy iterator
let iter = ds.query_length_iter(1, 1000)?;
for result in iter {
    let (ts, len) = result?;
    println!("ts={ts}: {len} bytes");
}

store.close()?;
```

## Write Operations

```rust
use timslite::Store;

let mut store = Store::open("/data/app", StoreConfig::default())?;
let ds = store.create_dataset("events", "logs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Normal write (timestamp > latest_written_timestamp)
ds.write(1, b"first event")?;
ds.write(2, b"second event")?;

// Correction write (timestamp == latest_written_timestamp)
ds.correct(2, b"corrected second event")?;

// Out-of-order write (timestamp < latest_written_timestamp)
// Appends data to latest segment and updates index entry
ds.write(1, b"corrected first event")?;

// Append to existing record (timestamp == latest_written_timestamp)
// Only works on uncompressed tail record
ds.append(2, b" appended data")?;

// Forward append (timestamp > latest_written_timestamp)
// Creates new record
ds.append(3, b"new record")?;

// Delete record
ds.delete(1)?;

store.close()?;
```

## Queue Usage

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig, QueueConsumerConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;
let ds = store.create_dataset("tasks", "jobs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Open queue for a dataset
let queue = ds.open_queue()?;

// Push data (auto-assigns next timestamp)
let ts = queue.push(b"task_payload")?;

// Open a consumer group
let config = QueueConsumerConfig {
    running_expired_secs: 60,
    max_retry_count: 3,
};
let mut consumer = queue.open_consumer("worker_group", config)?;

// Poll for data (blocks up to timeout)
if let Some((ts, data)) = consumer.poll(Duration::from_secs(5))? {
    println!("Got task at ts={ts}: {}", String::from_utf8_lossy(&data));
    consumer.ack(ts)?;
}

// Inspect consumer state
let inspect = consumer.inspect()?;
println!("Processed up to: {:?}", inspect.state.processed_ts);
println!("Pending entries: {}", inspect.state.pending_entries.len());

consumer.flush()?;
consumer.close()?;
queue.close()?;
ds.close()?;
store.close()?;
```

## Journal Queue

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;

// Query journal directly
if let Some(seq) = store.journal_latest_sequence()? {
    println!("Latest journal sequence: {seq}");
}

let records = store.journal_query(1, 100)?;
for (seq, data) in &records {
    println!("Journal #{seq}: {} bytes", data.len());
}

// Open journal queue for reliable consumption
let jq = store.open_journal_queue()?;
let mut jc = jq.open_consumer()?;

// Poll journal entries
if let Some((seq, data)) = jc.poll(Duration::from_secs(5))? {
    println!("Journal #{seq}: {} bytes", data.len());
    jc.ack(seq)?;
}

// Get specific entry without advancing cursor
if let Some((seq, data)) = jc.get(42)? {
    println!("Journal #42: {} bytes", data.len());
}

jc.close()?;
jq.close()?;
store.close()?;
```

## Background Tasks

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

// Disable background thread for manual control
let mut store = Store::open_with_config(
    "/data/app",
    StoreConfig::builder()
        .enable_background_thread(false)
        .build(),
)?;

// Write some data
let ds = store.create_dataset("metrics", "cpu", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
for i in 1..=1000 {
    ds.write(i, format!("cpu={:.2}", i as f64 * 0.01).as_bytes())?;
}

// Manually trigger background tasks
let result = store.tick_background_tasks()?;
println!("Executed {} tasks", result.executed_tasks);
println!("Next run in {}ms", result.next_delay_ms);

// Check next delay without executing
let delay = store.next_background_delay()?;
println!("Next task due in {}ms", delay.as_millis());

store.close()?;
```

## Inspection

```rust
use timslite::Store;

let mut store = Store::open("/data/app", StoreConfig::default())?;

// List all datasets
let names = store.get_dataset_names()?;
println!("Datasets: {:?}", names);

// List types for a dataset
let types = store.get_dataset_types("sensor")?;
println!("Sensor types: {:?}", types);

// Inspect dataset
let ds = store.open_dataset("sensor", "waveform")?;
let result = ds.inspect()?;

println!("=== Dataset Info ===");
println!("Name: {}", result.info.name);
println!("Type: {}", result.info.dataset_type);
println!("Compression: {}", if result.info.compress_type == 0 { "zstd" } else { "deflate" });
println!("Index mode: {}", if result.info.index_continuous == 0 { "sparse" } else { "continuous" });
println!("Retention: {}", result.info.retention_window);
println!("Journal: {}", result.info.enable_journal);

println!("\n=== Dataset State ===");
println!("Latest timestamp: {:?}", result.state.latest_written_timestamp);
println!("Data segments: {} ({} open)", result.state.data_segments, result.state.open_data_segments);
println!("Index segments: {} ({} open)", result.state.index_segments, result.state.open_index_segments);
println!("Total records: {}", result.state.total_record_count);
println!("Data size: {} bytes", result.state.total_data_size);
println!("Read-only: {}", result.state.read_only);
println!("Queue groups: {}", result.state.queue_consumer_groups);

ds.close()?;
store.close()?;
```

## Read-Only Mode

```rust
use timslite::{Store, StoreConfig};

// Force read-only mode
let mut store = Store::open(
    "/data/app",
    StoreConfig::builder()
        .read_only(Some(true))
        .build(),
)?;

// Auto-detect (default behavior)
let mut store = Store::open(
    "/data/app",
    StoreConfig::builder()
        .read_only(None)  // auto: writable if lock available, else read-only
        .build(),
)?;

// Read operations work normally
let ds = store.open_dataset("sensor", "waveform")?;
let record = ds.read(1)?;

// Write operations fail with InvalidData error
// ds.write(999, b"fail")?;  // Error: read-only dataset cannot be written

ds.close()?;
store.close()?;
```
