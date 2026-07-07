# Rust Examples

> Feature scenarios with copy-paste Rust examples.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```rust
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/sensors", StoreConfig::default())?;

// Create a dataset for sensor readings (sparse mode for irregular timestamps)
let ds = store.create_dataset(
    "temp_sensor", "readings",
    64 * 1024 * 1024,  // 64MB data segments
    4 * 1024 * 1024,   // 4MB index segments
    6,                 // compress level
    0,                 // sparse index mode
    0,                 // no retention
)?;

// Write sensor readings (timestamps must be monotonically increasing)
for i in 0..1000i64 {
    let data = format!("{{\"temp\": {:.1}, \"ts\": {}}}", 20.0 + i as f64 * 0.1, i).into_bytes();
    ds.write(i + 1, &data)?;
}

// Query a range (eager - loads all into memory)
let entries = ds.query(100, 200)?;
for (ts, data) in &entries {
    println!("ts={ts}: {}", String::from_utf8_lossy(data));
}

// Query with lazy iterator (for large ranges)
let iter = ds.query_iter(100, 200)?;
for result in iter {
    let (ts, data) = result?;
    println!("ts={ts}: {}", String::from_utf8_lossy(&data));
}

// Read a specific timestamp
if let Some((ts, data)) = ds.read(150)? {
    println!("Record at {ts}: {}", String::from_utf8_lossy(&data));
}

// Read the latest record
if let Some((ts, data)) = ds.read_latest()? {
    println!("Latest at {ts}: {}", String::from_utf8_lossy(&data));
}

store.close()?;
```

**Key points**:
- Timestamps must be `>= latest_written_timestamp` in sparse mode
- `query()` is eager (loads all into memory); use `query_iter()` for large ranges
- `read_latest()` is the only way to get the latest; `read(-1)` reads timestamp `-1`

---

## Scenario 2: Continuous Mode (Dense Sequential Timestamps)

**When**: Your timestamps are dense sequential integers (e.g., per-second readings with few gaps).

```rust
let mut store = Store::open("/data/metrics", StoreConfig::default())?;

// Create with index_continuous = 1
let ds = store.create_dataset(
    "cpu_usage", "per_second",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6,
    1,   // continuous mode — time_step = 1
    0,
)?;

// Write sequential timestamps (no gaps allowed in continuous mode)
for i in 1..=1000i64 {
    ds.write(i, &format!("cpu={:.2}", i as f64 * 0.01).into_bytes())?;
}

// Read by position (O(1) lookup)
let (_, data) = ds.read(500)?.unwrap();
println!("Record 500: {}", String::from_utf8_lossy(&data));

// Continuous mode is ideal for evenly-spaced data
// Gaps waste space but are allowed (sparse mode is better for irregular data)

store.close()?;
```

**Key points**:
- Continuous mode enables O(1) timestamp lookup
- Best for evenly-spaced timestamps with minimal gaps
- Gaps are allowed but waste index space

---

## Scenario 3: Append to Existing Record

**When**: You need to extend a record's data incrementally (e.g., building a payload in chunks).

```rust
let mut store = Store::open("/data/append", StoreConfig::default())?;
let ds = store.create_dataset("events", "chunked", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Write initial record
ds.write(1, b"chunk1")?;

// Append to the same timestamp (must be >= latest_written_timestamp)
ds.append(1, b",chunk2")?;
ds.append(1, b",chunk3")?;

// Read the full record
let (_, data) = ds.read(1)?.unwrap();
println!("{}", String::from_utf8_lossy(&data)); // "chunk1,chunk2,chunk3"

// Forward append (ts > latest_written_timestamp) creates new record
ds.append(2, b"new_record")?;

store.close()?;
```

**Key points**:
- `append(ts, data)` with `ts == latest_written_timestamp` extends the existing tail record
- `append(ts, data)` with `ts > latest_written_timestamp` creates a new record
- Append only works on uncompressed tail records

---

## Scenario 4: Correction and Deletion

**When**: You need to fix or remove previously written records.

```rust
let mut store = Store::open("/data/corrections", StoreConfig::default())?;
let ds = store.create_dataset("metrics", "correctable", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Write some records
ds.write(1, b"original_value")?;
ds.write(2, b"another_value")?;

// Correct a record (overwrites data at existing timestamp)
ds.correct(1, b"corrected_value")?;

// Verify correction
let (_, data) = ds.read(1)?.unwrap();
println!("{}", String::from_utf8_lossy(&data)); // "corrected_value"

// Delete a record
ds.delete(2)?;

// Verify deletion
let deleted = ds.read(2)?;
println!("{:?}", deleted); // None

store.close()?;
```

**Key points**:
- `correct(ts, data)` overwrites an existing record's data
- `delete(ts)` removes a record (soft delete — data remains until reclaimed)
- Deleted records return `None` from `read()`

---

## Scenario 5: Query Length (Header-Only Scan)

**When**: You need record sizes without reading full data (e.g., for capacity planning or selective reads).

```rust
let mut store = Store::open("/data/length", StoreConfig::default())?;
let ds = store.create_dataset("logs", "entries", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Write records of varying sizes
ds.write(1, b"short")?;
ds.write(2, b"a much longer record with more data")?;
ds.write(3, b"medium length")?;

// Query lengths only (no data transfer)
let lengths = ds.query_length(1, 3)?;
for (ts, length) in &lengths {
    println!("ts={ts}: {length} bytes");
}

// Use read_length for single record
let len = ds.read_length(2)?;
println!("Record 2 length: {:?}", len);

// Selective read based on length
for (ts, length) in &lengths {
    if *length > 10 {
        let (_, data) = ds.read(*ts)?.unwrap();
        println!("Large record at {ts}: {}", String::from_utf8_lossy(&data));
    }
}

store.close()?;
```

**Key points**:
- `query_length()` returns `(timestamp, length)` pairs without reading data
- `read_length(ts)` returns length for a single timestamp
- Useful for capacity planning and selective reads

---

## Scenario 6: Queue Consumer Pattern

**When**: You need reliable task processing with consumer groups.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig, QueueConsumerConfig};

let mut store = Store::open("/data/queue", StoreConfig::default())?;
let ds = store.create_dataset("tasks", "jobs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

let queue = ds.open_queue()?;

// Producer: push tasks
for i in 0..100 {
    let payload = format!("{{\"taskId\": {}, \"action\": \"process\"}}", i).into_bytes();
    let ts = queue.push(&payload)?;
    println!("Pushed task {i} at ts={ts}");
}

// Consumer: process tasks
let config = QueueConsumerConfig {
    running_expired_secs: 60,  // 60s before stuck task is retried
    max_retry_count: 3,        // max 3 retries before parked
};
let mut consumer = queue.open_consumer("worker_group", config)?;

// Poll and process
if let Some((ts, data)) = consumer.poll(Duration::from_secs(5))? {
    let task: serde_json::Value = serde_json::from_slice(&data)?;
    println!("Processing task {}", task["taskId"]);

    // Acknowledge successful processing
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

**Key points**:
- Each consumer group tracks its own position independently
- `poll()` blocks up to timeout
- `ack(ts)` advances the consumer position
- Stuck tasks are automatically retried after `running_expired_secs`

---

## Scenario 7: Iterator Control

**When**: You need fine-grained control over query iteration (reverse, skip, collect).

```rust
let mut store = Store::open("/data/iter", StoreConfig::default())?;
let ds = store.create_dataset("events", "controlled", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Write test data
for i in 1..=100i64 {
    ds.write(i, &format!("event_{i}").into_bytes())?;
}

// Forward iteration (default)
let iter = ds.query_iter(1, 10)?;
for result in iter {
    let (ts, _) = result?;
    println!("Forward: ts={ts}");
}

// Reverse iteration
let mut iter = ds.query_iter(1, 10)?;
iter.reverse();
for result in iter {
    let (ts, _) = result?;
    println!("Reverse: ts={ts}");
}

// Skip records
let mut iter = ds.query_iter(1, 10)?;
iter.skip(5); // skip first 5
for result in iter {
    let (ts, _) = result?;
    println!("After skip: ts={ts}"); // starts at 6
}

// Collect all into vector
let mut iter = ds.query_iter(1, 10)?;
let collected = iter.collect_all()?;
println!("Collected {} records", collected.len());

// QueryLengthIterator with same controls
let mut iter = ds.query_length_iter(1, 100)?;
iter.reverse();
iter.skip(10);
let lengths = iter.collect_all()?;
println!("Got {} length entries", lengths.len());

store.close()?;
```

**Key points**:
- `reverse()` must be called before first `next()`
- `skip(count)` must be called before first `next()`
- `collect_all()` eagerly loads all remaining records
- Iterators close automatically when iteration completes

---

## Scenario 8: Inspection and Monitoring

**When**: You need to inspect dataset state, consumer progress, or system health.

```rust
use timslite::Store;

let mut store = Store::open("/data/monitor", StoreConfig::default())?;

// List all datasets
let names = store.get_dataset_names()?;
println!("Datasets: {:?}", names);

// List types for a dataset
let types = store.get_dataset_types("sensor")?;
println!("Sensor types: {:?}", types);

// Inspect dataset
let ds = store.create_dataset(
    "sensor", "waveform",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6, 0, 0,
)?;

let inspect = ds.inspect()?;

println!("=== Dataset Info ===");
println!("Name: {}", inspect.info.name);
println!("Type: {}", inspect.info.dataset_type);
println!("Identifier: {}", inspect.info.identifier);
println!("Compression: {}", if inspect.info.compress_type == 0 { "zstd" } else { "deflate" });
println!("Index mode: {}", if inspect.info.index_continuous == 0 { "sparse" } else { "continuous" });
println!("Retention: {}", inspect.info.retention_window);
println!("Journal: {}", inspect.info.enable_journal);

println!("\n=== Dataset State ===");
println!("Latest timestamp: {:?}", inspect.state.latest_written_timestamp);
println!("Data segments: {} ({} open)", inspect.state.data_segments, inspect.state.open_data_segments);
println!("Index segments: {} ({} open)", inspect.state.index_segments, inspect.state.open_index_segments);
println!("Total records: {}", inspect.state.total_record_count);
println!("Data size: {} bytes", inspect.state.total_data_size);
println!("Read-only: {}", inspect.state.read_only);
println!("Queue groups: {}", inspect.state.queue_consumer_groups);

ds.close()?;
store.close()?;
```

**Key points**:
- `inspect()` returns both static config and runtime state
- Useful for monitoring, debugging, and capacity planning

---

## Scenario 9: Journal Queue for Audit/Sync

**When**: You need to consume journal entries for audit logging or cross-system sync.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/journal", StoreConfig::default())?;
let ds = store.create_dataset(
    "events", "audited",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6, 0, 0,
)?;

// Write events (automatically journaled)
ds.write(1, b"user_login")?;
ds.write(2, b"page_view")?;
ds.write(3, b"purchase")?;

// Query journal directly
if let Some(seq) = store.journal_latest_sequence()? {
    println!("Latest journal sequence: {seq}");
}

let records = store.journal_query(1, 10)?;
for (seq, data) in &records {
    println!("Journal #{seq}: {} bytes", data.len());
}

// Open journal queue for reliable consumption
let jq = store.open_journal_queue()?;
let mut jc = jq.open_consumer()?;

// Process journal entries
for _ in 0..10 {
    match jc.poll(Duration::from_secs(1))? {
        Some((seq, payload)) => {
            println!("Journal #{seq}: {} bytes", payload.len());
            jc.ack(seq)?;
        }
        None => break,
    }
}

// Get specific journal record without advancing cursor
if let Some((seq, data)) = jc.get(42)? {
    println!("Journal #{seq}: {} bytes", data.len());
}

jc.close()?;
jq.close()?;
ds.close()?;
store.close()?;
```

**Key points**:
- Journal entries are automatically created for write/delete/append operations
- `open_journal_queue()` creates a persistent consumer
- `poll()` returns `(sequence, payload)` tuples
- `get(sequence)` reads without advancing the cursor

---

## Scenario 10: Background Tasks

**When**: You need manual control over background maintenance (flush, eviction, retention).

```rust
use timslite::{Store, StoreConfig};

// Disable background thread for manual control
let mut store = Store::open(
    "/data/manual",
    StoreConfig::builder()
        .enable_background_thread(false)
        .build(),
)?;

let ds = store.create_dataset("metrics", "cpu", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Write data
for i in 1..=1000i64 {
    ds.write(i, &format!("cpu={:.2}", i as f64 * 0.01).into_bytes())?;
}

// Manually trigger background tasks
let result = store.tick_background_tasks()?;
println!("Executed {} tasks", result.executed_tasks);
println!("Next run in {}ms", result.next_delay_ms);

// Run again to flush
let result2 = store.tick_background_tasks()?;
println!("Executed {} tasks", result2.executed_tasks);

// Check next delay without executing
let delay = store.next_background_delay()?;
println!("Next task due in {}ms", delay.as_millis());

ds.close()?;
store.close()?;
```

**Key points**:
- `enable_background_thread: false` disables automatic background tasks
- `tick_background_tasks()` manually triggers flush, idle-close, eviction, retention
- Returns `TickResult` with execution count and next recommended delay

---

## Scenario 11: Error Handling

**When**: You need robust error handling for production use.

```rust
use timslite::{Store, StoreConfig, TmslError};

match Store::open("/data/errors", StoreConfig::default()) {
    Ok(mut store) => {
        // AlreadyExists error
        match store.create_dataset("test", "data", 64*1024*1024, 4*1024*1024, 6, 0, 0) {
            Ok(_) => println!("Created"),
            Err(TmslError::AlreadyExists(msg)) => println!("Already exists: {msg}"),
            Err(e) => println!("Error: {e}"),
        }

        // NotFound error
        match store.open_dataset("nonexistent", "data") {
            Ok(_) => println!("Opened"),
            Err(TmslError::NotFound(msg)) => println!("Not found: {msg}"),
            Err(e) => println!("Error: {e}"),
        }

        // InvalidData error
        match store.create_dataset("", "data", 64*1024*1024, 4*1024*1024, 6, 0, 0) {
            Ok(_) => println!("Created"),
            Err(TmslError::InvalidData(msg)) => println!("Invalid: {msg}"),
            Err(e) => println!("Error: {e}"),
        }

        store.close().ok();
    }
    Err(e) => println!("Failed to open store: {e}"),
}
```

**Error variants**:
- `AlreadyExists`: Dataset already exists
- `NotFound`: Dataset or record not found
- `InvalidData`: Invalid parameters
- `Expired`: Timestamp outside retention window
- `SegmentFull`: Segment is full
- `QueueAlreadyOpen`: Queue already opened
- `QueueClosed`: Queue closed
