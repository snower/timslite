# Rust Examples

> Feature scenarios with copy-paste Rust examples.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```rust
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/sensors", StoreConfig::default())?;

// Create a dataset for sensor readings (sparse mode for irregular timestamps)
store.create_dataset("temp_sensor", "readings",
    64 * 1024 * 1024,  // 64MB data segments
    4 * 1024 * 1024,   // 4MB index segments
    6,                 // compress level
    0,                 // sparse index mode
    0,                 // no retention
)?;

let handle = store.open_dataset("temp_sensor", "readings")?;
let ds = store.get_dataset(&handle)?;

// Write sensor readings (timestamps must be monotonically increasing)
for i in 0..1000i64 {
    let data = format!("{{\"temp\": {:.1}, \"ts\": {}}}", 20.0 + i as f64 * 0.1, i).into_bytes();
    ds.write(i + 1, &data)?;
}

// Query a range
let entries = ds.query(100, 200)?;
for (ts, data) in &entries {
    println!("ts={ts}: {}", String::from_utf8_lossy(data));
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
store.create_dataset("cpu_usage", "per_second",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6,
    1,   // continuous mode — time_step = 1
    0,
)?;

let handle = store.open_dataset("cpu_usage", "per_second")?;
let ds = store.get_dataset(&handle)?;

// Write sequential timestamps (no gaps allowed in continuous mode)
for i in 1..=1000i64 {
    ds.write(i, &format!("cpu={:.2}", i as f64 * 0.01).into_bytes())?;
}

// Read by position (O(1) lookup)
let (_, data) = ds.read(500)?.unwrap();
println!("Record 500: {}", String::from_utf8_lossy(&data));

// Continuous mode auto-fills gaps with None on read
// If you write ts=1 and ts=100, reading ts=50 returns None (filler)
```

**Key points**:
- Continuous mode assumes timestamps are dense sequential integers
- Missing timestamps become filler entries (read returns `None`)
- O(1) timestamp-to-position calculation within a segment

---

## Scenario 3: Queue (FIFO Consumer Groups)

**When**: You need ordered delivery with consumer group semantics (like Kafka consumer groups).

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;
store.create_dataset("tasks", "jobs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let handle = store.open_dataset("tasks", "jobs")?;

// Open queue for a dataset
let q = store.open_queue(handle)?;

// Push data (auto-assigns next timestamp)
let ts1 = store.queue_push(&q, b"task_1")?;
let ts2 = store.queue_push(&q, b"task_2")?;

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

**Key points**:
- `queue_push` auto-assigns `timestamp = latest_written_timestamp + 1`
- Multiple consumer groups are independent
- Multiple consumers in the same group share progress (mutual exclusion)

---

## Scenario 4: Queue with Retry and Expiry

**When**: You need automatic retry for failed tasks and expiry for stuck tasks.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig, QueueConsumerConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;
store.create_dataset("retry_queue", "jobs", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let handle = store.open_dataset("retry_queue", "jobs")?;
let q = store.open_queue(handle)?;

// Push some tasks
store.queue_push(&q, b"important_task")?;

// Open consumer with retry config
let config = QueueConsumerConfig {
    running_expired_seconds: 60,  // re-deliver after 60s if not acked
    max_retry_count: 3,           // drop after 3 retries
};
let consumer = store.open_consumer_with_config(&q, "retry_group", config)?;

// Poll and process
if let Some((ts, data)) = store.queue_poll(&consumer, Duration::from_secs(5))? {
    println!("Processing: {}", String::from_utf8_lossy(&data));
    // If processing fails, don't ack — it will be re-delivered after 60s
    // After 3 failures, the entry is dropped
    store.queue_ack(&consumer, ts)?;
}

store.close_queue(q)?;
store.close()?;
```

**Key points**:
- `running_expired_seconds`: re-deliver pending entries after this timeout
- `max_retry_count`: drop entries after this many retries (0 = unlimited)

---

## Scenario 5: Journal for Change Tracking / Hot Migration

**When**: You need to track all data changes for audit, sync to another system, or recovery.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;

// Journal is enabled by default. Create a dataset.
store.create_dataset("events", "user_actions",
    64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)?;
let handle = store.open_dataset("events", "user_actions")?;
let ds = store.get_dataset(&handle)?;

// Every write/delete/append automatically appends to the journal
ds.write(1, b"user_login")?;
ds.write(2, b"page_view")?;
ds.delete(1)?;

// Read journal records
let latest = store.journal_latest_sequence()?;  // e.g., Some(4) — create + 2 writes + delete
println!("Latest journal seq: {:?}", latest);

// Read individual journal record
if let Some((seq, payload)) = store.journal_read(1)? {
    println!("Journal record {seq}: {} bytes", payload.len());
}

// Query a range of journal records
let records = store.journal_query(1, latest.unwrap())?;
for (seq, payload) in &records {
    println!("Seq {seq}: {} bytes", payload.len());
}

// Consume journal via queue (for downstream sync)
let jq = store.open_journal_queue()?;
let consumer = jq.open_consumer("sync_worker")?;

// Each journal record is delivered as a queue entry
while let Some((seq, payload)) = consumer.poll(Duration::from_millis(100))? {
    println!("Consumed journal seq {seq}");
    // Use store.read_journal_source_record() to fetch the actual business data
    consumer.ack(seq)?;
}

store.close()?;
```

**Important journal semantics**:
- Journal is NOT a WAL — no transaction guarantees
- Journal records do NOT contain business payload; they reference source data via `index_info`
- Use `Store::read_journal_source_record(dataset_identifier, index_info)` to dereference
- Journal append failure does NOT roll back the main operation
- Disable per-dataset with `DataSetConfigBuilder::enable_journal(false)`

---

## Scenario 6: Retention Window (Time-Based Data Expiry)

**When**: You want old data to automatically expire and be reclaimed.

```rust
use timslite::{Store, StoreConfig, DataSetConfigBuilder};

let mut store = Store::open("/data/app", StoreConfig::default())?;

// Create dataset with 1-day retention (in timestamp units)
let builder = DataSetConfigBuilder::new()
    .retention_window(86400)  // 86400 seconds = 1 day
    .enable_journal(true);

store.create_dataset_with_config("metrics", "per_second", Some(builder))?;
let handle = store.open_dataset("metrics", "per_second")?;
let ds = store.get_dataset(&handle)?;

// Write data with timestamps
for i in 1..=1000i64 {
    ds.write(i, &format!("value={i}").into_bytes())?;
}

// Read old data (may be expired if retention_window > 0)
if let Some((ts, data)) = ds.read(1)? {
    println!("Old record: ts={ts}");
}

// Expired records return None on read
// Reclaim happens during background tasks (daily at retention_check_hour UTC)
```

**Key points**:
- `retention_window` uses the same units as dataset timestamps
- `retention_window = 0` means no limit
- `retention_check_hour` is UTC hour (0-23) for daily reclaim
- Expired records return `None` on read
- Reclaim deletes entire segments when all records are expired

---

## Scenario 7: Read-Only Mode

**When**: You want multiple processes to read the same store, or need to inspect data safely.

```rust
use timslite::{Store, StoreConfig};

// Force read-only mode
let config = StoreConfig {
    read_only: Some(true),
    ..Default::default()
};

let store = Store::open("/data/app", config)?;

// Read operations work normally
let handle = store.open_dataset("metrics", "cpu")?;
let ds = store.get_dataset(&handle)?;
let entries = ds.query(1, 100)?;
let info = ds.inspect()?;
println!("Dataset: {} records", info.state.total_record_count);

// Cannot write, create, drop, or open queues in read-only mode
// ds.write(1, b"data") would return an error
```

**Auto read-only mode** (default):
- If `.lock` can be acquired → writable
- If `.lock` is already held → falls back to read-only
- Use `read_only(Some(false))` to require writable (fail if locked)

---

## Scenario 8: Append (In-Place Tail Growth)

**When**: You want to append data to the latest record without creating a new timestamp.

```rust
let mut store = Store::open("/data/logs", StoreConfig::default())?;
store.create_dataset("app_log", "lines", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let handle = store.open_dataset("app_log", "lines")?;
let ds = store.get_dataset(&handle)?;

// Create initial record at timestamp 1
ds.append(1, b"line1\n")?;

// Append more data to the same timestamp (in-place tail growth)
ds.append(1, b"line2\n")?;
ds.append(1, b"line3\n")?;

// Read the combined record
let (_, data) = ds.read(1)?.unwrap();
assert_eq!(data, b"line1\nline2\nline3\n");

// Forward append creates a new record
ds.append(2, b"new_record")?;
```

**Append rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + append_len <= 4 MiB`
- Appended data must fit within the current pending block's capacity
- Does NOT re-notify queue when appending to existing record

---

## Scenario 9: Correction Write (Fix Latest Record)

**When**: You wrote data and need to correct the latest record's content.

```rust
let ds = /* ... */;

// Write a record
ds.write(100, b"wrong_data")?;

// Correct it by writing to the same timestamp
// (only works if latest_written_timestamp == 100 and the record is in a pending raw block)
ds.write(100, b"corrected_data")?;

// If the block has already been sealed/compressed, the correction
// automatically falls back to an "update write": new data is appended
// to the latest data segment, the index is updated, and the old
// record's segment gets invalid_record_count incremented.
```

**Correction behavior**:
- Triggers when `timestamp == latest_written_timestamp`
- If latest record is in pending raw block → in-place correction
- If latest record is in sealed/compressed block → update write (new data + index update)
- Cache invalidation occurs for affected blocks

---

## Scenario 10: Out-of-Order Write (Sparse Mode)

**When**: You need to update an existing timestamp (not the latest).

```rust
let ds = /* ... */;

// Write some records
ds.write(1, b"data_1")?;
ds.write(2, b"data_2")?;
ds.write(3, b"data_3")?;

// Update timestamp 1 (out-of-order write)
// Only works in sparse mode, and only if timestamp 1 already has an index entry
ds.write(1, b"updated_data_1")?;

// Out-of-order write to a timestamp with NO index entry → ERROR
// ds.write(1, b"data")?;  // would fail if timestamp 1 didn't exist
```

**In continuous mode**: Out-of-order writes are not supported. Timestamps must be `>= latest_written_timestamp`.

---

## Scenario 11: Large Record (Single-Record Block)

**When**: A single record's encoded size exceeds 64KB (the normal block payload limit).

```rust
let ds = /* ... */;

// Records larger than 64KB get their own exclusive block
// (SINGLE_RECORD flag set, immediately compressed)
let large_data = vec![0u8; 100_000];  // ~100KB
ds.write(1, &large_data)?;

// Reading works normally
let (_, data) = ds.read(1)?.unwrap();
assert_eq!(data.len(), 100_000);
```

**Rules**:
- Single record max: 4 MiB (`write` and `append` both enforce this)
- Records > 64KB encoded → exclusive single-record block (SINGLE_RECORD | SEALED | COMPRESSED)
- Records ≤ 64KB → aggregated into normal blocks (pending → sealed on overflow)

---

## Scenario 12: Multi-Dataset Isolation

**When**: You have multiple data streams that need independent storage.

```rust
let mut store = Store::open("/data/app", StoreConfig::default())?;

// Each (name, type) pair is an independent dataset
store.create_dataset("sensors", "temperature", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
store.create_dataset("sensors", "humidity",    64*1024*1024, 4*1024*1024, 6, 0, 0)?;
store.create_dataset("sensors", "pressure",    64*1024*1024, 4*1024*1024, 6, 0, 0)?;
store.create_dataset("events",  "user_action", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
store.create_dataset("events",  "system",      64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// List all datasets
for (name, dtype) in store.list_datasets()? {
    println!("{name}/{dtype}");
}

// Open and use each independently
let h1 = store.open_dataset("sensors", "temperature")?;
let h2 = store.open_dataset("events", "user_action")?;

let ds1 = store.get_dataset(&h1)?;
let ds2 = store.get_dataset(&h2)?;

ds1.write(1, b"23.5C")?;
ds2.write(1, b"login")?;

// Inspect a dataset
let inspect = store.inspect_dataset("sensors", "temperature")?;
println!("Records: {}", inspect.state.total_record_count);
println!("Data size: {} bytes", inspect.state.total_data_size);
```

**Directory layout**: Each dataset gets its own `{name}/{type}/` directory with independent `meta`, `data/`, `index/`, `state`, and optional `queue/`.

---

## Scenario 13: Efficient Existence Checking

**When**: You need to check if records exist without loading data (e.g., for deduplication or coverage checks).

```rust
let ds = /* ... */;

// Single record existence (index only, no data I/O)
let exists = ds.read_exist(12345)?;

// Range existence bitmap (index only, no data I/O)
let bitmap = ds.query_exist(1, 1000)?;
// bitmap[i] is set if record at (start_ts + i) exists

// Record length (reads 12-byte header only)
let len = ds.read_length(12345)?;  // Option<u32>

// Range lengths (reads headers only)
let lengths = ds.query_length(1, 1000)?;  // Vec<(i64, u32)>
```

**Performance hierarchy** (fastest to slowest):
1. `read_exist` / `query_exist` — index only, no data segment I/O
2. `read_length` / `query_length` / `query_length_iter` — reads 12-byte record header only
3. `read` / `query` / `query_iter` — reads full data

---

## Scenario 14: Using DataSetConfigBuilder for Fine Control

**When**: You need non-default dataset configuration.

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

---

## Scenario 15: Cross-Language via C FFI

**When**: You need to use timslite from C, C++, Go, or other languages with C interop.

```c
#include "timslite.h"
#include <stdio.h>
#include <string.h>

int main() {
    char err[256];

    // Open store with defaults
    void* store = tmsl_store_open("/data/timslite", err, sizeof(err));
    if (!store) { fprintf(stderr, "Open failed: %s\n", err); return 1; }

    // Create dataset
    if (tmsl_dataset_create(store, "sensors", "temp",
            67108864, 4194304, 6, 0, 0, err, sizeof(err)) != 0) {
        fprintf(stderr, "Create failed: %s\n", err); return 1;
    }

    // Open dataset
    uint64_t handle = 0;
    if (tmsl_dataset_open(store, "sensors", "temp", &handle, err, sizeof(err)) != 0) {
        fprintf(stderr, "Open failed: %s\n", err); return 1;
    }

    // Write a record
    const char* data = "temperature=23.5";
    if (tmsl_dataset_write(store, handle, 1, (const uint8_t*)data, strlen(data),
                           err, sizeof(err)) != 0) {
        fprintf(stderr, "Write failed: %s\n", err); return 1;
    }

    // Read a record
    uint8_t* out_data = NULL;
    size_t out_len = 0;
    int64_t out_ts = 0;
    int found = tmsl_dataset_read(store, handle, 1,
                                  &out_ts, &out_data, &out_len,
                                  err, sizeof(err));
    if (found == 1) {
        printf("Read: ts=%lld, data=%.*s\n", (long long)out_ts, (int)out_len, out_data);
        tmsl_free_string(out_data);
    }

    // Close
    tmsl_dataset_close(store, handle, err, sizeof(err));
    tmsl_store_close(store, err, sizeof(err));
    return 0;
}
```

**Key points**:
- All FFI functions return `int` (0=success, -1=error) or `void*` (opaque pointer)
- Error messages written to caller-provided buffer
- String outputs must be freed with `tmsl_free_string`
- See `include/timslite.h` for complete API