# timslite Feature Scenarios

> Copy-paste examples for common use cases, with analysis of when to use each approach.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```rust
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/sensors", StoreConfig::default())?;

// Create a dataset for sensor readings (sparse mode for irregular timestamps)
let ds = store.create_dataset("temp_sensor", "readings",
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

**Python equivalent**:
```python
with timslite.Store.open("/data/sensors") as store:
    store.create_dataset("temp_sensor", "readings")
    ds = store.open_dataset("temp_sensor", "readings")
    for i in range(1000):
        ds.write(i + 1, f'{{"temp": {20.0 + i * 0.1}}}'.encode())
    for ts, data in ds.query(100, 200):
        print(f"ts={ts}: {data.decode()}")
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
let ds = store.create_dataset("cpu_usage", "per_second",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6,
    1,   // continuous mode — time_step = 1
    0,
)?;

// Write with gaps — filler entries are auto-created for missing timestamps
ds.write(1, b"50%")?;    // base_timestamp = 1
ds.write(5, b"55%")?;    // timestamps 2,3,4 become filler
ds.write(100, b"80%")?;  // large gap — middle segments not created (logical hole)

// Reading a filler timestamp returns None
assert_eq!(ds.read(2)?, None);   // filler
assert_eq!(ds.read(3)?, None);   // filler

// Reading a real timestamp returns data
assert_eq!(ds.read(5)?, Some((5, b"55%".to_vec())));

// Query skips fillers
let entries = ds.query(1, 100)?;
assert_eq!(entries.len(), 3);  // only timestamps 1, 5, 100
```

**When NOT to use continuous mode**:
- Timestamps are event-driven and irregular
- Timestamps have large random gaps (wastes index space on fillers)
- You need out-of-order writes (continuous mode requires `ts >= latest`)

---

## Scenario 3: Queue-Based Consumer Groups

**When**: You need producer-consumer semantics with multiple worker groups.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/jobs", StoreConfig::default())?;
let ds = store.create_dataset("task_queue", "jobs",
    64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)?;

// Open queue
let queue = store.open_queue(&ds)?;

// Open two consumer groups — each gets independent progress
let worker_a = queue.open_consumer("workers_a")?;
let worker_b = queue.open_consumer("workers_b")?;

// Producer pushes data — timestamps auto-assigned (1, 2, 3, ...)
for i in 0..10 {
    let ts = queue.push(format!("job_{}", i).as_bytes())?;
    println!("Pushed job {i} at ts={ts}");
}

// Worker group A consumes
for _ in 0..10 {
    let (ts, data) = worker_a.poll(Duration::from_millis(100))?.unwrap();
    println!("Worker A got: {}", String::from_utf8_lossy(&data));
    worker_a.ack(ts)?;
}

// Worker group B can still consume the same data (independent progress)
let (ts, data) = worker_b.poll(Duration::from_millis(100))?.unwrap();
println!("Worker B got: {}", String::from_utf8_lossy(&data));
worker_b.ack(ts)?;

store.close()?;
```

**Key points**:
- Open consumers BEFORE pushing if you want them to consume from the beginning
- New consumers initialize from the current `latest_written_timestamp`
- `ack()` only updates the current consumer group's state
- `poll()` blocks up to `timeout` if no data; returns `Ok(None)` on timeout
- Filler/gap entries are NOT delivered to consumers

---

## Scenario 4: Consumer with Retry and Visibility Timeout

**When**: Workers may crash; you need automatic redelivery of unacked records.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig, QueueConsumerConfig};

let mut store = Store::open("/data/jobs", StoreConfig::default())?;
let ds = store.create_dataset("retry_queue", "jobs",
    64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)?;
let queue = store.open_queue(&ds)?;

// Configure: pending expires after 60 seconds, max 3 retries
let config = QueueConsumerConfig {
    running_expired_seconds: 60,
    max_retry_count: 3,
};
let consumer = queue.open_consumer_with_config("retry_group", config)?;

// Push and poll
let ts = queue.push(b"important_task")?;
let (polled_ts, data) = consumer.poll(Duration::from_millis(100))?.unwrap();

// If we crash here without acking, after 60 seconds the record becomes
// eligible for redelivery. After 3 failed retries, it's dropped.

// Ack to mark as processed
consumer.ack(polled_ts)?;
```

**Python equivalent**:
```python
cfg = timslite.StoreConfig(enable_background_thread=False)
with timslite.Store.open("/data/jobs", cfg) as store:
    store.create_dataset("retry_queue", "jobs")
    ds = store.open_dataset("retry_queue", "jobs")
    q = ds.open_queue()
    c = q.open_consumer("retry_group", running_expired_seconds=60, max_retry_count=3)
    ts = q.push(b"important_task")
    result = c.poll(100)
    if result:
        rts, data = result
        c.ack(rts)
```

---

## Scenario 5: Journal for Change Tracking / Hot Migration

**When**: You need to track all data changes for audit, sync to another system, or recovery.

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let mut store = Store::open("/data/app", StoreConfig::default())?;

// Journal is enabled by default. Create a dataset.
let ds = store.create_dataset("events", "user_actions",
    64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)?;

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
use timslite::{Store, StoreConfig};

// retention_check_hour = 0 means daily at UTC 00:00
let mut config = StoreConfig::default();
// Background thread must run for automatic reclaim
// (or call tick_background_tasks manually)

let mut store = Store::open("/data/metrics", config)?;

// Create dataset with 7-day retention (assuming timestamp = Unix seconds)
let seven_days = 7 * 24 * 60 * 60;  // 604800 seconds
let ds = store.create_dataset("daily_metrics", "events",
    64 * 1024 * 1024,
    4 * 1024 * 1024,
    6,
    0,
    seven_days,  // retention_window in timestamp units
)?;

// Write current data
let now = 1700000000i64;  // current timestamp
ds.write(now, b"current_data")?;

// Old data is automatically expired
ds.write(now - 800000, b"old_data")?;  // older than 7 days

// Reading expired data returns None
// (retention check happens at read time)
let old = ds.read(now - 800000)?;
// After retention reclaim, this returns None

// Delete/correction on expired timestamps is refused
```

**Retention rules**:
- `retention_window = 0` means no limit
- Unit is the same as your timestamp unit (seconds, milliseconds, etc.)
- `read(ts)` returns `None` for expired timestamps
- Delete, out-of-order rewrite, and correction are refused on expired timestamps
- Reclaim only deletes entire expired segments (data and index may not reclaim synchronously)
- `retention_check_hour` is UTC hour (0-23) for daily reclaim

---

## Scenario 7: Manual Background Task Loop

**When**: You can't spawn a background thread (e.g., embedded in a single-threaded event loop).

```rust
use std::time::Duration;
use timslite::{Store, StoreConfig};

let config = StoreConfig::builder()
    .enable_background_thread(false)
    .build();

let mut store = Store::open("/data/app", config)?;
let ds = store.create_dataset("my_ds", "events", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

ds.write(1, b"data")?;

// Manual tick — runs flush, idle-close, cache eviction, retention as needed
let tick = store.tick_background_tasks()?;
println!("Executed {} tasks, next in {:?}", tick.executed_tasks, tick.next_delay);

// Check delay without executing
let delay = store.next_background_delay()?;
println!("Next task due in {:?}", delay);

// Event loop integration
loop {
    let tick = store.tick_background_tasks()?;
    if tick.executed_tasks > 0 {
        println!("Ran {} background tasks", tick.executed_tasks);
    }
    std::thread::sleep(tick.next_delay);
}
```

**Python equivalent**:
```python
import time
cfg = timslite.StoreConfig(enable_background_thread=False)
store = timslite.Store.open("/data/app", cfg)

while True:
    executed, delay_ms = store.tick_background_tasks()
    if executed > 0:
        print(f"Ran {executed} background tasks")
    time.sleep(delay_ms / 1000.0)
```

**Tasks executed per tick (0-4)**:
1. Flush dirty segments
2. Idle-close inactive segments
3. Cache eviction
4. Retention reclaim

---

## Scenario 8: Read-Only Mode (Inspection / Backup)

**When**: You need to inspect or read data without acquiring the write lock.

```rust
use timslite::{Store, StoreConfig};

// Force read-only — doesn't check or take the .lock file
let config = StoreConfig::builder()
    .read_only(Some(true))
    .build();

let mut store = Store::open("/data/app", config)?;
assert!(store.is_read_only());

// Can open and read existing datasets
if let Ok(ds) = store.open_dataset("my_ds", "events") {
    let entries = ds.query(1, 100)?;
    let info = ds.inspect()?;
    println!("Dataset: {} records", info.state.total_record_count);
}

// Cannot write, create, drop, or open queues in read-only mode
// ds.write(1, b"data") would return an error
```

**Auto read-only mode** (default):
- If `.lock` can be acquired → writable
- If `.lock` is already held → falls back to read-only
- Use `read_only(Some(false))` to require writable (fail if locked)

---

## Scenario 9: Append (In-Place Tail Growth)

**When**: You want to append data to the latest record without creating a new timestamp.

```rust
let mut store = Store::open("/data/logs", StoreConfig::default())?;
let ds = store.create_dataset("app_log", "lines", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// Create initial record at timestamp 1
ds.append(1, b"line1\n")?;

// Append more data to the same timestamp (in-place tail growth)
ds.append(1, b"line2\n")?;
ds.append(1, b"line3\n")?;

// Read the combined record
let (_, data) = ds.read(1)?.unwrap();
assert_eq!(data, b"line1\nline2\nline3\n");

// Forward append (creates new record when timestamp > latest)
ds.append(2, b"new_record\n")?;
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

## Scenario 10: Correction Write (Fix Latest Record)

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
- If latest record is in a pending raw block → in-place modify (can change data length)
- If latest record is in a sealed/compressed block → fallback to update write
- Update write: appends new data, updates index, invalidates old cache key

---

## Scenario 11: Out-of-Order Write (Sparse Mode Only)

**When**: In sparse mode, you need to rewrite a past timestamp that already has an index entry.

```rust
let ds = /* ... */;

ds.write(1, b"first")?;
ds.write(2, b"second")?;
ds.write(3, b"third")?;

// latest_written_timestamp = 3

// Out-of-order write: timestamp < latest, but index entry exists
ds.write(2, b"updated_second")?;  // rewrites timestamp 2

// Reading returns the updated data
assert_eq!(ds.read(2)?, Some((2, b"updated_second".to_vec())));

// Out-of-order write to a timestamp with NO index entry → ERROR
// ds.write(1, b"data")?;  // would fail if timestamp 1 didn't exist
```

**In continuous mode**: Out-of-order writes are not supported. Timestamps must be `>= latest_written_timestamp`.

---

## Scenario 12: Large Record (Single-Record Block)

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

## Scenario 13: Multi-Dataset Isolation

**When**: You have multiple data streams that need independent storage.

```rust
let mut store = Store::open("/data/app", StoreConfig::default())?;

// Each (name, type) pair is an independent dataset
let ds1 = store.create_dataset("sensors", "temperature", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let ds2 = store.create_dataset("sensors", "humidity",    64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let ds3 = store.create_dataset("sensors", "pressure",    64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let ds4 = store.create_dataset("events",  "user_action", 64*1024*1024, 4*1024*1024, 6, 0, 0)?;
let ds5 = store.create_dataset("events",  "system",      64*1024*1024, 4*1024*1024, 6, 0, 0)?;

// List all datasets
for (name, dtype) in store.list_datasets()? {
    println!("{name}/{dtype}");
}

// Use each independently
ds1.write(1, b"23.5C")?;
ds4.write(1, b"login")?;

// Inspect a dataset
let inspect = store.inspect_dataset("sensors", "temperature")?;
println!("Records: {}", inspect.state.total_record_count);
println!("Data size: {} bytes", inspect.state.total_data_size);
```

**Directory layout**: Each dataset gets its own `{name}/{type}/` directory with independent `meta`, `data/`, `index/`, `state`, and optional `queue/`.

---

## Scenario 14: Efficient Existence Checking

**When**: You need to check if records exist without loading data (e.g., for filtering or validation).

```rust
let ds = /* ... */;

// Fast existence check — index lookup only, no data I/O
if ds.read_exist(42)? {
    println!("Record at timestamp 42 exists");
}

// Range existence bitmap — no data I/O
// Returns a bitmap where bit i corresponds to start_ts + i
let bitmap = ds.query_exist(1, 1000)?;
for i in 0..1000 {
    if bitmap[i / 8] & (1 << (i % 8)) != 0 {
        println!("Record exists at timestamp {}", 1 + i as i64);
    }
}

// Length-only query — reads only 12-byte record headers, not full data
let lengths = ds.query_length(1, 1000)?;
for (ts, len) in &lengths {
    println!("ts={ts}: {len} bytes");
}
```

**Performance tiers** (fastest to slowest):
1. `read_exist` / `query_exist` — index only, no data segment I/O
2. `read_length` / `query_length` / `query_length_iter` — reads 12-byte record header only
3. `read` / `query` / `query_iter` — reads full data

---

## Scenario 15: Using DataSetConfigBuilder for Fine Control

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

## Scenario 16: Cross-Language via C FFI

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
        tmsl_free_string(out_data, out_len);
    }

    // Close store
    tmsl_store_close(store, err, sizeof(err));
    return 0;
}
```

**FFI conventions**:
- Return `void*` for opaque handles, `int` (0=success, -1=error) for operations
- Error messages go to caller-provided `err_buf`
- String outputs must be freed with `tmsl_free_string`
- Iterators: create with `tmsl_dataset_query`, advance with `tmsl_iter_next`, free with `tmsl_iter_free`
