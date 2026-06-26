# Python Examples

> Feature scenarios with copy-paste Python examples.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```python
import timslite

with timslite.Store.open("/data/sensors") as store:
    # Create a dataset for sensor readings (sparse mode for irregular timestamps)
    store.create_dataset("temp_sensor", "readings")

    ds = store.open_dataset("temp_sensor", "readings")

    # Write sensor readings (timestamps must be monotonically increasing)
    for i in range(1000):
        data = f'{{"temp": {20.0 + i * 0.1}, "ts": {i}}}'.encode()
        ds.write(i + 1, data)

    # Query a range
    for ts, data in ds.query(100, 200):
        print(f"ts={ts}: {data.decode()}")

    # Read a specific timestamp
    record = ds.read(150)
    if record:
        ts, data = record
        print(f"Record at {ts}: {data.decode()}")

    # Read the latest record
    record = ds.read_latest()
    if record:
        ts, data = record
        print(f"Latest at {ts}: {data.decode()}")
```

**Key points**:
- Timestamps must be `>= latest_written_timestamp` in sparse mode
- `query()` returns a lazy iterator; use `query_all()` for an eager list
- `read_latest()` is the only way to get the latest; `read(-1)` reads timestamp `-1`

---

## Scenario 2: Continuous Mode (Dense Sequential Timestamps)

**When**: Your timestamps are dense sequential integers (e.g., per-second readings with few gaps).

```python
import timslite

with timslite.Store.open("/data/metrics") as store:
    # Create with index_continuous = 1
    config = timslite.DataSetConfig(index_continuous=1)
    store.create_dataset_with_config("cpu_usage", "per_second", config)

    ds = store.open_dataset("cpu_usage", "per_second")

    # Write sequential timestamps (no gaps allowed in continuous mode)
    for i in range(1, 1001):
        ds.write(i, f"cpu={i * 0.01:.2f}".encode())

    # Read by position (O(1) lookup)
    record = ds.read(500)
    if record:
        ts, data = record
        print(f"Record 500: {data.decode()}")

    # Continuous mode auto-fills gaps with None on read
    # If you write ts=1 and ts=100, reading ts=50 returns None (filler)
```

**Key points**:
- Continuous mode assumes timestamps are dense sequential integers
- Missing timestamps become filler entries (read returns `None`)
- O(1) timestamp-to-position calculation within a segment

---

## Scenario 3: Queue (FIFO Consumer Groups)

**When**: You need ordered delivery with consumer group semantics (like Kafka consumer groups).

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("tasks", "jobs")
    ds = store.open_dataset("tasks", "jobs")

    # Open queue for a dataset
    q = store.open_queue(ds.id)

    # Push data (auto-assigns next timestamp)
    ts1 = q.push(b"task_1")
    ts2 = q.push(b"task_2")

    # Open a consumer group
    consumer = q.open_consumer("worker_group")

    # Poll for records (with timeout in ms)
    result = consumer.poll(5000)
    if result:
        ts, data = result
        print(f"Got task at {ts}: {data.decode()}")
        # Acknowledge processing
        consumer.ack(ts)

    q.close()
```

**Key points**:
- `push` auto-assigns `timestamp = latest_written_timestamp + 1`
- Multiple consumer groups are independent
- Multiple consumers in the same group share progress (mutual exclusion)

---

## Scenario 4: Queue with Retry and Expiry

**When**: You need automatic retry for failed tasks and expiry for stuck tasks.

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("retry_queue", "jobs")
    ds = store.open_dataset("retry_queue", "jobs")
    q = store.open_queue(ds.id)

    # Push some tasks
    q.push(b"important_task")

    # Open consumer with retry config
    consumer = q.open_consumer(
        "retry_group",
        running_expired_seconds=60,  # re-deliver after 60s if not acked
        max_retry_count=3,           # drop after 3 retries
    )

    # Poll and process
    result = consumer.poll(5000)
    if result:
        ts, data = result
        print(f"Processing: {data.decode()}")
        # If processing fails, don't ack — it will be re-delivered after 60s
        # After 3 failures, the entry is dropped
        consumer.ack(ts)

    q.close()
```

**Key points**:
- `running_expired_seconds`: re-deliver pending entries after this timeout
- `max_retry_count`: drop entries after this many retries (0 = unlimited)

---

## Scenario 5: Journal for Change Tracking / Hot Migration

**When**: You need to track all data changes for audit, sync to another system, or recovery.

```python
import timslite

with timslite.Store.open("/data/app") as store:
    # Journal is enabled by default. Create a dataset.
    store.create_dataset("events", "user_actions")
    ds = store.open_dataset("events", "user_actions")

    # Every write/delete/append automatically appends to the journal
    ds.write(1, b"user_login")
    ds.write(2, b"page_view")
    ds.delete(1)

    # Read journal records
    latest = store.journal_latest_sequence()  # e.g., 4 — create + 2 writes + delete
    print(f"Latest journal seq: {latest}")

    # Read individual journal record
    record = store.journal_read(1)
    if record:
        seq, payload = record
        print(f"Journal record {seq}: {len(payload)} bytes")

    # Query a range of journal records
    records = store.journal_query(1, latest)
    for seq, payload in records:
        print(f"Seq {seq}: {len(payload)} bytes")

    # Consume journal via queue (for downstream sync)
    jq = store.open_journal_queue()
    consumer = jq.open_consumer("sync_worker")

    # Each journal record is delivered as a queue entry
    result = consumer.poll(100)
    if result:
        seq, payload = result
        print(f"Consumed journal seq {seq}")
        # Use store.read_journal_source_record() to fetch the actual business data
        consumer.ack(seq)

    jq.close()
```

**Important journal semantics**:
- Journal is NOT a WAL — no transaction guarantees
- Journal records do NOT contain business payload; they reference source data via `index_info`
- Use `store.read_journal_source_record(dataset_identifier, index_info)` to dereference
- Journal append failure does NOT roll back the main operation
- Disable per-dataset with `DataSetConfig(enable_journal=False)`

---

## Scenario 6: Retention Window (Time-Based Data Expiry)

**When**: You want old data to automatically expire and be reclaimed.

```python
import timslite

with timslite.Store.open("/data/app") as store:
    # Create dataset with 1-day retention (in timestamp units)
    config = timslite.DataSetConfig(retention_window=86400)
    store.create_dataset_with_config("metrics", "per_second", config)

    ds = store.open_dataset("metrics", "per_second")

    # Write data with timestamps
    for i in range(1, 1001):
        ds.write(i, f"value={i}".encode())

    # Read old data (may be expired if retention_window > 0)
    record = ds.read(1)
    if record:
        ts, data = record
        print(f"Old record: ts={ts}")

    # Expired records return None on read
    # Reclaim happens during background tasks (daily at retention_check_hour UTC)
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

```python
import timslite

# Force read-only mode
config = timslite.StoreConfig(read_only=True)

with timslite.Store.open("/data/app", config) as store:
    # Read operations work normally
    ds = store.open_dataset("metrics", "cpu")
    records = ds.query(1, 100)
    info = ds.inspect()
    print(f"Dataset: {info.state.total_record_count} records")

    # Cannot write, create, drop, or open queues in read-only mode
    # ds.write(1, b"data") would raise TmslError
```

**Auto read-only mode** (default):
- If `.lock` can be acquired → writable
- If `.lock` is already held → falls back to read-only
- Use `read_only=False` to require writable (fail if locked)

---

## Scenario 8: Append (In-Place Tail Growth)

**When**: You want to append data to the latest record without creating a new timestamp.

```python
import timslite

with timslite.Store.open("/data/logs") as store:
    store.create_dataset("app_log", "lines")
    ds = store.open_dataset("app_log", "lines")

    # Create initial record at timestamp 1
    ds.append(1, b"line1\n")

    # Append more data to the same timestamp (in-place tail growth)
    ds.append(1, b"line2\n")
    ds.append(1, b"line3\n")

    # Read the combined record
    ts, data = ds.read(1)
    assert data == b"line1\nline2\nline3\n"

    # Forward append creates a new record
    ds.append(2, b"new_record")
```

**Append rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + len(data) <= 4 MiB`
- Appended data must fit within the current pending block's capacity
- Does NOT re-notify queue when appending to existing record

---

## Scenario 9: Correction Write (Fix Latest Record)

**When**: You wrote data and need to correct the latest record's content.

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("events", "actions")
    ds = store.open_dataset("events", "actions")

    # Write a record
    ds.write(100, b"wrong_data")

    # Correct it by writing to the same timestamp
    # (only works if latest_written_timestamp == 100 and the record is in a pending raw block)
    ds.write(100, b"corrected_data")

    # If the block has already been sealed/compressed, the correction
    # automatically falls back to an "update write": new data is appended
    # to the latest data segment, the index is updated, and the old
    # record's segment gets invalid_record_count incremented.
```

**Correction behavior**:
- Triggers when `timestamp == latest_written_timestamp`
- If latest record is in pending raw block → in-place correction
- If latest record is in sealed/compressed block → update write (new data + index update)
- Cache invalidation occurs for affected blocks

---

## Scenario 10: Out-of-Order Write (Sparse Mode)

**When**: You need to update an existing timestamp (not the latest).

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("events", "actions")
    ds = store.open_dataset("events", "actions")

    # Write some records
    ds.write(1, b"data_1")
    ds.write(2, b"data_2")
    ds.write(3, b"data_3")

    # Update timestamp 1 (out-of-order write)
    # Only works in sparse mode, and only if timestamp 1 already has an index entry
    ds.write(1, b"updated_data_1")

    # Out-of-order write to a timestamp with NO index entry → ERROR
    # ds.write(1, b"data")  # would fail if timestamp 1 didn't exist
```

**In continuous mode**: Out-of-order writes are not supported. Timestamps must be `>= latest_written_timestamp`.

---

## Scenario 11: Large Record (Single-Record Block)

**When**: A single record's encoded size exceeds 64KB (the normal block payload limit).

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("blobs", "files")
    ds = store.open_dataset("blobs", "files")

    # Records larger than 64KB get their own exclusive block
    # (SINGLE_RECORD flag set, immediately compressed)
    large_data = b"x" * 100_000  # ~100KB
    ds.write(1, large_data)

    # Reading works normally
    ts, data = ds.read(1)
    assert len(data) == 100_000
```

**Rules**:
- Single record max: 4 MiB (`write` and `append` both enforce this)
- Records > 64KB encoded → exclusive single-record block (SINGLE_RECORD | SEALED | COMPRESSED)
- Records ≤ 64KB → aggregated into normal blocks (pending → sealed on overflow)

---

## Scenario 12: Multi-Dataset Isolation

**When**: You have multiple data streams that need independent storage.

```python
import timslite

with timslite.Store.open("/data/app") as store:
    # Each (name, type) pair is an independent dataset
    store.create_dataset("sensors", "temperature")
    store.create_dataset("sensors", "humidity")
    store.create_dataset("sensors", "pressure")
    store.create_dataset("events", "user_action")
    store.create_dataset("events", "system")

    # List all datasets
    for name, dtype in store.list_datasets():
        print(f"{name}/{dtype}")

    # Open and use each independently
    ds1 = store.open_dataset("sensors", "temperature")
    ds2 = store.open_dataset("events", "user_action")

    ds1.write(1, b"23.5C")
    ds2.write(1, b"login")

    # Inspect a dataset
    inspect = store.inspect_dataset("sensors", "temperature")
    print(f"Records: {inspect.state.total_record_count}")
    print(f"Data size: {inspect.state.total_data_size} bytes")
```

**Directory layout**: Each dataset gets its own `{name}/{type}/` directory with independent `meta`, `data/`, `index/`, `state`, and optional `queue/`.

---

## Scenario 13: Efficient Existence Checking

**When**: You need to check if records exist without loading data (e.g., for deduplication or coverage checks).

```python
import timslite

with timslite.Store.open("/data/app") as store:
    store.create_dataset("events", "actions")
    ds = store.open_dataset("events", "actions")

    # Single record existence (index only, no data I/O)
    exists = ds.read_exist(12345)

    # Range existence bitmap (index only, no data I/O)
    bitmap = ds.query_exist(1, 1000)
    # bitmap[i] is set if record at (start_ts + i) exists

    # Record length (reads 12-byte header only)
    length = ds.read_length(12345)  # int or None

    # Range lengths (reads headers only)
    lengths = ds.query_length(1, 1000)  # list of (ts, length)
```

**Performance hierarchy** (fastest to slowest):
1. `read_exist` / `query_exist` — index only, no data segment I/O
2. `read_length` / `query_length` — reads 12-byte record header only
3. `read` / `query` / `query_all` — reads full data

---

## Scenario 14: Manual Background Tasks

**When**: You need fine-grained control over background task execution.

```python
import timslite
import time

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
while True:
    executed, delay_ms = store.tick_background_tasks()
    if executed > 0:
        print(f"ran {executed} background tasks")
    time.sleep(delay_ms / 1000.0)

store.close()
```

**Key points**:
- When `enable_background_thread=False`, you must call `tick_background_tasks()` periodically
- Returns `(executed_tasks, next_delay_ms)`
- Use `next_background_delay()` to check when the next task is due without executing