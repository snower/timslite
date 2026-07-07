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
    # Create with index_continuous=True
    ds = store.create_dataset("cpu_usage", "per_second",
        index_continuous=True,
    )

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

**When**: You need ordered delivery with multiple consumers sharing progress.

```python
import timslite

with timslite.Store.open("/data/queue_demo") as store:
    store.create_dataset("events", "raw")
    ds = store.open_dataset("events", "raw")

    # Open queue for the dataset
    q = store.open_queue(ds.id)

    # Producer: push data (auto-assigns next timestamp)
    for i in range(10):
        ts = q.push(f"event_{i}".encode())
        print(f"Pushed event_{i} at ts={ts}")

    # Consumer: poll with timeout
    consumer = q.open_consumer("worker_group")

    for _ in range(10):
        result = consumer.poll(5000)  # 5 second timeout
        if result:
            ts, data = result
            print(f"Processing: {data.decode()}")
            consumer.ack(ts)

    # Inspect consumer state
    inspect_result = consumer.inspect()
    print(f"Processed up to: {inspect_result.state.processed_ts}")
    print(f"Pending entries: {len(inspect_result.state.pending_entries)}")

    consumer.close()
    q.close()
```

**Key points**:
- `push()` auto-increments timestamp and notifies waiting consumers
- Multiple consumer groups share the same queue but maintain independent progress
- `poll()` blocks until data is available or timeout expires
- Always `ack()` after processing to advance the consumer's position

---

## Scenario 4: Journal Queue (Change Data Capture)

**When**: You need to consume all changes across all datasets for replication or auditing.

```python
import timslite

with timslite.Store.open("/data/cdc", timslite.StoreConfig(enable_journal=True)) as store:
    # Create datasets with journal enabled
    store.create_dataset("users", "profiles", enable_journal=True)
    store.create_dataset("orders", "transactions", enable_journal=True)

    users = store.open_dataset("users", "profiles")
    orders = store.open_dataset("orders", "transactions")

    # Write data
    users.write(1, b'{"name": "Alice"}')
    orders.write(1, b'{"item": "widget", "qty": 5}')
    users.write(2, b'{"name": "Bob"}')

    # Open journal queue
    jq = store.open_journal_queue()
    consumer = jq.open_consumer("replicator")

    # Consume journal entries
    for _ in range(3):
        result = consumer.poll(5000)
        if result:
            seq, payload = result
            print(f"Journal seq={seq}, payload_len={len(payload)}")
            consumer.ack(seq)

    consumer.close()
    jq.close()
```

**Key points**:
- Journal captures all write operations across datasets
- Journal sequence is a monotonically increasing `i64`, not a wall-clock timestamp
- Each journal record encodes the operation type and dataset identifier

---

## Scenario 5: Append (Building Records Incrementally)

**When**: You're building a record piece by piece over time.

```python
import timslite

with timslite.Store.open("/data/append_demo") as store:
    store.create_dataset("logs", "entries")
    ds = store.open_dataset("logs", "entries")

    # Forward append: creates a new record at ts=100
    ds.append(100, b"line 1\n")

    # In-place append: appends to existing record at ts=100
    # Only works if the record is still in the uncompressed pending block
    ds.append(100, b"line 2\n")
    ds.append(100, b"line 3\n")

    # Read the complete record
    record = ds.read(100)
    if record:
        ts, data = record
        print(data.decode())
        # Output:
        # line 1
        # line 2
        # line 3

    # append_now: uses current Unix timestamp
    ds.append_now(b"current time log entry\n")
```

**Key points**:
- `append(ts, data)` with `ts > latest` creates a new record
- `append(ts, data)` with `ts == latest` appends to the existing uncompressed record
- In-place append only works while the record is in the pending (uncompressed) block
- Max combined record size: 4 MiB

---

## Scenario 6: Query Length (Header-Only Scan)

**When**: You need to know record sizes without reading full data.

```python
import timslite

with timslite.Store.open("/data/length_demo") as store:
    store.create_dataset("metrics", "cpu")
    ds = store.open_dataset("metrics", "cpu")

    # Write some records
    for i in range(1, 101):
        ds.write(i, f"value_{i}".encode())

    # Query lengths (reads only 12-byte headers, not full data)
    for ts, length in ds.query_length(1, 100):
        print(f"ts={ts}: {length} bytes")

    # Or use the eager version
    lengths = ds.query_length_all(1, 100)
    total = sum(length for _, length in lengths)
    print(f"Total data size: {total} bytes")

    # Check if a record exists without reading data
    exists = ds.read_exist(50)  # True or False
    print(f"Record 50 exists: {exists}")

    # Get just the length of a single record
    length = ds.read_length(50)
    print(f"Record 50 length: {length} bytes")
```

**Key points**:
- `query_length()` reads only the 12-byte record header per entry
- `read_exist()` checks index only, no data segment I/O
- `read_length()` reads only the header for a single record

---

## Scenario 7: QueryIterator Control

**When**: You need fine-grained control over iteration (reverse, skip, collect).

```python
import timslite

with timslite.Store.open("/data/iter_demo") as store:
    store.create_dataset("events", "log")
    ds = store.open_dataset("events", "log")

    # Write some records
    for i in range(1, 101):
        ds.write(i, f"event_{i}".encode())

    # Iterate forward (default)
    for ts, data in ds.query(1, 100):
        print(f"Forward: ts={ts}")

    # Iterate in reverse
    iter = ds.query(1, 100)
    iter.reverse()
    for ts, data in iter:
        print(f"Reverse: ts={ts}")

    # Skip the first 10 records
    iter = ds.query(1, 100)
    iter.skip(10)
    for ts, data in iter:
        print(f"After skip: ts={ts}")

    # Collect up to 5 records
    iter = ds.query(1, 100)
    records = iter.collect_take(5)
    print(f"First 5 records: {records}")

    # Collect all remaining records
    iter = ds.query(1, 100)
    all_records = iter.collect_all()
    print(f"Total records: {len(all_records)}")
```

**Key points**:
- `reverse()` changes iteration direction
- `skip(count)` skips the first `count` records
- `collect_take(n)` collects up to `n` records
- `collect_all()` collects all remaining records into a list

---

## Scenario 8: Inspection and Monitoring

**When**: You need to inspect dataset configuration and runtime state.

```python
import timslite

with timslite.Store.open("/data/inspect_demo") as store:
    store.create_dataset("sensor", "temp",
        compress_level=9,
        index_continuous=True,
    )
    ds = store.open_dataset("sensor", "temp")

    # Write some data
    for i in range(1, 101):
        ds.write(i, f"temp={i * 0.1:.1f}".encode())

    # Inspect dataset
    result = ds.inspect()
    info = result.info
    state = result.state

    print(f"Name: {info.name}")
    print(f"Type: {info.dataset_type}")
    print(f"Identifier: {info.identifier}")
    print(f"Compression: type={info.compress_type}, level={info.compress_level}")
    print(f"Index mode: {'continuous' if info.index_continuous else 'sparse'}")
    print(f"Retention window: {info.retention_window}")
    print(f"Latest timestamp: {state.latest_written_timestamp}")
    print(f"Data segments: {state.data_segments}")
    print(f"Total records: {state.total_record_count}")
    print(f"Total data size: {state.total_data_size} bytes")
    print(f"Total original size: {state.total_original_size} bytes")

    # List all datasets
    names = store.get_dataset_names()
    print(f"Dataset names: {names}")

    types = store.get_dataset_types("sensor")
    print(f"Types for 'sensor': {types}")

    # Inspect from store
    store_result = store.inspect_dataset("sensor", "temp")
    print(f"Store inspect - latest: {store_result.state.latest_written_timestamp}")
```

**Key points**:
- `inspect()` returns `DataSetInspectResult` with `info` (config) and `state` (runtime)
- `get_dataset_names()` lists all dataset names
- `get_dataset_types(name)` lists all types for a given name

---

## Scenario 9: Retention Window

**When**: You want automatic expiry of old data.

```python
import timslite

with timslite.Store.open("/data/retention_demo") as store:
    # Create dataset with 1-day retention (in timestamp units)
    ds = store.create_dataset("sensor", "temp",
        retention_window=86400,  # 86400 seconds = 1 day
    )

    # Write data
    ds.write(1000, b"old_data")
    ds.write(100000, b"new_data")

    # Reading expired timestamp returns None
    record = ds.read(1000)  # May return None if expired
    record = ds.read(100000)  # Returns (100000, b"new_data")

    # Retention is enforced at read time and during background reclamation
```

**Key points**:
- `retention_window` uses the same unit as the dataset timestamp
- `retention_window=0` means no limit
- Expired records return `None` on read
- Expired timestamps cannot be deleted, rewritten, or corrected
- Reclamation runs in background (or via `tick_background_tasks()`)

---

## Scenario 10: Error Handling

**When**: You need to handle specific error conditions.

```python
import timslite

try:
    with timslite.Store.open("/data/error_demo") as store:
        # Create dataset
        store.create_dataset("test", "data")
        ds = store.open_dataset("test", "data")

        # Try to create duplicate
        try:
            store.create_dataset("test", "data")
        except timslite.TmslAlreadyExistsError:
            print("Dataset already exists")

        # Try to open non-existent dataset
        try:
            store.open_dataset("nonexistent", "data")
        except timslite.TmslNotFoundError:
            print("Dataset not found")

        # Try to write oversized record
        try:
            ds.write(1, b"x" * (5 * 1024 * 1024))  # 5 MiB > 4 MiB limit
        except timslite.TmslInvalidDataError:
            print("Record too large")

        # Try to write out-of-order (sparse mode)
        ds.write(100, b"data_100")
        try:
            ds.write(50, b"data_50")  # 50 < 100
        except timslite.TmslInvalidDataError:
            print("Out-of-order write not allowed in sparse mode")

except timslite.TmslError as e:
    print(f"General timslite error: {e}")
```

**Key points**:
- All specific errors inherit from `TmslError`
- Catch specific errors first, then fall back to `TmslError`
- Error hierarchy: `TmslError` → `TmslNotFoundError`, `TmslAlreadyExistsError`, `TmslInvalidDataError`, etc.
