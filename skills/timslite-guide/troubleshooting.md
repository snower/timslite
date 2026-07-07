# timslite Troubleshooting

> Common errors, root causes, and solutions. Organized by symptom.

---

## Write Errors

### `InvalidData`: timestamp must be >= latest_written_timestamp

**Cause**: In sparse mode, you tried to write a timestamp older than the latest written timestamp, and the timestamp has no existing index entry.

**Solution**:
- If you need to update an existing timestamp → use `write()` with the same timestamp (correction write) or `append()`
- If the timestamp already has an index entry → `write()` performs an out-of-order rewrite (sparse mode only)
- If you need backward writes in continuous mode → not supported; redesign your write path

```rust
// WRONG: writing backward to a non-existent timestamp
ds.write(5, b"data")?;
ds.write(3, b"data")?;  // ERROR if ts=3 has no index entry

// CORRECT: correction write (same timestamp)
ds.write(5, b"wrong")?;
ds.write(5, b"correct")?;  // OK, corrects the latest record

// CORRECT: out-of-order rewrite (timestamp already exists)
ds.write(1, b"a")?;
ds.write(2, b"b")?;
ds.write(1, b"updated")?;  // OK, rewrites timestamp 1
```

### `InvalidData`: record data length exceeds 4 MiB

**Cause**: The data payload exceeds the 4 MiB resource limit.

**Solution**:
- Split the data into multiple records at different timestamps
- Store large blobs externally and keep only references in timslite
- For append: ensure `old_data_len + append_len <= 4 MiB`

### `SegmentFull`: segment is full

**Cause**: A data or index segment has reached its maximum capacity and cannot expand further.

**Solution**:
- This is normally handled automatically by creating a new segment
- If it persists, check that `data_segment_size` and `index_segment_size` are reasonable for your data volume
- Increase `data_segment_size` when creating the dataset

### `InvalidData`: compress_level must be <= 9

**Cause**: Compression level was set above 9.

**Solution**: Use 0-9. Level 6 is the default and recommended for most cases. Level 9 is slowest but best compression.

---

## Read Errors

### `read(ts)` returns `None` but data was written

**Possible causes**:

1. **Timestamp was deleted**: `delete(ts)` removes the record. `latest_written_timestamp` is NOT retroactively updated.
2. **Timestamp is expired**: If `retention_window > 0` and `ts < latest_written_timestamp - retention_window`, the record is expired.
3. **Timestamp is a filler** (continuous mode): Missing timestamps in a materialized segment are filler entries. `read()` returns `None` for fillers.
4. **Wrong timestamp**: `read(-1)` reads timestamp `-1`, NOT the latest. Use `read_latest()` for the latest.

**Diagnosis**:
```rust
// Check if the record ever existed
if ds.read_exist(ts)? {
    // Index says it exists, but data is gone — may be deleted or corrupted
} else {
    // No index entry — never written or fully reclaimed
}

// Check the latest timestamp
println!("Latest: {:?}", ds.latest_timestamp()?);

// Inspect the dataset for details
let inspect = ds.inspect()?;
println!("Records: {}", inspect.state.total_record_count);
println!("Invalid: {}", inspect.state.total_invalid_record_count);
```

### `read_latest()` returns `None` after delete

**Cause**: `read_latest()` reads the record at `latest_written_timestamp`. If that record was deleted, it returns `None` — it does NOT fall back to earlier records.

**Solution**: If you need the latest *valid* record, query backward:
```rust
let latest_ts = ds.latest_timestamp()?;
if let Some(mut ts) = latest_ts {
    while ts > 0 {
        if let Some((_, data)) = ds.read(ts)? {
            // Found the latest valid record
            break;
        }
        ts -= 1;
    }
}
```

---

## Queue Errors

### `QueueAlreadyOpen`: queue is already open

**Cause**: Each dataset can have only one `DatasetQueue` open at a time.

**Solution**: Close the existing queue before opening a new one, or share the same `DatasetQueue` handle (it's `Arc`-based and clone-safe).

### `ConsumerGroupExists`: consumer group already open with different config

**Cause**: You tried to open a consumer group with a different config than it was originally opened with.

**Solution**: Use the same config, or drop and recreate the consumer group:
```rust
queue.drop_consumer("my_group")?;
let consumer = queue.open_consumer_with_config("my_group", new_config)?;
```

### New consumer doesn't receive existing data

**Cause**: New consumers initialize from the current `latest_written_timestamp`. They only consume data pushed *after* they were opened.

**Solution**: Open the consumer before pushing data:
```rust
// CORRECT: open consumer first, then push
let consumer = queue.open_consumer("worker")?;
queue.push(b"data")?;  // consumer will receive this

// WRONG: push first, then open consumer
queue.push(b"data")?;  // consumer won't see this
let consumer = queue.open_consumer("worker")?;  // starts from here
```

### `QueueClosed`: poll on closed queue

**Cause**: The queue was closed while a consumer was polling.

**Solution**: Ensure the queue stays open for the lifetime of all consumers. Drop consumers before closing the queue.

### `PendingFull` / `TmslPendingFullError`: too many pending entries

**Cause**: The consumer has too many unacked records pending. The pending list is stored in a 4KB mmap state file with limited capacity.

**Solution**:
- Ack records promptly after processing
- Increase `running_expired_seconds` so pending entries expire and become redeliverable sooner
- Reduce `max_retry_count` to drop failed entries faster
- Process in smaller batches

---

## Journal Errors

### `NotFound`: journal is not enabled

**Cause**: `StoreConfig.enable_journal = false` or the `.journal/logs` directory doesn't exist.

**Solution**: Enable journal in the store config:
```rust
let config = StoreConfig::builder()
    .enable_journal(true)
    .build();
```

### Journal queue not supported in read-only mode

**Cause**: `open_journal_queue()` is not supported when the store is in read-only mode.

**Solution**: Journal queue requires a writable store. For read-only journal access, use `journal_read()` and `journal_query()` instead.

### Journal record references data that no longer exists

**Cause**: The source dataset was dropped, or the record was reclaimed by retention.

**Solution**: Journal is a pointer-based log, not a WAL. Consumers must handle missing source data gracefully:
```rust
match store.read_journal_source_record(identifier, index_info)? {
    Some((ts, data)) => { /* process data */ }
    None => { /* source gone — skip or log warning */ }
}
```

---

## Store / Lifecycle Errors

### Store open fails with lock error

**Cause**: Another process has the `.lock` file in the data directory.

**Solutions**:
- Use read-only mode: `StoreConfig::builder().read_only(Some(true)).build()`
- Wait for the other process to close
- Use auto mode (default): falls back to read-only automatically

### `RuntimeError: store is already closed` (Python)

**Cause**: You tried to use a store after calling `close()`.

**Solution**: Use the context manager pattern or ensure the store stays open:
```python
# CORRECT: context manager
with timslite.Store.open(path) as store:
    store.create_dataset("ds", "type")
    # store is automatically closed on exit

# WRONG: using after close
store = timslite.Store.open(path)
store.close()
store.create_dataset("ds", "type")  # RuntimeError
```

### Close fails: "dataset/queue/iterator still open"

**Cause**: Not all handles created from the store have been released.

**Solution**: Drop/close all datasets, iterators, queues, and consumers before closing the store.

---

## Background Task Issues

### Data not persisted after process crash

**Cause**: In-memory changes weren't flushed to disk before the crash.

**Solutions**:
- Call `ds.flush()` after critical writes
- Use the default background thread (`enable_background_thread = true`) — it flushes every 15 seconds
- Call `store.close()` for a clean shutdown
- Accept that crash recovery may lose the last few seconds of unflushed data

### Manual background mode: tasks not running

**Cause**: `enable_background_thread = false` but `tick_background_tasks()` is not called.

**Solution**: Call `tick_background_tasks()` periodically:
```rust
// In your event loop
loop {
    let tick = store.tick_background_tasks()?;
    std::thread::sleep(tick.next_delay);
}
```

### Read-only store: background APIs unavailable

**Cause**: Read-only stores don't create `BackgroundTasks`, even when `enable_background_thread = true`.

**Solution**: Read-only stores don't need background tasks (no dirty data to flush). Retention reclaim is rejected through the read-only dataset context.

---

## Performance Issues

### Query is slow for large ranges

**Cause**: `query()` loads all matching records into memory at once.

**Solution**: Use the lazy iterator:
```rust
// Instead of:
let entries = ds.query(1, 1_000_000)?;  // loads everything

// Use:
let iter = ds.query_iter(1, 1_000_000)?;  // lazy
for entry in iter {
    let (ts, data) = entry?;
    // process one at a time
}
```

For existence-only checks, use `query_exist()` or `read_exist()` — they skip data I/O entirely.

### High memory usage

**Possible causes**:
1. **Block cache too large**: Reduce `cache_max_memory` or set to `0` to disable
2. **Large queries**: Use `query_iter()` instead of `query()`
3. **Many open segments**: Reduce `idle_timeout` to close segments sooner
4. **Large records**: Each record can be up to 4 MiB

### Disk space not reclaimed after retention

**Cause**: Retention reclaim only runs daily at `retention_check_hour` (default UTC 00:00). Also, reclaim only deletes entire expired segments.

**Solutions**:
- Manually trigger: `store.tick_background_tasks()`
- Change `retention_check_hour` to run more frequently (it's still daily, just at a different hour)
- Ensure the background thread is running or call `tick_background_tasks()` manually

---

## Configuration Issues

### `InvalidData`: initial_data_segment_size must be <= data_segment_size

**Cause**: The lazy allocation initial size is larger than the max segment size.

**Solution**: Ensure `initial_*_segment_size <= *_segment_size`. The initial size is the starting file size; the segment grows to the max size via 2x expansion.

### `InvalidData`: dataset name must match ^[0-9A-Za-z_-]+$

**Cause**: The name contains invalid characters (spaces, dots, slashes, etc.).

**Solution**: Use only alphanumeric, hyphen, and underscore. Max 255 bytes. The reserved name `.journal` is rejected.

### Compression type mismatch on reopen

**Cause**: The dataset was created with one `compress_type` but you're trying to reopen with different store defaults.

**Solution**: `open_dataset` reads parameters from the `meta` file, not from store config. The store's `compress_type` default only applies to *new* datasets. Existing datasets always use their original compression setting.

---

## Crash Recovery

### Process crashed mid-write

**What happens**: 
- Pending block state is persisted in the data segment header
- On reopen, the last block is restored as pending (raw, mutable)
- No data corruption — mmap writes are page-aligned
- Unflushed data may be lost (last `flush_interval` worth of writes)

**Recovery**: Just reopen the store. The pending block resumes from its persisted state.

### Process crashed mid-expansion

**What happens**:
- If crash before `set_len`: reopen sees old file size
- If crash after `set_len`: reopen sees expanded file size, new region follows OS filesystem semantics
- Header meta always records `max_size`, not `initial/current` size

**Recovery**: The segment resumes with the physical file size it had at crash time. Next expansion continues from there.

### Power loss

**What happens**: 
- mmap pages may not be fully synced
- `flush()` calls `MS_SYNC` which forces writeback
- Without explicit flush, OS flushes at its discretion

**Recovery**: Reopen the store. Data up to the last OS flush is preserved. Pending blocks are recoverable from their persisted state. The last few seconds of writes may be lost.
