# Java Examples

> Feature scenarios with copy-paste Java examples.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```java
import io.github.snower.timslite.*;

StoreConfig config = StoreConfigBuilder.builder().build();

try (Store store = Store.open("/data/sensors", config)) {
    // Create a dataset for sensor readings (sparse mode for irregular timestamps)
    CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
            .config(DatasetConfigBuilder.builder()
                    .indexContinuous((byte) 0)  // sparse index mode
                    .retentionWindow(0)          // no retention
                    .build())
            .build();

    store.createDataset("temp_sensor", "readings", options);

    try (Dataset ds = store.openDataset("temp_sensor", "readings")) {
        // Write sensor readings (timestamps must be monotonically increasing)
        for (long i = 1; i <= 1000; i++) {
            String data = String.format("{\"temp\": %.1f, \"ts\": %d}", 20.0 + i * 0.1, i);
            ds.write(i, data.getBytes());
        }

        // Query a range
        for (Record rec : ds.query(100, 200)) {
            System.out.println("ts=" + rec.getTimestamp() + ": " + new String(rec.getData()));
        }

        // Read a specific timestamp
        Record rec = ds.read(150);
        if (rec != null) {
            System.out.println("Record at " + rec.getTimestamp() + ": " + new String(rec.getData()));
        }

        // Read the latest record
        Record latest = ds.readLatest();
        if (latest != null) {
            System.out.println("Latest at " + latest.getTimestamp() + ": " + new String(latest.getData()));
        }
    }
}
```

**Key points**:
- Timestamps must be `>= latest_written_timestamp` in sparse mode
- `query()` is eager (loads all into memory); use `queryIter()` for large ranges
- `readLatest()` is the only way to get the latest; `read(-1L)` reads timestamp `-1`

---

## Scenario 2: Continuous Mode (Dense Sequential Timestamps)

**When**: Your timestamps are dense sequential integers (e.g., per-second readings with few gaps).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/metrics", StoreConfigBuilder.builder().build())) {
    // Create with indexContinuous = 1
    CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
            .config(DatasetConfigBuilder.builder()
                    .indexContinuous((byte) 1)  // continuous mode
                    .build())
            .build();

    store.createDataset("cpu_usage", "per_second", options);

    try (Dataset ds = store.openDataset("cpu_usage", "per_second")) {
        // Write sequential timestamps (no gaps allowed in continuous mode)
        for (long i = 1; i <= 1000; i++) {
            ds.write(i, String.format("cpu=%.2f", i * 0.01).getBytes());
        }

        // Read by position (O(1) lookup)
        Record rec = ds.read(500);
        if (rec != null) {
            System.out.println("Record 500: " + new String(rec.getData()));
        }

        // Continuous mode auto-fills gaps with null on read
        // If you write ts=1 and ts=100, reading ts=50 returns null (filler)
    }
}
```

**Key points**:
- Continuous mode assumes timestamps are dense sequential integers
- Missing timestamps become filler entries (read returns `null`)
- O(1) timestamp-to-position calculation within a segment

---

## Scenario 3: Queue (FIFO Consumer Groups)

**When**: You need ordered delivery with consumer group semantics (like Kafka consumer groups).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("tasks", "jobs", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("tasks", "jobs");
         Queue queue = store.openQueue(ds)) {

        // Push data (auto-assigns next timestamp)
        long ts1 = queue.push("task_1".getBytes());
        long ts2 = queue.push("task_2".getBytes());

        // Open a consumer group
        try (QueueConsumer consumer = queue.openConsumer("worker_group",
                QueueConsumerOptionsBuilder.builder().build())) {

            // Poll for records (with timeout in ms)
            Record rec = consumer.poll(5000);
            if (rec != null) {
                System.out.println("Got task at " + rec.getTimestamp() + ": " + new String(rec.getData()));
                // Acknowledge processing
                consumer.ack(rec.getTimestamp());
            }
        }
    }
}
```

**Key points**:
- `push` auto-assigns `timestamp = latest_written_timestamp + 1`
- Multiple consumer groups are independent
- Multiple consumers in the same group share progress (mutual exclusion)

---

## Scenario 4: Queue with Retry and Expiry

**When**: You need automatic retry for failed tasks and expiry for stuck tasks.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("retry_queue", "jobs", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("retry_queue", "jobs");
         Queue queue = store.openQueue(ds)) {

        // Push some tasks
        queue.push("important_task".getBytes());

        // Open consumer with retry config
        QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
                .config(QueueConsumerConfigBuilder.builder()
                        .runningExpiredSeconds(60)  // re-deliver after 60s if not acked
                        .maxRetryCount((short) 3)   // drop after 3 retries
                        .build())
                .build();

        try (QueueConsumer consumer = queue.openConsumer("retry_group", options)) {
            // Poll and process
            Record rec = consumer.poll(5000);
            if (rec != null) {
                System.out.println("Processing: " + new String(rec.getData()));
                // If processing fails, don't ack — it will be re-delivered after 60s
                // After 3 failures, the entry is dropped
                consumer.ack(rec.getTimestamp());
            }
        }
    }
}
```

**Key points**:
- `runningExpiredSeconds`: re-deliver pending entries after this timeout
- `maxRetryCount`: drop entries after this many retries (0 = unlimited)

---

## Scenario 5: Journal for Change Tracking / Hot Migration

**When**: You need to track all data changes for audit, sync to another system, or recovery.

```java
import io.github.snower.timslite.*;

StoreConfig config = StoreConfigBuilder.builder()
        .enableJournal(true)
        .build();

try (Store store = Store.open("/data/app", config)) {
    // Journal is enabled by default. Create a dataset.
    store.createDataset("events", "user_actions", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("events", "user_actions")) {
        // Every write/delete/append automatically appends to the journal
        ds.write(1, "user_login".getBytes());
        ds.write(2, "page_view".getBytes());
        ds.delete(1);
    }

    // Read journal records
    Long latest = store.journalLatestSequence();  // e.g., 4 — create + 2 writes + delete
    System.out.println("Latest journal seq: " + latest);

    // Read individual journal record
    JournalRecord rec = store.journalRead(1);
    if (rec != null) {
        System.out.println("Journal record " + rec.getSequence() + ": " + rec.getData().length + " bytes");
    }

    // Query a range of journal records
    for (JournalRecord entry : store.journalQuery(1, latest)) {
        System.out.println("Seq " + entry.getSequence() + ": " + entry.getData().length + " bytes");
    }

    // Consume journal via queue (for downstream sync)
    try (JournalQueue jq = store.openJournalQueue();
         JournalQueueConsumer consumer = jq.openConsumer("sync_worker")) {

        JournalRecord journalRec = consumer.poll(100);
        if (journalRec != null) {
            System.out.println("Consumed journal seq " + journalRec.getSequence());
            // Use store.readJournalSourceRecord() to fetch the actual business data
            consumer.ack(journalRec.getSequence());
        }
    }
}
```

**Important journal semantics**:
- Journal is NOT a WAL — no transaction guarantees
- Journal records do NOT contain business payload; they reference source data via `index_info`
- Use `store.readJournalSourceRecord(dataset_identifier, index_info)` to dereference
- Journal append failure does NOT roll back the main operation
- Disable per-dataset with `DatasetConfigBuilder.enableJournal(false)`

---

## Scenario 6: Retention Window (Time-Based Data Expiry)

**When**: You want old data to automatically expire and be reclaimed.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    // Create dataset with 1-day retention (in timestamp units)
    CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
            .config(DatasetConfigBuilder.builder()
                    .retentionWindow(86400)  // 86400 seconds = 1 day
                    .enableJournal(true)
                    .build())
            .build();

    store.createDataset("metrics", "per_second", options);

    try (Dataset ds = store.openDataset("metrics", "per_second")) {
        // Write data with timestamps
        for (long i = 1; i <= 1000; i++) {
            ds.write(i, ("value=" + i).getBytes());
        }

        // Read old data (may be expired if retentionWindow > 0)
        Record rec = ds.read(1);
        if (rec != null) {
            System.out.println("Old record: ts=" + rec.getTimestamp());
        }

        // Expired records return null on read
        // Reclaim happens during background tasks (daily at retentionCheckHour UTC)
    }
}
```

**Key points**:
- `retentionWindow` uses the same units as dataset timestamps
- `retentionWindow = 0` means no limit
- `retentionCheckHour` is UTC hour (0-23) for daily reclaim
- Expired records return `null` on read
- Reclaim deletes entire segments when all records are expired

---

## Scenario 7: Read-Only Mode

**When**: You want multiple processes to read the same store, or need to inspect data safely.

```java
import io.github.snower.timslite.*;

// Force read-only mode
StoreConfig config = StoreConfigBuilder.builder()
        .readOnly(true)
        .build();

try (Store store = Store.open("/data/app", config)) {
    // Read operations work normally
    try (Dataset ds = store.openDataset("metrics", "cpu")) {
        for (Record rec : ds.query(1, 100)) {
            System.out.println(rec.getTimestamp() + ": " + new String(rec.getData()));
        }

        DataSetInspectResult info = ds.inspect();
        System.out.println("Dataset: " + info.getState().getTotalRecordCount() + " records");
    }

    // Cannot write, create, drop, or open queues in read-only mode
    // ds.write(1, "data".getBytes()) would throw ReadOnlyException
}
```

**Auto read-only mode** (default):
- If `.lock` can be acquired → writable
- If `.lock` is already held → falls back to read-only
- Use `readOnly(false)` to require writable (fail if locked)

---

## Scenario 8: Append (In-Place Tail Growth)

**When**: You want to append data to the latest record without creating a new timestamp.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/logs", StoreConfigBuilder.builder().build())) {
    store.createDataset("app_log", "lines", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("app_log", "lines")) {
        // Create initial record at timestamp 1
        ds.append(1, "line1\n".getBytes());

        // Append more data to the same timestamp (in-place tail growth)
        ds.append(1, "line2\n".getBytes());
        ds.append(1, "line3\n".getBytes());

        // Read the combined record
        Record rec = ds.read(1);
        assert new String(rec.getData()).equals("line1\nline2\nline3\n");

        // Forward append creates a new record
        ds.append(2, "new_record".getBytes());
    }
}
```

**Append rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + data.length <= 4 MiB`
- Appended data must fit within the current pending block's capacity
- Does NOT re-notify queue when appending to existing record

---

## Scenario 9: Correction Write (Fix Latest Record)

**When**: You wrote data and need to correct the latest record's content.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("events", "actions", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("events", "actions")) {
        // Write a record
        ds.write(100, "wrong_data".getBytes());

        // Correct it by writing to the same timestamp
        // (only works if latest_written_timestamp == 100 and the record is in a pending raw block)
        ds.write(100, "corrected_data".getBytes());

        // If the block has already been sealed/compressed, the correction
        // automatically falls back to an "update write": new data is appended
        // to the latest data segment, the index is updated, and the old
        // record's segment gets invalidRecordCount incremented.
    }
}
```

**Correction behavior**:
- Triggers when `timestamp == latest_written_timestamp`
- If latest record is in pending raw block → in-place correction
- If latest record is in sealed/compressed block → update write (new data + index update)
- Cache invalidation occurs for affected blocks

---

## Scenario 10: Out-of-Order Write (Sparse Mode)

**When**: You need to update an existing timestamp (not the latest).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("events", "actions", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("events", "actions")) {
        // Write some records
        ds.write(1, "data_1".getBytes());
        ds.write(2, "data_2".getBytes());
        ds.write(3, "data_3".getBytes());

        // Update timestamp 1 (out-of-order write)
        // Only works in sparse mode, and only if timestamp 1 already has an index entry
        ds.write(1, "updated_data_1".getBytes());

        // Out-of-order write to a timestamp with NO index entry → ERROR
        // ds.write(1, "data".getBytes());  // would fail if timestamp 1 didn't exist
    }
}
```

**In continuous mode**: Out-of-order writes are not supported. Timestamps must be `>= latest_written_timestamp`.

---

## Scenario 11: Large Record (Single-Record Block)

**When**: A single record's encoded size exceeds 64KB (the normal block payload limit).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("blobs", "files", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("blobs", "files")) {
        // Records larger than 64KB get their own exclusive block
        // (SINGLE_RECORD flag set, immediately compressed)
        byte[] largeData = new byte[100_000];  // ~100KB
        ds.write(1, largeData);

        // Reading works normally
        Record rec = ds.read(1);
        assert rec.getData().length == 100_000;
    }
}
```

**Rules**:
- Single record max: 4 MiB (`write` and `append` both enforce this)
- Records > 64KB encoded → exclusive single-record block (SINGLE_RECORD | SEALED | COMPRESSED)
- Records ≤ 64KB → aggregated into normal blocks (pending → sealed on overflow)

---

## Scenario 12: Multi-Dataset Isolation

**When**: You have multiple data streams that need independent storage.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    // Each (name, type) pair is an independent dataset
    CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder().build();
    store.createDataset("sensors", "temperature", options);
    store.createDataset("sensors", "humidity", options);
    store.createDataset("sensors", "pressure", options);
    store.createDataset("events", "user_action", options);
    store.createDataset("events", "system", options);

    // List all datasets
    for (String name : store.getDatasetNames()) {
        for (String type : store.getDatasetTypes(name)) {
            System.out.println(name + "/" + type);
        }
    }

    // Open and use each independently
    try (Dataset ds1 = store.openDataset("sensors", "temperature");
         Dataset ds2 = store.openDataset("events", "user_action")) {

        ds1.write(1, "23.5C".getBytes());
        ds2.write(1, "login".getBytes());
    }

    // Inspect a dataset
    DataSetInspectResult inspect = store.inspectDataset("sensors", "temperature");
    System.out.println("Records: " + inspect.getState().getTotalRecordCount());
    System.out.println("Data size: " + inspect.getState().getTotalDataSize() + " bytes");
}
```

**Directory layout**: Each dataset gets its own `{name}/{type}/` directory with independent `meta`, `data/`, `index/`, `state`, and optional `queue/`.

---

## Scenario 13: Efficient Existence Checking

**When**: You need to check if records exist without loading data (e.g., for deduplication or coverage checks).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/app", StoreConfigBuilder.builder().build())) {
    store.createDataset("events", "actions", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("events", "actions")) {
        // Single record existence (index only, no data I/O)
        boolean exists = ds.readExist(12345);

        // Range existence bitmap (index only, no data I/O)
        byte[] bitmap = ds.queryExist(1, 1000);
        // bitmap[i] is set if record at (startTs + i) exists

        // Record length (reads 12-byte header only)
        Long length = ds.readLength(12345);  // Long or null

        // Range lengths (reads headers only)
        List<LengthEntry> lengths = ds.queryLength(1, 1000);
    }
}
```

**Performance hierarchy** (fastest to slowest):
1. `readExist` / `queryExist` — index only, no data segment I/O
2. `readLength` / `queryLength` / `queryLengthIter` — reads 12-byte record header only
3. `read` / `query` / `queryIter` — reads full data