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

        // Check if record exists
        boolean exists = ds.readExist(150);
        System.out.println("Record 150 exists: " + exists);

        // Get record length
        int length = ds.readLength(150);
        System.out.println("Record 150 length: " + length + " bytes");
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
                    .retentionWindow(0)
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

        // Continuous mode auto-fills gaps with None on read
        // If you write ts=1 and ts=100, reading ts=50 returns None (filler)
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

            // Inspect consumer state
            QueueConsumerInspectResult result = consumer.inspect();
            System.out.println("Processed up to: " + result.getState().getProcessedTs());
            System.out.println("Pending: " + result.getState().getPendingEntries().size());
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
                // Process the task
                System.out.println("Processing: " + new String(rec.getData()));

                // If processing succeeds, acknowledge
                consumer.ack(rec.getTimestamp());

                // If processing fails, don't ack — it will be re-delivered after expiry
            }

            // Flush state to disk
            consumer.flush();
        }
    }
}
```

**Key points**:
- `runningExpiredSeconds` controls how long a record can be "in flight" before re-delivery
- `maxRetryCount` limits how many times a record can be re-delivered before being dropped
- Always `ack()` after successful processing

---

## Scenario 5: Journal (Change Data Capture)

**When**: You need to replicate or audit all changes across all datasets.

```java
import io.github.snower.timslite.*;

StoreConfig config = StoreConfigBuilder.builder()
        .enableJournal(true)
        .build();

try (Store store = Store.open("/data/cdc", config)) {
    // Create datasets with journal enabled
    store.createDataset("events", "user_actions",
            CreateDatasetOptionsBuilder.builder()
                    .config(DatasetConfigBuilder.builder()
                            .enableJournal(true)
                            .build())
                    .build());

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
- Use `store.readJournalSourceRecord(datasetIdentifier, indexInfo)` to dereference
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
        // Write data
        ds.write(1000, "old_data".getBytes());
        ds.write(100000, "new_data".getBytes());

        // Reading expired timestamp returns null
        Record old = ds.read(1000);  // May return null if expired
        Record fresh = ds.read(100000);  // Returns (100000, "new_data")

        // Retention is enforced at read time and during background reclamation
    }
}
```

**Key points**:
- `retentionWindow` uses the same unit as the dataset timestamp
- `retentionWindow=0` means no limit
- Expired records return `null` on read
- Expired timestamps cannot be deleted, rewritten, or corrected
- Reclamation runs in background (or via `tickBackgroundTasks()`)

---

## Scenario 7: Query Length (Header-Only Scan)

**When**: You need to know record sizes without reading full data.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/length", StoreConfigBuilder.builder().build())) {
    store.createDataset("metrics", "cpu", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("metrics", "cpu")) {
        // Write some records
        for (long i = 1; i <= 100; i++) {
            ds.write(i, String.format("value_%d", i).getBytes());
        }

        // Query lengths (reads only 12-byte headers, not full data)
        try (QueryLengthIterator it = ds.queryLength(1, 100)) {
            while (it.hasNext()) {
                LengthEntry entry = it.next();
                System.out.println("ts=" + entry.getTimestamp() + ": " + entry.getLength() + " bytes");
            }
        }

        // Or use the eager version
        List<LengthEntry> lengths = ds.queryLengthAll(1, 100);
        int total = 0;
        for (LengthEntry entry : lengths) {
            total += entry.getLength();
        }
        System.out.println("Total data size: " + total + " bytes");

        // Check if a record exists without reading data
        boolean exists = ds.readExist(50);
        System.out.println("Record 50 exists: " + exists);

        // Get just the length of a single record
        int length = ds.readLength(50);
        System.out.println("Record 50 length: " + length + " bytes");
    }
}
```

**Key points**:
- `queryLength()` reads only the 12-byte record header per entry
- `readExist()` checks index only, no data segment I/O
- `readLength()` reads only the header for a single record

---

## Scenario 8: QueryIterator Control

**When**: You need fine-grained control over iteration (reverse, skip).

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/iter", StoreConfigBuilder.builder().build())) {
    store.createDataset("events", "log", CreateDatasetOptionsBuilder.builder().build());

    try (Dataset ds = store.openDataset("events", "log")) {
        // Write some records
        for (long i = 1; i <= 100; i++) {
            ds.write(i, String.format("event_%d", i).getBytes());
        }

        // Iterate forward (default)
        try (QueryIterator it = ds.queryIter(1, 100)) {
            while (it.hasNext()) {
                Record rec = it.next();
                System.out.println("Forward: ts=" + rec.getTimestamp());
            }
        }

        // Iterate in reverse
        try (QueryIterator it = ds.queryIter(1, 100)) {
            it.reverse();
            while (it.hasNext()) {
                Record rec = it.next();
                System.out.println("Reverse: ts=" + rec.getTimestamp());
            }
        }

        // Skip the first 10 records
        try (QueryIterator it = ds.queryIter(1, 100)) {
            it.skip(10);
            while (it.hasNext()) {
                Record rec = it.next();
                System.out.println("After skip: ts=" + rec.getTimestamp());
            }
        }
    }
}
```

**Key points**:
- `reverse()` changes iteration direction
- `skip(count)` skips the first `count` records
- Always use try-with-resources to release native resources

---

## Scenario 9: Inspection and Monitoring

**When**: You need to inspect dataset configuration and runtime state.

```java
import io.github.snower.timslite.*;

try (Store store = Store.open("/data/inspect", StoreConfigBuilder.builder().build())) {
    CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
            .config(DatasetConfigBuilder.builder()
                    .compressLevel((byte) 9)
                    .indexContinuous((byte) 1)
                    .build())
            .build();

    store.createDataset("sensor", "temp", options);

    try (Dataset ds = store.openDataset("sensor", "temp")) {
        // Write some data
        for (long i = 1; i <= 100; i++) {
            ds.write(i, String.format("temp=%.1f", i * 0.1).getBytes());
        }

        // Inspect dataset
        InspectResult result = ds.inspect();
        DatasetInfo info = result.getInfo();
        DatasetState state = result.getState();

        System.out.println("Name: " + info.getName());
        System.out.println("Type: " + info.getDatasetType());
        System.out.println("Identifier: " + info.getIdentifier());
        System.out.println("Compression: type=" + info.getCompressType() + ", level=" + info.getCompressLevel());
        System.out.println("Index mode: " + (info.getIndexContinuous() == 1 ? "continuous" : "sparse"));
        System.out.println("Retention window: " + info.getRetentionWindow());
        System.out.println("Latest timestamp: " + state.getLatestWrittenTimestamp());
        System.out.println("Data segments: " + state.getDataSegments());
        System.out.println("Total records: " + state.getTotalRecordCount());
        System.out.println("Total data size: " + state.getTotalDataSize() + " bytes");
        System.out.println("Total uncompressed size: " + state.getTotalUncompressedSize() + " bytes");
    }

    // List all datasets
    List<String> names = store.getDatasetNames();
    System.out.println("Dataset names: " + names);

    List<String> types = store.getDatasetTypes("sensor");
    System.out.println("Types for 'sensor': " + types);

    // Inspect from store
    InspectResult storeResult = store.inspectDataset("sensor", "temp");
    System.out.println("Store inspect - latest: " + storeResult.getState().getLatestWrittenTimestamp());
}
```

**Key points**:
- `inspect()` returns `InspectResult` with `info` (config) and `state` (runtime)
- `getDatasetNames()` lists all dataset names
- `getDatasetTypes(name)` lists all types for a given name

---

## Scenario 10: Background Tasks

**When**: You need to manually control background tasks (flush, idle-close, cache eviction, retention reclaim).

```java
import io.github.snower.timslite.*;

// Disable automatic background thread
StoreConfig config = StoreConfigBuilder.builder()
        .enableBackgroundThread(false)
        .build();

try (Store store = Store.open("/data/bg", config)) {
    // ... write data ...

    // Manually trigger background tasks
    TickResult result = store.tickBackgroundTasks();
    System.out.println("Executed " + result.getExecutedTasks() + " tasks");
    System.out.println("Next delay: " + result.getNextDelayMs() + " ms");

    // Sleep for the recommended delay
    Thread.sleep(result.getNextDelayMs());

    // Tick again
    result = store.tickBackgroundTasks();
}
```

**Key points**:
- Use `enableBackgroundThread(false)` to disable automatic background tasks
- `tickBackgroundTasks()` returns the number of tasks executed and recommended delay
- Use the recommended delay to avoid busy-waiting

---

## Scenario 11: Error Handling

**When**: You need to handle specific error conditions.

```java
import io.github.snower.timslite.*;
import io.github.snower.timslite.errors.*;

try {
    try (Store store = Store.open("/data/error", StoreConfigBuilder.builder().build())) {
        // Create dataset
        store.createDataset("test", "data", CreateDatasetOptionsBuilder.builder().build());

        try (Dataset ds = store.openDataset("test", "data")) {
            // Try to create duplicate
            try {
                store.createDataset("test", "data", CreateDatasetOptionsBuilder.builder().build());
            } catch (AlreadyExistsException e) {
                System.out.println("Dataset already exists");
            }

            // Try to open non-existent dataset
            try {
                store.openDataset("nonexistent", "data");
            } catch (NotFoundException e) {
                System.out.println("Dataset not found");
            }

            // Try to write oversized record
            try {
                ds.write(1, new byte[5 * 1024 * 1024]);  // 5 MiB > 4 MiB limit
            } catch (InvalidDataException e) {
                System.out.println("Record too large");
            }

            // Try to write out-of-order (sparse mode)
            ds.write(100, "data_100".getBytes());
            try {
                ds.write(50, "data_50".getBytes());  // 50 < 100
            } catch (InvalidDataException e) {
                System.out.println("Out-of-order write not allowed in sparse mode");
            }
        }
    }
} catch (TmslException e) {
    System.out.println("General timslite error: " + e.code() + " - " + e.getMessage());
}
```

**Key points**:
- All specific errors inherit from `TmslException`
- Catch specific errors first, then fall back to `TmslException`
- Use `e.code()` to inspect the error category without catching individual subclasses
