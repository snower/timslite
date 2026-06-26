---
name: timslite-guide-java
description: Java guide for timslite time-series storage library - installation, quick start, API reference, and examples
---

# timslite Java Guide

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.snower</groupId>
    <artifactId>timslite</artifactId>
    <version>0.1.1</version>
</dependency>
```

### Native Library

The wrapper uses UniFFI 0.31 with a Kotlin/JVM backend and JNA for native library loading. The JAR includes native libraries for all supported platforms under `META-INF/native/<platform>/` with standard OS library names:

- `META-INF/native/macos-x86_64/libtimslite_java.dylib`
- `META-INF/native/macos-aarch64/libtimslite_java.dylib`
- `META-INF/native/linux-x86_64/libtimslite_java.so`
- `META-INF/native/linux-aarch64/libtimslite_java.so`
- `META-INF/native/windows-x86_64/timslite_java.dll`
- `META-INF/native/windows-aarch64/timslite_java.dll`

The `NativeLibraryLoader` automatically detects the current OS/architecture and loads the correct library. No additional configuration is needed.

### Building from Source

```bash
# Build Rust cdylib and Kotlin bindings, compile, test
mvn clean verify

# Generate Javadoc
mvn javadoc:javadoc
```

## Quick Start

### Opening a Store

```java
import io.github.snower.timslite.*;

StoreConfig config = StoreConfigBuilder.builder()
        .enableJournal(true)
        .enableBackgroundThread(true)
        .build();

try (Store store = Store.open("/path/to/data", config)) {
    // use the store
}
```

### Creating a Dataset

```java
CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(DatasetConfigBuilder.builder()
                .indexContinuous((byte) 0)
                .retentionWindow(0)
                .build())
        .build();

store.createDataset("metrics", "cpu", options);
```

### Writing and Reading Data

```java
try (Dataset ds = store.openDataset("metrics", "cpu")) {
    // Write a record
    ds.write(1700000000L, new byte[]{1, 2, 3});

    // Read by timestamp
    Record rec = ds.read(1700000000L);
    if (rec != null) {
        byte[] data = rec.getData();
        long ts = rec.getTimestamp();
    }

    // Read the latest record
    Record latest = ds.readLatest();

    // Append to the latest record (must be >= latest timestamp)
    ds.append(1700000000L, new byte[]{4, 5});

    // Delete a record
    ds.delete(1700000000L);
}
```

## Documentation

- **[API Reference](api-reference.md)** — Complete Java API signatures, parameters, return types, and semantics
- **[Examples](examples.md)** — Feature scenarios with copy-paste Java examples

## Key Concepts

### Store and DataSet

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `DataSet` handles per-dataset read/write operations, segment management, and indexing
- Each `(dataset_name, dataset_type)` pair is an independent dataset with its own segments

### Timestamps

- All timestamps are `long` values (signed 64-bit integers)
- In sparse mode: timestamps must be `>= latest_written_timestamp`
- In continuous mode: timestamps must be `>= latest_written_timestamp`
- Use `readLatest()` to get the most recent record
- Use `read(-1L)` to read the record at timestamp `-1` (not the latest)

### Blocks and Compression

- Records are aggregated into blocks (max 64KB payload)
- Blocks are lazily compressed on seal (zstd or deflate)
- Large records (>64KB) get their own single-record block
- Max record size: 4 MiB

### AutoCloseable Pattern

All lifecycle types implement `AutoCloseable`: `Store`, `Dataset`, `Queue`, `QueueConsumer`, `JournalQueue`, `JournalQueueConsumer`, `QueryIterator`, `QueryLengthIterator`. Use try-with-resources to guarantee cleanup.

## Configuration

### StoreConfig

```java
StoreConfig config = StoreConfigBuilder.builder()
        .flushIntervalMs(15000)           // 15 seconds
        .idleTimeoutMs(1800000)           // 30 minutes
        .dataSegmentSize(64 * 1024 * 1024)  // 64 MiB
        .indexSegmentSize(4 * 1024 * 1024)   // 4 MiB
        .compressLevel(6)                  // 0-9
        .cacheMaxMemory(256 * 1024 * 1024)  // 256 MiB
        .enableBackgroundThread(true)
        .enableJournal(true)
        .readOnly(null)                    // null=auto, true=force RO, false=require writable
        .build();
```

### CreateDatasetOptions

```java
CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(DatasetConfigBuilder.builder()
                .dataSegmentSize(128 * 1024 * 1024)  // 128 MiB
                .indexSegmentSize(8 * 1024 * 1024)    // 8 MiB
                .compressLevel(9)                      // max compression
                .indexContinuous((byte) 1)             // continuous mode
                .retentionWindow(86400)                // 1 day in timestamp units
                .enableJournal(true)
                .build())
        .build();
```

## Common Patterns

### Batch Writes

```java
for (long i = 1; i <= 1000; i++) {
    byte[] data = String.format("{\"value\": %d}", i).getBytes();
    ds.write(i, data);
}
```

### Range Queries

```java
// Eager query (list)
List<Record> records = ds.query(100L, 200L);
for (Record rec : records) {
    System.out.println(rec.getTimestamp() + ": " + new String(rec.getData()));
}

// Lazy query (iterator)
try (QueryIterator it = ds.queryIter(100L, 200L)) {
    while (it.hasNext()) {
        Record rec = it.next();
        System.out.println(rec.getTimestamp() + ": " + new String(rec.getData()));
    }
}
```

### Queue Consumption

```java
try (Dataset ds = store.openDataset("tasks", "jobs");
     Queue queue = store.openQueue(ds)) {

    // Push data (auto-assigns next timestamp)
    long ts = queue.push("task_payload".getBytes());

    // Open a consumer group
    try (QueueConsumer consumer = queue.openConsumer("worker_group")) {
        // Poll for records (with timeout in ms)
        Record rec = consumer.poll(5000);
        if (rec != null) {
            System.out.println("Got task: " + new String(rec.getData()));
            // Acknowledge processing
            consumer.ack(rec.getTimestamp());
        }
    }
}
```

### Journal Consumption

```java
try (JournalQueue jq = store.openJournalQueue();
     JournalQueueConsumer consumer = jq.openConsumer("sync_worker")) {

    JournalRecord rec = consumer.poll(100);  // timeout in ms
    if (rec != null) {
        System.out.println("Journal seq: " + rec.getSequence());
        consumer.ack(rec.getSequence());
    }
}
```

## Error Handling

All errors are thrown as subclasses of `TmslException`, which extends `RuntimeException`.

```java
import io.github.snower.timslite.errors.*;

try {
    ds.read(1700000000L);
} catch (ExpiredException e) {
    // timestamp outside retention window
} catch (NotFoundException e) {
    // record not found
} catch (TmslException e) {
    // handle other errors
}
```

See [Troubleshooting](../troubleshooting.md) for detailed error solutions.