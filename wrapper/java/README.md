# timslite Java Wrapper

Java/Kotlin UniFFI bindings for the [timslite](https://github.com/snower/timslite) time-series storage engine.

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.snower</groupId>
    <artifactId>timslite</artifactId>
    <version>0.1.3</version>
</dependency>
```

### Native Library

The wrapper uses UniFFI 0.31 with a Kotlin/JVM backend and JNA for native library loading. The JAR includes native libraries for all supported platforms under `META-INF/native/<platform>/` with standard OS library names:

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

### Querying Data with Range

```java
try (Dataset ds = store.openDataset("metrics", "cpu")) {
    List<Record> records = ds.query(1700000000L, 1700003600L);
    for (Record rec : records) {
        System.out.println(rec.getTimestamp() + ": " + rec.getData().length + " bytes");
    }
}
```

### Using Query Iterators

For large result sets, use iterators to avoid loading everything into memory:

```java
try (Dataset ds = store.openDataset("metrics", "cpu");
     QueryIterator it = ds.queryIter(1700000000L, 1700003600L)) {
    while (it.hasNext()) {
        Record rec = it.next();
        // process record
    }
}
```

Length-only iteration:

```java
try (QueryLengthIterator it = ds.queryLengthIter(startTs, endTs)) {
    while (it.hasNext()) {
        LengthEntry entry = it.next();
        System.out.println(entry.getTimestamp() + ": " + entry.getLength() + " bytes");
    }
}
```

## Queue

Queues provide durable, persistent message delivery backed by a dataset.

### Opening a Queue

```java
try (Dataset ds = store.openDataset("events", "orders");
     Queue queue = store.openQueue(ds)) {
    // use the queue
}
```

### Pushing Data

```java
long timestamp = queue.push(new byte[]{10, 20, 30});
```

### Polling with a Consumer

```java
QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
        .config(QueueConsumerConfigBuilder.builder()
                .runningExpiredSeconds(30)
                .maxRetryCount((short) 3)
                .build())
        .build();

try (QueueConsumer consumer = queue.openConsumer("workers", options)) {
    Record rec = consumer.poll(5000); // blocks up to 5 seconds
    if (rec != null) {
        // process the record
        consumer.ack(rec.getTimestamp());
    }
}
```

### Acknowledging Records

Call `ack(timestamp)` after processing to advance the consumer position. Unacknowledged records may be redelivered on the next poll.

## Journal

When store journal is enabled and a dataset is created with `enableJournal(true)`, timslite records dataset changes (create, write, delete, append) as sequential log entries. Dataset journal recording is disabled by default.

### Enabling Journal

```java
StoreConfig config = StoreConfigBuilder.builder()
        .enableJournal(true)
        .build();
CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(DatasetConfigBuilder.builder().enableJournal(true).build())
        .build();
```

### Reading Journal Records

```java
// Direct read by sequence
JournalRecord rec = store.journalRead(1L);
if (rec != null) {
    long seq = rec.getSequence();
    byte[] payload = rec.getData();
}

// Query a range
List<JournalRecord> entries = store.journalQuery(1L, 100L);
```

### Consuming Journal via Queue

```java
try (JournalQueue jq = store.openJournalQueue();
     JournalQueueConsumer consumer = jq.openConsumer("replay")) {
    JournalRecord rec = consumer.poll(5000);
    if (rec != null) {
        // replay the change
        consumer.ack(rec.getSequence());
    }
}
```

## Error Handling

All errors are thrown as subclasses of `TmslException`, which extends `RuntimeException`.

### Catching TmslException

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

### Using TmslErrorCode

```java
try {
    store.createDataset("metrics", "cpu", options);
} catch (TmslException e) {
    if (e.code() == TmslErrorCode.ALREADY_EXISTS) {
        // dataset already exists, open it instead
    }
}
```

## Notes

### Java 8 Compatibility

This wrapper targets Java 8. It does not use any Java 9+ APIs.

### AutoCloseable Pattern

All lifecycle types implement `AutoCloseable`: `Store`, `Dataset`, `Queue`, `QueueConsumer`, `JournalQueue`, `JournalQueueConsumer`, `QueryIterator`, `QueryLengthIterator`. Use try-with-resources to guarantee cleanup.

### Thread Safety

- Queue and consumer operations are thread-safe.
- Individual dataset operations (read, write, query) are not thread-safe. Synchronize externally when sharing a `Dataset` across threads.
- A `Store` can be shared across threads, but individual dataset handles should not be used concurrently without synchronization.

### Timestamps

Timestamp values are signed 64-bit business timestamps (`long` in Java). `0` and negative values are valid exact timestamps; use `Dataset.readLatest()` to read the latest written timestamp.

### Data Payloads

Data parameters are raw `byte[]` arrays. Individual records are capped at 4 MiB.
