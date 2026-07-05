# Java Quick Start

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.snower</groupId>
    <artifactId>timslite</artifactId>
    <version>0.1.2</version>
</dependency>
```

### Building from Source

```bash
# Build Rust cdylib and Kotlin bindings, compile, test
mvn clean verify

# Generate Javadoc
mvn javadoc:javadoc
```

## Basic Usage

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

## Queue Usage

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

## Journal Usage

When journal is enabled, timslite records dataset changes (create, write, delete, append) as sequential log entries.

### Enabling Journal

```java
StoreConfig config = StoreConfigBuilder.builder()
        .enableJournal(true)
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

## Next Steps

- See [API Reference](api-reference.md) for complete API documentation
- See [Examples](examples.md) for more feature scenarios