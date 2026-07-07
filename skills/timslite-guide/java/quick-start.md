# Java Quick Start

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.snower</groupId>
    <artifactId>timslite</artifactId>
    <version>0.1.3</version>
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

// Default configuration
try (Store store = Store.open("/path/to/data")) {
    // use the store
}

// Custom configuration
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
                .indexContinuous((byte) 0)  // 0=sparse, 1=continuous
                .retentionWindow(0)          // no retention limit
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

    // Check if record exists
    boolean exists = ds.readExist(1700000000L);

    // Get record length
    int length = ds.readLength(1700000000L);

    // Append to the latest record (must be >= latest timestamp)
    ds.append(1700000000L, new byte[]{4, 5});

    // Correct a record
    ds.correct(1700000000L, new byte[]{6, 7, 8});

    // Delete a record
    ds.delete(1700000000L);
}
```

### Querying Data with Range

```java
try (Dataset ds = store.openDataset("metrics", "cpu")) {
    // Eager query (loads all into memory)
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

Length-only iteration (reads only 12-byte headers):

```java
try (QueryLengthIterator it = ds.queryLength(startTs, endTs)) {
    while (it.hasNext()) {
        LengthEntry entry = it.next();
        System.out.println(entry.getTimestamp() + ": " + entry.getLength() + " bytes");
    }
}

// Or eager version
List<LengthEntry> lengths = ds.queryLengthAll(startTs, endTs);
```

### Iterator Control

```java
try (QueryIterator it = ds.queryIter(1L, 100L)) {
    // Reverse iteration
    it.reverse();
    
    // Skip records
    it.skip(10);
    
    while (it.hasNext()) {
        Record rec = it.next();
        // process
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

### Consumer Management

```java
// Flush consumer state
consumer.flush();

// Inspect consumer state
QueueConsumerInspectResult result = consumer.inspect();
QueueConsumerInfo info = result.getInfo();
QueueConsumerState state = result.getState();
System.out.println("Processed up to: " + state.getProcessedTs());
System.out.println("Pending entries: " + state.getPendingEntries().size());

// Drop consumer group
consumer.drop();
```

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

// Get latest sequence
Long latestSeq = store.journalLatestSequence();
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

## Inspection

```java
try (Dataset ds = store.openDataset("metrics", "cpu")) {
    InspectResult result = ds.inspect();
    DatasetInfo info = result.getInfo();
    DatasetState state = result.getState();

    System.out.println("Name: " + info.getName());
    System.out.println("Type: " + info.getDatasetType());
    System.out.println("Latest: " + state.getLatestWrittenTimestamp());
    System.out.println("Records: " + state.getTotalRecordCount());
    System.out.println("Data size: " + state.getTotalDataSize());
}

// List datasets
List<String> names = store.getDatasetNames();
List<String> types = store.getDatasetTypes("metrics");
```

## Background Tasks

```java
// Manual tick (when enableBackgroundThread=false)
TickResult result = store.tickBackgroundTasks();
System.out.println("Executed: " + result.getExecutedTasks() + " tasks");
System.out.println("Next delay: " + result.getNextDelayMs() + " ms");
```

## Error Handling

All errors are thrown as subclasses of `TmslException`, which extends `RuntimeException`.

### Catching TmslException

```java
import io.github.snower.timslite.errors.*;

try {
    store.createDataset("metrics", "cpu", options);
} catch (AlreadyExistsException e) {
    System.out.println("Dataset already exists");
} catch (NotFoundException e) {
    System.out.println("Dataset not found");
} catch (InvalidDataException e) {
    System.out.println("Invalid data: " + e.getMessage());
} catch (ExpiredException e) {
    System.out.println("Timestamp expired");
} catch (TmslException e) {
    System.out.println("Error: " + e.code() + " - " + e.getMessage());
}
```

### Using Error Codes

```java
try {
    ds.read(1700000000L);
} catch (TmslException e) {
    switch (e.code()) {
        case EXPIRED:
            // timestamp outside retention window
            break;
        case NOT_FOUND:
            // record not found
            break;
        default:
            // other error
    }
}
```
