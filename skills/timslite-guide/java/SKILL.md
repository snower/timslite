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
    <version>0.1.3</version>
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

## Documentation

- **[API Reference](api-reference.md)** — Complete Java API signatures, parameters, return types, and semantics
- **[Quick Start](quick-start.md)** — Getting started with common operations
- **[Examples](examples.md)** — Feature scenarios with copy-paste Java examples

## Key Concepts

### Store and Dataset

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `Dataset` handles per-dataset read/write operations, segment management, and indexing
- Each `(dataset_name, dataset_type)` pair is an independent dataset with its own segments

### Timestamps

- All timestamps are signed 64-bit values (`long`)
- In sparse mode: timestamps must be `>= latest_written_timestamp`
- In continuous mode: timestamps must be `>= latest_written_timestamp`
- Use `readLatest()` to get the most recent record
- Use `read(-1L)` to read the record at timestamp `-1` (not the latest)

### Blocks and Compression

- Records are aggregated into blocks (max 64KB payload)
- Blocks are lazily compressed on seal (zstd or deflate)
- Large records (>64KB) get their own single-record block
- Max record size: 4 MiB

### Index Modes

- **Sparse mode** (`indexContinuous = 0`): Flexible timestamps, O(log n) lookup
- **Continuous mode** (`indexContinuous = 1`): Dense sequential timestamps, O(1) lookup

## Configuration

### StoreConfig

```java
StoreConfig config = StoreConfigBuilder.builder()
        .flushIntervalSecs(15)           // seconds
        .idleTimeoutSecs(1800)           // 30 minutes
        .dataSegmentSize(64 * 1024 * 1024)  // 64 MiB
        .indexSegmentSize(4 * 1024 * 1024)   // 4 MiB
        .initialDataSegmentSize(256 * 1024)  // 256 KiB
        .initialIndexSegmentSize(4 * 1024)   // 4 KiB
        .compressLevel(6)                // 0-9
        .cacheMaxMemory(256 * 1024 * 1024)  // 256 MiB
        .cacheIdleTimeoutSecs(1800)      // 30 minutes
        .retentionCheckHour((byte) 0)    // UTC hour 0-23
        .enableBackgroundThread(true)
        .enableJournal(true)
        .readOnly(null)                  // null=auto, True=force RO, False=require writable
        .build();
```

### Dataset Configuration

Dataset configuration is passed via `DatasetConfigBuilder`:

```java
DatasetConfig config = DatasetConfigBuilder.builder()
        .dataSegmentSize(128 * 1024 * 1024)  // 128 MiB
        .indexSegmentSize(8 * 1024 * 1024)    // 8 MiB
        .compressLevel((byte) 9)              // max compression
        .indexContinuous((byte) 1)            // continuous mode
        .retentionWindow(86400)               // 1 day in timestamp units
        .enableJournal(true)
        .build();

CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(config)
        .build();
```

## Module Exports

```java
import io.github.snower.timslite.*;
import io.github.snower.timslite.errors.*;
```

### Main Classes

- `Store` — Main entry point
- `Dataset` — Dataset operations
- `Queue` — Queue operations
- `QueueConsumer` — Queue consumer
- `JournalQueue` — Journal queue
- `JournalQueueConsumer` — Journal consumer

### Configuration Builders

- `StoreConfigBuilder` — Store configuration
- `DatasetConfigBuilder` — Dataset configuration
- `CreateDatasetOptionsBuilder` — Dataset creation options
- `QueueConsumerOptionsBuilder` — Queue consumer options
- `QueueConsumerConfigBuilder` — Queue consumer config

### Iterators

- `QueryIterator` — Query result iterator
- `QueryLengthIterator` — Query length iterator

### Data Types

- `Record` — Timestamp + data
- `JournalRecord` — Sequence + data
- `LengthEntry` — Timestamp + length
- `InspectResult` — Dataset inspect result
- `DatasetInfo` — Dataset configuration
- `DatasetState` — Dataset runtime state
- `QueueConsumerInspectResult` — Consumer inspect result
- `QueueConsumerInfo` — Consumer configuration
- `QueueConsumerState` — Consumer state
- `QueueConsumerPendingEntry` — Pending entry state
- `TickResult` — Background task result

## Error Handling

```java
import io.github.snower.timslite.errors.*;

try {
    store.createDataset("sensor", "waveform", options);
} catch (AlreadyExistsException e) {
    System.out.println("Dataset already exists");
} catch (NotFoundException e) {
    System.out.println("Dataset not found");
} catch (TmslException e) {
    System.out.println("Error: " + e.code() + " - " + e.getMessage());
}
```

Error hierarchy:
- `TmslException` (base, extends `RuntimeException`)
  - `IoException`
  - `InvalidMagicException`
  - `InvalidVersionException`
  - `MmapException`
  - `CompressionException`
  - `DecompressionException`
  - `InvalidDataException`
  - `NotFoundException`
  - `ExpiredException`
  - `AlreadyExistsException`
  - `SegmentFullException`
  - `QueueAlreadyOpenException`
  - `QueueNotOpenException`
  - `ConsumerGroupNotFoundException`
  - `ConsumerGroupExistsException`
  - `QueueClosedException`
  - `PendingFullException`
  - `StoreClosedException`
  - `DatasetClosedException`
  - `QueueBridgeClosedException`
  - `IteratorExhaustedException`
