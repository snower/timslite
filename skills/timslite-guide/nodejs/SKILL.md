---
name: timslite-guide-nodejs
description: Node.js guide for timslite time-series storage library - installation, quick start, API reference, and examples
---

# timslite Node.js Guide

## Installation

```bash
npm install timslite
```

Prebuilt native bindings are included for:

- macOS arm64
- Linux x64 GNU
- Linux arm64 GNU
- Windows x64 MSVC
- Windows arm64 MSVC

If the current platform does not have a prebuilt binding, the package attempts to build from source during `postinstall`. Source builds require a Rust toolchain and the platform's native C/C++ build tools.

Set `TIMSLITE_SKIP_SOURCE_BUILD=1` to skip the source build attempt. Set `TIMSLITE_BUILD_FROM_SOURCE=1` to force a local source build even when a prebuilt binding exists.

## Quick Start

```js
const { Store } = require("timslite");

const store = Store.open("./data");

// Create dataset (returns void)
store.createDataset("metrics", "cpu");

// Open dataset handle
const ds = store.openDataset("metrics", "cpu");

// Write a record
ds.write(1n, Buffer.from("hello"));

// Read a record
const record = ds.read(1n);
if (record) {
  const [timestamp, data] = record;
  console.log(timestamp, data.toString());
}

ds.close();
store.close();
```

## Documentation

- **[API Reference](api-reference.md)** — Complete Node.js API signatures, parameters, return types, and semantics
- **[Quick Start](quick-start.md)** — Getting started with common operations
- **[Examples](examples.md)** — Feature scenarios with copy-paste Node.js examples

## Key Concepts

### Store and Dataset

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `Dataset` handles per-dataset read/write operations, segment management, and indexing
- Each `(dataset_name, dataset_type)` pair is an independent dataset with its own segments

### Timestamps

- All timestamps are `bigint` values to preserve the Rust `i64` timestamp range
- In sparse mode: timestamps must be `>= latest_written_timestamp`
- In continuous mode: timestamps must be `>= latest_written_timestamp`
- Use `readLatest()` to get the most recent record
- Use `read(-1n)` to read the record at timestamp `-1` (not the latest)

### Blocks and Compression

- Records are aggregated into blocks (max 64KB payload)
- Blocks are lazily compressed on seal (zstd or deflate)
- Large records (>64KB) get their own single-record block
- Max record size: 4 MiB

### Index Modes

- **Sparse mode** (`indexContinuous: false`): Flexible timestamps, O(log n) lookup
- **Continuous mode** (`indexContinuous: true`): Dense sequential timestamps, O(1) lookup

## Configuration

### StoreConfig

```js
const store = Store.open("./data", {
  flushIntervalMs: 15000,           // 15 seconds
  idleTimeoutMs: 1800000,           // 30 minutes
  dataSegmentSize: 64 * 1024 * 1024,  // 64 MiB
  indexSegmentSize: 4 * 1024 * 1024,   // 4 MiB
  initialDataSegmentSize: 256 * 1024,  // 256 KiB
  initialIndexSegmentSize: 4096,       // 4 KiB
  compressLevel: 6,                    // 0-9
  compressType: 1,                     // 0=deflate, 1=zstd
  cacheMaxMemory: 256 * 1024 * 1024,  // 256 MiB
  cacheIdleTimeoutMs: 1800000,         // 30 minutes
  retentionCheckHour: 0,               // UTC hour 0-23
  enableBackgroundThread: true,
  enableJournal: true,
  readOnly: null,                      // null=auto, true=force RO, false=require writable
});
```

### CreateDatasetOptions

```js
store.createDataset("metrics", "cpu", {
  dataSegmentSize: 128 * 1024 * 1024,
  indexSegmentSize: 8 * 1024 * 1024,
  initialDataSegmentSize: 512 * 1024,
  initialIndexSegmentSize: 8192,
  compressLevel: 9,
  compressType: 1,                     // 0=deflate, 1=zstd
  indexContinuous: false,              // false=sparse, true=continuous
  retentionWindow: 86400n,             // 0=unlimited
  enableJournal: true,
});
```

## Module Exports

```js
const {
  Store, Dataset, QueryIterator, QueryLengthIterator,
  Queue, QueueConsumer, JournalQueue, JournalQueueConsumer,
  version
} = require("timslite");
```

### Main Classes

- `Store` — Main entry point
- `Dataset` — Dataset operations
- `Queue` — Queue operations
- `QueueConsumer` — Queue consumer
- `JournalQueue` — Journal queue
- `JournalQueueConsumer` — Journal consumer

### Iterators

- `QueryIterator` — Query result iterator
- `QueryLengthIterator` — Query length iterator

### Functions

- `version()` — Returns the native library version string (e.g., `"0.1.3"`)

## Error Handling

```js
try {
  store.createDataset("sensor", "waveform");
} catch (err) {
  if (err.code === "AlreadyExists") {
    console.log("Dataset already exists");
  } else if (err.code === "NotFound") {
    console.log("Dataset not found");
  } else {
    console.error("Error:", err.code, err.message);
  }
}
```

Error codes:
- `Io`, `InvalidMagic`, `InvalidVersion`, `Mmap`, `Compression`, `Decompression`
- `InvalidData`, `NotFound`, `Expired`, `AlreadyExists`, `SegmentFull`
- `QueueAlreadyOpen`, `QueueNotOpen`, `ConsumerGroupNotFound`, `ConsumerGroupExists`
- `QueueClosed`, `PendingFull`, `StoreClosed`, `DatasetClosed`, `IteratorExhausted`
