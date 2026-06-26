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

const store = Store.open("./data", {
  enableBackgroundThread: false,
});

const dataset = store.createDataset("metrics", "cpu");
dataset.write(1n, Buffer.from("hello"));

const record = dataset.read(1n);
if (record) {
  const [timestamp, data] = record;
  console.log(timestamp, data.toString());
}

dataset.close();
store.close();
```

## Documentation

- **[API Reference](api-reference.md)** — Complete Node.js API signatures, parameters, return types, and semantics
- **[Examples](examples.md)** — Feature scenarios with copy-paste Node.js examples

## Key Concepts

### Store and DataSet

- `Store` is the top-level facade managing data directory, datasets, journal, cache, and background tasks
- `DataSet` handles per-dataset read/write operations, segment management, and indexing
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

## Configuration

### StoreConfig

```js
const store = Store.open("./data", {
  flushIntervalMs: 15000,           // 15 seconds
  idleTimeoutMs: 1800000,           // 30 minutes
  dataSegmentSize: 64 * 1024 * 1024,  // 64 MiB
  indexSegmentSize: 4 * 1024 * 1024,   // 4 MiB
  compressLevel: 6,                  // 0-9
  cacheMaxMemory: 256 * 1024 * 1024,  // 256 MiB
  enableBackgroundThread: true,
  enableJournal: true,
  readOnly: null,                    // null=auto, true=force RO, false=require writable
});
```

### CreateDatasetOptions

```js
store.createDataset("metrics", "cpu", {
  dataSegmentSize: 128 * 1024 * 1024,  // 128 MiB
  indexSegmentSize: 8 * 1024 * 1024,    // 8 MiB
  compressLevel: 9,                      // max compression
  indexContinuous: true,                 // continuous mode
  retentionWindow: 86400n,               // 1 day in timestamp units
  enableJournal: true,
});
```

## Common Patterns

### Batch Writes

```js
for (let i = 0n; i < 1000n; i++) {
  const data = Buffer.from(JSON.stringify({ value: Number(i) }));
  dataset.write(i + 1n, data);
}
```

### Range Queries

```js
// Lazy query (iterator)
const iter = dataset.query(100n, 200n);
for (const [ts, data] of iter) {
  console.log(`ts=${ts}: ${data.toString()}`);
}

// Eager query (array)
const records = dataset.queryAll(100n, 200n);
for (const [ts, data] of records) {
  console.log(`ts=${ts}: ${data.toString()}`);
}
```

### Queue Consumption

```js
const queue = store.openQueue(dataset);
const consumer = queue.openConsumer("worker_group");

// Async poll
const result = await consumer.poll(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Got task: ${data.toString()}`);
  consumer.ack(ts);
}

// Sync poll
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Got task: ${data.toString()}`);
  consumer.ack(ts);
}

queue.close();
```

### Journal Consumption

```js
const journalQueue = store.openJournalQueue();
const consumer = journalQueue.openConsumer("sync_worker");

const result = await consumer.poll(100);
if (result) {
  const [seq, payload] = result;
  console.log(`Journal seq: ${seq}`);
  consumer.ack(seq);
}

journalQueue.close();
```

### Manual Background Tasks

When `enableBackgroundThread: false`, the store does not spawn an internal background thread. You must call `store.tickBackgroundTasks()` periodically to drive flush, idle-close, cache eviction, and retention reclaim.

```js
const store = Store.open("./data", {
  enableBackgroundThread: false,
});

store.createDataset("sensor", "waveform");
const dataset = store.openDataset("sensor", "waveform");
dataset.write(1n, Buffer.from("reading_1"));

// Manually execute a tick
const { executedTasks, nextDelayMs } = store.tickBackgroundTasks();
console.log(`executed=${executedTasks}, next in ${nextDelayMs}ms`);

// Check the delay without executing anything
const delay = store.nextBackgroundDelay();
console.log(`next task due in ${delay}ms`);

// In an event loop:
while (true) {
  const { executedTasks, nextDelayMs } = store.tickBackgroundTasks();
  if (executedTasks > 0) {
    console.log(`ran ${executedTasks} background tasks`);
  }
  await new Promise(resolve => setTimeout(resolve, nextDelayMs));
}

store.close();
```

## Error Handling

All operations throw `Error` on failure. Common errors:

- `AlreadyExists` — dataset already exists
- `InvalidData` — invalid parameters or data
- `NotFound` — dataset or record not found
- `SegmentFull` — segment capacity exceeded
- `ReadOnly` — write attempted on read-only store

See [Troubleshooting](../troubleshooting.md) for detailed error solutions.