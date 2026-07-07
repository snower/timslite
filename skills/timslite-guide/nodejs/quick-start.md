# Node.js Quick Start

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

## Basic Usage

```js
const { Store } = require("timslite");

// Open store with default config
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

// Close handles
ds.close();
store.close();
```

## Using CreateDatasetOptions

For fine-grained control over dataset configuration:

```js
const { Store } = require("timslite");

const store = Store.open("./data");

// Create dataset with custom config
store.createDataset("metrics", "cpu", {
  dataSegmentSize: 128 * 1024 * 1024,  // 128 MiB
  indexSegmentSize: 8 * 1024 * 1024,    // 8 MiB
  compressLevel: 9,                      // max compression
  indexContinuous: true,                 // continuous mode
  retentionWindow: 86400n,               // 1 day in timestamp units
  enableJournal: true,
});

// Create dataset with default config
store.createDataset("simple", "events");

store.close();
```

## Queue Usage

```js
const { Store } = require("timslite");

const store = Store.open("./data");
store.createDataset("tasks", "jobs");
const ds = store.openDataset("tasks", "jobs");

// Open queue for a dataset
const queue = store.openQueue(ds);

// Push data (auto-assigns next timestamp)
const ts = queue.push(Buffer.from("task_payload"));

// Open a consumer group
const consumer = queue.openConsumer("worker_group");

// Poll for records (synchronous, timeout in ms)
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Got task: ${data.toString()}`);
  // Acknowledge processing
  consumer.ack(ts);
}

// Async polling
const asyncResult = await consumer.poll(5000);

// Inspect consumer state
const inspect = consumer.inspect();
console.log(`Processed: ${inspect.state.processedTs}`);

consumer.close();
queue.close();
ds.close();
store.close();
```

## Journal Usage

```js
const { Store } = require("timslite");

const store = Store.open("./data");
store.createDataset("events", "user_actions", { enableJournal: true });
const ds = store.openDataset("events", "user_actions");

// Every write/delete/append automatically appends to the journal
ds.write(1n, Buffer.from("action1"));
ds.write(2n, Buffer.from("action2"));

// Read journal
const latest = store.journalLatestSequence();
console.log(`Latest journal sequence: ${latest}`);

const first = store.journalRead(1n);
if (first) {
  const [seq, payload] = first;
  console.log(`Sequence ${seq}: ${payload.length} bytes`);
}

// Query journal range
const entries = store.journalQuery(1n, latest);
for (const [seq, payload] of entries) {
  console.log(`Sequence ${seq}`);
}

ds.close();
store.close();
```

## Journal Queue Usage

```js
const { Store } = require("timslite");

const store = Store.open("./data");
store.createDataset("events", "logs", { enableJournal: true });
const ds = store.openDataset("events", "logs");

// Write some data to generate journal entries
ds.write(1n, Buffer.from("log1"));

// Open journal queue
const jq = store.openJournalQueue();
const jc = jq.openConsumer();

// Poll for journal records
const result = await jc.poll(5000);
if (result) {
  const [seq, payload] = result;
  console.log(`Journal sequence ${seq}: ${payload.length} bytes`);
  jc.ack(seq);
}

// Get specific journal record
const record = jc.get(1n);

jc.close();
jq.close();
ds.close();
store.close();
```

## Inspection and Monitoring

```js
const { Store } = require("timslite");

const store = Store.open("./data");

// List datasets
const names = store.getDatasetNames();
console.log("Datasets:", names);

const types = store.getDatasetTypes("metrics");
console.log("Types for 'metrics':", types);

// Inspect dataset
store.createDataset("sensor", "waveform");
const inspect = store.inspectDataset("sensor", "waveform");
console.log("Info:", inspect.info);
console.log("State:", inspect.state);

const ds = store.openDataset("sensor", "waveform");

// Dataset-level inspection
const dsInspect = ds.inspect();
console.log("Dataset info:", dsInspect.info);
console.log("Dataset state:", dsInspect.state);

ds.close();
store.close();
```

## Background Tasks

```js
const { Store } = require("timslite");

// Manual background tasks (no background thread)
const store = Store.open("./data", { enableBackgroundThread: false });

// ... do work ...

// Manually trigger background tasks
const result = store.tickBackgroundTasks();
console.log(`Executed ${result.executedTasks} tasks`);
console.log(`Next run in ${result.nextDelayMs}ms`);

store.close();
```

## Error Handling

```js
const { Store } = require("timslite");

try {
  const store = Store.open("./data");
  store.createDataset("test", "data");
  store.createDataset("test", "data"); // AlreadyExists
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

## Read-Only Mode

```js
const { Store } = require("timslite");

// Open in read-only mode
const store = Store.open("./data", { readOnly: true });

const ds = store.openDataset("metrics", "cpu");
const record = ds.read(1n); // OK

// ds.write(2n, Buffer.from("fail")); // Throws "Store is read-only"

ds.close();
store.close();
```
