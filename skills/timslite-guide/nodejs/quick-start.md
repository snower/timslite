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

## Basic Usage

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
const dataset = store.openDataset("tasks", "jobs");

// Open queue for a dataset
const queue = store.openQueue(dataset);

// Push data (auto-assigns next timestamp)
const ts = queue.push(Buffer.from("task_payload"));

// Open a consumer group
const consumer = queue.openConsumer("worker_group");

// Poll for records (with timeout in ms)
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Got task: ${data.toString()}`);
  // Acknowledge processing
  consumer.ack(ts);
}

queue.close();
store.close();
```

## Journal Usage

```js
const { Store } = require("timslite");

const store = Store.open("./data");
store.createDataset("events", "user_actions");
const dataset = store.openDataset("events", "user_actions");

// Every write/delete/append automatically appends to the journal
dataset.write(1n, Buffer.from("user_login"));
dataset.write(2n, Buffer.from("page_view"));
dataset.delete(1n);

// Read journal records
const latest = store.journalLatestSequence();  // bigint or null
console.log(`Latest journal seq: ${latest}`);

// Consume journal via queue (for downstream sync)
const journalQueue = store.openJournalQueue();
const consumer = journalQueue.openConsumer("sync_worker");

const result = consumer.pollSync(100);  // timeout in ms
if (result) {
  const [seq, payload] = result;
  console.log(`Consumed journal seq ${seq}`);
  consumer.ack(seq);
}

journalQueue.close();
store.close();
```

## Manual Background Tasks

When `enableBackgroundThread: false`, the store does not spawn an internal background thread. You must call `store.tickBackgroundTasks()` periodically to drive flush, idle-close, cache eviction, and retention reclaim.

```js
const { Store } = require("timslite");

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

All operations throw `Error` on failure:

```js
const { Store } = require("timslite");

try {
  const store = Store.open("./data");
  store.createDataset("sensor", "waveform");
  store.close();
} catch (e) {
  console.error(`Error: ${e.message}`);
}
```

## Next Steps

- See [API Reference](api-reference.md) for complete API documentation
- See [Examples](examples.md) for more feature scenarios