# Node.js Examples

> Feature scenarios with copy-paste Node.js examples.

---

## Scenario 1: Basic Time-Series Storage (Sensor Data)

**When**: You have periodic sensor readings keyed by timestamp and need range queries.

```js
const { Store } = require("timslite");

const store = Store.open("/data/sensors");

// Create a dataset for sensor readings (sparse mode for irregular timestamps)
store.createDataset("temp_sensor", "readings", {
  dataSegmentSize: 64 * 1024 * 1024,  // 64MB data segments
  indexSegmentSize: 4 * 1024 * 1024,   // 4MB index segments
  compressLevel: 6,
  indexContinuous: false,              // sparse index mode
  retentionWindow: 0n,                 // no retention
});

const ds = store.openDataset("temp_sensor", "readings");

// Write sensor readings (timestamps must be monotonically increasing)
for (let i = 0n; i < 1000n; i++) {
  const data = Buffer.from(JSON.stringify({ temp: 20.0 + Number(i) * 0.1, ts: Number(i) }));
  ds.write(i + 1n, data);
}

// Query a range
const iter = ds.query(100n, 200n);
for (const [ts, data] of iter) {
  console.log(`ts=${ts}: ${data.toString()}`);
}

// Read a specific timestamp
const record = ds.read(150n);
if (record) {
  const [ts, data] = record;
  console.log(`Record at ${ts}: ${data.toString()}`);
}

// Read the latest record
const latest = ds.readLatest();
if (latest) {
  const [ts, data] = latest;
  console.log(`Latest at ${ts}: ${data.toString()}`);
}

ds.close();
store.close();
```

**Key points**:
- Timestamps must be `>= latest_written_timestamp` in sparse mode
- `query()` returns a lazy iterator; use `queryAll()` for an eager array
- `readLatest()` is the only way to get the latest; `read(-1n)` reads timestamp `-1`

---

## Scenario 2: Continuous Mode (Dense Sequential Timestamps)

**When**: Your timestamps are dense sequential integers (e.g., per-second readings with few gaps).

```js
const { Store } = require("timslite");

const store = Store.open("/data/metrics");

// Create with indexContinuous = true
store.createDataset("cpu_usage", "per_second", {
  indexContinuous: true,  // continuous mode — time_step = 1
});

const ds = store.openDataset("cpu_usage", "per_second");

// Write sequential timestamps (no gaps allowed in continuous mode)
for (let i = 1n; i <= 1000n; i++) {
  ds.write(i, Buffer.from(`cpu=${(Number(i) * 0.01).toFixed(2)}`));
}

// Read by position (O(1) lookup)
const record = ds.read(500n);
if (record) {
  const [ts, data] = record;
  console.log(`Record 500: ${data.toString()}`);
}

// Continuous mode is ideal for evenly-spaced data
// Gaps waste space but are allowed (sparse mode is better for irregular data)

ds.close();
store.close();
```

**Key points**:
- Continuous mode enables O(1) timestamp lookup
- Best for evenly-spaced timestamps with minimal gaps
- Gaps are allowed but waste index space

---

## Scenario 3: Append to Existing Record

**When**: You need to extend a record's data incrementally (e.g., building a payload in chunks).

```js
const { Store } = require("timslite");

const store = Store.open("/data/append");
store.createDataset("events", "chunked");
const ds = store.openDataset("events", "chunked");

// Write initial record
ds.write(1n, Buffer.from("chunk1"));

// Append to the same timestamp (must be >= latest_written_timestamp)
ds.append(1n, Buffer.from(",chunk2"));
ds.append(1n, Buffer.from(",chunk3"));

// Read the full record
const record = ds.read(1n);
if (record) {
  console.log(record[1].toString()); // "chunk1,chunk2,chunk3"
}

// Forward append (ts > latest_written_timestamp) creates new record
ds.append(2n, Buffer.from("new_record"));

ds.close();
store.close();
```

**Key points**:
- `append(ts, data)` with `ts == latest_written_timestamp` extends the existing tail record
- `append(ts, data)` with `ts > latest_written_timestamp` creates a new record
- Append only works on uncompressed tail records

---

## Scenario 4: Correction and Deletion

**When**: You need to fix or remove previously written records.

```js
const { Store } = require("timslite");

const store = Store.open("/data/corrections");
store.createDataset("metrics", "correctable");
const ds = store.openDataset("metrics", "correctable");

// Write some records
ds.write(1n, Buffer.from("original_value"));
ds.write(2n, Buffer.from("another_value"));

// Correct a record (overwrites data at existing timestamp)
ds.correct(1n, Buffer.from("corrected_value"));

// Verify correction
const record = ds.read(1n);
console.log(record[1].toString()); // "corrected_value"

// Delete a record
ds.delete(2n);

// Verify deletion
const deleted = ds.read(2n);
console.log(deleted); // null

ds.close();
store.close();
```

**Key points**:
- `correct(ts, data)` overwrites an existing record's data
- `delete(ts)` removes a record (soft delete — data remains until reclaimed)
- Deleted records return `null` from `read()`

---

## Scenario 5: Query Length (Header-Only Scan)

**When**: You need record sizes without reading full data (e.g., for capacity planning or selective reads).

```js
const { Store } = require("timslite");

const store = Store.open("/data/length");
store.createDataset("logs", "entries");
const ds = store.openDataset("logs", "entries");

// Write records of varying sizes
ds.write(1n, Buffer.from("short"));
ds.write(2n, Buffer.from("a much longer record with more data"));
ds.write(3n, Buffer.from("medium length"));

// Query lengths only (no data transfer)
const lengths = ds.queryLength(1n, 3n);
for (const [ts, length] of lengths) {
  console.log(`ts=${ts}: ${length} bytes`);
}

// Use readLength for single record
const len = ds.readLength(2n);
console.log(`Record 2 length: ${len}`);

// Selective read based on length
for (const [ts, length] of lengths) {
  if (length > 10) {
    const record = ds.read(ts);
    console.log(`Large record at ${ts}: ${record[1].toString()}`);
  }
}

ds.close();
store.close();
```

**Key points**:
- `queryLength()` returns `[timestamp, length]` pairs without reading data
- `readLength(ts)` returns length for a single timestamp
- Useful for capacity planning and selective reads

---

## Scenario 6: Queue Consumer Pattern

**When**: You need reliable task processing with consumer groups.

```js
const { Store } = require("timslite");

const store = Store.open("/data/queue");
store.createDataset("tasks", "jobs");
const ds = store.openDataset("tasks", "jobs");

const queue = store.openQueue(ds);

// Producer: push tasks
for (let i = 0; i < 100; i++) {
  const payload = Buffer.from(JSON.stringify({ taskId: i, action: "process" }));
  const ts = queue.push(payload);
  console.log(`Pushed task ${i} at ts=${ts}`);
}

// Consumer: process tasks
const consumer = queue.openConsumer("worker_group", {
  runningExpiredSeconds: 60,  // 60s before stuck task is retried
  maxRetryCount: 3,           // max 3 retries before parked
});

// Poll and process
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  const task = JSON.parse(data.toString());
  console.log(`Processing task ${task.taskId}`);

  // Acknowledge successful processing
  consumer.ack(ts);
}

// Async polling
const asyncResult = await consumer.poll(5000);
if (asyncResult) {
  consumer.ack(asyncResult[0]);
}

// Inspect consumer state
const inspect = consumer.inspect();
console.log(`Processed up to: ${inspect.state.processedTs}`);
console.log(`Pending entries: ${inspect.state.pendingEntries.length}`);

consumer.close();
queue.close();
ds.close();
store.close();
```

**Key points**:
- Each consumer group tracks its own position independently
- `pollSync()` blocks synchronously; `poll()` returns a Promise
- `ack(ts)` advances the consumer position
- Stuck tasks are automatically retried after `runningExpiredSeconds`

---

## Scenario 7: Iterator Control

**When**: You need fine-grained control over query iteration (reverse, skip, collect).

```js
const { Store } = require("timslite");

const store = Store.open("/data/iter");
store.createDataset("events", "controlled");
const ds = store.openDataset("events", "controlled");

// Write test data
for (let i = 1n; i <= 100n; i++) {
  ds.write(i, Buffer.from(`event_${i}`));
}

// Forward iteration (default)
const forward = ds.query(1n, 10n);
for (const [ts, data] of forward) {
  console.log(`Forward: ts=${ts}`);
}

// Reverse iteration
const reverse = ds.query(1n, 10n);
reverse.reverse();
for (const [ts, data] of reverse) {
  console.log(`Reverse: ts=${ts}`);
}

// Skip records
const skipped = ds.query(1n, 10n);
skipped.skip(5); // skip first 5
for (const [ts, data] of skipped) {
  console.log(`After skip: ts=${ts}`); // starts at 6
}

// Collect all into array
const collected = ds.query(1n, 10n).collectAll();
console.log(`Collected ${collected.length} records`);

// QueryLengthIterator with same controls
const lengthIter = ds.queryLengthIter(1n, 100n);
lengthIter.reverse();
lengthIter.skip(10);
const lengths = lengthIter.collectAll();
console.log(`Got ${lengths.length} length entries`);

ds.close();
store.close();
```

**Key points**:
- `reverse()` must be called before first `next()`
- `skip(count)` must be called before first `next()`
- `collectAll()` eagerly loads all remaining records
- `close()` is automatic when iteration completes

---

## Scenario 8: Inspection and Monitoring

**When**: You need to inspect dataset state, consumer progress, or system health.

```js
const { Store } = require("timslite");

const store = Store.open("/data/monitor");

// List all datasets
const names = store.getDatasetNames();
console.log("Datasets:", names);

// List types for a dataset
const types = store.getDatasetTypes("sensor");
console.log("Sensor types:", types);

// Inspect store-level dataset
store.createDataset("sensor", "waveform", { enableJournal: true });
const inspect = store.inspectDataset("sensor", "waveform");

console.log("=== Dataset Info ===");
console.log(`Name: ${inspect.info.name}`);
console.log(`Type: ${inspect.info.datasetType}`);
console.log(`Identifier: ${inspect.info.identifier}`);
console.log(`Compression: ${inspect.info.compressType === 0 ? "deflate" : "zstd"}`);
console.log(`Index mode: ${inspect.info.indexContinuous === 0 ? "sparse" : "continuous"}`);
console.log(`Retention: ${inspect.info.retentionWindow}`);
console.log(`Journal: ${inspect.info.enableJournal}`);

console.log("\n=== Dataset State ===");
console.log(`Latest timestamp: ${inspect.state.latestWrittenTimestamp}`);
console.log(`Data segments: ${inspect.state.dataSegments} (${inspect.state.openDataSegments} open)`);
console.log(`Index segments: ${inspect.state.indexSegments} (${inspect.state.openIndexSegments} open)`);
console.log(`Total records: ${inspect.state.totalRecordCount}`);
console.log(`Data size: ${inspect.state.totalDataSize} bytes`);
console.log(`Read-only: ${inspect.state.readOnly}`);
console.log(`Queue groups: ${inspect.state.queueConsumerGroups}`);

const ds = store.openDataset("sensor", "waveform");

// Dataset-level inspection
const dsInspect = ds.inspect();
console.log("\n=== Dataset Handle State ===");
console.log(`Closed: ${ds.closed}`);

ds.close();
store.close();
```

**Key points**:
- `inspectDataset()` returns both static config and runtime state
- `inspect()` on Dataset handle returns the same data
- Useful for monitoring, debugging, and capacity planning

---

## Scenario 9: Journal Queue for Audit/Sync

**When**: You need to consume journal entries for audit logging or cross-system sync.

```js
const { Store } = require("timslite");

const store = Store.open("/data/journal");
store.createDataset("events", "audited", { enableJournal: true });
const ds = store.openDataset("events", "audited");

// Write events (automatically journaled)
ds.write(1n, Buffer.from("user_login"));
ds.write(2n, Buffer.from("page_view"));
ds.write(3n, Buffer.from("purchase"));

// Open journal queue
const jq = store.openJournalQueue();
const jc = jq.openConsumer();

// Process journal entries
for (let i = 0; i < 10; i++) {
  const entry = await jc.poll(1000);
  if (entry) {
    const [seq, payload] = entry;
    console.log(`Journal #${seq}: ${payload.length} bytes`);
    jc.ack(seq);
  } else {
    break;
  }
}

// Get specific journal record without advancing cursor
const specific = jc.get(1n);
if (specific) {
  console.log(`Journal #${specific[0]}: ${specific[1].length} bytes`);
}

jc.close();
jq.close();
ds.close();
store.close();
```

**Key points**:
- Journal entries are automatically created for write/delete/append operations
- `openJournalQueue()` creates a persistent consumer
- `poll()` returns `[sequence, payload]` tuples
- `get(sequence)` reads without advancing the cursor

---

## Scenario 10: Background Tasks

**When**: You need manual control over background maintenance (flush, eviction, retention).

```js
const { Store } = require("timslite");

// Disable background thread for manual control
const store = Store.open("/data/manual", { enableBackgroundThread: false });

store.createDataset("metrics", "cpu");
const ds = store.openDataset("metrics", "cpu");

// Write data
for (let i = 1n; i <= 1000n; i++) {
  ds.write(i, Buffer.from(`cpu=${Number(i) * 0.01}`));
}

// Manually trigger background tasks
const result = store.tickBackgroundTasks();
console.log(`Executed ${result.executedTasks} tasks`);
console.log(`Next run in ${result.nextDelayMs}ms`);

// Run again to flush
const result2 = store.tickBackgroundTasks();
console.log(`Executed ${result2.executedTasks} tasks`);

ds.close();
store.close();
```

**Key points**:
- `enableBackgroundThread: false` disables automatic background tasks
- `tickBackgroundTasks()` manually triggers flush, idle-close, eviction, retention
- Returns `TickResult` with execution count and next recommended delay

---

## Scenario 11: Error Handling

**When**: You need robust error handling for production use.

```js
const { Store } = require("timslite");

try {
  const store = Store.open("/data/errors");

  // AlreadyExists error
  store.createDataset("test", "data");
  try {
    store.createDataset("test", "data");
  } catch (err) {
    if (err.code === "AlreadyExists") {
      console.log("Dataset already exists — expected");
    }
  }

  // NotFound error
  try {
    store.openDataset("nonexistent", "data");
  } catch (err) {
    if (err.code === "NotFound") {
      console.log("Dataset not found — expected");
    }
  }

  // InvalidData error
  try {
    store.createDataset("", "data"); // empty name
  } catch (err) {
    if (err.code === "InvalidData") {
      console.log("Invalid name — expected");
    }
  }

  // Read-only store
  const roStore = Store.open("/data/errors", { readOnly: true });
  const ds = roStore.openDataset("test", "data");
  try {
    ds.write(999n, Buffer.from("fail"));
  } catch (err) {
    if (err.code === "StoreClosed") {
      console.log("Cannot write to read-only store — expected");
    }
  }
  ds.close();
  roStore.close();

  store.close();
} catch (err) {
  console.error("Unexpected error:", err.code, err.message);
}
```

**Error codes**:
- `AlreadyExists`: Dataset already exists
- `NotFound`: Dataset or record not found
- `InvalidData`: Invalid parameters
- `StoreClosed`: Operation on closed store
- `DatasetClosed`: Operation on closed dataset
- `QueueClosed`: Operation on closed queue
- `ConsumerGroupExists`: Consumer group already open
- `ConsumerGroupNotFound`: Consumer group not found
- `Expired`: Record has expired
- `SegmentFull`: Segment is full
