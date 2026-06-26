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

// Continuous mode auto-fills gaps with null on read
// If you write ts=1 and ts=100, reading ts=50 returns null (filler)

store.close();
```

**Key points**:
- Continuous mode assumes timestamps are dense sequential integers
- Missing timestamps become filler entries (read returns `null`)
- O(1) timestamp-to-position calculation within a segment

---

## Scenario 3: Queue (FIFO Consumer Groups)

**When**: You need ordered delivery with consumer group semantics (like Kafka consumer groups).

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("tasks", "jobs");
const ds = store.openDataset("tasks", "jobs");

// Open queue for a dataset
const queue = store.openQueue(ds);

// Push data (auto-assigns next timestamp)
const ts1 = queue.push(Buffer.from("task_1"));
const ts2 = queue.push(Buffer.from("task_2"));

// Open a consumer group
const consumer = queue.openConsumer("worker_group");

// Poll for records (with timeout in ms)
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Got task at ${ts}: ${data.toString()}`);
  // Acknowledge processing
  consumer.ack(ts);
}

queue.close();
store.close();
```

**Key points**:
- `push` auto-assigns `timestamp = latest_written_timestamp + 1`
- Multiple consumer groups are independent
- Multiple consumers in the same group share progress (mutual exclusion)

---

## Scenario 4: Queue with Retry and Expiry

**When**: You need automatic retry for failed tasks and expiry for stuck tasks.

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("retry_queue", "jobs");
const ds = store.openDataset("retry_queue", "jobs");
const queue = store.openQueue(ds);

// Push some tasks
queue.push(Buffer.from("important_task"));

// Open consumer with retry config
const consumer = queue.openConsumer("retry_group", {
  runningExpiredSeconds: 60,  // re-deliver after 60s if not acked
  maxRetryCount: 3,           // drop after 3 retries
});

// Poll and process
const result = consumer.pollSync(5000);
if (result) {
  const [ts, data] = result;
  console.log(`Processing: ${data.toString()}`);
  // If processing fails, don't ack — it will be re-delivered after 60s
  // After 3 failures, the entry is dropped
  consumer.ack(ts);
}

queue.close();
store.close();
```

**Key points**:
- `runningExpiredSeconds`: re-deliver pending entries after this timeout
- `maxRetryCount`: drop entries after this many retries (0 = unlimited)

---

## Scenario 5: Journal for Change Tracking / Hot Migration

**When**: You need to track all data changes for audit, sync to another system, or recovery.

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");

// Journal is enabled by default. Create a dataset.
store.createDataset("events", "user_actions");
const ds = store.openDataset("events", "user_actions");

// Every write/delete/append automatically appends to the journal
ds.write(1n, Buffer.from("user_login"));
ds.write(2n, Buffer.from("page_view"));
ds.delete(1n);

// Read journal records
const latest = store.journalLatestSequence();  // e.g., 4n — create + 2 writes + delete
console.log(`Latest journal seq: ${latest}`);

// Read individual journal record
const record = store.journalRead(1n);
if (record) {
  const [seq, payload] = record;
  console.log(`Journal record ${seq}: ${payload.length} bytes`);
}

// Query a range of journal records
const records = store.journalQuery(1n, latest);
for (const [seq, payload] of records) {
  console.log(`Seq ${seq}: ${payload.length} bytes`);
}

// Consume journal via queue (for downstream sync)
const journalQueue = store.openJournalQueue();
const consumer = journalQueue.openConsumer("sync_worker");

// Each journal record is delivered as a queue entry
const result = consumer.pollSync(100);
if (result) {
  const [seq, payload] = result;
  console.log(`Consumed journal seq ${seq}`);
  // Use store.readJournalSourceRecord() to fetch the actual business data
  consumer.ack(seq);
}

journalQueue.close();
store.close();
```

**Important journal semantics**:
- Journal is NOT a WAL — no transaction guarantees
- Journal records do NOT contain business payload; they reference source data via `index_info`
- Use `store.readJournalSourceRecord(dataset_identifier, index_info)` to dereference
- Journal append failure does NOT roll back the main operation
- Disable per-dataset with `enableJournal: false`

---

## Scenario 6: Retention Window (Time-Based Data Expiry)

**When**: You want old data to automatically expire and be reclaimed.

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");

// Create dataset with 1-day retention (in timestamp units)
store.createDataset("metrics", "per_second", {
  retentionWindow: 86400n,  // 86400 seconds = 1 day
  enableJournal: true,
});

const ds = store.openDataset("metrics", "per_second");

// Write data with timestamps
for (let i = 1n; i <= 1000n; i++) {
  ds.write(i, Buffer.from(`value=${i}`));
}

// Read old data (may be expired if retentionWindow > 0)
const record = ds.read(1n);
if (record) {
  const [ts, data] = record;
  console.log(`Old record: ts=${ts}`);
}

// Expired records return null on read
// Reclaim happens during background tasks (daily at retentionCheckHour UTC)

store.close();
```

**Key points**:
- `retentionWindow` uses the same units as dataset timestamps
- `retentionWindow = 0n` means no limit
- `retentionCheckHour` is UTC hour (0-23) for daily reclaim
- Expired records return `null` on read
- Reclaim deletes entire segments when all records are expired

---

## Scenario 7: Read-Only Mode

**When**: You want multiple processes to read the same store, or need to inspect data safely.

```js
const { Store } = require("timslite");

// Force read-only mode
const store = Store.open("/data/app", {
  readOnly: true,
});

// Read operations work normally
const ds = store.openDataset("metrics", "cpu");
const records = ds.queryAll(1n, 100n);
const info = ds.inspect();
console.log(`Dataset: ${info.state.totalRecordCount} records`);

// Cannot write, create, drop, or open queues in read-only mode
// ds.write(1n, Buffer.from("data")) would throw an error

store.close();
```

**Auto read-only mode** (default):
- If `.lock` can be acquired → writable
- If `.lock` is already held → falls back to read-only
- Use `readOnly: false` to require writable (fail if locked)

---

## Scenario 8: Append (In-Place Tail Growth)

**When**: You want to append data to the latest record without creating a new timestamp.

```js
const { Store } = require("timslite");

const store = Store.open("/data/logs");
store.createDataset("app_log", "lines");
const ds = store.openDataset("app_log", "lines");

// Create initial record at timestamp 1
ds.append(1n, Buffer.from("line1\n"));

// Append more data to the same timestamp (in-place tail growth)
ds.append(1n, Buffer.from("line2\n"));
ds.append(1n, Buffer.from("line3\n"));

// Read the combined record
const [ts, data] = ds.read(1n);
console.assert(data.equals(Buffer.from("line1\nline2\nline3\n")));

// Forward append creates a new record
ds.append(2n, Buffer.from("new_record"));

store.close();
```

**Append rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + data.length <= 4 MiB`
- Appended data must fit within the current pending block's capacity
- Does NOT re-notify queue when appending to existing record

---

## Scenario 9: Correction Write (Fix Latest Record)

**When**: You wrote data and need to correct the latest record's content.

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("events", "actions");
const ds = store.openDataset("events", "actions");

// Write a record
ds.write(100n, Buffer.from("wrong_data"));

// Correct it by writing to the same timestamp
// (only works if latest_written_timestamp == 100 and the record is in a pending raw block)
ds.write(100n, Buffer.from("corrected_data"));

// If the block has already been sealed/compressed, the correction
// automatically falls back to an "update write": new data is appended
// to the latest data segment, the index is updated, and the old
// record's segment gets invalidRecordCount incremented.

store.close();
```

**Correction behavior**:
- Triggers when `timestamp == latest_written_timestamp`
- If latest record is in pending raw block → in-place correction
- If latest record is in sealed/compressed block → update write (new data + index update)
- Cache invalidation occurs for affected blocks

---

## Scenario 10: Out-of-Order Write (Sparse Mode)

**When**: You need to update an existing timestamp (not the latest).

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("events", "actions");
const ds = store.openDataset("events", "actions");

// Write some records
ds.write(1n, Buffer.from("data_1"));
ds.write(2n, Buffer.from("data_2"));
ds.write(3n, Buffer.from("data_3"));

// Update timestamp 1 (out-of-order write)
// Only works in sparse mode, and only if timestamp 1 already has an index entry
ds.write(1n, Buffer.from("updated_data_1"));

// Out-of-order write to a timestamp with NO index entry → ERROR
// ds.write(1n, Buffer.from("data"))  // would fail if timestamp 1 didn't exist

store.close();
```

**In continuous mode**: Out-of-order writes are not supported. Timestamps must be `>= latest_written_timestamp`.

---

## Scenario 11: Large Record (Single-Record Block)

**When**: A single record's encoded size exceeds 64KB (the normal block payload limit).

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("blobs", "files");
const ds = store.openDataset("blobs", "files");

// Records larger than 64KB get their own exclusive block
// (SINGLE_RECORD flag set, immediately compressed)
const largeData = Buffer.alloc(100_000, 0x78);  // ~100KB
ds.write(1n, largeData);

// Reading works normally
const [ts, data] = ds.read(1n);
console.assert(data.length === 100_000);

store.close();
```

**Rules**:
- Single record max: 4 MiB (`write` and `append` both enforce this)
- Records > 64KB encoded → exclusive single-record block (SINGLE_RECORD | SEALED | COMPRESSED)
- Records ≤ 64KB → aggregated into normal blocks (pending → sealed on overflow)

---

## Scenario 12: Multi-Dataset Isolation

**When**: You have multiple data streams that need independent storage.

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");

// Each (name, type) pair is an independent dataset
store.createDataset("sensors", "temperature");
store.createDataset("sensors", "humidity");
store.createDataset("sensors", "pressure");
store.createDataset("events", "user_action");
store.createDataset("events", "system");

// List all datasets
for (const name of store.getDatasetNames()) {
  for (const type of store.getDatasetTypes(name)) {
    console.log(`${name}/${type}`);
  }
}

// Open and use each independently
const ds1 = store.openDataset("sensors", "temperature");
const ds2 = store.openDataset("events", "user_action");

ds1.write(1n, Buffer.from("23.5C"));
ds2.write(1n, Buffer.from("login"));

// Inspect a dataset
const inspect = store.inspectDataset("sensors", "temperature");
console.log(`Records: ${inspect.state.totalRecordCount}`);
console.log(`Data size: ${inspect.state.totalDataSize} bytes`);

store.close();
```

**Directory layout**: Each dataset gets its own `{name}/{type}/` directory with independent `meta`, `data/`, `index/`, `state`, and optional `queue/`.

---

## Scenario 13: Efficient Existence Checking

**When**: You need to check if records exist without loading data (e.g., for deduplication or coverage checks).

```js
const { Store } = require("timslite");

const store = Store.open("/data/app");
store.createDataset("events", "actions");
const ds = store.openDataset("events", "actions");

// Single record existence (index only, no data I/O)
const exists = ds.readExist(12345n);

// Range existence bitmap (index only, no data I/O)
const bitmap = ds.queryExist(1n, 1000n);
// bitmap[i] is set if record at (startTs + i) exists

// Record length (reads 12-byte header only)
const length = ds.readLength(12345n);  // number or null

// Range lengths (reads headers only)
const lengths = ds.queryLengthAll(1n, 1000n);  // Array<[bigint, number]>

store.close();
```

**Performance hierarchy** (fastest to slowest):
1. `readExist` / `queryExist` — index only, no data segment I/O
2. `readLength` / `queryLength` / `queryLengthAll` — reads 12-byte record header only
3. `read` / `query` / `queryAll` — reads full data

---

## Scenario 14: Manual Background Tasks

**When**: You need fine-grained control over background task execution.

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

**Key points**:
- When `enableBackgroundThread: false`, you must call `tickBackgroundTasks()` periodically
- Returns `{ executedTasks, nextDelayMs }`
- Use `nextBackgroundDelay()` to check when the next task is due without executing