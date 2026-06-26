# Node.js API Reference

> Complete Node.js API signatures, parameters, return types, and semantics.

---

## 1. Module Exports

```js
const {
  Store, Dataset, QueryIterator, QueryLengthIterator,
  Queue, QueueConsumer, JournalQueue, JournalQueueConsumer,
  version
} = require("timslite");
```

---

## 2. Store

### 2.1 Lifecycle

#### `Store.open(dataDir: string, config?: StoreConfig): Store`

Opens or connects to a store at `dataDir`. Acquires a `.lock` file for exclusive write access. If the lock is already held and `config.readOnly` is `null` (auto), falls back to read-only mode.

**Parameters**:
- `dataDir`: Path to the data directory (created if not exists)
- `config`: Optional store configuration

**Returns**: `Store` on success, throws `Error` on failure.

**Side effects**: Creates `dataDir` if missing. Creates `.lock` file. Initializes `BlockCache`, `JournalManager`, and `BackgroundTasks` (if enabled). Scans existing datasets.

#### `store.close(): void`

Flushes all dirty segments, closes all open datasets, closes the journal, and releases the store lock.

#### `store.readOnly: boolean`

Returns whether this store resolved to read-only mode at open time.

#### `store.closed: boolean`

Returns whether the store has been closed.

### 2.2 Dataset Management

#### `store.createDataset(name: string, datasetType: string, options?: CreateDatasetOptions): void`

Creates a new dataset with optional configuration.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `datasetType`: Dataset type, same naming rules
- `options`: Optional dataset configuration

**Errors**:
- `AlreadyExists` if dataset already exists
- `InvalidData` if name/type invalid

#### `store.openDataset(name: string, datasetType: string): Dataset`

Opens an existing dataset and returns a `Dataset` object.

**Errors**: `NotFound` if dataset doesn't exist.

#### `store.openDatasetByIdentifier(identifier: number | bigint): Dataset`

Opens a dataset by its numeric identifier (assigned at creation time).

#### `store.dropDataset(name: string, datasetType: string): void`

Deletes a dataset and all its files (data segments, index segments, meta, state, queue). Irreversible.

#### `store.getDatasetNames(): string[]`

Lists all dataset names in the store.

#### `store.getDatasetTypes(name: string): string[]`

Lists all dataset types for a given name.

#### `store.inspectDataset(name: string, datasetType: string): DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

### 2.3 Background Tasks

#### `store.tickBackgroundTasks(): TickResult`

Manually executes pending background tasks. Returns `{ executedTasks, nextDelayMs }`.

#### `store.nextBackgroundDelay(): number`

Returns delay in milliseconds until the next background task is due.

---

## 3. Dataset

### 3.1 Identity

#### `dataset.id: bigint`

Numeric dataset ID assigned by Store.

#### `dataset.identifier: bigint`

Alias for `id`.

#### `dataset.dataDir: string`

Path to the dataset's data directory.

#### `dataset.latestTimestamp: bigint | null`

Returns the `latest_written_timestamp` for this dataset. Returns `null` if never written.

#### `dataset.closed: boolean`

Returns whether the dataset has been closed.

### 3.2 Write Operations

#### `dataset.write(timestamp: number | bigint, data: Buffer | Uint8Array): void`

Writes a record at the given timestamp.

**Rules**:
- `timestamp >= latest_written_timestamp` → new write (or correction if equal)
- `timestamp < latest_written_timestamp` and index entry exists → out-of-order rewrite (sparse mode only)
- `timestamp < latest_written_timestamp` and no index entry → error
- `data.length <= 4 MiB` (otherwise `InvalidData`)

#### `dataset.append(timestamp: number | bigint, data: Buffer | Uint8Array): void`

Forward append or in-place tail append.

**Rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + data.length <= 4 MiB`

#### `dataset.delete(timestamp: number | bigint): void`

Deletes a record by timestamp. Does NOT retroactively update `latest_written_timestamp`.

### 3.3 Read Operations

#### `dataset.read(timestamp: number | bigint): [bigint, Buffer] | null`

Reads a single complete record by exact timestamp.

**Returns**:
- `[timestamp, data]` if the record exists and is valid
- `null` if the record doesn't exist, is a filler, is deleted, or is expired

**Note**: `read(-1n)` reads the record at timestamp `-1`, NOT the latest. Use `readLatest()` for the latest.

#### `dataset.readLatest(): [bigint, Buffer] | null`

Reads the record at `latest_written_timestamp`. Returns `null` if never written, deleted, or expired.

#### `dataset.readExist(timestamp: number | bigint): boolean`

Fast existence check — index lookup only, no data segment I/O.

#### `dataset.readLength(timestamp: number | bigint): number | null`

Reads only the data length of a record (reads the 12-byte record header, not the full data).

### 3.4 Query Operations

#### `dataset.query(startTs: number | bigint, endTs: number | bigint): QueryIterator`

Lazy range query iterator. Records are read on-demand via iteration.

**Returns**: `QueryIterator` that yields `[timestamp, data]` tuples.

#### `dataset.queryAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, Buffer]>`

Eager range query. Loads all data into memory.

#### `dataset.queryExist(startTs: number | bigint, endTs: number | bigint): Buffer`

Fast range existence bitmap. Returns a bitmap where bit `i` is set if a record exists at `startTs + i`.

#### `dataset.queryLength(startTs: number | bigint, endTs: number | bigint): QueryLengthIterator`

Lazy iterator over `[timestamp, data_length]` pairs.

#### `dataset.queryLengthAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, number]>`

Eager range query returning `[timestamp, data_length]` pairs.

### 3.5 Maintenance

#### `dataset.flush(): void`

Flushes all dirty segments for this dataset to disk.

#### `dataset.close(): void`

Closes the dataset and releases resources.

#### `dataset.inspect(): DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

---

## 4. QueryIterator

Lazy iterator returned by `query()`.

```js
const iter = dataset.query(1n, 1000n);
for (const [ts, data] of iter) {
  console.log(`ts=${ts}: ${data.toString()}`);
}
```

**Properties**:
- `iter.remaining: number` — Number of remaining records

**Behavior**:
- Reads records on-demand from data segments
- Implements `Iterable<[bigint, Buffer]>`
- Automatically releases resources when dropped

---

## 5. QueryLengthIterator

Lazy iterator returned by `queryLength()`.

```js
const iter = dataset.queryLength(1n, 1000n);
for (const [ts, length] of iter) {
  console.log(`ts=${ts}: ${length} bytes`);
}
```

**Properties**:
- `iter.remaining: number` — Number of remaining records

---

## 6. Configuration

### 6.1 StoreConfig

```js
const config = {
  flushIntervalMs: 15000,           // 15 seconds
  idleTimeoutMs: 1800000,           // 30 minutes
  dataSegmentSize: 64 * 1024 * 1024,  // 64 MiB (number or bigint)
  indexSegmentSize: 4 * 1024 * 1024,   // 4 MiB (number or bigint)
  initialDataSegmentSize: 256 * 1024,  // 256 KiB (number or bigint)
  initialIndexSegmentSize: 4 * 1024,   // 4 KiB (number or bigint)
  compressLevel: 6,                  // 0-9
  compressType: 0,                   // 0=zstd, 1=deflate
  cacheMaxMemory: 256 * 1024 * 1024,  // 256 MiB (number or bigint)
  cacheIdleTimeoutMs: 1800000,       // 30 minutes
  retentionCheckHour: 0,             // UTC hour 0-23
  enableBackgroundThread: true,
  enableJournal: true,
  readOnly: null,                    // null=auto, true=force RO, false=require writable
};
```

### 6.2 CreateDatasetOptions

```js
const options = {
  dataSegmentSize: 128 * 1024 * 1024,  // 128 MiB (number or bigint)
  indexSegmentSize: 8 * 1024 * 1024,    // 8 MiB (number or bigint)
  initialDataSegmentSize: 512 * 1024,  // 512 KiB (number or bigint)
  initialIndexSegmentSize: 4 * 1024,   // 4 KiB (number or bigint)
  compressLevel: 9,                      // 0-9
  compressType: 0,                       // 0=zstd, 1=deflate
  indexContinuous: true,                 // false=sparse, true=continuous
  retentionWindow: 86400n,               // 1 day in timestamp units (number or bigint)
  enableJournal: true,
};
```

### 6.3 QueueConsumerOptions

```js
const options = {
  runningExpiredSeconds: 900,  // default 900, max 65535
  maxRetryCount: 3,           // default 3, max 255
};
```

---

## 7. Queue Types

### 7.1 Queue

Obtained via `store.openQueue(dataset)`.

**Key behavior**:
- `push(data)` auto-assigns `timestamp = latest_written_timestamp + 1`
- `poll(timeout_ms)` returns the next unacked record for this consumer group
- `ack(timestamp)` marks a record as processed
- Multiple consumer groups are independent; each maintains its own progress

#### `queue.push(data: Buffer | Uint8Array): bigint`

Push data to the queue. Returns the assigned timestamp.

#### `queue.openConsumer(groupName: string, options?: QueueConsumerOptions): QueueConsumer`

Open a consumer group.

#### `queue.dropConsumer(groupName: string): void`

Drop a consumer group.

#### `queue.close(): void`

Close the queue.

### 7.2 QueueConsumer

Obtained via `queue.openConsumer("group_name")`.

#### `consumer.poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>`

Async poll for the next unacked record. Returns `[timestamp, data]` or `null` on timeout.

#### `consumer.pollSync(timeoutMs?: number): [bigint, Buffer] | null`

Sync poll for the next unacked record. Returns `[timestamp, data]` or `null` on timeout.

#### `consumer.ack(timestamp: number | bigint): void`

Acknowledge a polled record.

#### `consumer.pollCallback(callback: (() => void) | null): void`

Set a callback to be called when new data is available.

---

## 8. Journal

### 8.1 Journal API

#### `store.journalLatestSequence(): bigint | null`

Get the latest journal sequence number.

#### `store.journalRead(sequence: number | bigint): [bigint, Buffer] | null`

Read a journal record by sequence.

#### `store.journalQuery(startSequence: number | bigint, endSequence: number | bigint): Array<[bigint, Buffer]>`

Range query journal records.

#### `store.readJournalSourceRecord(identifier: number | bigint, indexInfo: JournalIndexInfo): [bigint, Buffer]`

Dereference a journal record to its source data.

### 8.2 JournalQueue

#### `store.openJournalQueue(): JournalQueue`

Open a journal queue for consumption.

#### `journalQueue.openConsumer(groupName: string, options?: QueueConsumerOptions): JournalQueueConsumer`

Open a consumer group for journal consumption.

#### `journalQueue.close(): void`

Close the journal queue.

### 8.3 JournalQueueConsumer

#### `consumer.poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>`

Async poll for the next journal record. Returns `[sequence, payload]` or `null` on timeout.

#### `consumer.pollSync(timeoutMs?: number): [bigint, Buffer] | null`

Sync poll for the next journal record. Returns `[sequence, payload]` or `null` on timeout.

#### `consumer.ack(sequence: number | bigint): void`

Acknowledge a journal record.

#### `consumer.pollCallback(callback: (() => void) | null): void`

Set a callback to be called when new journal records are available.

---

## 9. Types

### 9.1 DataSetInfo

```ts
interface DataSetInfo {
  name: string
  datasetType: string
  baseDir: string
  identifier: bigint
  dataSize: bigint
  indexSize: bigint
  initialDataSize: bigint
  initialIndexSize: bigint
  compressType: number
  compressLevel: number
  indexContinuous: number
  retentionWindow: bigint
  enableJournal: boolean
  createTime: bigint
}
```

### 9.2 DataSetState

```ts
interface DataSetState {
  latestWrittenTimestamp: bigint | null
  openDataSegments: number
  dataSegments: number
  totalRecordCount: bigint
  totalDataSize: bigint
  totalUncompressedSize: bigint
  totalInvalidRecordCount: bigint
  minTimestamp: bigint | null
  maxTimestamp: bigint | null
  openIndexSegments: number
  indexSegments: number
  pendingIndexEntries: number
  baseTimestamp: bigint | null
  readOnly: boolean
  hasBlockCache: boolean
  hasJournal: boolean
  hasQueue: boolean
  queueConsumerGroups: number
}
```

### 9.3 TickResult

```ts
interface TickResult {
  executedTasks: number
  nextDelayMs: number
}
```

### 9.4 JournalIndexInfo

```ts
interface JournalIndexInfo {
  timestamp: bigint
  blockOffset: bigint
  inBlockOffset: number
}
```

---

## 10. Index Modes

### Sparse Mode (`indexContinuous = false`)

- Binary search on `(timestamp, block_offset)` pairs
- Supports arbitrary timestamp values
- Out-of-order writes allowed (if timestamp already exists)
- Choose when: timestamps are irregular, event-driven, or have large gaps

### Continuous Mode (`indexContinuous = true`)

- Mathematical formula: `position = (timestamp - base_timestamp) / time_step`
- Timestamps must be dense sequential integers
- `write(ts)` fills the appropriate position, creating filler prefixes as needed
- O(1) timestamp-to-position calculation within a segment
- Choose when: timestamps are dense sequential integers (e.g., per-second sensor readings with few gaps)