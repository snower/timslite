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
- `config`: Optional store configuration (uses defaults if omitted)

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

Returns `DataSetInspectResult` with dataset metadata and runtime state.

### 2.3 Queue Management

#### `store.openQueue(dataset: Dataset): Queue`

Opens a queue on an existing dataset. Only one queue can be open per dataset at a time.

**Errors**: `QueueAlreadyOpen` if a queue is already open for this dataset.

#### `store.openJournalQueue(): JournalQueue`

Opens the journal queue. Only one journal queue can be open per store.

### 2.4 Journal

#### `store.journalLatestSequence(): bigint | null`

Returns the latest journal sequence number, or `null` if no journal exists.

#### `store.journalRead(sequence: bigint): [bigint, Uint8Array] | null`

Reads a journal record by sequence number. Returns `[sequence, payload]` or `null`.

#### `store.journalQuery(startSequence: bigint, endSequence: bigint): [bigint, Uint8Array][]`

Returns all journal records in the range `[startSequence, endSequence]`.

#### `store.readJournalSourceRecord(datasetIdOrName: number | bigint, journalRecord: [bigint, Uint8Array]): [bigint, Uint8Array] | null`

Resolves the source dataset record referenced by a journal `write`/`append` entry (type `0x11`/`0x13`). The second argument can be the raw journal entry returned by `journalRead`/`journalQuery`.

### 2.5 Background Tasks

#### `store.tickBackgroundTasks(): TickResult`

Manually triggers background tasks (flush, idle-close, cache eviction, retention reclaim). Returns `TickResult` with `executedTasks` and `nextDelayMs`.

---

## 3. Dataset

### 3.1 Write Operations

#### `dataset.write(ts: bigint, data: Uint8Array): void`

Writes a record at timestamp `ts`.

**Parameters**:
- `ts`: Timestamp (must be `>= latest_written_timestamp`)
- `data`: Record payload (max 4 MiB)

**Errors**:
- `InvalidData` if `ts < latest_written_timestamp`
- `InvalidData` if `data.length > 4 MiB`
- `DatasetClosed` if dataset is closed

#### `dataset.append(ts: bigint, data: Uint8Array): void`

Appends data to the record at timestamp `ts`. If `ts > latest_written_timestamp`, creates a new record. If `ts == latest_written_timestamp`, appends to the existing uncompressed tail record.

**Parameters**:
- `ts`: Timestamp (must be `>= latest_written_timestamp`)
- `data`: Data to append

**Errors**:
- `InvalidData` if `ts < latest_written_timestamp`
- `InvalidData` if appending would exceed block capacity
- `DatasetClosed` if dataset is closed

#### `dataset.correct(ts: bigint, data: Uint8Array): void`

Corrects (overwrites) the record at timestamp `ts`.

**Parameters**:
- `ts`: Timestamp (must exist)
- `data`: New record payload

**Errors**:
- `NotFound` if no record at `ts`
- `Expired` if record has expired
- `DatasetClosed` if dataset is closed

#### `dataset.delete(ts: bigint): void`

Deletes the record at timestamp `ts`.

**Parameters**:
- `ts`: Timestamp to delete

**Errors**:
- `NotFound` if no record at `ts`
- `Expired` if record has expired
- `DatasetClosed` if dataset is closed

### 3.2 Read Operations

#### `dataset.read(ts: bigint): [bigint, Uint8Array] | null`

Reads a record by timestamp.

**Returns**: `[timestamp, data]` tuple, or `null` if not found or expired.

#### `dataset.readLatest(): [bigint, Uint8Array] | null`

Reads the record at `latest_written_timestamp`.

**Returns**: `[timestamp, data]` tuple, or `null` if deleted/expired.

#### `dataset.readExist(ts: bigint): boolean`

Checks if a record exists at timestamp `ts` (not deleted, not expired).

#### `dataset.readLength(ts: bigint): number | null`

Returns the uncompressed data length for the record at `ts`, or `null` if not found.

### 3.3 Query Operations

#### `dataset.query(startTs: bigint, endTs: bigint): QueryIterator`

Returns a lazy iterator over records in `[startTs, endTs]`.

**Usage**:
```js
for (const [ts, data] of dataset.query(1n, 100n)) {
  console.log(ts, data);
}
```

#### `dataset.queryAll(startTs: bigint, endTs: bigint): [bigint, Uint8Array][]`

Eagerly collects all records in `[startTs, endTs]` into an array.

#### `dataset.queryLength(startTs: bigint, endTs: bigint): [bigint, number][]`

Returns timestamp and uncompressed data length pairs for records in `[startTs, endTs]`.

#### `dataset.queryLengthAll(startTs: bigint, endTs: bigint): [bigint, number][]`

Same as `queryLength` but eager (returns array).

#### `dataset.queryLengthIter(startTs: bigint, endTs: bigint): QueryLengthIterator`

Returns a lazy iterator over timestamp and length pairs.

### 3.4 Inspection

#### `dataset.inspect(): DataSetInspectResult`

Returns dataset metadata and runtime state.

### 3.5 Lifecycle

#### `dataset.close(): void`

Closes the dataset handle. Does not flush—use `store.close()` or wait for background flush.

#### `dataset.closed: boolean`

Returns whether the dataset handle has been closed.

---

## 4. QueryIterator

```js
const iter = dataset.query(1n, 100n);
```

### 4.1 Iteration

Implements `Symbol.asyncIterator`, so you can use `for await...of`:

```js
for await (const [ts, data] of iter) {
  // process record
}
```

Or synchronous `for...of` (works because `next()` returns a plain object):

```js
for (const [ts, data] of iter) {
  // process record
}
```

### 4.2 Methods

#### `iter.next(): { value: [bigint, Uint8Array] | undefined, done: boolean }`

Advances to the next record.

#### `iter.reverse(): void`

Reverses iteration order. Must be called before first `next()`.

#### `iter.skip(count: number): void`

Skips the next `count` records. Must be called before first `next()`.

#### `iter.collectAll(): [bigint, Uint8Array][]`

Eagerly collects all remaining records into an array.

#### `iter.close(): void`

Closes the iterator and releases native resources. Automatically called when iteration completes.

---

## 5. QueryLengthIterator

```js
const iter = dataset.queryLengthIter(1n, 100n);
```

### 5.1 Iteration

Implements `Symbol.asyncIterator`:

```js
for await (const [ts, length] of iter) {
  console.log(`ts=${ts}, length=${length}`);
}
```

### 5.2 Methods

#### `iter.next(): { value: [bigint, number] | undefined, done: boolean }`

Advances to the next entry.

#### `iter.reverse(): void`

Reverses iteration order. Must be called before first `next()`.

#### `iter.skip(count: number): void`

Skips the next `count` entries. Must be called before first `next()`.

#### `iter.collectAll(): [bigint, number][]`

Eagerly collects all remaining entries into an array.

#### `iter.close(): void`

Closes the iterator and releases native resources.

---

## 6. Queue

### 6.1 Push

#### `queue.push(data: Uint8Array): bigint`

Pushes a record to the queue. Auto-assigns the next timestamp.

**Returns**: The assigned timestamp.

**Errors**:
- `StoreClosed` if store is closed
- `DatasetClosed` if dataset is closed

### 6.2 Consumer Management

#### `queue.openConsumer(groupName: string, options?: QueueConsumerOptions): QueueConsumer`

Opens a consumer for the given group.

**Parameters**:
- `groupName`: Consumer group name, must match `^[0-9A-Za-z_-]+$`
- `options`: Optional consumer configuration

**Errors**: `ConsumerGroupExists` if group already has an open consumer.

### 6.3 Lifecycle

#### `queue.close(): void`

Closes the queue handle.

---

## 7. QueueConsumer

### 7.1 Poll

#### `consumer.pollSync(timeoutMs: number): [bigint, Uint8Array] | null`

Synchronously polls for the next record. Blocks up to `timeoutMs` milliseconds.

**Returns**: `[timestamp, data]` tuple, or `null` on timeout.

#### `consumer.poll(timeoutMs: number): Promise<[bigint, Uint8Array] | null>`

Asynchronous poll. Returns a Promise that resolves to the next record or `null` on timeout.

### 7.2 Acknowledge

#### `consumer.ack(ts: bigint): void`

Acknowledges processing of the record at `ts`.

### 7.3 Management

#### `consumer.flush(): void`

Flushes the consumer state file to disk.

#### `consumer.drop(): void`

Removes the consumer group from the queue state.

#### `consumer.inspect(): QueueConsumerInspectResult`

Returns consumer configuration and runtime state.

### 7.4 Lifecycle

#### `consumer.close(): void`

Closes the consumer handle.

---

## 8. JournalQueue

### 8.1 Push

#### `journalQueue.push(data: Uint8Array): bigint`

Pushes a record to the journal queue.

**Returns**: The assigned journal sequence number.

### 8.2 Consumer Management

#### `journalQueue.openConsumer(): JournalQueueConsumer`

Opens a journal queue consumer.

### 8.3 Lifecycle

#### `journalQueue.close(): void`

Closes the journal queue handle.

---

## 9. JournalQueueConsumer

### 9.1 Poll

#### `consumer.poll(timeoutMs: number): Promise<[bigint, Uint8Array] | null>`

Polls for the next journal record. Returns a Promise.

**Returns**: `[sequence, payload]` tuple, or `null` on timeout.

### 9.2 Acknowledge

#### `consumer.ack(sequence: bigint): void`

Acknowledges processing of the journal record at `sequence`.

### 9.3 Read

#### `consumer.get(sequence: bigint): [bigint, Uint8Array] | null`

Reads a specific journal record by sequence number without advancing the consumer cursor.

### 9.4 Lifecycle

#### `consumer.close(): void`

Closes the journal consumer handle.

---

## 10. Configuration Interfaces

### StoreConfig

```js
{
  flushIntervalMs?: number,         // Flush interval in milliseconds (default: 5000)
  idleTimeoutMs?: number,           // Segment idle timeout in milliseconds (default: 60000)
  dataSegmentSize?: number | bigint, // Max data segment file size (default: 64 MiB)
  indexSegmentSize?: number | bigint, // Max index segment file size (default: 4 MiB)
  initialDataSegmentSize?: number | bigint, // Initial data segment size (default: 256 KiB)
  initialIndexSegmentSize?: number | bigint, // Initial index segment size (default: 4 KiB)
  compressLevel?: number,           // Compression level 0-9 (default: 6)
  compressType?: 0 | 1,             // 0=deflate, 1=zstd (default: 1)
  cacheMaxMemory?: number | bigint, // Max block cache memory (default: 256 MiB)
  cacheIdleTimeoutMs?: number,      // Cache entry idle timeout in milliseconds (default: 1800000)
  retentionCheckHour?: number,      // UTC hour 0-23 for retention check (default: 0)
  enableBackgroundThread?: boolean, // Enable background thread (default: true)
  enableJournal?: boolean,          // Enable journal (default: true)
  readOnly?: boolean | null,        // null=auto, true=force RO, false=require writable
}
```

### CreateDatasetOptions

```js
{
  dataSegmentSize?: number | bigint,
  indexSegmentSize?: number | bigint,
  initialDataSegmentSize?: number | bigint,
  initialIndexSegmentSize?: number | bigint,
  compressLevel?: number,
  compressType?: 0 | 1,             // 0=deflate, 1=zstd
  indexContinuous?: boolean,        // false=sparse, true=continuous (default: false)
  retentionWindow?: number | bigint, // 0=unlimited
  enableJournal?: boolean,          // default: false
}
```

### QueueConsumerOptions

```js
{
  runningExpiredSeconds?: number, // Seconds before pending entry is considered stuck (default: 300)
  maxRetryCount?: number,         // Max retry count before entry is parked (default: 5)
}
```

---

## 11. Data Types

### Record

`[bigint, Uint8Array]` — timestamp and data tuple.

### JournalRecord

`[bigint, Uint8Array]` — sequence number and payload tuple.

### DataSetInspectResult

```js
{
  info: DataSetInfo,
  state: DataSetState,
}
```

### DataSetInfo

```js
{
  name: string,
  datasetType: string,
  baseDir: string,
  identifier: bigint,
  dataSize: bigint,
  indexSize: bigint,
  initialDataSize: bigint,
  initialIndexSize: bigint,
  compressType: number,
  compressLevel: number,
  indexContinuous: number,       // 0=sparse, 1=continuous
  retentionWindow: bigint,
  enableJournal: boolean,
  createTime: bigint,
}
```

### DataSetState

```js
{
  latestWrittenTimestamp: bigint | null,
  openDataSegments: number,
  dataSegments: number,
  totalRecordCount: bigint,
  totalDataSize: bigint,
  totalUncompressedSize: bigint,
  totalInvalidRecordCount: bigint,
  minTimestamp: bigint | null,
  maxTimestamp: bigint | null,
  openIndexSegments: number,
  indexSegments: number,
  pendingIndexEntries: number,
  baseTimestamp: bigint | null,
  readOnly: boolean,
  hasBlockCache: boolean,
  hasJournal: boolean,
  hasQueue: boolean,
  queueConsumerGroups: number,
}
```

### QueueConsumerInspectResult

```js
{
  info: QueueConsumerInfo,
  state: QueueConsumerState,
}
```

### QueueConsumerInfo

```js
{
  groupName: string,
  runningExpiredSeconds: number,
  maxRetryCount: number,
}
```

### QueueConsumerState

```js
{
  processedTs: bigint,
  pendingEntries: QueueConsumerPendingEntry[],
}
```

### QueueConsumerPendingEntry

```js
{
  timestamp: bigint,
  startTime: bigint,
  status: number,
  retryCount: number,
}
```

### TickResult

```js
{
  executedTasks: number,
  nextDelayMs: number,
}
```

---

## 12. Error Handling

All errors are standard JavaScript `Error` objects with a `code` property indicating the error type.

### Error Codes

| Code | Description |
|------|-------------|
| `Io` | I/O error |
| `InvalidMagic` | Invalid file magic bytes |
| `InvalidVersion` | Unsupported file version |
| `Mmap` | Memory mapping error |
| `Compression` | Compression error |
| `Decompression` | Decompression error |
| `InvalidData` | Invalid data or parameters |
| `NotFound` | Dataset or record not found |
| `Expired` | Record has expired |
| `AlreadyExists` | Dataset already exists |
| `SegmentFull` | Segment is full |
| `QueueAlreadyOpen` | Queue already open for dataset |
| `QueueNotOpen` | Queue not open |
| `ConsumerGroupNotFound` | Consumer group not found |
| `ConsumerGroupExists` | Consumer group already exists |
| `QueueClosed` | Queue is closed |
| `PendingFull` | Pending queue is full |
| `StoreClosed` | Store is closed |
| `DatasetClosed` | Dataset is closed |
| `IteratorExhausted` | Iterator has no more items |

### Error Handling Pattern

```js
try {
  store.createDataset("test", "data");
} catch (err) {
  if (err.code === "AlreadyExists") {
    console.log("Dataset already exists");
  } else {
    console.error("Error:", err.message);
  }
}
```
