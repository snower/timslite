# Java API Reference

> Complete Java API signatures, parameters, return types, and semantics.

---

## 1. Module Exports

```java
import io.github.snower.timslite.*;
import io.github.snower.timslite.errors.*;
```

---

## 2. Store

### 2.1 Lifecycle

#### `Store.open(String dataDir) -> Store`

Opens or connects to a store at `dataDir` with default configuration.

**Parameters**:
- `dataDir`: Path to the data directory (created if not exists)

**Returns**: `Store` on success, throws `TmslException` on failure.

#### `Store.open(String dataDir, StoreConfig config) -> Store`

Opens or connects to a store at `dataDir` with explicit configuration. Acquires a `.lock` file for exclusive write access. If the lock is already held and `config.readOnly` is `null` (auto), falls back to read-only mode.

**Parameters**:
- `dataDir`: Path to the data directory (created if not exists)
- `config`: Store configuration

**Returns**: `Store` on success, throws `TmslException` on failure.

**Side effects**: Creates `dataDir` if missing. Creates `.lock` file. Initializes `BlockCache`, `JournalManager`, and `BackgroundTasks` (if enabled). Scans existing datasets.

#### `store.close() -> void`

Flushes all dirty segments, closes all open datasets, closes the journal, and releases the store lock.

**Implements**: `AutoCloseable` for try-with-resources.

#### `store.isReadOnly() -> boolean`

Returns whether this store resolved to read-only mode at open time.

### 2.2 Dataset Management

#### `store.createDataset(String name, String datasetType, CreateDatasetOptions options) -> void`

Creates a new dataset with explicit parameters.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `datasetType`: Dataset type, same naming rules
- `options`: Dataset configuration options (use `CreateDatasetOptionsBuilder`)

**Errors**:
- `AlreadyExistsException` if dataset already exists
- `InvalidDataException` if name/type invalid

#### `store.openDataset(String name, String datasetType) -> Dataset`

Opens an existing dataset and returns a `Dataset` object.

**Implements**: `AutoCloseable` for try-with-resources.

**Errors**: `NotFoundException` if dataset doesn't exist.

#### `store.openDatasetByIdentifier(long identifier) -> Dataset`

Opens a dataset by its numeric identifier (assigned at creation time).

#### `store.dropDataset(String name, String datasetType) -> void`

Deletes a dataset and all its files (data segments, index segments, meta, state, queue). Irreversible.

#### `store.getDatasetNames() -> List<String>`

Lists all dataset names in the store.

#### `store.getDatasetTypes(String name) -> List<String>`

Lists all dataset types for a given name.

#### `store.inspectDataset(String name, String datasetType) -> InspectResult`

Returns `InspectResult` with dataset metadata and runtime statistics.

### 2.3 Queue Management

#### `store.openQueue(Dataset dataset) -> Queue`

Opens a queue for the given dataset.

**Parameters**:
- `dataset`: The dataset to open a queue for

**Returns**: `Queue` handle

#### `store.openQueue(long datasetIdentifier) -> Queue`

Opens a queue for the dataset identified by its numeric identifier.

### 2.4 Journal Access

#### `store.openJournalQueue() -> JournalQueue`

Opens a journal queue for consuming change-log records.

**Errors**: `QueueAlreadyOpenException` if journal queue is already open.

#### `store.journalLatestSequence() -> Long`

Returns the latest journal sequence number, or `null` if journal is empty.

#### `store.journalRead(long sequence) -> JournalRecord`

Reads a single journal record by sequence number.

**Returns**: `JournalRecord` or `null` if not found.

#### `store.journalQuery(long startSequence, long endSequence) -> List<JournalRecord>`

Queries journal records in the range `[startSequence, endSequence)`.

#### `store.readJournalSourceRecord(long datasetIdentifier, String indexInfo) -> Record`

Reads the source dataset record referenced by a journal entry. Used to fetch the actual business data when processing journal change-log entries.

**Returns**: `Record` or `null` if source record is deleted/expired.

### 2.5 Background Tasks

#### `store.tickBackgroundTasks() -> TickResult`

Manually triggers background tasks (flush, idle-close, cache eviction, retention reclaim). Use when `enableBackgroundThread` is `false`.

**Returns**: `TickResult` with executed task count and next recommended delay.

---

## 3. StoreConfig

Use `StoreConfigBuilder` to create a `StoreConfig`:

```java
StoreConfig config = StoreConfigBuilder.builder()
        .flushIntervalSecs(15)
        .idleTimeoutSecs(1800)
        .dataSegmentSize(64 * 1024 * 1024)
        .indexSegmentSize(4 * 1024 * 1024)
        .initialDataSegmentSize(256 * 1024)
        .initialIndexSegmentSize(4 * 1024)
        .compressLevel(6)
        .cacheMaxMemory(256 * 1024 * 1024)
        .cacheIdleTimeoutSecs(1800)
        .retentionCheckHour((byte) 0)
        .enableBackgroundThread(true)
        .enableJournal(true)
        .readOnly(null)  // null=auto, true=force RO, false=require writable
        .build();
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flushIntervalSecs` | `long` | 15 | Seconds between periodic flushes (0 = disabled) |
| `idleTimeoutSecs` | `long` | 1800 | Seconds before idle segment is closed |
| `dataSegmentSize` | `long` | 64 MiB | Max data segment file size |
| `indexSegmentSize` | `long` | 4 MiB | Max index segment file size |
| `initialDataSegmentSize` | `long` | 256 KiB | Initial data segment allocation |
| `initialIndexSegmentSize` | `long` | 4 KiB | Initial index segment allocation |
| `compressLevel` | `byte` | 6 | Compression level (0-9) |
| `cacheMaxMemory` | `long` | 256 MiB | Max block cache memory |
| `cacheIdleTimeoutSecs` | `long` | 1800 | Seconds before idle cache entry is evicted |
| `retentionCheckHour` | `byte` | 0 | UTC hour for retention check (0-23) |
| `enableBackgroundThread` | `boolean` | true | Enable background task thread |
| `enableJournal` | `boolean` | true | Enable journal change-log |
| `readOnly` | `Boolean` | null | null=auto, true=force RO, false=require writable |

---

## 4. Dataset

### 4.1 Write Operations

#### `dataset.write(long timestamp, byte[] data) -> void`

Writes a record at the given timestamp. If a record already exists at that timestamp, a correction is applied.

**Parameters**:
- `timestamp`: Signed 64-bit timestamp
- `data`: Record payload, up to 4 MiB

**Errors**:
- `InvalidDataException` if data exceeds 4 MiB
- `ExpiredException` if timestamp falls outside retention window
- `InvalidDataException` if timestamp < latest_written_timestamp (sparse mode)

#### `dataset.append(long timestamp, byte[] data) -> void`

Appends data to the latest record or creates a new one.

- If `timestamp > latest_written_timestamp`: creates new record
- If `timestamp == latest_written_timestamp`: appends to existing uncompressed record
- If `timestamp < latest_written_timestamp`: throws error

**Errors**:
- `InvalidDataException` if append would exceed 4 MiB block limit
- `InvalidDataException` if timestamp < latest_written_timestamp

#### `dataset.correct(long timestamp, byte[] data) -> void`

Corrects a record at the given timestamp. Equivalent to `write()` but semantically indicates a correction.

#### `dataset.delete(long timestamp) -> void`

Deletes the record at the given timestamp.

**Errors**: `ExpiredException` if timestamp falls outside retention window.

### 4.2 Read Operations

#### `dataset.read(long timestamp) -> Record`

Reads the record at the exact timestamp.

**Returns**: `Record` or `null` if not found or expired.

**Note**: `read(-1L)` reads the record at timestamp `-1`, not the latest.

#### `dataset.readLatest() -> Record`

Reads the record at `latest_written_timestamp`.

**Returns**: `Record` or `null` if latest is deleted/expired.

#### `dataset.readExist(long timestamp) -> boolean`

Checks if a record exists at the given timestamp without reading data.

**Returns**: `true` if record exists, `false` otherwise.

#### `dataset.readLength(long timestamp) -> int`

Reads the length of the record at the given timestamp.

**Returns**: Record length in bytes, or `0` if not found.

### 4.3 Query Operations

#### `dataset.query(long startTimestamp, long endTimestamp) -> List<Record>`

Eagerly loads all records in the range `[startTimestamp, endTimestamp]`.

**Returns**: `List<Record>` (may be empty).

**Note**: For large ranges, prefer `queryIter()` to avoid loading everything into memory.

#### `dataset.queryIter(long startTimestamp, long endTimestamp) -> QueryIterator`

Returns a lazy iterator over records in the range `[startTimestamp, endTimestamp]`.

**Implements**: `AutoCloseable` for try-with-resources.

#### `dataset.queryLength(long startTimestamp, long endTimestamp) -> QueryLengthIterator`

Returns a lazy iterator over record lengths (reads only 12-byte headers).

**Implements**: `AutoCloseable` for try-with-resources.

#### `dataset.queryLengthIter(long startTimestamp, long endTimestamp) -> QueryLengthIterator`

Alias for `queryLength()`.

#### `dataset.queryLengthAll(long startTimestamp, long endTimestamp) -> List<LengthEntry>`

Eagerly loads all record lengths in the range.

**Returns**: `List<LengthEntry>` with timestamp and length pairs.

### 4.4 Inspection

#### `dataset.inspect() -> InspectResult`

Returns `InspectResult` with dataset configuration and runtime state.

### 4.5 Lifecycle

#### `dataset.close() -> void`

Closes this dataset handle.

#### `dataset.isClosed() -> boolean`

Returns whether this dataset has been closed.

---

## 5. CreateDatasetOptions

Use `CreateDatasetOptionsBuilder` to create options:

```java
CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(DatasetConfigBuilder.builder()
                .dataSegmentSize(128 * 1024 * 1024)
                .indexSegmentSize(8 * 1024 * 1024)
                .compressLevel((byte) 9)
                .indexContinuous((byte) 1)  // 0=sparse, 1=continuous
                .retentionWindow(86400)
                .enableJournal(true)
                .build())
        .build();
```

---

## 6. DatasetConfig

Use `DatasetConfigBuilder` to create a `DatasetConfig`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dataSegmentSize` | `long` | inherit | Max data segment file size |
| `indexSegmentSize` | `long` | inherit | Max index segment file size |
| `initialDataSegmentSize` | `long` | inherit | Initial data segment allocation |
| `initialIndexSegmentSize` | `long` | inherit | Initial index segment allocation |
| `compressLevel` | `byte` | inherit | Compression level (0-9) |
| `compressType` | `byte` | auto | Compression type (auto-selected if not set) |
| `indexContinuous` | `byte` | 0 | Index mode: 0=sparse, 1=continuous |
| `retentionWindow` | `long` | 0 | Retention window in timestamp units (0=no limit) |
| `enableJournal` | `boolean` | false | Enable journal for this dataset |

---

## 7. Queue

### 7.1 Push

#### `queue.push(byte[] data) -> long`

Pushes a record to the queue. Auto-assigns `timestamp = latest_written_timestamp + 1`.

**Returns**: The assigned timestamp.

### 7.2 Consumer Management

#### `queue.openConsumer(String groupName, QueueConsumerOptions options) -> QueueConsumer`

Opens a consumer for the given group.

**Parameters**:
- `groupName`: Consumer group name, must match `^[0-9A-Za-z_-]+$`
- `options`: Consumer configuration options

**Returns**: `QueueConsumer` handle.

#### `queue.openConsumer(String groupName) -> QueueConsumer`

Opens a consumer with default options.

### 7.3 Lifecycle

#### `queue.close() -> void`

Closes this queue handle.

#### `queue.isClosed() -> boolean`

Returns whether this queue has been closed.

---

## 8. QueueConsumer

### 8.1 Poll

#### `consumer.poll(long timeoutMs) -> Record`

Polls for the next record, blocking up to the specified timeout.

**Parameters**:
- `timeoutMs`: Maximum time to wait in milliseconds; 0 returns immediately

**Returns**: `Record` or `null` if timeout expires.

### 8.2 Acknowledge

#### `consumer.ack(long timestamp) -> void`

Acknowledges processing of the record at the given timestamp. Advances the consumer position.

### 8.3 Management

#### `consumer.flush() -> void`

Flushes consumer state to disk.

#### `consumer.drop() -> void`

Drops this consumer group. Removes the consumer state file.

#### `consumer.inspect() -> QueueConsumerInspectResult`

Inspects the consumer group state.

**Returns**: `QueueConsumerInspectResult` with info and state.

### 8.4 Lifecycle

#### `consumer.close() -> void`

Closes this consumer handle.

#### `consumer.isClosed() -> boolean`

Returns whether this consumer has been closed.

---

## 9. QueueConsumerOptions

Use `QueueConsumerOptionsBuilder` to create options:

```java
QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
        .config(QueueConsumerConfigBuilder.builder()
                .runningExpiredSeconds(30)
                .maxRetryCount((short) 3)
                .build())
        .build();
```

---

## 10. QueueConsumerConfig

Use `QueueConsumerConfigBuilder` to create a config:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `runningExpiredSeconds` | `long` | 0 | Seconds before a running record is considered expired (0=disabled) |
| `maxRetryCount` | `short` | 0 | Max retry count for expired records (0=unlimited) |

---

## 11. JournalQueue

### 11.1 Consumer Management

#### `journalQueue.openConsumer(String groupName, QueueConsumerOptions options) -> JournalQueueConsumer`

Opens a journal consumer for the given group.

**Parameters**:
- `groupName`: Consumer group name
- `options`: Consumer configuration options

**Returns**: `JournalQueueConsumer` handle.

#### `journalQueue.openConsumer(String groupName) -> JournalQueueConsumer`

Opens a journal consumer with default options.

### 11.2 Lifecycle

#### `journalQueue.close() -> void`

Closes this journal queue handle.

#### `journalQueue.isClosed() -> boolean`

Returns whether this journal queue has been closed.

---

## 12. JournalQueueConsumer

### 12.1 Poll

#### `consumer.poll(long timeoutMs) -> JournalRecord`

Polls for the next journal record, blocking up to the specified timeout.

**Parameters**:
- `timeoutMs`: Maximum time to wait in milliseconds; 0 returns immediately

**Returns**: `JournalRecord` or `null` if timeout expires.

### 12.2 Acknowledge

#### `consumer.ack(long sequence) -> void`

Acknowledges processing of the journal record at the given sequence.

### 12.3 Lifecycle

#### `consumer.close() -> void`

Closes this journal consumer handle.

#### `consumer.isClosed() -> boolean`

Returns whether this journal consumer has been closed.

---

## 13. QueryIterator

Iterator over query results. Implements `AutoCloseable` for try-with-resources.

```java
try (QueryIterator it = ds.queryIter(startTs, endTs)) {
    while (it.hasNext()) {
        Record rec = it.next();
        // process record
    }
}
```

### Methods

#### `it.hasNext() -> boolean`

Returns `true` if more records are available.

#### `it.next() -> Record`

Returns the next record, or `null` if exhausted.

#### `it.reverse() -> QueryIterator`

Reverses the iteration direction. Returns `this` for chaining.

#### `it.skip(long count) -> QueryIterator`

Skips the next `count` records. Returns `this` for chaining.

#### `it.close() -> void`

Closes the iterator and releases native resources.

#### `it.isClosed() -> boolean`

Returns whether the iterator is closed.

**Chaining example**:
```java
try (QueryIterator it = ds.queryIter(startTs, endTs)) {
    List<Record> results = it.reverse().skip(5).collectAll();
}
```

---

## 14. QueryLengthIterator

Iterator over query-length results. Implements `AutoCloseable`.

```java
try (QueryLengthIterator it = ds.queryLength(startTs, endTs)) {
    while (it.hasNext()) {
        LengthEntry entry = it.next();
        // process entry
    }
}
```

### Methods

#### `it.hasNext() -> boolean`

Returns `true` if more entries are available.

#### `it.next() -> LengthEntry`

Returns the next length entry, or `null` if exhausted.

#### `it.reverse() -> QueryLengthIterator`

Reverses the iteration direction. Returns `this` for chaining.

#### `it.skip(long count) -> QueryLengthIterator`

Skips the next `count` entries. Returns `this` for chaining.

#### `it.close() -> void`

Closes the iterator and releases native resources.

#### `it.isClosed() -> boolean`

Returns whether the iterator is closed.

**Chaining example**:
```java
try (QueryLengthIterator it = ds.queryLength(startTs, endTs)) {
    List<LengthEntry> results = it.reverse().skip(5).collectAll();
}
```

Returns whether the iterator is closed.

---

## 15. Data Types

### Record

A time-series record containing a timestamp and payload data.

| Method | Returns | Description |
|--------|---------|-------------|
| `getTimestamp()` | `long` | Record timestamp |
| `getData()` | `byte[]` | Defensive copy of payload |

### JournalRecord

A journal change-log record containing a sequence number and payload data.

| Method | Returns | Description |
|--------|---------|-------------|
| `getSequence()` | `long` | Journal sequence number (starts at 1) |
| `getData()` | `byte[]` | Defensive copy of payload |

### LengthEntry

A timestamp and record-length pair.

| Method | Returns | Description |
|--------|---------|-------------|
| `getTimestamp()` | `long` | Record timestamp |
| `getLength()` | `int` | Record length in bytes |

### InspectResult

Result of inspecting a dataset.

| Method | Returns | Description |
|--------|---------|-------------|
| `getInfo()` | `DatasetInfo` | Dataset configuration and metadata |
| `getState()` | `DatasetState` | Runtime state and statistics |

### DatasetInfo

Configuration and metadata about a dataset.

| Method | Returns | Description |
|--------|---------|-------------|
| `getName()` | `String` | Dataset name |
| `getDatasetType()` | `String` | Dataset type |
| `getBaseDir()` | `String` | Base directory path |
| `getIdentifier()` | `long` | Numeric dataset identifier |
| `getDataSegmentSize()` | `long` | Max data segment size |
| `getIndexSegmentSize()` | `long` | Max index segment size |
| `getInitialDataSegmentSize()` | `long` | Initial data segment size |
| `getInitialIndexSegmentSize()` | `long` | Initial index segment size |
| `getCompressType()` | `short` | Compression type |
| `getCompressLevel()` | `short` | Compression level |
| `getIndexContinuous()` | `short` | Index mode (0=sparse, 1=continuous) |
| `getRetentionWindow()` | `long` | Retention window |
| `getEnableJournal()` | `boolean` | Journal enabled |
| `getCreateTime()` | `long` | Creation timestamp |

### DatasetState

Runtime state and statistics of a dataset.

| Method | Returns | Description |
|--------|---------|-------------|
| `getLatestWrittenTimestamp()` | `Long` | Latest written timestamp (nullable) |
| `getOpenDataSegments()` | `int` | Number of open data segments |
| `getDataSegments()` | `int` | Total data segment count |
| `getTotalRecordCount()` | `long` | Total record count |
| `getTotalDataSize()` | `long` | Total compressed data size |
| `getTotalUncompressedSize()` | `long` | Total uncompressed size |
| `getTotalInvalidRecordCount()` | `long` | Invalid record count |
| `getMinTimestamp()` | `Long` | Minimum timestamp (nullable) |
| `getMaxTimestamp()` | `Long` | Maximum timestamp (nullable) |
| `getOpenIndexSegments()` | `int` | Open index segment count |
| `getIndexSegments()` | `int` | Total index segment count |
| `getPendingIndexEntries()` | `int` | Pending index entries |
| `getBaseTimestamp()` | `Long` | Base timestamp (nullable) |
| `isReadOnly()` | `boolean` | Whether dataset is read-only |
| `isHasBlockCache()` | `boolean` | Whether block cache is enabled |
| `isHasJournal()` | `boolean` | Whether journal is enabled |
| `isHasQueue()` | `boolean` | Whether queue is open |
| `getQueueConsumerGroups()` | `int` | Number of queue consumer groups |

### QueueConsumerInspectResult

Result of inspecting a queue consumer group.

| Method | Returns | Description |
|--------|---------|-------------|
| `getInfo()` | `QueueConsumerInfo` | Consumer group configuration |
| `getState()` | `QueueConsumerState` | Consumer group state |

### QueueConsumerInfo

Public configuration for a queue consumer group.

| Method | Returns | Description |
|--------|---------|-------------|
| `getGroupName()` | `String` | Consumer group name |
| `getRunningExpiredSeconds()` | `long` | Running expiry time |
| `getMaxRetryCount()` | `int` | Max retry count |

### QueueConsumerState

Durable queue consumer state.

| Method | Returns | Description |
|--------|---------|-------------|
| `getProcessedTs()` | `long` | Last processed timestamp |
| `getPendingEntries()` | `List<QueueConsumerPendingEntry>` | Pending entries list |

### QueueConsumerPendingEntry

Pending queue record state.

| Method | Returns | Description |
|--------|---------|-------------|
| `getTimestamp()` | `long` | Record timestamp |
| `getStartTime()` | `long` | Processing start time |
| `getStatus()` | `short` | Entry status |
| `getRetryCount()` | `short` | Retry count |

### TickResult

Result of a background task tick.

| Method | Returns | Description |
|--------|---------|-------------|
| `getExecutedTasks()` | `long` | Number of tasks executed |
| `getNextDelayMs()` | `long` | Recommended delay before next tick (ms) |

---

## 16. Error Handling

All errors are thrown as subclasses of `TmslException` (extends `RuntimeException`).

### Error Hierarchy

```
TmslException
├── IoException
├── InvalidMagicException
├── InvalidVersionException
├── MmapException
├── CompressionException
├── DecompressionException
├── InvalidDataException
├── NotFoundException
├── ExpiredException
├── AlreadyExistsException
├── SegmentFullException
├── QueueAlreadyOpenException
├── QueueNotOpenException
├── ConsumerGroupNotFoundException
├── ConsumerGroupExistsException
├── QueueClosedException
├── PendingFullException
├── StoreClosedException
├── DatasetClosedException
├── QueueBridgeClosedException
└── IteratorExhaustedException
```

### Error Codes

Use `e.code()` to inspect the error category:

| Code | Exception | Description |
|------|-----------|-------------|
| `IO` | `IoException` | I/O error |
| `INVALID_MAGIC` | `InvalidMagicException` | Invalid file magic |
| `INVALID_VERSION` | `InvalidVersionException` | Unsupported format version |
| `MMAP` | `MmapException` | Memory-mapping error |
| `COMPRESSION` | `CompressionException` | Compression error |
| `DECOMPRESSION` | `DecompressionException` | Decompression error |
| `INVALID_DATA` | `InvalidDataException` | Invalid or corrupt data |
| `NOT_FOUND` | `NotFoundException` | Record/dataset not found |
| `EXPIRED` | `ExpiredException` | Timestamp outside retention |
| `ALREADY_EXISTS` | `AlreadyExistsException` | Already exists |
| `SEGMENT_FULL` | `SegmentFullException` | Data segment full |
| `QUEUE_ALREADY_OPEN` | `QueueAlreadyOpenException` | Queue already open |
| `QUEUE_NOT_OPEN` | `QueueNotOpenException` | Queue not open |
| `CONSUMER_GROUP_NOT_FOUND` | `ConsumerGroupNotFoundException` | Consumer group not found |
| `CONSUMER_GROUP_EXISTS` | `ConsumerGroupExistsException` | Consumer group exists |
| `QUEUE_CLOSED` | `QueueClosedException` | Queue closed |
| `PENDING_FULL` | `PendingFullException` | Pending queue full |
| `STORE_CLOSED` | `StoreClosedException` | Store closed |
| `DATASET_CLOSED` | `DatasetClosedException` | Dataset closed |
| `QUEUE_BRIDGE_CLOSED` | `QueueBridgeClosedException` | Queue bridge closed |
| `ITERATOR_EXHAUSTED` | `IteratorExhaustedException` | Iterator exhausted |

### Example

```java
try {
    ds.read(1700000000L);
} catch (TmslException e) {
    if (e.code() == TmslErrorCode.EXPIRED) {
        // timestamp outside retention window
    }
}
```
