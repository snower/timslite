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

#### `Store.open(String dataDir, StoreConfig config) -> Store`

Opens or connects to a store at `dataDir`. Acquires a `.lock` file for exclusive write access. If the lock is already held and `config.readOnly` is `null` (auto), falls back to read-only mode.

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
- `options`: Dataset configuration options

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

#### `store.inspectDataset(String name, String datasetType) -> DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

### 2.3 Background Tasks

#### `store.tickBackgroundTasks() -> TickResult`

Manually executes pending background tasks. Returns `TickResult { executedTasks, nextDelayMs }`.

#### `store.nextBackgroundDelay() -> long`

Returns delay in milliseconds until the next background task is due.

---

## 3. Dataset

### 3.1 Identity

#### `ds.getIdentifier() -> long`

Numeric dataset ID assigned by Store.

#### `ds.getId() -> long`

Alias for `getIdentifier()`.

#### `ds.getDataDir() -> String`

Path to the dataset's data directory.

#### `ds.getLatestTimestamp() -> Long`

Returns the `latest_written_timestamp` for this dataset. Returns `null` if never written.

#### `ds.isClosed() -> boolean`

Returns whether the dataset has been closed.

### 3.2 Write Operations

#### `ds.write(long timestamp, byte[] data) -> void`

Writes a record at the given timestamp.

**Rules**:
- `timestamp >= latest_written_timestamp` → new write (or correction if equal)
- `timestamp < latest_written_timestamp` and index entry exists → out-of-order rewrite (sparse mode only)
- `timestamp < latest_written_timestamp` and no index entry → error
- `data.length <= 4 MiB` (otherwise `InvalidDataException`)

#### `ds.append(long timestamp, byte[] data) -> void`

Forward append or in-place tail append.

**Rules**:
- `timestamp < latest_written_timestamp` → error
- `timestamp > latest_written_timestamp` → forward append (new record)
- `timestamp == latest_written_timestamp` → in-place append (only if latest record is in uncompressed pending block)
- Empty data is a no-op (after timestamp/retention checks)
- `old_len + data.length <= 4 MiB`

#### `ds.delete(long timestamp) -> void`

Deletes a record by timestamp. Does NOT retroactively update `latest_written_timestamp`.

### 3.3 Read Operations

#### `ds.read(long timestamp) -> Record`

Reads a single complete record by exact timestamp.

**Returns**:
- `Record { timestamp, data }` if the record exists and is valid
- `null` if the record doesn't exist, is a filler, is deleted, or is expired

**Note**: `read(-1L)` reads the record at timestamp `-1`, NOT the latest. Use `readLatest()` for the latest.

#### `ds.readLatest() -> Record`

Reads the record at `latest_written_timestamp`. Returns `null` if never written, deleted, or expired.

#### `ds.readExist(long timestamp) -> boolean`

Fast existence check — index lookup only, no data segment I/O.

#### `ds.readLength(long timestamp) -> Long`

Reads only the data length of a record (reads the 12-byte record header, not the full data). Returns `null` if not found.

### 3.4 Query Operations

#### `ds.query(long startTs, long endTs) -> List<Record>`

Eager range query. Loads all data into memory. Returns `List<Record>` sorted by timestamp.

#### `ds.queryIter(long startTs, long endTs) -> QueryIterator`

Lazy range query iterator. Records are read on-demand via `hasNext()`/`next()`.

**Implements**: `AutoCloseable` for try-with-resources.

#### `ds.queryExist(long startTs, long endTs) -> byte[]`

Fast range existence bitmap. Returns a bitmap where bit `i` is set if a record exists at `startTs + i`.

#### `ds.queryLength(long startTs, long endTs) -> List<LengthEntry>`

Eager range query returning `List<LengthEntry>` with timestamp and data length.

#### `ds.queryLengthIter(long startTs, long endTs) -> QueryLengthIterator`

Lazy iterator over `LengthEntry` pairs.

**Implements**: `AutoCloseable` for try-with-resources.

### 3.5 Maintenance

#### `ds.flush() -> void`

Flushes all dirty segments for this dataset to disk.

#### `ds.inspect() -> DataSetInspectResult`

Returns `DataSetInspectResult` with dataset metadata and runtime statistics.

---

## 4. Record

Returned by read and query operations.

```java
public class Record {
    public long getTimestamp();  // record timestamp
    public byte[] getData();     // record data payload
}
```

---

## 5. LengthEntry

Returned by query length operations.

```java
public class LengthEntry {
    public long getTimestamp();  // record timestamp
    public int getLength();      // data length in bytes
}
```

---

## 6. QueryIterator

Lazy iterator returned by `queryIter()`.

```java
try (QueryIterator it = ds.queryIter(1L, 1000L)) {
    while (it.hasNext()) {
        Record rec = it.next();
        System.out.println(rec.getTimestamp() + ": " + new String(rec.getData()));
    }
}
```

**Implements**: `Iterator<Record>`, `AutoCloseable`.

---

## 7. QueryLengthIterator

Lazy iterator returned by `queryLengthIter()`.

```java
try (QueryLengthIterator it = ds.queryLengthIter(1L, 1000L)) {
    while (it.hasNext()) {
        LengthEntry entry = it.next();
        System.out.println(entry.getTimestamp() + ": " + entry.getLength() + " bytes");
    }
}
```

**Implements**: `Iterator<LengthEntry>`, `AutoCloseable`.

---

## 8. Configuration

### 8.1 StoreConfig / StoreConfigBuilder

```java
StoreConfig config = StoreConfigBuilder.builder()
        .flushIntervalMs(15000)           // 15 seconds
        .idleTimeoutMs(1800000)           // 30 minutes
        .dataSegmentSize(64 * 1024 * 1024)  // 64 MiB
        .indexSegmentSize(4 * 1024 * 1024)   // 4 MiB
        .initialDataSegmentSize(256 * 1024)  // 256 KiB
        .initialIndexSegmentSize(4 * 1024)   // 4 KiB
        .compressLevel(6)                  // 0-9
        .compressType(0)                   // 0=zstd, 1=deflate
        .cacheMaxMemory(256 * 1024 * 1024)  // 256 MiB
        .cacheIdleTimeoutMs(1800000)       // 30 minutes
        .retentionCheckHour(0)             // UTC hour 0-23
        .enableBackgroundThread(true)
        .enableJournal(true)
        .readOnly(null)                    // null=auto, true=force RO, false=require writable
        .build();
```

### 8.2 CreateDatasetOptions / CreateDatasetOptionsBuilder

```java
CreateDatasetOptions options = CreateDatasetOptionsBuilder.builder()
        .config(DatasetConfigBuilder.builder()
                .dataSegmentSize(128 * 1024 * 1024)  // 128 MiB
                .indexSegmentSize(8 * 1024 * 1024)    // 8 MiB
                .initialDataSegmentSize(512 * 1024)  // 512 KiB
                .initialIndexSegmentSize(4 * 1024)   // 4 KiB
                .compressLevel(9)                      // 0-9
                .compressType(0)                       // 0=zstd, 1=deflate
                .indexContinuous((byte) 1)             // 0=sparse, 1=continuous
                .retentionWindow(86400)                // 1 day in timestamp units
                .enableJournal(true)
                .build())
        .build();
```

### 8.3 QueueConsumerOptions / QueueConsumerOptionsBuilder

```java
QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
        .config(QueueConsumerConfigBuilder.builder()
                .runningExpiredSeconds(900)  // default 900, max 65535
                .maxRetryCount((short) 3)   // default 3, max 255
                .build())
        .build();
```

---

## 9. Queue Types

### 9.1 Queue

Obtained via `store.openQueue(dataset)`.

**Key behavior**:
- `push(data)` auto-assigns `timestamp = latest_written_timestamp + 1`
- `poll(timeout_ms)` returns the next unacked record for this consumer group
- `ack(timestamp)` marks a record as processed
- Multiple consumer groups are independent; each maintains its own progress

**Implements**: `AutoCloseable`.

#### `queue.push(byte[] data) -> long`

Push data to the queue. Returns the assigned timestamp.

#### `queue.openConsumer(String groupName, QueueConsumerOptions options) -> QueueConsumer`

Open a consumer group.

#### `queue.dropConsumer(String groupName) -> void`

Drop a consumer group.

#### `queue.close() -> void`

Close the queue.

### 9.2 QueueConsumer

Obtained via `queue.openConsumer("group_name", options)`.

**Implements**: `AutoCloseable`.

#### `consumer.poll(long timeoutMs) -> Record`

Poll for the next unacked record. Returns `Record` or `null` on timeout.

#### `consumer.ack(long timestamp) -> void`

Acknowledge a polled record.

#### `consumer.close() -> void`

Close the consumer.

---

## 10. Journal

### 10.1 Journal API

#### `store.journalLatestSequence() -> Long`

Get the latest journal sequence number. Returns `null` if journal is empty.

#### `store.journalRead(long sequence) -> JournalRecord`

Read a journal record by sequence. Returns `null` if not found.

#### `store.journalQuery(long startSequence, long endSequence) -> List<JournalRecord>`

Range query journal records.

#### `store.readJournalSourceRecord(long identifier, JournalIndexInfo indexInfo) -> Record`

Dereference a journal record to its source data.

### 10.2 JournalQueue

#### `store.openJournalQueue() -> JournalQueue`

Open a journal queue for consumption.

**Implements**: `AutoCloseable`.

#### `journalQueue.openConsumer(String groupName, QueueConsumerOptions options) -> JournalQueueConsumer`

Open a consumer group for journal consumption.

#### `journalQueue.close() -> void`

Close the journal queue.

### 10.3 JournalQueueConsumer

**Implements**: `AutoCloseable`.

#### `consumer.poll(long timeoutMs) -> JournalRecord`

Poll for the next journal record. Returns `JournalRecord` or `null` on timeout.

#### `consumer.ack(long sequence) -> void`

Acknowledge a journal record.

#### `consumer.close() -> void`

Close the consumer.

---

## 11. JournalRecord

Returned by journal operations.

```java
public class JournalRecord {
    public long getSequence();  // journal sequence number
    public byte[] getData();    // journal payload
}
```

---

## 12. Error Types

All errors are thrown as subclasses of `TmslException`, which extends `RuntimeException`.

```java
import io.github.snower.timslite.errors.*;

// Base exception
public class TmslException extends RuntimeException {
    public TmslErrorCode code();  // error code
}

// Specific exceptions
public class AlreadyExistsException extends TmslException {}
public class NotFoundException extends TmslException {}
public class InvalidDataException extends TmslException {}
public class SegmentFullException extends TmslException {}
public class ReadOnlyException extends TmslException {}
public class ExpiredException extends TmslException {}
```

### TmslErrorCode

```java
public enum TmslErrorCode {
    ALREADY_EXISTS,
    NOT_FOUND,
    INVALID_DATA,
    SEGMENT_FULL,
    READ_ONLY,
    EXPIRED,
    IO_ERROR,
    // ... other variants
}
```

---

## 13. Index Modes

### Sparse Mode (`indexContinuous = 0`)

- Binary search on `(timestamp, block_offset)` pairs
- Supports arbitrary timestamp values
- Out-of-order writes allowed (if timestamp already exists)
- Choose when: timestamps are irregular, event-driven, or have large gaps

### Continuous Mode (`indexContinuous = 1`)

- Mathematical formula: `position = (timestamp - base_timestamp) / time_step`
- Timestamps must be dense sequential integers
- `write(ts)` fills the appropriate position, creating filler prefixes as needed
- O(1) timestamp-to-position calculation within a segment
- Choose when: timestamps are dense sequential integers (e.g., per-second sensor readings with few gaps)