# .NET API Reference

> Complete API signatures, parameters, return types, and semantics for the .NET wrapper.

---

## 1. Store

The `Store` is the top-level facade. All dataset lifecycle, queue lifecycle, and journal operations go through `Store`.

### 1.1 Lifecycle

#### `Store.Open(string path) -> Store`

Opens or connects to a store at `path` with default configuration.

**Parameters**:
- `path`: Path to the data directory (created if not exists)

**Returns**: `Store` instance.

**Throws**: `TmslException` on failure.

#### `Store.Open(string path, StoreConfig config) -> Store`

Opens or connects to a store at `path` with custom configuration.

**Parameters**:
- `path`: Path to the data directory (created if not exists)
- `config`: Store configuration

**Returns**: `Store` instance.

**Throws**: `TmslException` on failure.

#### `Store.Dispose()`

Closes the store, flushes all dirty segments, closes all open datasets, closes the journal, and releases the store lock.

**Side effects**: Stops background thread (if running), flushes pending data, releases resources.

#### `Store.IsClosed -> bool`

Returns whether this store has been disposed.

#### `Store.IsReadOnly() -> bool`

Returns whether this store resolved to read-only mode at open time. Read-only stores cannot create/drop datasets, write data, or open queues.

**Throws**: `ObjectDisposedException` if store is closed.

### 1.2 Dataset Management

#### `Store.CreateDataset(string name, string datasetType)`

Creates a new dataset with default parameters.

**Parameters**:
- `name`: Dataset name, must match `^[0-9A-Za-z_-]+$`, max 255 bytes
- `datasetType`: Dataset type, same naming rules

**Throws**:
- `TmslException` with `TmslErrorCode.AlreadyExists` if dataset already exists
- `TmslException` with `TmslErrorCode.InvalidData` if name/type invalid

#### `Store.CreateDataset(string name, string datasetType, CreateDatasetOptions options)`

Creates a new dataset with custom options.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type
- `options`: Creation options including optional `DatasetConfig`

**Throws**: Same as above.

#### `Store.OpenDataset(string name, string datasetType) -> Dataset`

Opens an existing dataset.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type

**Returns**: `Dataset` instance.

**Throws**:
- `TmslException` with `TmslErrorCode.NotFound` if dataset does not exist

#### `Store.OpenDatasetByIdentifier(ulong identifier) -> Dataset`

Opens an existing dataset by its numeric identifier.

**Parameters**:
- `identifier`: Dataset identifier (obtained from `Dataset.Identifier` or inspection)

**Returns**: `Dataset` instance.

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if not found.

#### `Store.DropDataset(string name, string datasetType)`

Drops a dataset and removes its files.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if not found.

#### `Store.GetDatasetNames() -> string[]`

Returns all dataset names in the store.

**Returns**: Array of dataset name strings.

#### `Store.GetDatasetTypes(string name) -> string[]`

Returns all dataset types for a given name.

**Parameters**:
- `name`: Dataset name

**Returns**: Array of dataset type strings.

#### `Store.InspectDataset(string name, string datasetType) -> DataSetInspectResult`

Returns static config and runtime state for a dataset.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type

**Returns**: `DataSetInspectResult` with `Info` and `State`.

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if not found.

### 1.3 Queue Management

#### `Store.OpenQueue(Dataset dataset) -> Queue`

Opens a queue for the given dataset.

**Parameters**:
- `dataset`: An opened `Dataset` instance

**Returns**: `Queue` instance.

**Throws**: `TmslException` with `TmslErrorCode.QueueAlreadyOpen` if already opened.

### 1.4 Journal Management

#### `Store.OpenJournalQueue() -> JournalQueue`

Opens the journal queue for consuming journal entries.

**Returns**: `JournalQueue` instance.

**Throws**: `TmslException` if journal is not enabled or in read-only mode.

#### `Store.JournalLatestSequence() -> long?`

Returns the latest journal sequence number.

**Returns**: `long?` — `null` if journal is empty, otherwise the latest sequence.

#### `Store.JournalRead(long sequence) -> JournalRecord?`

Reads a specific journal record by sequence.

**Parameters**:
- `sequence`: Journal sequence number

**Returns**: `JournalRecord?` — `null` if not found.

#### `Store.JournalQuery(long startSequence, long endSequence) -> IReadOnlyList<JournalRecord>`

Queries journal records in a sequence range (inclusive).

**Parameters**:
- `startSequence`: Start sequence (inclusive)
- `endSequence`: End sequence (inclusive)

**Returns**: List of journal records.

#### `Store.ReadJournalSourceRecord(ulong datasetIdentifier, JournalIndexInfo indexInfo) -> Record`

Reads the source dataset record referenced by a journal entry. Used to resolve `0x11`, `0x12`, `0x13` journal records.

**Parameters**:
- `datasetIdentifier`: Dataset identifier from the journal entry
- `indexInfo`: `JournalIndexInfo` with `Timestamp`, `BlockOffset`, `InBlockOffset`

**Returns**: `Record` with the source data.

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if source record is missing.

### 1.5 Background Tasks

#### `Store.TickBackgroundTasks() -> TickResult`

Manually triggers background tasks (flush, idle-close, cache eviction, retention reclaim).

**Returns**: `TickResult` with `ExecutedTasks` count and `NextDelayMs`.

#### `Store.NextBackgroundDelayMs() -> ulong`

Returns the delay in milliseconds until the next background task is due.

**Returns**: Delay in milliseconds.

---

## 2. Dataset

A `Dataset` is a handle for read/write operations on a specific dataset.

### 2.1 Properties

#### `Dataset.Identifier -> ulong`

Returns the dataset's numeric identifier.

#### `Dataset.IsClosed -> bool`

Returns whether this dataset handle has been disposed.

### 2.2 Write Operations

#### `Dataset.Write(long timestamp, byte[] data)`

Writes a record. Timestamp must be `>= latest_written_timestamp`.

**Parameters**:
- `timestamp`: Record timestamp (must be monotonically increasing)
- `data`: Record data (max 4 MiB)

**Throws**:
- `TmslException` with `TmslErrorCode.InvalidData` if timestamp < latest
- `TmslException` with `TmslErrorCode.InvalidData` if data > 4 MiB

#### `Dataset.WriteNow(byte[] data)`

Writes a record with an auto-generated timestamp (current time in dataset units).

**Parameters**:
- `data`: Record data

#### `Dataset.Append(long timestamp, byte[] data)`

Appends data to an existing record or creates a new one.

**Parameters**:
- `timestamp`: Must be `>= latest_written_timestamp`
- `data`: Data to append

**Behavior**:
- `timestamp == latest_written_timestamp`: Appends to uncompressed tail record
- `timestamp > latest_written_timestamp`: Creates new record

#### `Dataset.AppendNow(byte[] data)`

Appends with an auto-generated timestamp.

**Parameters**:
- `data`: Data to append

#### `Dataset.Correct(long timestamp, byte[] data)`

Overwrites an existing record's data (correction).

**Parameters**:
- `timestamp`: Existing record timestamp
- `data`: New data

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if record doesn't exist.

#### `Dataset.Delete(long timestamp)`

Deletes a record (soft delete).

**Parameters**:
- `timestamp`: Record timestamp to delete

**Throws**: `TmslException` with `TmslErrorCode.NotFound` if record doesn't exist.

#### `Dataset.Flush()`

Flushes pending data to disk.

### 2.3 Read Operations

#### `Dataset.Read(long timestamp) -> Record?`

Reads a record by timestamp.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: `Record?` — `null` if not found or expired.

#### `Dataset.ReadLatest() -> Record?`

Reads the latest record.

**Returns**: `Record?` — `null` if no records or latest is deleted/expired.

#### `Dataset.ReadExist(long timestamp) -> bool`

Checks if a record exists at the given timestamp.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: `true` if record exists and is not expired.

#### `Dataset.ReadLength(long timestamp) -> uint?`

Reads the length of a record without reading its data.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: `uint?` — `null` if not found.

### 2.4 Query Operations

#### `Dataset.Query(long startTs, long endTs) -> IReadOnlyList<Record>`

Queries records in a timestamp range (inclusive, eager).

**Parameters**:
- `startTs`: Start timestamp (inclusive)
- `endTs`: End timestamp (inclusive)

**Returns**: List of records (all loaded into memory).

#### `Dataset.QueryExist(long startTs, long endTs) -> byte[]`

Queries existence flags for a timestamp range.

**Parameters**:
- `startTs`: Start timestamp (inclusive)
- `endTs`: End timestamp (inclusive)

**Returns**: Byte array where each byte is `1` if record exists, `0` otherwise.

#### `Dataset.QueryLength(long startTs, long endTs) -> IReadOnlyList<LengthEntry>`

Queries record lengths without reading data.

**Parameters**:
- `startTs`: Start timestamp (inclusive)
- `endTs`: End timestamp (inclusive)

**Returns**: List of `LengthEntry` with `Timestamp` and `Length`.

#### `Dataset.QueryIter(long startTs, long endTs) -> QueryIterator`

Returns a lazy iterator for query results.

**Parameters**:
- `startTs`: Start timestamp (inclusive)
- `endTs`: End timestamp (inclusive)

**Returns**: `QueryIterator` (implements `IEnumerator<Record>`, `IEnumerable<Record>`).

#### `Dataset.QueryLengthIter(long startTs, long endTs) -> QueryLengthIterator`

Returns a lazy iterator for query lengths.

**Parameters**:
- `startTs`: Start timestamp (inclusive)
- `endTs`: End timestamp (inclusive)

**Returns**: `QueryLengthIterator` (implements `IEnumerator<LengthEntry>`, `IEnumerable<LengthEntry>`).

---

## 3. QueryIterator

Lazy iterator for query results. Implements `IEnumerator<Record>`, `IEnumerable<Record>`, `IDisposable`.

### Methods

#### `QueryIterator.MoveNext() -> bool`

Advances to the next record. Returns `false` when exhausted.

#### `QueryIterator.Current -> Record`

Returns the current record. Throws `InvalidOperationException` if no current record.

#### `QueryIterator.Reverse() -> QueryIterator`

Reverses iteration order. Must be called before first `MoveNext()`. Returns `this` for chaining.

#### `QueryIterator.Skip(uint count) -> QueryIterator`

Skips the first `count` records. Must be called before first `MoveNext()`. Returns `this` for chaining.

**Parameters**:
- `count`: Number of records to skip

#### `QueryIterator.CollectAll() -> IReadOnlyList<Record>`

Collects all remaining records into a list. Exhausts the iterator.

**Returns**: List of remaining records.

**Chaining example**:
```csharp
var results = ds.QueryIter(startTs, endTs).Reverse().Skip(5).CollectAll();
```

---

## 4. QueryLengthIterator

Lazy iterator for query lengths. Implements `IEnumerator<LengthEntry>`, `IEnumerable<LengthEntry>`, `IDisposable`.

### Methods

#### `QueryLengthIterator.MoveNext() -> bool`

Advances to the next entry. Returns `false` when exhausted.

#### `QueryLengthIterator.Current -> LengthEntry`

Returns the current entry. Throws `InvalidOperationException` if no current entry.

#### `QueryLengthIterator.Reverse() -> QueryLengthIterator`

Reverses iteration order. Must be called before first `MoveNext()`. Returns `this` for chaining.

#### `QueryLengthIterator.Skip(uint count) -> QueryLengthIterator`

Skips the first `count` entries. Must be called before first `MoveNext()`. Returns `this` for chaining.

**Parameters**:
- `count`: Number of entries to skip

#### `QueryLengthIterator.CollectAll() -> IReadOnlyList<LengthEntry>`

Collects all remaining entries into a list. Exhausts the iterator.

**Returns**: List of remaining entries.

**Chaining example**:
```csharp
var results = ds.QueryLengthIter(startTs, endTs).Reverse().Skip(5).CollectAll();
```

---

## 5. Queue

A `Queue` is a handle for pushing records and opening consumers on a dataset queue.

### Methods

#### `Queue.Push(byte[] data) -> long`

Pushes a record to the queue. Returns the assigned timestamp.

**Parameters**:
- `data`: Record data (max 4 MiB)

**Returns**: Timestamp assigned to the record.

#### `Queue.OpenConsumer(string groupName) -> QueueConsumer`

Opens a consumer for the given group with default options.

**Parameters**:
- `groupName`: Consumer group name, must match `^[0-9A-Za-z_-]+$`

**Returns**: `QueueConsumer` instance.

**Throws**: `TmslException` with `TmslErrorCode.ConsumerGroupExists` if already opened.

#### `Queue.OpenConsumer(string groupName, QueueConsumerOptions options) -> QueueConsumer`

Opens a consumer with custom options.

**Parameters**:
- `groupName`: Consumer group name
- `options`: Consumer options

**Returns**: `QueueConsumer` instance.

#### `Queue.GetConsumerGroupNames() -> string[]`

Returns all consumer group names registered on this queue.

**Returns**: Array of group name strings.

#### `Queue.DropConsumer(string groupName)`

Drops a consumer group and its state.

**Parameters**:
- `groupName`: Consumer group name to drop

**Throws**: `TmslException` with `TmslErrorCode.ConsumerGroupNotFound` if not found.

---

## 6. QueueConsumer

A consumer that polls and acknowledges queue records.

### Methods

#### `QueueConsumer.Poll(TimeSpan timeout) -> Record?`

Polls for the next record, blocking up to the specified timeout.

**Parameters**:
- `timeout`: Maximum wait time

**Returns**: `Record?` — `null` if no record available within timeout.

#### `QueueConsumer.PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default) -> Task<Record?>`

Asynchronous version of `Poll`.

**Parameters**:
- `timeout`: Maximum wait time
- `cancellationToken`: Cancellation token

**Returns**: `Task<Record?>`.

#### `QueueConsumer.Ack(long timestamp)`

Acknowledges that a record has been processed.

**Parameters**:
- `timestamp`: Record timestamp to acknowledge

#### `QueueConsumer.Flush()`

Flushes pending consumer state to disk.

#### `QueueConsumer.Inspect() -> QueueConsumerInspectResult`

Returns consumer group info and runtime state.

**Returns**: `QueueConsumerInspectResult` with `Info` and `State`.

---

## 7. JournalQueue

A `JournalQueue` is a handle for consuming journal entries.

### Methods

#### `JournalQueue.OpenConsumer(string groupName) -> JournalQueueConsumer`

Opens a journal consumer for the given group with default options.

**Parameters**:
- `groupName`: Consumer group name

**Returns**: `JournalQueueConsumer` instance.

#### `JournalQueue.OpenConsumer(string groupName, QueueConsumerOptions options) -> JournalQueueConsumer`

Opens a journal consumer with custom options.

**Parameters**:
- `groupName`: Consumer group name
- `options`: Consumer options

**Returns**: `JournalQueueConsumer` instance.

---

## 8. JournalQueueConsumer

A consumer that polls and acknowledges journal records.

### Methods

#### `JournalQueueConsumer.Poll(TimeSpan timeout) -> JournalRecord?`

Polls for the next journal record, blocking up to the specified timeout.

**Parameters**:
- `timeout`: Maximum wait time

**Returns**: `JournalRecord?` — `null` if no record available within timeout.

#### `JournalQueueConsumer.PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default) -> Task<JournalRecord?>`

Asynchronous version of `Poll`.

**Parameters**:
- `timeout`: Maximum wait time
- `cancellationToken`: Cancellation token

**Returns**: `Task<JournalRecord?>`.

#### `JournalQueueConsumer.Ack(long sequence)`

Acknowledges that a journal record has been processed.

**Parameters**:
- `sequence`: Journal sequence number to acknowledge

---

## 9. Configuration Types

### 9.1 StoreConfig

Immutable configuration for opening a Store. All properties are optional; `null` means use the Rust default.

```csharp
var config = new StoreConfig
{
    FlushIntervalSeconds = 30,        // flush interval in seconds
    IdleTimeoutSeconds = 600,         // idle-close timeout in seconds
    DataSegmentSize = 128 * 1024 * 1024,  // data segment size
    IndexSegmentSize = 8 * 1024 * 1024,   // index segment size
    InitialDataSegmentSize = 4 * 1024 * 1024,
    InitialIndexSegmentSize = 1 * 1024 * 1024,
    CompressLevel = 6,                // compression level (0-9)
    CacheMaxMemory = 512 * 1024 * 1024,   // block cache max memory
    CacheIdleTimeoutSeconds = 300,    // cache idle timeout
    RetentionCheckHour = 0,           // UTC hour for retention check (0-23)
    EnableBackgroundThread = true,    // enable background task thread
    EnableJournal = true,             // enable journal
    ReadOnly = false                  // read-only mode
};
```

### 9.2 DatasetConfig

Immutable per-dataset configuration overrides. All properties are optional.

```csharp
var dsConfig = new DatasetConfig
{
    DataSegmentSize = 64 * 1024 * 1024,
    IndexSegmentSize = 4 * 1024 * 1024,
    InitialDataSegmentSize = 4 * 1024 * 1024,
    InitialIndexSegmentSize = 1 * 1024 * 1024,
    CompressLevel = 6,
    CompressType = 0,           // 0=zstd, 1=deflate
    IndexContinuous = 0,        // 0=sparse, 1=continuous
    RetentionWindow = 0,        // 0=no limit (in dataset timestamp units)
    EnableJournal = false       // enable journal for this dataset
};
```

### 9.3 CreateDatasetOptions

Options for creating a dataset.

```csharp
var options = new CreateDatasetOptions
{
    Config = new DatasetConfig { ... }
};
```

### 9.4 QueueConsumerConfig

Configuration for a queue consumer group.

```csharp
var consumerConfig = new QueueConsumerConfig
{
    RunningExpiredSeconds = 60,  // stuck task retry timeout
    MaxRetryCount = 3            // max retries before parked
};
```

### 9.5 QueueConsumerOptions

Options for opening a queue consumer.

```csharp
var options = new QueueConsumerOptions
{
    Config = new QueueConsumerConfig { ... }
};
```

---

## 10. Record Types

### 10.1 Record

Data record with timestamp and data.

```csharp
public sealed record Record(long Timestamp, byte[] Data);
```

- `Timestamp`: Record timestamp (i64)
- `Data`: Record data (defensive copy)

### 10.2 JournalRecord

Journal record with sequence and data.

```csharp
public sealed record JournalRecord(long Sequence, byte[] Data);
```

- `Sequence`: Journal sequence number (i64, starting from 1)
- `Data`: Journal entry data (defensive copy)

### 10.3 LengthEntry

Length entry with timestamp and length.

```csharp
public sealed record LengthEntry(long Timestamp, uint Length);
```

### 10.4 DataSetInfo

Static dataset configuration.

```csharp
public sealed record DataSetInfo(
    string Name,
    string DatasetType,
    string BaseDir,
    ulong Identifier,
    ulong DataSegmentSize,
    ulong IndexSegmentSize,
    ulong InitialDataSegmentSize,
    ulong InitialIndexSegmentSize,
    byte CompressType,        // 0=zstd, 1=deflate
    byte CompressLevel,
    byte IndexContinuous,     // 0=sparse, 1=continuous
    ulong RetentionWindow,    // 0=no limit
    bool EnableJournal,
    long CreateTime
);
```

### 10.5 DataSetState

Runtime dataset state.

```csharp
public sealed record DataSetState(
    long? LatestWrittenTimestamp,
    uint OpenDataSegments,
    uint DataSegments,
    ulong TotalRecordCount,
    ulong TotalDataSize,
    ulong TotalUncompressedSize,
    ulong TotalInvalidRecordCount,
    long? MinTimestamp,
    long? MaxTimestamp,
    uint OpenIndexSegments,
    uint IndexSegments,
    uint PendingIndexEntries,
    long? BaseTimestamp,
    bool ReadOnly,
    bool HasBlockCache,
    bool HasJournal,
    bool HasQueue,
    uint QueueConsumerGroups
);
```

### 10.6 DataSetInspectResult

Combined dataset info and state.

```csharp
public sealed record DataSetInspectResult(DataSetInfo Info, DataSetState State);
```

### 10.7 QueueConsumerInfo

Consumer group configuration.

```csharp
public sealed record QueueConsumerInfo(
    string GroupName,
    ulong RunningExpiredSeconds,
    ushort MaxRetryCount
);
```

### 10.8 QueueConsumerState

Consumer runtime state.

```csharp
public sealed record QueueConsumerState(
    long ProcessedTs,
    IReadOnlyList<QueueConsumerPendingEntry> PendingEntries
);
```

### 10.9 QueueConsumerPendingEntry

Pending record in consumer.

```csharp
public sealed record QueueConsumerPendingEntry(
    long Timestamp,
    long StartTime,
    byte Status,      // 0=running, 1=parked
    byte RetryCount
);
```

### 10.10 QueueConsumerInspectResult

Combined consumer info and state.

```csharp
public sealed record QueueConsumerInspectResult(QueueConsumerInfo Info, QueueConsumerState State);
```

### 10.11 TickResult

Background task execution result.

```csharp
public sealed record TickResult(ulong ExecutedTasks, ulong NextDelayMs);
```

### 10.12 JournalIndexInfo

Journal index entry for source record lookup.

```csharp
public sealed record JournalIndexInfo(
    long Timestamp,
    ulong BlockOffset,
    ushort InBlockOffset
);
```

---

## 11. Error Types

### 11.1 TmslException

Exception with error code and message.

```csharp
public class TmslException : Exception
{
    public TmslErrorCode Code { get; }
}
```

### 11.2 TmslErrorCode

Error codes for `TmslException`.

```csharp
public enum TmslErrorCode
{
    Io,                    // I/O error
    InvalidMagic,          // invalid file magic
    InvalidVersion,        // invalid file version
    MmapError,             // mmap error
    CompressionError,      // compression error
    DecompressionError,    // decompression error
    InvalidData,           // invalid parameters
    NotFound,              // dataset/record not found
    Expired,               // timestamp outside retention window
    AlreadyExists,         // dataset already exists
    SegmentFull,           // segment is full
    QueueAlreadyOpen,      // queue already opened
    QueueNotOpen,          // queue not opened
    ConsumerGroupNotFound, // consumer group not found
    ConsumerGroupExists,   // consumer group already exists
    QueueClosed,           // queue closed
    PendingFull,           // pending queue full
    StoreClosed,           // store closed
    DatasetClosed,         // dataset closed
    QueueBridgeClosed,     // queue bridge closed
    IteratorExhausted,     // iterator exhausted
}
```

---

## 12. Utility

### 12.1 TimsliteInfo

Static class for library version information.

#### `TimsliteInfo.Version() -> string`

Returns the native library version string.

**Returns**: Version string (e.g., `"0.1.1"`).
