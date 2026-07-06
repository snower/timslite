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
- `identifier`: Numeric dataset ID

**Returns**: `Dataset` instance.

**Throws**:
- `TmslException` with `TmslErrorCode.NotFound` if identifier not found

#### `Store.DropDataset(string name, string datasetType)`

Drops a dataset and removes all its data.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type

**Throws**:
- `TmslException` with `TmslErrorCode.NotFound` if dataset does not exist

#### `Store.GetDatasetNames() -> string[]`

Returns the list of dataset names in this store.

#### `Store.GetDatasetTypes(string name) -> string[]`

Returns the list of dataset types for a given name.

**Parameters**:
- `name`: Dataset name

#### `Store.InspectDataset(string name, string datasetType) -> DataSetInspectResult`

Returns detailed information about a dataset.

**Parameters**:
- `name`: Dataset name
- `datasetType`: Dataset type

**Returns**: `DataSetInspectResult` containing `DataSetInfo` and `DataSetState`.

### 1.3 Queue Management

#### `Store.OpenQueue(Dataset dataset) -> Queue`

Opens a queue for the given dataset.

**Parameters**:
- `dataset`: An open `Dataset` instance

**Returns**: `Queue` instance.

**Throws**:
- `TmslException` with `TmslErrorCode.QueueAlreadyOpen` if queue is already open for this dataset

#### `Store.OpenJournalQueue() -> JournalQueue`

Opens the journal queue for consuming journal records.

**Returns**: `JournalQueue` instance.

**Throws**:
- `TmslException` with `TmslErrorCode.QueueAlreadyOpen` if journal queue is already open

### 1.4 Journal Operations

#### `Store.JournalLatestSequence() -> long?`

Returns the latest journal sequence number, or `null` if journal is empty.

#### `Store.JournalRead(long sequence) -> JournalRecord?`

Reads a journal record by sequence number.

**Parameters**:
- `sequence`: Journal sequence number

**Returns**: `JournalRecord` or `null` if not found.

#### `Store.JournalQuery(long startSequence, long endSequence) -> IReadOnlyList<JournalRecord>`

Queries journal records in a sequence range.

**Parameters**:
- `startSequence`: Start sequence (inclusive)
- `endSequence`: End sequence (inclusive)

**Returns**: List of journal records.

#### `Store.ReadJournalSourceRecord(ulong datasetIdentifier, JournalIndexInfo indexInfo) -> Record`

Reads the source record referenced by a journal entry.

**Parameters**:
- `datasetIdentifier`: Dataset identifier
- `indexInfo`: Journal index info containing timestamp, block offset, and in-block offset

**Returns**: The source `Record`.

### 1.5 Background Tasks

#### `Store.TickBackgroundTasks() -> TickResult`

Manually triggers background tasks (flush, idle-close, cache eviction, retention reclaim).

**Returns**: `TickResult` with `ExecutedTasks` and `NextDelayMs`.

#### `Store.NextBackgroundDelayMs() -> ulong`

Returns the delay in milliseconds until the next background task should run.

---

## 2. Dataset

The `Dataset` provides read/write access to a specific `(name, type)` pair.

### 2.1 Properties

#### `Dataset.IsClosed -> bool`

Returns whether this dataset has been disposed.

#### `Dataset.Identifier -> ulong`

Returns the numeric identifier for this dataset.

### 2.2 Write Operations

#### `Dataset.Write(long timestamp, byte[] data)`

Writes a record at the specified timestamp.

**Parameters**:
- `timestamp`: Record timestamp (signed 64-bit integer)
- `data`: Record data (max 4 MiB)

**Throws**:
- `TmslException` with `TmslErrorCode.InvalidData` if data exceeds 4 MiB
- `TmslException` with `TmslErrorCode.Expired` if timestamp is expired

#### `Dataset.WriteNow(byte[] data)`

Writes a record with the current wall-clock timestamp.

**Parameters**:
- `data`: Record data (max 4 MiB)

#### `Dataset.Append(long timestamp, byte[] data)`

Appends data to an existing record at the specified timestamp, or creates a new record.

**Parameters**:
- `timestamp`: Record timestamp (must be >= latest written timestamp)
- `data`: Data to append

**Throws**:
- `TmslException` with `TmslErrorCode.InvalidData` if timestamp < latest written timestamp

#### `Dataset.AppendNow(byte[] data)`

Appends data to the latest record using current wall-clock timestamp.

**Parameters**:
- `data`: Data to append

### 2.3 Read Operations

#### `Dataset.Read(long timestamp) -> Record?`

Reads a record by timestamp.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: `Record` or `null` if not found or expired.

#### `Dataset.ReadExist(long timestamp) -> bool`

Checks if a record exists at the specified timestamp.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: `true` if record exists and is not expired.

#### `Dataset.ReadLatest() -> Record?`

Reads the latest written record.

**Returns**: `Record` or `null` if no records or latest is deleted/expired.

#### `Dataset.ReadLength(long timestamp) -> uint?`

Reads the data length of a record without loading the data.

**Parameters**:
- `timestamp`: Record timestamp

**Returns**: Data length in bytes, or `null` if not found.

### 2.4 Delete Operations

#### `Dataset.Delete(long timestamp)`

Deletes a record by timestamp.

**Parameters**:
- `timestamp`: Record timestamp

**Throws**:
- `TmslException` with `TmslErrorCode.Expired` if timestamp is expired

### 2.5 Query Operations

#### `Dataset.Query(long startTimestamp, long endTimestamp) -> IReadOnlyList<Record>`

Queries records in a timestamp range.

**Parameters**:
- `startTimestamp`: Start timestamp (inclusive)
- `endTimestamp`: End timestamp (inclusive)

**Returns**: List of records.

#### `Dataset.QueryIter(long startTimestamp, long endTimestamp) -> QueryIterator`

Creates an iterator for querying records in a timestamp range.

**Parameters**:
- `startTimestamp`: Start timestamp (inclusive)
- `endTimestamp`: End timestamp (inclusive)

**Returns**: `QueryIterator` (implements `IEnumerator<Record>`, `IEnumerable<Record>`, `IDisposable`).

#### `Dataset.QueryLengthIter(long startTimestamp, long endTimestamp) -> QueryLengthIterator`

Creates an iterator for querying record lengths without loading data.

**Parameters**:
- `startTimestamp`: Start timestamp (inclusive)
- `endTimestamp`: End timestamp (inclusive)

**Returns**: `QueryLengthIterator` (implements `IEnumerator<LengthEntry>`, `IEnumerable<LengthEntry>`, `IDisposable`).

---

## 3. QueryIterator

Iterates over records in a query result set.

### 3.1 IEnumerator/IDisposable

#### `QueryIterator.MoveNext() -> bool`

Advances to the next record. Returns `false` when exhausted.

#### `QueryIterator.Current -> Record`

Returns the current record.

#### `QueryIterator.Dispose()`

Releases the iterator resources.

### 3.2 Additional Methods

#### `QueryIterator.Reverse()`

Reverses the iteration direction.

#### `QueryIterator.Skip(uint count)`

Skips the specified number of records.

**Parameters**:
- `count`: Number of records to skip

#### `QueryIterator.CollectAll() -> IReadOnlyList<Record>`

Collects all remaining records into a list.

**Returns**: List of remaining records.

---

## 4. QueryLengthIterator

Iterates over record lengths without loading data.

### 4.1 IEnumerator/IDisposable

#### `QueryLengthIterator.MoveNext() -> bool`

Advances to the next entry. Returns `false` when exhausted.

#### `QueryLengthIterator.Current -> LengthEntry`

Returns the current entry.

#### `QueryLengthIterator.Dispose()`

Releases the iterator resources.

### 4.2 Additional Methods

#### `QueryLengthIterator.Reverse()`

Reverses the iteration direction.

#### `QueryLengthIterator.Skip(uint count)`

Skips the specified number of entries.

**Parameters**:
- `count`: Number of entries to skip

#### `QueryLengthIterator.CollectAll() -> IReadOnlyList<LengthEntry>`

Collects all remaining entries into a list.

**Returns**: List of remaining entries.

---

## 5. Queue

Represents an open dataset queue for pushing records and opening consumers.

### 5.1 Properties

#### `Queue.IsClosed -> bool`

Returns whether this queue has been disposed.

### 5.2 Operations

#### `Queue.Push(byte[] data) -> long`

Pushes a record to the queue. Returns the timestamp assigned to the record.

**Parameters**:
- `data`: Record data

**Returns**: Timestamp assigned to the record.

#### `Queue.OpenConsumer(string groupName) -> QueueConsumer`

Opens a consumer for the given consumer group.

**Parameters**:
- `groupName`: Consumer group name, must match `^[0-9A-Za-z_-]+$`

**Returns**: `QueueConsumer` instance.

#### `Queue.OpenConsumer(string groupName, QueueConsumerOptions options) -> QueueConsumer`

Opens a consumer with custom options.

**Parameters**:
- `groupName`: Consumer group name
- `options`: Consumer options including optional `QueueConsumerConfig`

**Returns**: `QueueConsumer` instance.

#### `Queue.GetConsumerGroupNames() -> string[]`

Returns the list of consumer group names registered on this queue.

#### `Queue.DropConsumerGroup(string groupName)`

Drops a consumer group and its state.

**Parameters**:
- `groupName`: Consumer group name

#### `Queue.Dispose()`

Closes the queue and releases resources.

---

## 6. QueueConsumer

A queue consumer that can poll for records and acknowledge processing.

### 6.1 Properties

#### `QueueConsumer.IsClosed -> bool`

Returns whether this consumer has been disposed.

### 6.2 Operations

#### `QueueConsumer.Poll(TimeSpan timeout) -> Record?`

Polls for the next record, blocking up to the specified timeout.

**Parameters**:
- `timeout`: Maximum wait time

**Returns**: `Record` or `null` if no record available within timeout.

#### `QueueConsumer.PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default) -> Task<Record?>`

Asynchronously polls for the next record.

**Parameters**:
- `timeout`: Maximum wait time
- `cancellationToken`: Cancellation token

**Returns**: Task containing `Record` or `null`.

#### `QueueConsumer.Ack(long timestamp)`

Acknowledges that a record has been processed.

**Parameters**:
- `timestamp`: Record timestamp to acknowledge

#### `QueueConsumer.Flush()`

Flushes pending consumer state to disk.

#### `QueueConsumer.GetPendingEntries() -> IReadOnlyList<QueueConsumerPendingEntry>`

Returns the list of pending entries for this consumer.

**Returns**: List of pending entries with timestamp, start time, status, and retry count.

#### `QueueConsumer.Inspect() -> QueueConsumerInspectResult`

Returns detailed information about this consumer.

**Returns**: `QueueConsumerInspectResult` containing `QueueConsumerInfo` and `QueueConsumerState`.

#### `QueueConsumer.Dispose()`

Closes the consumer and releases resources.

---

## 7. JournalQueue

Represents an open journal queue for consuming journal records.

### 7.1 Properties

#### `JournalQueue.IsClosed -> bool`

Returns whether this journal queue has been disposed.

### 7.2 Operations

#### `JournalQueue.OpenConsumer(string groupName) -> JournalQueueConsumer`

Opens a consumer for the given consumer group.

**Parameters**:
- `groupName`: Consumer group name

**Returns**: `JournalQueueConsumer` instance.

#### `JournalQueue.OpenConsumer(string groupName, QueueConsumerOptions options) -> JournalQueueConsumer`

Opens a consumer with custom options.

**Parameters**:
- `groupName`: Consumer group name
- `options`: Consumer options

**Returns**: `JournalQueueConsumer` instance.

#### `JournalQueue.Dispose()`

Closes the journal queue and releases resources.

---

## 8. JournalQueueConsumer

A journal queue consumer that can poll for journal records and acknowledge processing.

### 8.1 Properties

#### `JournalQueueConsumer.IsClosed -> bool`

Returns whether this consumer has been disposed.

### 8.2 Operations

#### `JournalQueueConsumer.Poll(TimeSpan timeout) -> JournalRecord?`

Polls for the next journal record, blocking up to the specified timeout.

**Parameters**:
- `timeout`: Maximum wait time

**Returns**: `JournalRecord` or `null` if no record available within timeout.

#### `JournalQueueConsumer.PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default) -> Task<JournalRecord?>`

Asynchronously polls for the next journal record.

**Parameters**:
- `timeout`: Maximum wait time
- `cancellationToken`: Cancellation token

**Returns**: Task containing `JournalRecord` or `null`.

#### `JournalQueueConsumer.Ack(long sequence)`

Acknowledges that a journal record has been processed.

**Parameters**:
- `sequence`: Journal sequence number to acknowledge

#### `JournalQueueConsumer.Flush()`

Flushes pending consumer state to disk.

#### `JournalQueueConsumer.Dispose()`

Closes the consumer and releases resources.

---

## 9. Configuration Types

### 9.1 StoreConfig

Immutable configuration for opening a Store. All properties are optional; `null` means use the Rust default.

```csharp
public sealed record StoreConfig
{
    public ulong? FlushIntervalSeconds { get; init; }
    public ulong? IdleTimeoutSeconds { get; init; }
    public ulong? DataSegmentSize { get; init; }
    public ulong? IndexSegmentSize { get; init; }
    public ulong? InitialDataSegmentSize { get; init; }
    public ulong? InitialIndexSegmentSize { get; init; }
    public byte? CompressLevel { get; init; }
    public ulong? CacheMaxMemory { get; init; }
    public ulong? CacheIdleTimeoutSeconds { get; init; }
    public byte? RetentionCheckHour { get; init; }
    public bool? EnableBackgroundThread { get; init; }
    public bool? EnableJournal { get; init; }
    public bool? ReadOnly { get; init; }
}
```

### 9.2 DatasetConfig

Immutable per-dataset configuration overrides.

```csharp
public sealed record DatasetConfig
{
    public ulong? DataSegmentSize { get; init; }
    public ulong? IndexSegmentSize { get; init; }
    public ulong? InitialDataSegmentSize { get; init; }
    public ulong? InitialIndexSegmentSize { get; init; }
    public byte? CompressLevel { get; init; }
    public byte? CompressType { get; init; }
    public byte? IndexContinuous { get; init; }
    public ulong? RetentionWindow { get; init; }
    public bool? EnableJournal { get; init; }
}
```

### 9.3 CreateDatasetOptions

Options for creating a dataset.

```csharp
public sealed record CreateDatasetOptions
{
    public DatasetConfig? Config { get; init; }
}
```

### 9.4 QueueConsumerConfig

Configuration for a queue consumer group.

```csharp
public sealed record QueueConsumerConfig
{
    public ulong? RunningExpiredSeconds { get; init; }
    public ushort? MaxRetryCount { get; init; }
}
```

### 9.5 QueueConsumerOptions

Options for opening a queue consumer.

```csharp
public sealed record QueueConsumerOptions
{
    public QueueConsumerConfig? Config { get; init; }
}
```

---

## 10. Data Types

### 10.1 Record

A timestamped data record.

```csharp
public sealed record Record(long Timestamp, byte[] Data);
```

### 10.2 JournalRecord

A journal record with sequence number.

```csharp
public sealed record JournalRecord(long Sequence, byte[] Data);
```

### 10.3 LengthEntry

A timestamp-length pair for length-only queries.

```csharp
public sealed record LengthEntry(long Timestamp, uint Length);
```

### 10.4 DataSetInfo

Dataset metadata.

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
    byte CompressType,
    byte CompressLevel,
    byte IndexContinuous,
    ulong RetentionWindow,
    bool EnableJournal,
    long CreateTime
);
```

### 10.5 DataSetState

Dataset runtime state.

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

Queue consumer metadata.

```csharp
public sealed record QueueConsumerInfo(
    string GroupName,
    ulong RunningExpiredSeconds,
    ushort MaxRetryCount
);
```

### 10.8 QueueConsumerPendingEntry

A pending queue entry.

```csharp
public sealed record QueueConsumerPendingEntry(
    long Timestamp,
    long StartTime,
    byte Status,
    byte RetryCount
);
```

### 10.9 QueueConsumerState

Queue consumer runtime state.

```csharp
public sealed record QueueConsumerState(
    long ProcessedTs,
    IReadOnlyList<QueueConsumerPendingEntry> PendingEntries
);
```

### 10.10 QueueConsumerInspectResult

Combined queue consumer info and state.

```csharp
public sealed record QueueConsumerInspectResult(QueueConsumerInfo Info, QueueConsumerState State);
```

### 10.11 JournalIndexInfo

Journal index information for reading source records.

```csharp
public sealed record JournalIndexInfo(
    long Timestamp,
    ulong BlockOffset,
    ushort InBlockOffset
);
```

### 10.12 TickResult

Background task execution result.

```csharp
public sealed record TickResult(ulong ExecutedTasks, ulong NextDelayMs);
```

---

## 11. Error Handling

### 11.1 TmslException

All timslite errors throw `TmslException` with a `Code` property.

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
    Io,
    InvalidMagic,
    InvalidVersion,
    MmapError,
    CompressionError,
    DecompressionError,
    InvalidData,
    NotFound,
    Expired,
    AlreadyExists,
    SegmentFull,
    QueueAlreadyOpen,
    QueueNotOpen,
    ConsumerGroupNotFound,
    ConsumerGroupExists,
    QueueClosed,
    PendingFull,
    StoreClosed,
    DatasetClosed,
    QueueBridgeClosed,
    IteratorExhausted,
}
```

---

## 12. Utility Types

### 12.1 TimsliteInfo

Static class for version information.

```csharp
public static class TimsliteInfo
{
    public static string Version();
}
```
