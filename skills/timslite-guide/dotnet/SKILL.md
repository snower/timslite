---
name: timslite-guide-dotnet
description: .NET guide for timslite time-series storage library - installation, quick start, API reference, and examples
---

# timslite .NET Guide

## Installation

### NuGet

```bash
dotnet add package Timslite
```

Or add to your `.csproj`:

```xml
<PackageReference Include="Timslite" Version="0.1.1" />
```

### Native Library

The wrapper uses UniFFI for code generation with a Kotlin/JVM backend. The NuGet package includes native libraries for all supported platforms under `runtimes/<rid>/native/`:

- `runtimes/osx-x64/native/libtimslite_dotnet.dylib`
- `runtimes/osx-arm64/native/libtimslite_dotnet.dylib`
- `runtimes/linux-x64/native/libtimslite_dotnet.so`
- `runtimes/linux-arm64/native/libtimslite_dotnet.so`
- `runtimes/win-x64/native/timslite_dotnet.dll`
- `runtimes/win-arm64/native/timslite_dotnet.dll`

The `NativeLibraryLoader` uses .NET's `NativeLibrary.TryLoad()` API to automatically detect the current OS/architecture and load the correct library. No additional configuration is needed.

### Environment Variables

- `TIMSLITE_NATIVE_LIBRARY_PATH`: Override the native library path for custom builds or debugging.

### Building from Source

```bash
# Build the .NET wrapper
dotnet build wrapper/dotnet/src/Timslite/Timslite.csproj

# Run tests
dotnet test wrapper/dotnet/tests/Timslite.Tests/Timslite.Tests.csproj
```

## Quick Start

### Opening a Store

```csharp
using Timslite;

var config = new StoreConfig
{
    EnableJournal = true,
    EnableBackgroundThread = true
};

using var store = Store.Open("/path/to/data", config);
```

### Creating a Dataset

```csharp
// Simple creation with defaults
store.CreateDataset("metrics", "cpu");

// With custom options
var options = new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = 0,  // sparse mode
        RetentionWindow = 0   // no retention limit
    }
};

store.CreateDataset("metrics", "cpu", options);
```

### Writing and Reading Data

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write a record
ds.Write(1700000000L, new byte[] { 1, 2, 3 });

// Write with auto-generated timestamp
ds.WriteNow(new byte[] { 4, 5, 6 });

// Read by timestamp
var rec = ds.Read(1700000000L);
if (rec != null)
{
    byte[] data = rec.Data;
    long ts = rec.Timestamp;
}

// Read the latest record
var latest = ds.ReadLatest();

// Check if record exists
bool exists = ds.ReadExist(1700000000L);

// Read only the length
uint? len = ds.ReadLength(1700000000L);

// Append to the latest record (must be >= latest timestamp)
ds.Append(1700000000L, new byte[] { 4, 5 });

// Delete a record
ds.Delete(1700000000L);
```

### Querying Data with Range

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Eager query (loads all into memory)
var records = ds.Query(1700000000L, 1700003600L);
foreach (var rec in records)
{
    Console.WriteLine($"{rec.Timestamp}: {rec.Data.Length} bytes");
}

// Query existence only (byte array of 0/1 flags)
byte[] existFlags = ds.QueryExist(1700000000L, 1700003600L);

// Query lengths only
var lengths = ds.QueryLength(1700000000L, 1700003600L);
foreach (var entry in lengths)
{
    Console.WriteLine($"{entry.Timestamp}: {entry.Length} bytes");
}
```

### Using Query Iterators

For large result sets, use iterators to avoid loading everything into memory:

```csharp
using var ds = store.OpenDataset("metrics", "cpu");
using var iter = ds.QueryIter(1700000000L, 1700003600L);
while (iter.MoveNext())
{
    var rec = iter.Current;
    // process record
}
```

Length-only iteration:

```csharp
using var iter = ds.QueryLengthIter(startTs, endTs);
while (iter.MoveNext())
{
    var entry = iter.Current;
    Console.WriteLine($"{entry.Timestamp}: {entry.Length} bytes");
}
```

Collect all results at once:

```csharp
using var iter = ds.QueryIter(startTs, endTs);
var allRecords = iter.CollectAll();
```

### Using Queues

```csharp
using var ds = store.OpenDataset("tasks", "jobs");

// Open queue
var queue = store.OpenQueue(ds);

// Push records
long ts = queue.Push(new byte[] { 1, 2, 3 });

// List consumer groups
string[] groups = queue.GetConsumerGroupNames();

// Open consumer
var options = new QueueConsumerOptions
{
    Config = new QueueConsumerConfig
    {
        RunningExpiredSeconds = 60,
        MaxRetryCount = 3
    }
};
var consumer = queue.OpenConsumer("worker_group", options);

// Poll for records
var record = consumer.Poll(TimeSpan.FromSeconds(5));
if (record != null)
{
    Console.WriteLine($"Got record at {record.Timestamp}");
    consumer.Ack(record.Timestamp);
}

// Flush consumer state
consumer.Flush();

// Inspect consumer state
var inspect = consumer.Inspect();
Console.WriteLine($"Processed up to: {inspect.State.ProcessedTs}");

// Drop a consumer group
queue.DropConsumer("old_group");
```

### Using Journal Queue

```csharp
// Query journal directly
long? seq = store.JournalLatestSequence();
var journalRecords = store.JournalQuery(1, 100);

// Open journal queue for reliable consumption
var jq = store.OpenJournalQueue();
var jc = jq.OpenConsumer("audit_group");

var jrecord = jc.Poll(TimeSpan.FromSeconds(5));
if (jrecord != null)
{
    Console.WriteLine($"Journal seq={jrecord.Sequence}, {jrecord.Data.Length} bytes");
    jc.Ack(jrecord.Sequence);
}
```

### Background Tasks

```csharp
// Manual background task execution
var result = store.TickBackgroundTasks();
Console.WriteLine($"Executed {result.ExecutedTasks} tasks");
Console.WriteLine($"Next run in {result.NextDelayMs}ms");

// Check next delay without executing
ulong delayMs = store.NextBackgroundDelayMs();
```

### Inspection

```csharp
// List all datasets
string[] names = store.GetDatasetNames();

// List types for a dataset
string[] types = store.GetDatasetTypes("metrics");

// Inspect dataset
var result = store.InspectDataset("metrics", "cpu");
Console.WriteLine($"Records: {result.State.TotalRecordCount}");
Console.WriteLine($"Data size: {result.State.TotalDataSize} bytes");
Console.WriteLine($"Latest ts: {result.State.LatestWrittenTimestamp}");

// Get library version
string version = TimsliteInfo.Version();
```

## Architecture

### Key Classes

| Class | Purpose |
|-------|---------|
| `Store` | Top-level facade for store lifecycle, dataset/queue management, journal |
| `Dataset` | Handle for read/write operations on a specific dataset |
| `Queue` | Dataset queue for pushing records and opening consumers |
| `QueueConsumer` | Consumer that polls and acknowledges records |
| `JournalQueue` | Journal queue for consuming journal records |
| `JournalQueueConsumer` | Consumer for journal records |
| `QueryIterator` | Lazy iterator for query results |
| `QueryLengthIterator` | Lazy iterator for query lengths |
| `StoreConfig` | Configuration for opening a store |
| `DatasetConfig` | Per-dataset configuration overrides |
| `CreateDatasetOptions` | Options for dataset creation |
| `QueueConsumerConfig` | Configuration for queue consumers |
| `QueueConsumerOptions` | Options for opening queue consumers |
| `TmslException` | Exception with error code and message |

### Data Records

| Record | Purpose |
|--------|---------|
| `Record` | Data record with `Timestamp` and `Data` |
| `JournalRecord` | Journal record with `Sequence` and `Data` |
| `LengthEntry` | Length entry with `Timestamp` and `Length` |
| `DataSetInfo` | Static dataset configuration |
| `DataSetState` | Runtime dataset state |
| `DataSetInspectResult` | Combined info and state |
| `QueueConsumerInfo` | Consumer group configuration |
| `QueueConsumerState` | Consumer runtime state |
| `QueueConsumerPendingEntry` | Pending record in consumer |
| `TickResult` | Background task execution result |
| `JournalIndexInfo` | Journal index entry for source record lookup |

### Namespace

```csharp
using Timslite;          // Public facade classes
using Timslite.Errors;   // TmslException, TmslErrorCode
```

### Disposable Pattern

All resource classes implement `IDisposable`:

- `Store` — closes store, flushes data, releases lock
- `Dataset` — closes dataset handle
- `Queue` — releases queue handle
- `QueueConsumer` — releases consumer handle
- `JournalQueue` — releases journal queue handle
- `JournalQueueConsumer` — releases journal consumer handle
- `QueryIterator` — releases iterator handle
- `QueryLengthIterator` — releases iterator handle

Use `using` statements for automatic cleanup:

```csharp
using var store = Store.Open("/path/to/data");
using var ds = store.OpenDataset("metrics", "cpu");
// resources auto-disposed at end of scope
```

### Error Handling

```csharp
using Timslite.Errors;

try
{
    store.CreateDataset("metrics", "cpu");
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.AlreadyExists)
{
    Console.WriteLine("Dataset already exists");
}
catch (TmslException ex)
{
    Console.WriteLine($"Error {ex.Code}: {ex.Message}");
}
```
