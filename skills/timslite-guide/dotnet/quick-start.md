# .NET Quick Start

## Installation

### NuGet

```bash
dotnet add package Timslite
```

Or add to your `.csproj`:

```xml
<PackageReference Include="Timslite" Version="0.1.1" />
```

### Building from Source

```bash
# Build the .NET wrapper
dotnet build wrapper/dotnet/src/Timslite/Timslite.csproj

# Run tests
dotnet test wrapper/dotnet/tests/Timslite.Tests/Timslite.Tests.csproj
```

## Basic Usage

### Opening a Store

```csharp
using Timslite;

// Simple store with defaults
using var store = Store.Open("/path/to/data");

// Store with custom configuration
var config = new StoreConfig
{
    EnableJournal = true,
    EnableBackgroundThread = true,
    FlushIntervalSeconds = 30,
    CacheMaxMemory = 512 * 1024 * 1024  // 512 MiB
};

using var store2 = Store.Open("/path/to/data2", config);
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
        IndexContinuous = 0,   // sparse mode (default)
        RetentionWindow = 0,   // no retention limit
        CompressLevel = 6
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
    Console.WriteLine($"ts={rec.Timestamp}, {rec.Data.Length} bytes");
}

// Read the latest record
var latest = ds.ReadLatest();

// Check if record exists
bool exists = ds.ReadExist(1700000000L);

// Read only the length
uint? len = ds.ReadLength(1700000000L);

// Append to the latest record (must be >= latest timestamp)
ds.Append(1700000000L, new byte[] { 4, 5 });

// Append with auto-generated timestamp
ds.AppendNow(new byte[] { 6, 7 });

// Correct a record (overwrites data at existing timestamp)
ds.Correct(1700000000L, new byte[] { 7, 8, 9 });

// Delete a record
ds.Delete(1700000000L);

// Flush pending data to disk
ds.Flush();
```

### Querying Data

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

// Forward iteration
using var iter = ds.QueryIter(1700000000L, 1700003600L);
while (iter.MoveNext())
{
    var rec = iter.Current;
    Console.WriteLine($"{rec.Timestamp}: {rec.Data.Length} bytes");
}

// Reverse iteration
using var iter2 = ds.QueryIter(1700000000L, 1700003600L);
iter2.Reverse();
while (iter2.MoveNext())
{
    var rec = iter2.Current;
    Console.WriteLine($"{rec.Timestamp}: {rec.Data.Length} bytes");
}

// Skip records
using var iter3 = ds.QueryIter(1700000000L, 1700003600L);
iter3.Skip(10); // skip first 10
while (iter3.MoveNext())
{
    // starts at 11th record
}

// Collect all results at once
using var iter4 = ds.QueryIter(1700000000L, 1700003600L);
var allRecords = iter4.CollectAll();

// Length-only iteration
using var liter = ds.QueryLengthIter(1700000000L, 1700003600L);
while (liter.MoveNext())
{
    var entry = liter.Current;
    Console.WriteLine($"{entry.Timestamp}: {entry.Length} bytes");
}
```

### Using Queues

```csharp
using var ds = store.OpenDataset("tasks", "jobs");

// Open queue
var queue = store.OpenQueue(ds);

// Push records
long ts = queue.Push(new byte[] { 1, 2, 3 });
Console.WriteLine($"Pushed at ts={ts}");

// List consumer groups
string[] groups = queue.GetConsumerGroupNames();

// Open consumer with default options
var consumer = queue.OpenConsumer("worker_group");

// Open consumer with custom options
var options = new QueueConsumerOptions
{
    Config = new QueueConsumerConfig
    {
        RunningExpiredSeconds = 60,
        MaxRetryCount = 3
    }
};
var consumer2 = queue.OpenConsumer("worker_group2", options);

// Poll for records
var record = consumer.Poll(TimeSpan.FromSeconds(5));
if (record != null)
{
    Console.WriteLine($"Got record at {record.Timestamp}");
    consumer.Ack(record.Timestamp);
}

// Async poll
var record2 = await consumer.PollAsync(TimeSpan.FromSeconds(5));
if (record2 != null)
{
    consumer.Ack(record2.Timestamp);
}

// Flush consumer state
consumer.Flush();

// Inspect consumer state
var inspect = consumer.Inspect();
Console.WriteLine($"Processed up to: {inspect.State.ProcessedTs}");
Console.WriteLine($"Pending: {inspect.State.PendingEntries.Count}");

// Drop a consumer group
queue.DropConsumer("old_group");
```

### Using Journal Queue

```csharp
// Query journal directly
long? seq = store.JournalLatestSequence();
if (seq.HasValue)
{
    Console.WriteLine($"Latest journal sequence: {seq.Value}");
}

// Read specific journal record
var jrec = store.JournalRead(42);
if (jrec != null)
{
    Console.WriteLine($"Journal seq={jrec.Sequence}: {jrec.Data.Length} bytes");
}

// Query journal range
var journalRecords = store.JournalQuery(1, 100);
foreach (var jr in journalRecords)
{
    Console.WriteLine($"Journal seq={jr.Sequence}: {jr.Data.Length} bytes");
}

// Open journal queue for reliable consumption
var jq = store.OpenJournalQueue();
var jc = jq.OpenConsumer("audit_group");

var jrecord = jc.Poll(TimeSpan.FromSeconds(5));
if (jrecord != null)
{
    Console.WriteLine($"Journal seq={jrecord.Sequence}, {jrecord.Data.Length} bytes");
    jc.Ack(jrecord.Sequence);
}

// Async poll
var jrecord2 = await jc.PollAsync(TimeSpan.FromSeconds(5));
```

### Background Tasks

```csharp
// Manual background task execution
var result = store.TickBackgroundTasks();
Console.WriteLine($"Executed {result.ExecutedTasks} tasks");
Console.WriteLine($"Next run in {result.NextDelayMs}ms");

// Check next delay without executing
ulong delayMs = store.NextBackgroundDelayMs();
Console.WriteLine($"Next task due in {delayMs}ms");
```

### Inspection

```csharp
// List all datasets
string[] names = store.GetDatasetNames();
foreach (var name in names)
{
    Console.WriteLine($"Dataset: {name}");
}

// List types for a dataset
string[] types = store.GetDatasetTypes("metrics");
foreach (var type in types)
{
    Console.WriteLine($"Type: {type}");
}

// Inspect dataset
var result = store.InspectDataset("metrics", "cpu");
Console.WriteLine($"Records: {result.State.TotalRecordCount}");
Console.WriteLine($"Data size: {result.State.TotalDataSize} bytes");
Console.WriteLine($"Latest ts: {result.State.LatestWrittenTimestamp}");

// Open by identifier
ulong id = ds.Identifier;
using var ds2 = store.OpenDatasetByIdentifier(id);

// Check read-only
bool ro = store.IsReadOnly();

// Get library version
string version = TimsliteInfo.Version();
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
catch (TmslException ex) when (ex.Code == TmslErrorCode.NotFound)
{
    Console.WriteLine("Dataset not found");
}
catch (TmslException ex)
{
    Console.WriteLine($"Error {ex.Code}: {ex.Message}");
}
```
