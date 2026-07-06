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

var config = new StoreConfig
{
    EnableJournal = true,
    EnableBackgroundThread = true
};

using var store = Store.Open("/path/to/data", config);
// use the store
```

### Creating a Dataset

```csharp
var options = new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = 0,
        RetentionWindow = 0
    }
};

store.CreateDataset("metrics", "cpu", options);
```

### Writing and Reading Data

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write a record
ds.Write(1700000000L, new byte[] { 1, 2, 3 });

// Read by timestamp
var rec = ds.Read(1700000000L);
if (rec != null)
{
    byte[] data = rec.Data;
    long ts = rec.Timestamp;
}

// Read the latest record
var latest = ds.ReadLatest();

// Append to the latest record (must be >= latest timestamp)
ds.Append(1700000000L, new byte[] { 4, 5 });

// Delete a record
ds.Delete(1700000000L);
```

### Querying Data with Range

```csharp
using var ds = store.OpenDataset("metrics", "cpu");
var records = ds.Query(1700000000L, 1700003600L);
foreach (var rec in records)
{
    Console.WriteLine($"{rec.Timestamp}: {rec.Data.Length} bytes");
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
using var ds = store.OpenDataset("metrics", "cpu");
using var queue = store.OpenQueue(ds);

// Push a record to the queue
long ts = queue.Push(new byte[] { 10, 20, 30 });

// Open a consumer group
using var consumer = queue.OpenConsumer("my-group");

// Poll for records (blocking with timeout)
var record = consumer.Poll(TimeSpan.FromSeconds(5));
if (record != null)
{
    Console.WriteLine($"Got record at {record.Timestamp}");
    consumer.Ack(record.Timestamp);
}

// Async polling
var record = await consumer.PollAsync(TimeSpan.FromSeconds(5));
```

### Using Journal Queue

```csharp
using var journalQueue = store.OpenJournalQueue();
using var consumer = journalQueue.OpenConsumer("sync-group");

var journalRecord = consumer.Poll(TimeSpan.FromSeconds(5));
if (journalRecord != null)
{
    Console.WriteLine($"Journal seq: {journalRecord.Sequence}");
    consumer.Ack(journalRecord.Sequence);
}
```

## Exception Handling

All errors throw `TmslException` with a `Code` property of type `TmslErrorCode`:

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

## Complete Example

```csharp
using Timslite;
using Timslite.Errors;

try
{
    // Open store with journal enabled
    var config = new StoreConfig
    {
        EnableJournal = true,
        EnableBackgroundThread = true
    };
    
    using var store = Store.Open("/tmp/timslite-example", config);
    
    // Create dataset with 1-hour retention
    store.CreateDataset("metrics", "cpu", new CreateDatasetOptions
    {
        Config = new DatasetConfig
        {
            RetentionWindow = 3600,
            IndexContinuous = 0
        }
    });
    
    // Open dataset and write some data
    using var ds = store.OpenDataset("metrics", "cpu");
    
    for (int i = 0; i < 100; i++)
    {
        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + i;
        ds.Write(ts, BitConverter.GetBytes(i));
    }
    
    // Query the data
    using var iter = ds.QueryIter(
        DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100
    );
    
    foreach (var rec in iter)
    {
        int value = BitConverter.ToInt32(rec.Data, 0);
        Console.WriteLine($"ts={rec.Timestamp}, value={value}");
    }
    
    // Use queue for real-time processing
    using var queue = store.OpenQueue(ds);
    queue.Push(new byte[] { 1, 2, 3 });
    
    using var consumer = queue.OpenConsumer("processor");
    var record = consumer.Poll(TimeSpan.FromSeconds(1));
    if (record != null)
    {
        consumer.Ack(record.Timestamp);
    }
    
    // Tick background tasks manually if not using background thread
    // store.TickBackgroundTasks();
}
catch (TmslException ex)
{
    Console.WriteLine($"timslite error: {ex.Code} - {ex.Message}");
}
```
