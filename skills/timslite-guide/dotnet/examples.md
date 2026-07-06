# .NET Examples

> Feature scenarios with .NET examples demonstrating timslite capabilities.

---

## 1. Store and Dataset Lifecycle

### Opening a Store with Custom Configuration

```csharp
using Timslite;

var config = new StoreConfig
{
    FlushIntervalSeconds = 30,
    IdleTimeoutSeconds = 600,
    DataSegmentSize = 128 * 1024 * 1024, // 128 MiB
    IndexSegmentSize = 8 * 1024 * 1024,  // 8 MiB
    CompressLevel = 6,
    CacheMaxMemory = 512 * 1024 * 1024,  // 512 MiB
    EnableBackgroundThread = true,
    EnableJournal = true
};

using var store = Store.Open("/path/to/data", config);

if (store.IsReadOnly())
{
    Console.WriteLine("Store opened in read-only mode");
}
```

### Creating Datasets with Different Configurations

```csharp
// Sparse index (default) - good for irregular timestamps
store.CreateDataset("metrics", "cpu", new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = 0,
        RetentionWindow = 0, // No retention limit
        CompressLevel = 6
    }
});

// Continuous index - good for regular, high-frequency data
store.CreateDataset("events", "click", new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = 1,
        RetentionWindow = 86400, // 24 hours
        CompressLevel = 3 // Faster compression
    }
});

// Dataset with journal enabled for sync/audit
store.CreateDataset("audit", "log", new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        EnableJournal = true,
        RetentionWindow = 604800 // 7 days
    }
});
```

### Listing Datasets

```csharp
var names = store.GetDatasetNames();
foreach (var name in names)
{
    var types = store.GetDatasetTypes(name);
    foreach (var type in types)
    {
        Console.WriteLine($"Dataset: {name}/{type}");
        
        var inspect = store.InspectDataset(name, type);
        Console.WriteLine($"  Records: {inspect.State.TotalRecordCount}");
        Console.WriteLine($"  Data size: {inspect.State.TotalDataSize} bytes");
        Console.WriteLine($"  Latest ts: {inspect.State.LatestWrittenTimestamp}");
    }
}
```

### Dropping Datasets

```csharp
try
{
    store.DropDataset("metrics", "cpu");
    Console.WriteLine("Dataset dropped");
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.NotFound)
{
    Console.WriteLine("Dataset not found");
}
```

---

## 2. Basic Read/Write Operations

### Writing Individual Records

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write with explicit timestamp
ds.Write(1700000000L, BitConverter.GetBytes(42.5));

// Write with current timestamp
ds.WriteNow(BitConverter.GetBytes(43.2));

// Write with byte array
var data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
ds.Write(1700000001L, data);
```

### Reading Records

```csharp
// Read by timestamp
var rec = ds.Read(1700000000L);
if (rec != null)
{
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}, value={value}");
}

// Check existence without reading data
if (ds.ReadExist(1700000000L))
{
    Console.WriteLine("Record exists");
}

// Read latest record
var latest = ds.ReadLatest();
if (latest != null)
{
    Console.WriteLine($"Latest ts: {latest.Timestamp}");
}

// Read data length only
var length = ds.ReadLength(1700000000L);
if (length != null)
{
    Console.WriteLine($"Data length: {length} bytes");
}
```

### Appending to Records

```csharp
// Forward append (new timestamp)
ds.Append(1700000002L, BitConverter.GetBytes(50.0));

// Append to existing record (same timestamp)
ds.Append(1700000002L, BitConverter.GetBytes(51.0));

// Append with current timestamp
ds.AppendNow(BitConverter.GetBytes(52.0));
```

### Deleting Records

```csharp
ds.Delete(1700000000L);

// Verify deletion
var rec = ds.Read(1700000000L);
Console.WriteLine(rec == null ? "Deleted" : "Still exists");
```

---

## 3. Query Operations

### Range Query with List

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

var records = ds.Query(1700000000L, 1700003600L);
Console.WriteLine($"Found {records.Count} records");

foreach (var rec in records)
{
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}, value={value}");
}
```

### Using QueryIterator for Large Results

```csharp
using var iter = ds.QueryIter(1700000000L, 1700003600L);

while (iter.MoveNext())
{
    var rec = iter.Current;
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}, value={value}");
}
```

### Using foreach with QueryIterator

```csharp
using var iter = ds.QueryIter(1700000000L, 1700003600L);

foreach (var rec in iter)
{
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}, value={value}");
}
```

### Collecting All Results

```csharp
using var iter = ds.QueryIter(1700000000L, 1700003600L);
var allRecords = iter.CollectAll();

Console.WriteLine($"Collected {allRecords.Count} records");
```

### Reversing Iteration Order

```csharp
using var iter = ds.QueryIter(1700000000L, 1700003600L);
iter.Reverse();

foreach (var rec in iter)
{
    Console.WriteLine($"ts={rec.Timestamp}");
}
```

### Skipping Records

```csharp
using var iter = ds.QueryIter(1700000000L, 1700003600L);
iter.Skip(100); // Skip first 100 records

foreach (var rec in iter)
{
    Console.WriteLine($"ts={rec.Timestamp}");
}
```

### Length-Only Query

```csharp
using var iter = ds.QueryLengthIter(1700000000L, 1700003600L);

ulong totalSize = 0;
foreach (var entry in iter)
{
    totalSize += entry.Length;
}

Console.WriteLine($"Total data size: {totalSize} bytes");
```

---

## 4. Queue Operations

### Basic Queue Usage

```csharp
using var ds = store.OpenDataset("events", "click");
using var queue = store.OpenQueue(ds);

// Push records to queue
for (int i = 0; i < 100; i++)
{
    var data = BitConverter.GetBytes(i);
    long ts = queue.Push(data);
    Console.WriteLine($"Pushed at ts={ts}");
}
```

### Consumer Group Processing

```csharp
using var consumer = queue.OpenConsumer("processor");

// Poll with timeout
var record = consumer.Poll(TimeSpan.FromSeconds(5));
if (record != null)
{
    int value = BitConverter.ToInt32(record.Data, 0);
    Console.WriteLine($"Processing: {value}");
    
    // Acknowledge processing
    consumer.Ack(record.Timestamp);
}
```

### Async Consumer Processing

```csharp
using var consumer = queue.OpenConsumer("async-processor");

var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var record = await consumer.PollAsync(TimeSpan.FromSeconds(1), cts.Token);
        if (record != null)
        {
            // Process record
            await ProcessRecordAsync(record);
            consumer.Ack(record.Timestamp);
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Processing cancelled");
}
```

### Multiple Consumer Groups

```csharp
// Group 1: Real-time processing
using var realtimeConsumer = queue.OpenConsumer("realtime");

// Group 2: Analytics
using var analyticsConsumer = queue.OpenConsumer("analytics");

// Both groups receive all records independently
var record1 = realtimeConsumer.Poll(TimeSpan.FromSeconds(1));
var record2 = analyticsConsumer.Poll(TimeSpan.FromSeconds(1));
```

### Inspecting Consumer State

```csharp
var inspect = consumer.Inspect();
Console.WriteLine($"Group: {inspect.Info.GroupName}");
Console.WriteLine($"Processed ts: {inspect.State.ProcessedTs}");
Console.WriteLine($"Pending entries: {inspect.State.PendingEntries.Count}");

foreach (var entry in inspect.State.PendingEntries)
{
    Console.WriteLine($"  ts={entry.Timestamp}, status={entry.Status}, retries={entry.RetryCount}");
}
```

### Managing Consumer Groups

```csharp
// List all consumer groups
var groups = queue.GetConsumerGroupNames();
foreach (var group in groups)
{
    Console.WriteLine($"Consumer group: {group}");
}

// Drop a consumer group
queue.DropConsumerGroup("old-group");
```

---

## 5. Journal Operations

### Reading Journal Directly

```csharp
// Get latest sequence
var latestSeq = store.JournalLatestSequence();
Console.WriteLine($"Latest journal sequence: {latestSeq}");

// Read specific journal record
var journalRec = store.JournalRead(1L);
if (journalRec != null)
{
    Console.WriteLine($"Seq={journalRec.Sequence}, data_len={journalRec.Data.Length}");
}

// Query journal range
var journalRecords = store.JournalQuery(1L, 100L);
foreach (var rec in journalRecords)
{
    Console.WriteLine($"Seq={rec.Sequence}");
}
```

### Journal Queue Consumer

```csharp
using var journalQueue = store.OpenJournalQueue();
using var consumer = journalQueue.OpenConsumer("sync-group");

// Process journal records
while (true)
{
    var record = consumer.Poll(TimeSpan.FromSeconds(5));
    if (record == null) break;
    
    Console.WriteLine($"Journal seq: {record.Sequence}");
    
    // Process journal record (e.g., sync to another system)
    await SyncToRemoteAsync(record);
    
    consumer.Ack(record.Sequence);
}
```

### Reading Source Records from Journal

```csharp
// When processing journal records of type 0x11 (write), 0x12 (delete), 0x13 (append),
// you can read the source record using the journal index info
var journalRec = store.JournalRead(42L);
if (journalRec != null)
{
    // Parse journal record to get dataset identifier and index info
    // (parsing depends on journal record type)
    ulong datasetId = /* parsed from journal record */;
    var indexInfo = new JournalIndexInfo(
        Timestamp: /* parsed */,
        BlockOffset: /* parsed */,
        InBlockOffset: /* parsed */
    );
    
    var sourceRecord = store.ReadJournalSourceRecord(datasetId, indexInfo);
    Console.WriteLine($"Source ts: {sourceRecord.Timestamp}");
}
```

---

## 6. Retention and Expiration

### Setting Retention Window

```csharp
// Create dataset with 1-hour retention
store.CreateDataset("metrics", "cpu", new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        RetentionWindow = 3600 // 1 hour in seconds
    }
});
```

### Working with Retained Data

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write data
ds.Write(1700000000L, BitConverter.GetBytes(42.0));

// After retention window expires:
var rec = ds.Read(1700000000L);
// rec will be null if timestamp is expired

// Expired timestamps cannot be written to
try
{
    ds.Write(1700000000L, BitConverter.GetBytes(43.0));
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.Expired)
{
    Console.WriteLine("Cannot write to expired timestamp");
}
```

### Retention Check Hour

```csharp
// Configure retention check at 2 AM UTC
var config = new StoreConfig
{
    RetentionCheckHour = 2
};

using var store = Store.Open("/path/to/data", config);
```

---

## 7. Background Tasks

### Using Background Thread

```csharp
var config = new StoreConfig
{
    EnableBackgroundThread = true
};

using var store = Store.Open("/path/to/data", config);
// Background tasks run automatically
```

### Manual Background Task Execution

```csharp
var config = new StoreConfig
{
    EnableBackgroundThread = false
};

using var store = Store.Open("/path/to/data", config);

// Manually tick background tasks
var result = store.TickBackgroundTasks();
Console.WriteLine($"Executed {result.ExecutedTasks} tasks");
Console.WriteLine($"Next delay: {result.NextDelayMs}ms");

// Or use a timer
var timer = new System.Threading.Timer(_ =>
{
    var r = store.TickBackgroundTasks();
    // Schedule next tick
}, null, 0, (int)store.NextBackgroundDelayMs());
```

---

## 8. Error Handling Patterns

### Catching Specific Errors

```csharp
using Timslite.Errors;

try
{
    store.CreateDataset("metrics", "cpu");
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.AlreadyExists)
{
    Console.WriteLine("Dataset already exists, opening instead");
    var ds = store.OpenDataset("metrics", "cpu");
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.InvalidData)
{
    Console.WriteLine($"Invalid data: {ex.Message}");
}
catch (TmslException ex)
{
    Console.WriteLine($"Error {ex.Code}: {ex.Message}");
}
```

### Handling Queue Errors

```csharp
try
{
    using var queue = store.OpenQueue(ds);
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.QueueAlreadyOpen)
{
    Console.WriteLine("Queue already open for this dataset");
}
```

### Handling Iterator Exhaustion

```csharp
using var iter = ds.QueryIter(startTs, endTs);

try
{
    while (true)
    {
        var rec = iter.Current; // May throw if not called MoveNext first
        // process
        iter.MoveNext();
    }
}
catch (InvalidOperationException)
{
    // No current record
}
```

---

## 9. Performance Optimization

### Batch Writing

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write records in sequence
for (long ts = 1700000000L; ts < 1700003600L; ts++)
{
    ds.Write(ts, BitConverter.GetBytes(ts * 1.0));
}
```

### Using Length Iterator for Statistics

```csharp
// Calculate total data size without loading data
using var iter = ds.QueryLengthIter(startTs, endTs);

ulong totalSize = 0;
uint count = 0;

foreach (var entry in iter)
{
    totalSize += entry.Length;
    count++;
}

Console.WriteLine($"Records: {count}, Total size: {totalSize} bytes");
Console.WriteLine($"Average record size: {totalSize / count} bytes");
```

### Skipping Unneeded Records

```csharp
// Sample every 100th record
using var iter = ds.QueryIter(startTs, endTs);

while (iter.MoveNext())
{
    var rec = iter.Current;
    // process sampled record
    iter.Skip(99); // Skip 99, process 1
}
```

---

## 10. Complete Application Example

```csharp
using Timslite;
using Timslite.Errors;

class TimeSeriesApp
{
    private readonly Store _store;
    
    public TimeSeriesApp(string dataDir)
    {
        var config = new StoreConfig
        {
            EnableJournal = true,
            EnableBackgroundThread = true,
            CacheMaxMemory = 256 * 1024 * 1024 // 256 MiB
        };
        
        _store = Store.Open(dataDir, config);
    }
    
    public void InitializeDatasets()
    {
        // Metrics dataset with 24-hour retention
        _store.CreateDataset("metrics", "cpu", new CreateDatasetOptions
        {
            Config = new DatasetConfig
            {
                RetentionWindow = 86400,
                IndexContinuous = 1,
                EnableJournal = true
            }
        });
        
        // Events dataset with 7-day retention
        _store.CreateDataset("events", "user", new CreateDatasetOptions
        {
            Config = new DatasetConfig
            {
                RetentionWindow = 604800,
                IndexContinuous = 0
            }
        });
    }
    
    public void WriteMetric(string name, string type, long timestamp, double value)
    {
        using var ds = _store.OpenDataset(name, type);
        ds.Write(timestamp, BitConverter.GetBytes(value));
    }
    
    public List<(long Timestamp, double Value)> QueryMetrics(
        string name, string type, long start, long end)
    {
        using var ds = _store.OpenDataset(name, type);
        using var iter = ds.QueryIter(start, end);
        
        var results = new List<(long, double)>();
        foreach (var rec in iter)
        {
            double value = BitConverter.ToDouble(rec.Data, 0);
            results.Add((rec.Timestamp, value));
        }
        
        return results;
    }
    
    public void ProcessQueue(string name, string type, string groupName, 
        Action<Record> handler)
    {
        using var ds = _store.OpenDataset(name, type);
        using var queue = _store.OpenQueue(ds);
        using var consumer = queue.OpenConsumer(groupName);
        
        while (true)
        {
            var record = consumer.Poll(TimeSpan.FromSeconds(1));
            if (record == null) break;
            
            handler(record);
            consumer.Ack(record.Timestamp);
        }
    }
    
    public void SyncJournal(string consumerGroupName, Action<JournalRecord> handler)
    {
        using var journalQueue = _store.OpenJournalQueue();
        using var consumer = journalQueue.OpenConsumer(consumerGroupName);
        
        while (true)
        {
            var record = consumer.Poll(TimeSpan.FromSeconds(1));
            if (record == null) break;
            
            handler(record);
            consumer.Ack(record.Sequence);
        }
    }
    
    public void Dispose()
    {
        _store.Dispose();
    }
}

// Usage
var app = new TimeSeriesApp("/tmp/timslite-app");
app.InitializeDatasets();

// Write some metrics
for (int i = 0; i < 100; i++)
{
    long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + i;
    app.WriteMetric("metrics", "cpu", ts, i * 1.5);
}

// Query metrics
var results = app.QueryMetrics("metrics", "cpu",
    DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
    DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 100);

foreach (var (ts, value) in results)
{
    Console.WriteLine($"ts={ts}, value={value}");
}

app.Dispose();
```
