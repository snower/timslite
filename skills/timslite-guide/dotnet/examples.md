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
ds.Write(1700000001L, BitConverter.GetBytes(43.1));

// Write with auto-generated timestamp
ds.WriteNow(BitConverter.GetBytes(44.0));
```

### Reading Records

```csharp
// Read by timestamp
var rec = ds.Read(1700000000L);
if (rec != null)
{
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}: {value}");
}

// Read the latest record
var latest = ds.ReadLatest();
if (latest != null)
{
    Console.WriteLine($"Latest: ts={latest.Timestamp}");
}

// Check existence
bool exists = ds.ReadExist(1700000000L);
Console.WriteLine($"Record exists: {exists}");

// Read length only
uint? len = ds.ReadLength(1700000000L);
Console.WriteLine($"Record length: {len} bytes");
```

### Appending to Records

```csharp
// Write initial record
ds.Write(1700000000L, Encoding.UTF8.GetBytes("chunk1"));

// Append to same timestamp
ds.Append(1700000000L, Encoding.UTF8.GetBytes(",chunk2"));
ds.Append(1700000000L, Encoding.UTF8.GetBytes(",chunk3"));

// Read full record
var rec = ds.Read(1700000000L);
Console.WriteLine(Encoding.UTF8.GetString(rec.Data)); // "chunk1,chunk2,chunk3"

// Forward append (creates new record)
ds.Append(1700000001L, Encoding.UTF8.GetBytes("new_record"));
```

### Correcting Records

```csharp
// Write original
ds.Write(1700000000L, Encoding.UTF8.GetBytes("wrong_value"));

// Correct it
ds.Correct(1700000000L, Encoding.UTF8.GetBytes("correct_value"));

// Verify
var rec = ds.Read(1700000000L);
Console.WriteLine(Encoding.UTF8.GetString(rec.Data)); // "correct_value"
```

### Deleting Records

```csharp
ds.Write(1700000000L, new byte[] { 1, 2, 3 });
ds.Delete(1700000000L);

var rec = ds.Read(1700000000L);
Console.WriteLine(rec == null); // true
```

---

## 3. Query Operations

### Eager Query

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Write test data
for (long i = 1; i <= 100; i++)
{
    ds.Write(i, BitConverter.GetBytes(i * 0.1));
}

// Query range
var records = ds.Query(10, 20);
foreach (var rec in records)
{
    double value = BitConverter.ToDouble(rec.Data, 0);
    Console.WriteLine($"ts={rec.Timestamp}: {value}");
}
```

### Lazy Iterator

```csharp
// Forward iteration
using var iter = ds.QueryIter(1, 100);
while (iter.MoveNext())
{
    var rec = iter.Current;
    Console.WriteLine($"ts={rec.Timestamp}: {rec.Data.Length} bytes");
}

// Reverse iteration
using var iter2 = ds.QueryIter(1, 100);
iter2.Reverse();
while (iter2.MoveNext())
{
    var rec = iter2.Current;
    Console.WriteLine($"ts={rec.Timestamp}: {rec.Data.Length} bytes");
}

// Skip records
using var iter3 = ds.QueryIter(1, 100);
iter3.Skip(10); // skip first 10
while (iter3.MoveNext())
{
    // starts at 11th record
}

// Collect all
using var iter4 = ds.QueryIter(1, 100);
var allRecords = iter4.CollectAll();
Console.WriteLine($"Collected {allRecords.Count} records");
```

### Query Existence

```csharp
byte[] existFlags = ds.QueryExist(1, 100);
for (int i = 0; i < existFlags.Length; i++)
{
    if (existFlags[i] == 1)
    {
        Console.WriteLine($"Record at ts={i + 1} exists");
    }
}
```

### Query Lengths

```csharp
// Eager
var lengths = ds.QueryLength(1, 100);
foreach (var entry in lengths)
{
    Console.WriteLine($"ts={entry.Timestamp}: {entry.Length} bytes");
}

// Lazy iterator
using var liter = ds.QueryLengthIter(1, 100);
while (liter.MoveNext())
{
    var entry = liter.Current;
    Console.WriteLine($"ts={entry.Timestamp}: {entry.Length} bytes");
}
```

---

## 4. Queue Consumer Pattern

### Basic Queue Usage

```csharp
using var ds = store.OpenDataset("tasks", "jobs");

// Open queue
var queue = store.OpenQueue(ds);

// Push tasks
for (int i = 0; i < 100; i++)
{
    var payload = Encoding.UTF8.GetBytes($"{{\"taskId\": {i}}}");
    long ts = queue.Push(payload);
    Console.WriteLine($"Pushed task {i} at ts={ts}");
}

// Open consumer
var consumer = queue.OpenConsumer("worker_group");

// Process tasks
for (int i = 0; i < 100; i++)
{
    var record = consumer.Poll(TimeSpan.FromSeconds(5));
    if (record != null)
    {
        var task = JsonSerializer.Deserialize<TaskPayload>(record.Data);
        Console.WriteLine($"Processing task {task.TaskId}");
        
        // Acknowledge
        consumer.Ack(record.Timestamp);
    }
}

// Flush and close
consumer.Flush();
consumer.Dispose();
queue.Dispose();
```

### Async Queue Processing

```csharp
using var ds = store.OpenDataset("tasks", "jobs");
var queue = store.OpenQueue(ds);
var consumer = queue.OpenConsumer("async_worker");

while (true)
{
    var record = await consumer.PollAsync(TimeSpan.FromSeconds(1));
    if (record != null)
    {
        await ProcessRecordAsync(record);
        consumer.Ack(record.Timestamp);
    }
}
```

### Custom Consumer Options

```csharp
var options = new QueueConsumerOptions
{
    Config = new QueueConsumerConfig
    {
        RunningExpiredSeconds = 120,  // 2 minutes before stuck task retry
        MaxRetryCount = 5            // max 5 retries before parked
    }
};

var consumer = queue.OpenConsumer("reliable_worker", options);
```

### Inspecting Consumer State

```csharp
var inspect = consumer.Inspect();

Console.WriteLine($"Group: {inspect.Info.GroupName}");
Console.WriteLine($"Running expired: {inspect.Info.RunningExpiredSeconds}s");
Console.WriteLine($"Max retry: {inspect.Info.MaxRetryCount}");
Console.WriteLine($"Processed up to: {inspect.State.ProcessedTs}");
Console.WriteLine($"Pending entries: {inspect.State.PendingEntries.Count}");

foreach (var entry in inspect.State.PendingEntries)
{
    Console.WriteLine($"  ts={entry.Timestamp}, status={entry.Status}, retries={entry.RetryCount}");
}
```

### Managing Consumer Groups

```csharp
// List consumer groups
string[] groups = queue.GetConsumerGroupNames();
foreach (var group in groups)
{
    Console.WriteLine($"Consumer group: {group}");
}

// Drop a consumer group
queue.DropConsumer("old_group");
```

---

## 5. Journal Queue for Audit/Sync

### Direct Journal Query

```csharp
// Get latest sequence
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
```

### Journal Queue Consumer

```csharp
// Open journal queue
var jq = store.OpenJournalQueue();
var jc = jq.OpenConsumer("audit_group");

// Process journal entries
for (int i = 0; i < 1000; i++)
{
    var jrecord = jc.Poll(TimeSpan.FromSeconds(1));
    if (jrecord != null)
    {
        Console.WriteLine($"Journal seq={jrecord.Sequence}: {jrecord.Data.Length} bytes");
        
        // Process journal entry...
        
        // Acknowledge
        jc.Ack(jrecord.Sequence);
    }
    else
    {
        break; // no more entries
    }
}

// Async processing
var jrecord2 = await jc.PollAsync(TimeSpan.FromSeconds(5));
if (jrecord2 != null)
{
    jc.Ack(jrecord2.Sequence);
}
```

### Resolving Source Records from Journal

```csharp
// When processing journal entries of type 0x11, 0x12, 0x13,
// you may need to read the source dataset record
var journalRecords = store.JournalQuery(1, 100);
foreach (var jr in journalRecords)
{
    // Parse journal entry to get dataset identifier and index info
    // (parsing logic depends on journal entry type)
    
    // Example: read source record
    var indexInfo = new JournalIndexInfo(
        Timestamp: 1700000000L,
        BlockOffset: 0,
        InBlockOffset: 0
    );
    
    try
    {
        var sourceRecord = store.ReadJournalSourceRecord(datasetIdentifier, indexInfo);
        Console.WriteLine($"Source record: ts={sourceRecord.Timestamp}, {sourceRecord.Data.Length} bytes");
    }
    catch (TmslException ex) when (ex.Code == TmslErrorCode.NotFound)
    {
        Console.WriteLine("Source record no longer exists");
    }
}
```

---

## 6. Background Tasks

### Manual Background Task Execution

```csharp
// Disable automatic background thread
var config = new StoreConfig
{
    EnableBackgroundThread = false
};

using var store = Store.Open("/path/to/data", config);

// Write some data
using var ds = store.CreateDataset("metrics", "cpu");
for (long i = 1; i <= 1000; i++)
{
    ds.Write(i, BitConverter.GetBytes(i * 0.1));
}

// Manually trigger background tasks
var result = store.TickBackgroundTasks();
Console.WriteLine($"Executed {result.ExecutedTasks} tasks");
Console.WriteLine($"Next run in {result.NextDelayMs}ms");

// Run again to flush
var result2 = store.TickBackgroundTasks();
Console.WriteLine($"Executed {result2.ExecutedTasks} tasks");

// Check next delay without executing
ulong delayMs = store.NextBackgroundDelayMs();
Console.WriteLine($"Next task due in {delayMs}ms");
```

---

## 7. Inspection and Monitoring

### Dataset Inspection

```csharp
using var ds = store.OpenDataset("metrics", "cpu");

// Get dataset identifier
ulong id = ds.Identifier;
Console.WriteLine($"Dataset identifier: {id}");

// Inspect via store
var result = store.InspectDataset("metrics", "cpu");

Console.WriteLine("=== Dataset Info ===");
Console.WriteLine($"Name: {result.Info.Name}");
Console.WriteLine($"Type: {result.Info.DatasetType}");
Console.WriteLine($"Identifier: {result.Info.Identifier}");
Console.WriteLine($"Compression: {(result.Info.CompressType == 0 ? "zstd" : "deflate")}");
Console.WriteLine($"Index mode: {(result.Info.IndexContinuous == 0 ? "sparse" : "continuous")}");
Console.WriteLine($"Retention: {result.Info.RetentionWindow}");
Console.WriteLine($"Journal: {result.Info.EnableJournal}");

Console.WriteLine("\n=== Dataset State ===");
Console.WriteLine($"Latest timestamp: {result.State.LatestWrittenTimestamp}");
Console.WriteLine($"Data segments: {result.State.DataSegments} ({result.State.OpenDataSegments} open)");
Console.WriteLine($"Index segments: {result.State.IndexSegments} ({result.State.OpenIndexSegments} open)");
Console.WriteLine($"Total records: {result.State.TotalRecordCount}");
Console.WriteLine($"Data size: {result.State.TotalDataSize} bytes");
Console.WriteLine($"Read-only: {result.State.ReadOnly}");
Console.WriteLine($"Queue groups: {result.State.QueueConsumerGroups}");
```

### Library Version

```csharp
string version = TimsliteInfo.Version();
Console.WriteLine($"timslite version: {version}");
```

---

## 8. Error Handling

### Basic Error Handling

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
catch (TmslException ex) when (ex.Code == TmslErrorCode.InvalidData)
{
    Console.WriteLine($"Invalid data: {ex.Message}");
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.Expired)
{
    Console.WriteLine("Record expired");
}
catch (TmslException ex)
{
    Console.WriteLine($"Error {ex.Code}: {ex.Message}");
}
```

### Comprehensive Error Handling

```csharp
using Timslite.Errors;

try
{
    // Your timslite operations
}
catch (TmslException ex)
{
    switch (ex.Code)
    {
        case TmslErrorCode.Io:
            Console.WriteLine($"I/O error: {ex.Message}");
            break;
        case TmslErrorCode.InvalidMagic:
        case TmslErrorCode.InvalidVersion:
            Console.WriteLine($"Corrupted data: {ex.Message}");
            break;
        case TmslErrorCode.MmapError:
            Console.WriteLine($"Memory mapping error: {ex.Message}");
            break;
        case TmslErrorCode.CompressionError:
        case TmslErrorCode.DecompressionError:
            Console.WriteLine($"Compression error: {ex.Message}");
            break;
        case TmslErrorCode.SegmentFull:
            Console.WriteLine("Segment full, consider increasing segment size");
            break;
        case TmslErrorCode.QueueAlreadyOpen:
            Console.WriteLine("Queue already opened by another consumer");
            break;
        case TmslErrorCode.ConsumerGroupNotFound:
            Console.WriteLine("Consumer group not found");
            break;
        case TmslErrorCode.ConsumerGroupExists:
            Console.WriteLine("Consumer group already exists");
            break;
        case TmslErrorCode.QueueClosed:
            Console.WriteLine("Queue is closed");
            break;
        case TmslErrorCode.PendingFull:
            Console.WriteLine("Pending queue is full");
            break;
        case TmslErrorCode.StoreClosed:
            Console.WriteLine("Store is closed");
            break;
        case TmslErrorCode.DatasetClosed:
            Console.WriteLine("Dataset is closed");
            break;
        case TmslErrorCode.QueueBridgeClosed:
            Console.WriteLine("Queue bridge is closed");
            break;
        case TmslErrorCode.IteratorExhausted:
            Console.WriteLine("Iterator is exhausted");
            break;
        default:
            Console.WriteLine($"Unknown error {ex.Code}: {ex.Message}");
            break;
    }
}
```

---

## 9. Complete Example: Time-Series Data Pipeline

```csharp
using Timslite;
using Timslite.Errors;

// Configuration
var storeConfig = new StoreConfig
{
    FlushIntervalSeconds = 30,
    EnableBackgroundThread = true,
    EnableJournal = true,
    CacheMaxMemory = 512 * 1024 * 1024
};

var datasetConfig = new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = 0,     // sparse mode for irregular timestamps
        RetentionWindow = 86400, // 24 hours retention
        CompressLevel = 6
    }
};

// Open store
using var store = Store.Open("/data/timeseries", storeConfig);

// Create dataset
try
{
    store.CreateDataset("sensor", "temperature", datasetConfig);
}
catch (TmslException ex) when (ex.Code == TmslErrorCode.AlreadyExists)
{
    // Dataset already exists, continue
}

// Open dataset
using var ds = store.OpenDataset("sensor", "temperature");

// Write sensor data
for (long i = 1; i <= 1000; i++)
{
    var value = 20.0 + Math.Sin(i * 0.01) * 5.0;
    ds.Write(i, BitConverter.GetBytes(value));
}

// Query last hour
var records = ds.Query(900, 1000);
Console.WriteLine($"Last hour: {records.Count} records");

// Query with iterator for large ranges
using var iter = ds.QueryIter(1, 1000);
iter.Reverse(); // newest first
iter.Skip(10);  // skip 10 most recent
var remaining = iter.CollectAll();
Console.WriteLine($"Remaining: {remaining.Count} records");

// Check lengths
var lengths = ds.QueryLength(1, 100);
foreach (var entry in lengths)
{
    Console.WriteLine($"ts={entry.Timestamp}: {entry.Length} bytes");
}

// Inspect dataset
var inspect = store.InspectDataset("sensor", "temperature");
Console.WriteLine($"Total records: {inspect.State.TotalRecordCount}");
Console.WriteLine($"Data size: {inspect.State.TotalDataSize} bytes");

// Manual background tasks
var tick = store.TickBackgroundTasks();
Console.WriteLine($"Background: {tick.ExecutedTasks} tasks, next in {tick.NextDelayMs}ms");
```
