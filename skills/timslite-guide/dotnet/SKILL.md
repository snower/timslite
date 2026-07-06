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

The wrapper uses UniFFI with a Kotlin/JVM backend and JNA for native library loading. The NuGet package includes native libraries for all supported platforms under `runtimes/<rid>/native/`:

- `runtimes/osx-x64/native/libtimslite_dotnet.dylib`
- `runtimes/osx-arm64/native/libtimslite_dotnet.dylib`
- `runtimes/linux-x64/native/libtimslite_dotnet.so`
- `runtimes/linux-arm64/native/libtimslite_dotnet.so`
- `runtimes/win-x64/native/timslite_dotnet.dll`
- `runtimes/win-arm64/native/timslite_dotnet.dll`

The `NativeLibraryLoader` automatically detects the current OS/architecture and loads the correct library. No additional configuration is needed.

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

## Documentation

- **[API Reference](api-reference.md)** — Complete .NET API signatures, parameters, return types
- **[Examples](examples.md)** — Feature scenarios with .NET examples

## Key Patterns

### IDisposable Pattern

All major types implement `IDisposable` for deterministic resource cleanup:

```csharp
// Using statement ensures proper cleanup
using var store = Store.Open("/path/to/data");
using var ds = store.OpenDataset("metrics", "cpu");
using var iter = ds.QueryIter(startTs, endTs);

foreach (var rec in iter)
{
    // process record
}
// iter, ds, store are disposed in reverse order
```

### Exception Handling

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

### Nullable Return Types

Methods that may not find a result return nullable types:

```csharp
var rec = ds.Read(12345L); // Returns Record? (null if not found)
if (rec != null)
{
    // process record
}
```

### Read-Only Stores

```csharp
var config = new StoreConfig { ReadOnly = true };
using var store = Store.Open("/path/to/data", config);

if (store.IsReadOnly())
{
    Console.WriteLine("Store is read-only");
}
```

## Platform Support

| Platform | Architecture | RID |
|----------|--------------|-----|
| macOS | x86_64 | osx-x64 |
| macOS | ARM64 | osx-arm64 |
| Linux | x86_64 | linux-x64 |
| Linux | ARM64 | linux-arm64 |
| Windows | x86_64 | win-x64 |
| Windows | ARM64 | win-arm64 |

## Minimum Requirements

- .NET 8.0 or later
- Supported OS: macOS, Linux, Windows
