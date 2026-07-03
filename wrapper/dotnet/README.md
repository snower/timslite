# Timslite .NET

[![.NET Release](https://github.com/snower/timslite/actions/workflows/dotnet-release.yml/badge.svg)](https://github.com/snower/timslite/actions/workflows/dotnet-release.yml)

.NET bindings for the [timslite](https://github.com/snower/timslite) time-series storage engine, built with [UniFFI](https://mozilla.github.io/uniffi-rs/).

## Installation

```shell
dotnet add package Timslite
```

## Quick Start

```csharp
using Timslite;

// Open or create a store.
using var store = Store.Open("/path/to/data");

// Create a dataset.
var config = new DatasetConfig();
store.CreateDataset("metrics", "gauge", config);

// Write a record.
var ds = store.OpenDataset("metrics", "gauge");
ds.Write(1700000000, new byte[] { 0x01, 0x02 });

// Read it back.
var record = ds.Read(1700000000);
Console.WriteLine($"Timestamp: {record.Timestamp}, Length: {record.Data.Length}");

// Query a range.
foreach (var r in ds.Query(1700000000, 1700000060))
{
    Console.WriteLine($"{r.Timestamp}: {r.Data.Length} bytes");
}

// Get the library version.
Console.WriteLine($"timslite {TimsliteInfo.Version()}");
```

## Queue Example

```csharp
using Timslite;

using var store = Store.Open("/path/to/data");

var config = new DatasetConfig();
store.CreateDataset("events", "log", config);

var ds = store.OpenDataset("events", "log");
var queue = ds.OpenQueue();

// Push a message.
queue.Push(1700000000, new byte[] { 0x01 });

// Open a consumer and poll.
using var consumer = queue.OpenConsumer("my-group");
var entry = consumer.Poll();
if (entry != null)
{
    Console.WriteLine($"Polled: {entry.Timestamp}");
    consumer.Ack(entry.Timestamp);
}
```

## Journal Example

```csharp
using Timslite;

// Open store with journal enabled.
var storeConfig = new StoreConfig { EnableJournal = true };
using var store = Store.Open("/path/to/data", storeConfig);

// Read journal entries.
var journalQueue = store.OpenJournalQueue();
using var consumer = journalQueue.OpenConsumer("replay");
var entry = consumer.Poll();
if (entry != null)
{
    Console.WriteLine($"Journal seq: {entry.Timestamp}");
    consumer.Ack(entry.Timestamp);
}
```

## Requirements

- .NET 8.0 or later (`net8.0`).
- Supported package platforms: Windows (x64/arm64), Linux glibc (x64/arm64), Linux musl (x64/arm64), macOS (arm64).

## Native Library Loading

The package includes pre-built native libraries for all supported platforms. The loader resolves them automatically from the NuGet restore output layout.

### Override

Set `TIMSLITE_NATIVE_LIBRARY_PATH` to point directly to a native library binary:

```shell
export TIMSLITE_NATIVE_LIBRARY_PATH=/opt/timslite/libtimslite_dotnet.so
```

## Supported Platforms

| RID | Library |
|-----|---------|
| `win-x64` | `timslite_dotnet.dll` |
| `win-arm64` | `timslite_dotnet.dll` |
| `linux-x64` | `libtimslite_dotnet.so` |
| `linux-arm64` | `libtimslite_dotnet.so` |
| `linux-musl-x64` | `libtimslite_dotnet.so` |
| `linux-musl-arm64` | `libtimslite_dotnet.so` |
| `osx-arm64` | `libtimslite_dotnet.dylib` |

### Unsupported Platforms

The following platforms are **not supported** in the current release. Attempting to load the native library on an unsupported RID will throw a clear error at runtime.

- **macOS x64** (`osx-x64`) - not built by the default NuGet release workflow.
- **NativeAOT** - pre-compiled native trimming is not yet validated.
- **FreeBSD**, **Android**, **iOS**, **wasm** - no native binaries are shipped.

If you need support for one of these platforms, please [open an issue](https://github.com/snower/timslite/issues).

## Building from Source

```shell
# Build the native library for the current platform.
cargo build --manifest-path wrapper/dotnet/native/Cargo.toml

# Copy the native binary into the runtimes layout.
bash wrapper/dotnet/scripts/prepare-publish.sh

# Build and test the .NET wrapper.
dotnet build wrapper/dotnet/Timslite.slnx
dotnet test wrapper/dotnet/Timslite.slnx
```

## License

MIT
