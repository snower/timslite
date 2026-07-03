# timslite .NET Wrapper - UniFFI 设计

> 目标: 在 `wrapper/dotnet` 中提供基于 UniFFI 的 .NET 绑定, 让 .NET 8+ 项目可以使用当前 timslite Rust library。最低目标框架为 `.NET 8` (`net8.0`)。

---

## 1. 设计目标

.NET wrapper 应当是 timslite 的薄包装层, 不重新实现存储、索引、queue、journal 或磁盘格式逻辑。核心行为继续由 Rust crate 的 public API 负责, .NET 层只处理:

- UniFFI C# binding 生成、native library 加载和 NuGet 包装。
- C# 友好的 public facade: `IDisposable`, options object, `byte[]`, `long`, `ulong`, `IReadOnlyList<T>`, `Task<T>`。
- C# generated binding 与 Rust 类型之间的安全转换。
- timslite 错误到 .NET exception 层次的稳定映射。
- Store/Dataset/Queue/Journal 对象生命周期和 `Dispose()` 后访问保护。
- .NET 测试、XML doc、README、NuGet 产物和 RID native asset 分发。

不改变当前 Rust、C ABI、Python wrapper、Node.js wrapper、Java wrapper 或磁盘格式契约。主 `timslite` crate 仍保持标准 Rust library; .NET wrapper 直接依赖 Rust public API, 不通过 `wrapper/cffi` 裸指针 ABI。

## 2. 技术方案

### 2.0 工具链确认 (DOTNET-0)

UniFFI 官方主线内置 Kotlin、Swift、Python 和 Ruby binding; C# binding 由第三方 `uniffi-bindgen-cs` 提供。因此 .NET wrapper 的第一阶段必须先固定并验证 C# generator 与本仓库 UniFFI 版本的兼容性。

**候选版本约束:**

| 组件 | 候选版本 | 说明 |
|------|----------|------|
| `uniffi` (Rust crate) | `0.31` | 与当前 `wrapper/java/native` 对齐 |
| `uniffi_bindgen` / scaffolding | `0.31` | Rust-side scaffolding 和 UDL 处理 |
| `uniffi-bindgen-cs` | `v0.11.0+v0.31.0` | 第三方 C# generator, 目标 UniFFI `0.31.0` |
| Rust toolchain | 待验证, generator README 要求 `1.88+` | 若本仓库 MSRV 更低, CI 需要单独处理 generator 安装 |
| .NET SDK | `8.0.x` 或更新 | `TargetFramework=net8.0` |

**.NET 8 兼容性配置:**

- `TargetFramework`: `net8.0`
- `Nullable`: `enable`
- `AllowUnsafeBlocks`: `true` (C# generator 需要 unsafe interop)
- Public facade 不依赖 .NET Framework、.NET Standard 或 Windows-only API。
- Native packages 使用 .NET portable RID, 例如 `win-x64`, `win-arm64`, `linux-x64`, `linux-arm64`, `osx-x64`, `osx-arm64`。

**NuGet 坐标建议:**

- `PackageId`: `Timslite`
- root namespace: `Timslite`
- generated binding namespace: `Timslite.Uniffi`
- public exception namespace: `Timslite.Errors`
- version: `0.1.1` (与 Rust crate 同步)

如果发布前 `Timslite` NuGet ID 不可用, 需要在发布阶段统一改为 `Snower.Timslite` 或其它确认后的 ID, 并同步 README、plan 和 release workflow。

### 2.1 推荐方案: UniFFI C# binding + C# facade

```text
.NET applications
        |
        v
Timslite C# facade classes
        |
        v
Timslite.Uniffi generated C# bindings
        |
        v
wrapper/dotnet/native Rust UniFFI bridge cdylib
        |
        v
timslite Rust public API
```

.NET 用户只依赖 `Timslite.*` facade。UniFFI 生成的 C# binding 放在 `Timslite.Uniffi`, 不作为长期稳定 API 承诺。这样可以复用 UniFFI 的 object handle、serialization、error lowering/lifting 和 native binding 生成, 同时避免 .NET 调用方直接面对 generated naming、unsafe interop 和 generator 版本细节。

Native library 名称固定为:

- Rust crate package: `timslite-dotnet`
- Rust lib name: `timslite_dotnet`
- Windows: `timslite_dotnet.dll`
- Linux: `libtimslite_dotnet.so`
- macOS: `libtimslite_dotnet.dylib`

### 2.2 备选方案

| 方案 | 优点 | 主要问题 | 结论 |
|------|------|----------|------|
| UniFFI C# binding + C# facade | 复用 Rust public API, 与 Java UniFFI wrapper 结构相近, facade 可保持 C# 风格 | C# backend 是第三方 generator, 必须固定版本并验证生成代码稳定性 | 推荐 |
| C# P/Invoke 直接调用 `wrapper/cffi` | 避免第三方 UniFFI C# generator, NuGet native loading 简单 | 需要复制 C ABI handle registry、malloc/free、err_buf、iterator close 规则, 容易与 Rust public API 分叉 | 不作为主线 |
| 手写 C# P/Invoke + 新 Rust C ABI bridge | 控制力最高 | 维护成本高, 等于重新实现 UniFFI 生成层, 与“UniFFI 包装”目标不符 | 不采用 |

## 3. 支持范围

### 3.1 MVP 必须覆盖

- Store lifecycle: open / close / read-only config / manual background tick。
- Dataset lifecycle: create / open / open by identifier / drop / inspect。
- Dataset data operations: write / write now / append / append now / delete / read / read latest / flush。
- Query operations: query iterator / query all / read exist / query exist / read length / query length。
- DatasetQueue: open queue / push / open consumer / list consumer groups / drop consumer / close。
- DatasetQueueConsumer: blocking poll / async poll facade / ack / flush / inspect。
- Journal read/query/latest/source-record dereference and JournalQueue consumer。
- .NET exception mapping、XML documentation、integration tests。
- Local build, `dotnet test`, `dotnet pack`, and NuGet-local package verification。

### 3.2 MVP 不覆盖

- 高级 journal payload 解码 helper。MVP 先返回 encoded journal payload `byte[]`, 与 Python/Node/Java wrapper 保持一致。
- .NET Framework、Xamarin、MAUI、Blazor、Unity 或 NativeAOT。
- Linux musl native artifact。可在发布矩阵稳定后追加 `linux-musl-x64` / `linux-musl-arm64`。
- `ReadOnlySpan<byte>` / `ReadOnlyMemory<byte>` 零拷贝 overload。MVP 使用 `byte[]`; 后续可在 facade 层加 overload。
- Queue poll callback。C# callback/delegate 跨 UniFFI 与 native notification thread 的生命周期需要单独 gate; 未验证前不纳入 MVP 完成标准。

## 4. 目标目录结构

目标实现阶段的目录结构:

```text
wrapper/dotnet/
├── design.md                              # 本文件
├── plan.md                                # 开发计划
├── README.md                              # .NET usage and packaging notes
├── Timslite.sln
├── native/                                # Rust UniFFI bridge crate
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── uniffi.toml                        # C# namespace and cdylib config
│   ├── build.rs
│   └── src/
│       ├── lib.rs                         # UniFFI scaffolding root
│       ├── timslite.udl                   # .NET-facing UniFFI interface
│       ├── bridge.rs                      # Store/Dataset/Queue wrapper objects
│       ├── config.rs                      # .NET-facing config records
│       ├── errors.rs                      # TmslError -> UniFFI error conversion
│       ├── query.rs                       # Query iterator bridge objects
│       ├── queue.rs                       # Dataset/journal queue bridge objects
│       └── bin/uniffi_bindgen.rs          # optional local bindgen helper
├── generated/
│   └── Timslite.Uniffi.cs                 # generated C# binding, build artifact
├── src/Timslite/
│   ├── Timslite.csproj                    # managed facade and NuGet metadata
│   ├── TimsliteInfo.cs
│   ├── NativeLibraryLoader.cs
│   ├── Store.cs
│   ├── StoreConfig.cs
│   ├── Dataset.cs
│   ├── DatasetConfig.cs
│   ├── QueryIterator.cs
│   ├── QueryLengthIterator.cs
│   ├── Queue.cs
│   ├── QueueConsumer.cs
│   ├── JournalQueue.cs
│   ├── JournalQueueConsumer.cs
│   ├── Records.cs
│   └── Errors/
│       └── *.cs
├── tests/Timslite.Tests/
│   ├── Timslite.Tests.csproj
│   └── *.cs
└── scripts/
    ├── prepare-publish.ps1
    └── prepare-publish.sh
```

`Cargo.toml`, `.csproj`, C# sources, Rust sources, tests, README, scripts and generated bindings are implementation-stage files. This design stage only creates `design.md` and `plan.md`。

## 5. Public .NET API

### 5.1 Entry point

```csharp
using Timslite;

string version = TimsliteInfo.Version();
```

`TimsliteInfo.Version()` calls the generated `version()` binding after ensuring native library resolution has been initialized.

### 5.2 Store

```csharp
using Timslite;

var config = new StoreConfig
{
    EnableJournal = true,
    EnableBackgroundThread = true,
    ReadOnly = StoreReadOnly.Auto,
};

using var store = Store.Open("./data/timslite", config);
store.CreateDataset("sensor_001", "events");

using var dataset = store.OpenDataset("sensor_001", "events");
dataset.Write(1, new byte[] { 1, 2, 3 });
```

Core methods:

```csharp
public sealed class Store : IDisposable
{
    public static Store Open(string dataDir);
    public static Store Open(string dataDir, StoreConfig? config);

    public void Dispose();
    public void Close();
    public bool IsClosed { get; }
    public bool IsReadOnly { get; }

    public void CreateDataset(string name, string datasetType);
    public void CreateDataset(string name, string datasetType, CreateDatasetOptions options);
    public Dataset OpenDataset(string name, string datasetType);
    public Dataset OpenDatasetByIdentifier(ulong identifier);
    public void DropDataset(string name, string datasetType);

    public DatasetQueue OpenQueue(Dataset dataset);
    public JournalQueue OpenJournalQueue();

    public long? JournalLatestSequence();
    public JournalRecord? JournalRead(long sequence);
    public IReadOnlyList<JournalRecord> JournalQuery(long startSequence, long endSequence);
    public Record ReadJournalSourceRecord(ulong datasetIdentifier, JournalIndexInfo indexInfo);

    public TickResult TickBackgroundTasks();
    public TimeSpan NextBackgroundDelay();
    public IReadOnlyList<string> GetDatasetNames();
    public IReadOnlyList<string> GetDatasetTypes(string name);
    public DatasetInspectResult InspectDataset(string name, string datasetType);
}
```

`Close()` / `Dispose()` should be idempotent at the facade level. Operations after close throw `TmslStoreClosedException` or `ObjectDisposedException` wrapped consistently through `TmslException` subclasses; implementation should choose one stable rule and test it.

### 5.3 Config objects

.NET public API uses immutable or init-only options objects. Generated UniFFI dictionaries stay internal.

```csharp
var storeConfig = new StoreConfig
{
    FlushInterval = TimeSpan.FromSeconds(15),
    IdleTimeout = TimeSpan.FromMinutes(30),
    DataSegmentSize = 64UL * 1024 * 1024,
    IndexSegmentSize = 16UL * 1024 * 1024,
    InitialDataSegmentSize = 256UL * 1024,
    InitialIndexSegmentSize = 16UL * 1024,
    CompressLevel = 6,
    CacheMaxMemory = 256UL * 1024 * 1024,
    CacheIdleTimeout = TimeSpan.FromMinutes(30),
    RetentionCheckHour = 0,
    EnableBackgroundThread = true,
    EnableJournal = true,
    ReadOnly = StoreReadOnly.Auto,
};

var createOptions = new CreateDatasetOptions
{
    Config = new DatasetConfig
    {
        IndexContinuous = false,
        RetentionWindow = 0,
        EnableJournal = true,
    },
};
```

`StoreReadOnly` maps to Rust `Option<bool>`:

| .NET value | Rust semantic |
|------------|---------------|
| `Auto` | `None`: try writable `.lock`, fall back to read-only |
| `WritableRequired` | `Some(false)`: writable lock required |
| `ReadOnly` | `Some(true)`: force read-only |

### 5.4 Dataset

```csharp
public sealed class Dataset : IDisposable
{
    public void Write(long timestamp, byte[] data);
    public void WriteNow(byte[] data);
    public void Append(long timestamp, byte[] data);
    public void AppendNow(byte[] data);
    public void Delete(long timestamp);

    public Record? Read(long timestamp);
    public Record? ReadLatest();

    public QueryIterator Query(long startTimestamp, long endTimestamp);
    public IReadOnlyList<Record> QueryAll(long startTimestamp, long endTimestamp);

    public bool ReadExist(long timestamp);
    public byte[] QueryExist(long startTimestamp, long endTimestamp);
    public uint? ReadLength(long timestamp);
    public QueryLengthIterator QueryLength(long startTimestamp, long endTimestamp);
    public IReadOnlyList<LengthEntry> QueryLengthAll(long startTimestamp, long endTimestamp);

    public void Flush();
    public DatasetInspectResult Inspect();
    public void Close();
    public void Dispose();

    public ulong Id { get; }
    public ulong Identifier { get; }
    public string DataDir { get; }
    public long? LatestTimestamp { get; }
    public bool IsClosed { get; }
}
```

`Read` / `ReadLatest` return `null` when no visible record exists. `Record` is an immutable value object:

```csharp
public sealed record Record(long Timestamp, byte[] Data);
public sealed record LengthEntry(long Timestamp, uint Length);
public sealed record JournalRecord(long Sequence, byte[] Data);
```

Returned `byte[]` must be owned by the caller. The facade should defensively copy arrays at public boundaries unless implementation tests prove generated UniFFI already provides independent ownership.

### 5.5 Query iterators

`QueryIterator` and `QueryLengthIterator` are C# facade objects over UniFFI bridge iterator handles:

```csharp
using var iter = dataset.Query(1, 100);
foreach (Record record in iter)
{
    // process record
}
```

They should implement `IEnumerable<T>`, `IEnumerator<T>` and `IDisposable` where practical. Early `Dispose()` releases the generated/native iterator handle. Exhaustion closes the native iterator best-effort; explicit `Dispose()` remains recommended for early exit.

Iterator helper methods can mirror the current generated bridge:

```csharp
public QueryIterator Reverse();
public QueryIterator Skip(uint count);
public IReadOnlyList<Record> CollectAll();
public IReadOnlyList<Record> CollectTake(uint count);
```

### 5.6 Queue and journal queue

```csharp
using var queue = store.OpenQueue(dataset);
using var consumer = queue.OpenConsumer("worker");

long ts = queue.Push(payload);
Record? record = consumer.Poll(TimeSpan.FromSeconds(1));
if (record is not null)
{
    consumer.Ack(record.Timestamp);
}
```

Core methods:

```csharp
public sealed class DatasetQueue : IDisposable
{
    public long Push(byte[] data);
    public QueueConsumer OpenConsumer(string groupName);
    public QueueConsumer OpenConsumer(string groupName, QueueConsumerOptions options);
    public IReadOnlyList<string> GetConsumerGroupNames();
    public void DropConsumer(string groupName);
    public void Close();
    public void Dispose();
}

public sealed class QueueConsumer : IDisposable
{
    public Record? Poll(TimeSpan timeout);
    public Task<Record?> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
    public void Ack(long timestamp);
    public void Flush();
    public QueueConsumerInspectResult Inspect();
    public void Close();
    public void Dispose();
}

public sealed class JournalQueue : IDisposable
{
    public JournalQueueConsumer OpenConsumer(string groupName);
    public JournalQueueConsumer OpenConsumer(string groupName, QueueConsumerOptions options);
    public void Close();
    public void Dispose();
}

public sealed class JournalQueueConsumer : IDisposable
{
    public JournalRecord? Poll(TimeSpan timeout);
    public Task<JournalRecord?> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
    public void Ack(long sequence);
    public void Close();
    public void Dispose();
}
```

`Poll(TimeSpan timeout)` blocks the calling .NET thread. `PollAsync` is a C# facade convenience that offloads blocking poll work so application request threads can stay responsive. Timeout returns `null`; operational errors throw `TmslException` subclasses.

`PollCallback(Action? callback)` is a separate implementation gate. If implemented, callback support must keep delegates alive, clear them deterministically, reject duplicate non-null registration, and prove callbacks are safe when invoked from Rust notification paths.

## 6. Type mapping

| Rust / UniFFI concept | .NET public type | Notes |
|-----------------------|------------------|-------|
| `i64` timestamp / journal sequence | `long` | Signed timestamp contract maps directly |
| `u64` identifier / sizes | `ulong` | .NET supports unsigned values directly |
| `u32` length | `uint` | Public API can expose `uint`; list counts remain `int` |
| `u16` retry count / offsets | `ushort` | No silent narrowing |
| `u8` flags / levels | `byte` or enum | Prefer enum for semantic fields such as compression |
| `Vec<u8>` / byte buffer | `byte[]` | Defensive copy at facade boundaries |
| `String` | `string` | UTF-8 through generated binding |
| `Option<i64>` | `long?` | Latest timestamp and journal sequence |
| `Vec<T>` | `IReadOnlyList<T>` | Facade owns a stable snapshot |
| Rust `Duration` | `TimeSpan` | Convert to seconds or milliseconds at bridge boundary |
| Rust `Result<T, TmslError>` | return `T` or throw `TmslException` | Stable .NET exception hierarchy |

`uniffi-bindgen-cs` currently documents an `i32` size limit for strings, `byte[]` and lists. timslite's 4 MiB record limit fits safely inside that boundary, but very large eager query results can still hit generated-list limits. Public docs should recommend iterators for large result sets.

## 7. Error mapping

Generated UniFFI errors are wrapped into .NET exceptions under `Timslite.Errors`. The stable catching boundary is exception type plus `TmslErrorCode`.

| Rust error | .NET exception | Error code |
|------------|----------------|------------|
| `Io` | `TmslIoException` | `TmslErrorCode.Io` |
| `NotFound` | `TmslNotFoundException` | `TmslErrorCode.NotFound` |
| `AlreadyExists` | `TmslAlreadyExistsException` | `TmslErrorCode.AlreadyExists` |
| `InvalidData`, `InvalidMagic`, `InvalidVersion` | `TmslInvalidDataException` | `TmslErrorCode.InvalidData` |
| `SegmentFull` | `TmslSegmentFullException` | `TmslErrorCode.SegmentFull` |
| `MmapError` | `TmslMmapException` | `TmslErrorCode.Mmap` |
| `CompressionError` | `TmslCompressionException` | `TmslErrorCode.Compression` |
| `DecompressionError` | `TmslDecompressionException` | `TmslErrorCode.Decompression` |
| `Expired` | `TmslExpiredException` | `TmslErrorCode.Expired` |
| Queue errors | matching `TmslQueue*Exception` | matching queue code |
| Wrapper closed state | `TmslStoreClosedException` / `TmslDatasetClosedException` | matching closed code |

Exceptions should be unchecked .NET exceptions derived from `Exception`; C# does not use checked exceptions. Generated exception types must not leak into the public facade.

## 8. Lifecycle and ownership

- `Store` is the root object; `Dataset`, `DatasetQueue`, and `JournalQueue` are child handles.
- Facade objects keep generated UniFFI objects private and guard closed state before delegation.
- `Close()` / `Dispose()` should be explicit, idempotent where practical, and safe in `using` statements.
- Finalizers are not part of the public contract. If a finalizer or `SafeHandle` is added, it is only best-effort cleanup.
- Store close invalidates child facade objects. Child objects throw stable closed exceptions after invalidation.
- Native library resolver initialization happens once per assembly before the first generated binding call.

## 9. Native loading and NuGet packaging

### 9.1 Local development

Implementation should support:

```powershell
cargo check --manifest-path wrapper/dotnet/native/Cargo.toml
cargo test --manifest-path wrapper/dotnet/native/Cargo.toml
dotnet build wrapper/dotnet/Timslite.sln
dotnet test wrapper/dotnet/Timslite.sln
dotnet pack wrapper/dotnet/src/Timslite/Timslite.csproj -c Release
```

### 9.2 Native asset layout

NuGet native libraries should use standard `.NET 5+` / `.NET 8` package conventions:

```text
ref/net8.0/Timslite.dll
runtimes/any/lib/net8.0/Timslite.dll
runtimes/win-x64/native/timslite_dotnet.dll
runtimes/win-arm64/native/timslite_dotnet.dll
runtimes/linux-x64/native/libtimslite_dotnet.so
runtimes/linux-arm64/native/libtimslite_dotnet.so
runtimes/osx-x64/native/libtimslite_dotnet.dylib
runtimes/osx-arm64/native/libtimslite_dotnet.dylib
```

The initial release matrix should cover:

- Windows x64 MSVC
- Windows ARM64 MSVC
- Linux x64 GNU
- Linux ARM64 GNU
- macOS x64
- macOS ARM64

### 9.3 Native library resolver

The generated binding should use the logical native name `timslite_dotnet`. .NET's default P/Invoke probing can load native files from `runtimes/{rid}/native/`. The facade may also register a `NativeLibrary.SetDllImportResolver` to support:

- `TIMSLITE_NATIVE_LIBRARY_PATH` environment variable for local debugging.
- Explicit absolute native path in tests.
- Default runtime probing fallback when no override is configured.

## 10. Release dependency strategy

Development checkout should use:

```toml
timslite = { path = "../../..", version = "=0.1.1" }
```

Before publishing source/native artifacts, release automation should rewrite the dependency to the exact crates.io version:

```toml
timslite = { version = "=0.1.1" }
```

This mirrors the Python、Node.js and Java release model and prevents published source/native packages from depending on repository-relative paths. The NuGet package itself should normally contain prebuilt native libraries; unsupported platforms fail with an actionable `PlatformNotSupportedException` or native-load exception until source-build fallback is explicitly designed.

## 11. Testing strategy

Each integration test must use an isolated temporary directory.

| Category | Focus |
|----------|-------|
| Smoke | load native library, `TimsliteInfo.Version()`, open/close store |
| Config | defaults, options objects, read-only tri-state |
| Lifecycle | create/open/drop/recreate datasets, open by identifier |
| Dataset IO | write, write now, append, append now, delete, read, read latest, flush |
| Query | iterator, query all, exist bitmap, read length, query length |
| Persistence | close/reopen and data survives |
| Inspect | dataset info/state nullability and numeric fields |
| Queue | push, poll timeout, ack, flush, inspect, consumer group listing |
| Journal | latest sequence, read/query raw payload, source-record dereference, journal queue |
| Errors | each stable .NET exception type and code |
| Packaging | NuGet-local install, RID native load, custom native path override |

Full implementation verification must include root crate checks because the bridge crate directly depends on the Rust public API.

## 12. Risks and constraints

| Risk | Impact | Mitigation |
|------|--------|------------|
| C# UniFFI backend is third-party | Generated API or feature support can drift | DOTNET-0 locks `uniffi-bindgen-cs` version and checks generated smoke binding before implementation |
| Generator requires newer Rust than root crate | CI/developer setup may fail | Treat generator installation as wrapper-specific toolchain requirement; document and verify early |
| Callback support across .NET delegates is subtle | GC/lifetime/thread bugs | Gate `PollCallback` behind dedicated tests; keep it out of MVP until proven |
| Eager `byte[]`/list limits | Large query-all results can fail | Prefer iterators for large result sets; document generator size limit |
| Native asset RID selection | Runtime load failures | Use portable .NET 8 RIDs and `runtimes/{rid}/native/`; add packaging tests |
| Wrapper API drift from Python/Node/Java | User-facing behavior differs by language | Keep Store-managed public boundary and shared integration scenarios |

## 13. Implementation status

All phases documented in [plan.md](plan.md) are complete:

| Phase | Description | Status |
|-------|-------------|--------|
| DOTNET-0 | UniFFI C# and .NET 8 toolchain confirmation | ✅ Complete |
| DOTNET-1 | Rust UniFFI bridge crate skeleton | ✅ Complete |
| DOTNET-2 | C# binding generation and .NET project skeleton | ✅ Complete |
| DOTNET-3 | Types, configuration, error layer | ✅ Complete |
| DOTNET-4 | Store and Dataset lifecycle | ✅ Complete |
| DOTNET-5 | Data read/write and query | ✅ Complete |
| DOTNET-6 | Queue and Journal API | ✅ Complete |
| DOTNET-7 | Native loading and NuGet packaging | ✅ Complete |
| DOTNET-8 | Integration tests and regression verification | ✅ Complete |
| DOTNET-9 | CI/native release preparation | ✅ Complete |
| DOTNET-10 | Cross-layer documentation sync | ✅ Complete |

## 14. External references

- UniFFI user guide: <https://mozilla.github.io/uniffi-rs/latest/>
- UniFFI README, third-party C# binding reference: <https://github.com/mozilla/uniffi-rs>
- `uniffi-bindgen-cs`: <https://github.com/NordSecurity/uniffi-bindgen-cs>
- NuGet native files in .NET packages: <https://learn.microsoft.com/en-us/nuget/create-packages/native-files-in-net-packages>
- .NET RID catalog: <https://learn.microsoft.com/en-us/dotnet/core/rid-catalog>
- .NET native library loading: <https://learn.microsoft.com/en-us/dotnet/standard/native-interop/native-library-loading>
