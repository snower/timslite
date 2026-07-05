# timslite .NET Wrapper - 开发计划

> 基于 [design.md](design.md)
> 目标: 在 `wrapper/dotnet` 中实现基于 UniFFI 的 .NET 8+ 绑定, 通过 C# facade 暴露 timslite Store/Dataset/Queue/Journal API。

---

## 计划状态总览

| Phase | 描述 | 状态 | 产物 |
|-------|------|------|------|
| DOTNET-0 | UniFFI C# 与 .NET 8 工具链确认 | ✅ 完成 | `design.md`, `plan.md`, generator/version candidates |
| DOTNET-1 | Rust UniFFI bridge crate 骨架 | ✅ 完成 | `native/Cargo.toml`, `native/src/lib.rs`, `native/src/timslite.udl` |
| DOTNET-2 | C# binding 生成与 .NET 项目骨架 | ✅ 完成 | `Timslite.sln`, `Timslite.csproj`, generated C# smoke binding |
| DOTNET-3 | 类型、配置、错误层 | ✅ 完成 | options records/classes, exception hierarchy, conversion tests |
| DOTNET-4 | Store 与 Dataset lifecycle | ✅ 完成 | `Store`, `Dataset`, inspect/listing, lifecycle tests |
| DOTNET-5 | 数据读写与查询 | ✅ 完成 | write/read/query/length/exist APIs, iterator tests |
| DOTNET-6 | Queue 与 Journal API | ✅ 完成 | queue poll/ack/inspect, journal read/query/queue tests |
| DOTNET-7 | Native loading 与 NuGet packaging | ✅ 完成 | RID native assets, local NuGet install, loader tests |
| DOTNET-8 | 集成测试与回归验证 | ✅ 完成 | .NET tests plus Rust/root verification |
| DOTNET-9 | CI/native 发布准备 | ✅ 完成 | release workflow, native matrix, publish prep scripts |
| DOTNET-10 | 跨层文档同步 | ✅ 完成 | README/root design/plan references after implementation |

所有阶段已完成。.NET wrapper 现已可用, 详见 [README.md](README.md)。

---

## 开发原则

- 直接调用 timslite Rust public API, 不通过 `wrapper/cffi` C ABI 裸指针接口。
- C# generated binding 放在 `Timslite.Uniffi` 内部命名空间; public API 只暴露 C# facade。
- 最低目标框架为 `.NET 8` (`net8.0`)。
- 所有 public lifecycle 类型实现 `IDisposable`, 示例使用 `using` / `using var`。
- timestamp 和 journal sequence 使用 `long`; identifier 和大小字段使用 `ulong`。
- payload 输入输出使用 `byte[]`; public 边界默认做防御性复制。
- Queue `Poll()` 是阻塞调用; C# facade 额外提供 `Task<T?> PollAsync(...)`。
- `PollCallback` 单独 gate, 未验证 .NET delegate 生命周期和 Rust 通知线程安全前不标记 MVP 完成。
- 完成后先不要 git commit, 等审核确认。

---

## 目标目录结构

```text
wrapper/dotnet/
├── design.md
├── plan.md
├── README.md
├── Timslite.sln
├── native/                         # Rust UniFFI bridge crate
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── uniffi.toml
│   ├── build.rs
│   └── src/
│       ├── lib.rs
│       ├── timslite.udl
│       ├── bridge.rs
│       ├── config.rs
│       ├── errors.rs
│       ├── query.rs
│       ├── queue.rs
│       └── bin/uniffi_bindgen.rs
├── generated/
│   └── Timslite.Uniffi.cs
├── src/Timslite/
│   ├── Timslite.csproj
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

---

## Phase DOTNET-0: UniFFI C# 与 .NET 8 工具链确认

文件:

- Modify: `wrapper/dotnet/design.md`
- Modify: `wrapper/dotnet/plan.md`

任务:

- [x] 记录推荐技术路线。
  - `uniffi = 0.31` 与当前 `wrapper/java/native` 对齐。
  - `uniffi-bindgen-cs v0.11.0+v0.31.0` 作为 C# generator 候选。
  - `.NET SDK 8.0+`, `TargetFramework=net8.0`, `AllowUnsafeBlocks=true`。
- [ ] 验证本地 Rust toolchain 是否满足 `uniffi-bindgen-cs` 安装要求。
  - Run: `rustc --version`
  - Expected: version is compatible with generator requirement or CI/toolchain plan documents how to install a compatible toolchain for this wrapper.
- [ ] 安装或缓存 C# generator。
  - Run: `cargo install uniffi-bindgen-cs --git https://github.com/NordSecurity/uniffi-bindgen-cs --tag v0.11.0+v0.31.0`
  - Expected: `uniffi-bindgen-cs --help` exits successfully.
- [ ] 用最小 UDL 验证 C# 生成路径。
  - Input UDL exposes only `string version()`.
  - Run: `uniffi-bindgen-cs wrapper/dotnet/native/src/timslite.udl --config wrapper/dotnet/native/uniffi.toml`
  - Expected: `generated/Timslite.Uniffi.cs` is generated and compiles in `net8.0`.
- [ ] 确认 NuGet package ID。
  - Preferred: `Timslite`.
  - If unavailable before release, update design, plan, README and workflow to the final ID.

验收标准:

- 后续实现不需要重新决定 UniFFI、C# generator、.NET SDK、NuGet ID 和 native library name。
- 若 `uniffi-bindgen-cs` 与当前 UniFFI 版本不兼容, 本 phase 必须停下并重新出设计修订, 不能进入 DOTNET-1。

---

## Phase DOTNET-1: Rust UniFFI bridge crate 骨架

文件:

- Create: `wrapper/dotnet/native/Cargo.toml`
- Create: `wrapper/dotnet/native/Cargo.lock`
- Create: `wrapper/dotnet/native/build.rs`
- Create: `wrapper/dotnet/native/uniffi.toml`
- Create: `wrapper/dotnet/native/src/lib.rs`
- Create: `wrapper/dotnet/native/src/timslite.udl`
- Create: `wrapper/dotnet/native/src/bridge.rs`
- Create: `wrapper/dotnet/native/src/errors.rs`

任务:

- [ ] 创建 Rust bridge crate。
  - `[lib] crate-type = ["cdylib", "rlib"]`。
  - `name = "timslite_dotnet"`。
  - `timslite = { path = "../../..", version = "=0.1.2" }`。
  - `uniffi = { version = "0.31", features = ["cli"] }`。
- [ ] 创建最小 UDL。
  - namespace 使用 `timslite`。
  - 暴露 `version() -> string`。
  - 暴露基础 `TmslError` error enum。
- [ ] 创建 `src/lib.rs` scaffolding root。
  - 注册 UniFFI scaffolding。
  - 只接入 smoke function, 不暴露 Store/Dataset。
- [ ] 创建 `uniffi.toml`。
  - C# namespace 使用 `Timslite.Uniffi`。
  - cdylib name 使用 `timslite_dotnet`。
- [ ] 验证 Rust bridge 编译。
  - Run: `cargo check --manifest-path wrapper/dotnet/native/Cargo.toml`
  - Expected: exit code 0.

验收标准:

- bridge crate 可独立 `cargo check`。
- UniFFI 可从 bridge crate 生成 C# binding。

---

## Phase DOTNET-2: C# binding 生成与 .NET 项目骨架

文件:

- Create: `wrapper/dotnet/Timslite.sln`
- Create: `wrapper/dotnet/src/Timslite/Timslite.csproj`
- Create: `wrapper/dotnet/generated/Timslite.Uniffi.cs`
- Create: `wrapper/dotnet/src/Timslite/TimsliteInfo.cs`
- Create: `wrapper/dotnet/src/Timslite/NativeLibraryLoader.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/Timslite.Tests.csproj`
- Create: `wrapper/dotnet/tests/Timslite.Tests/SmokeTests.cs`

任务:

- [ ] 创建 .NET solution 和 class library。
  - `TargetFramework=net8.0`。
  - `Nullable=enable`。
  - `AllowUnsafeBlocks=true`。
  - package metadata 使用 `PackageId=Timslite`, version `0.1.2`。
- [ ] 添加 generated C# binding 到编译输入。
  - Generated file stays under `generated/`.
  - Public facade does not expose generated types.
- [ ] 实现 `NativeLibraryLoader` smoke path。
  - 支持 `TIMSLITE_NATIVE_LIBRARY_PATH` override。
  - 无 override 时回退 .NET default P/Invoke probing。
- [ ] 实现 `TimsliteInfo.Version()`。
  - Calls native loader before generated `version()`.
- [ ] 添加 smoke test。
  - Verifies native library loads.
  - Verifies `TimsliteInfo.Version()` returns non-empty.
- [ ] 验证 .NET build/test。
  - Run: `dotnet build wrapper/dotnet/Timslite.sln`
  - Run: `dotnet test wrapper/dotnet/Timslite.sln`

验收标准:

- .NET test can call Rust `version()` through generated C# binding.
- `Timslite.Uniffi` generated types are not presented as the stable public API.

---

## Phase DOTNET-3: 类型、配置、错误层

文件:

- Create: `wrapper/dotnet/native/src/config.rs`
- Modify: `wrapper/dotnet/native/src/errors.rs`
- Modify: `wrapper/dotnet/native/src/timslite.udl`
- Create: `wrapper/dotnet/src/Timslite/StoreConfig.cs`
- Create: `wrapper/dotnet/src/Timslite/DatasetConfig.cs`
- Create: `wrapper/dotnet/src/Timslite/CreateDatasetOptions.cs`
- Create: `wrapper/dotnet/src/Timslite/QueueConsumerOptions.cs`
- Create: `wrapper/dotnet/src/Timslite/Records.cs`
- Create: `wrapper/dotnet/src/Timslite/Errors/*.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/ConfigTests.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/ErrorTests.cs`

任务:

- [ ] 设计 .NET-facing UniFFI dictionaries。
  - Store config 覆盖当前 `StoreConfig` 字段。
  - Dataset config 覆盖当前 `DataSetConfigBuilder` 字段。
  - Queue consumer options 覆盖 retry config 字段。
  - Inspect records 覆盖 `DataSetInfo` / `DataSetState` / queue inspect state。
- [ ] 实现 Rust config conversion。
  - 缺省字段使用 root crate defaults。
  - `StoreReadOnly` 映射到 Rust `Option<bool>`。
  - `TimeSpan` facade 转为 seconds/milliseconds 后再进入 generated dictionary。
- [ ] 实现 Rust error conversion。
  - 覆盖所有当前 `TmslError` 变体。
  - closed-state wrapper errors 有稳定 code。
- [ ] 实现 C# options/value objects。
  - Options 使用 init-only properties 或 immutable records。
  - Public records defensively copy `byte[]` where needed。
- [ ] 实现 C# exception hierarchy。
  - Base: `TmslException : Exception`。
  - Enum: `TmslErrorCode`。
  - Generated exceptions converted by `TmslException.FromUniFFI(...)` helper.
- [ ] 添加配置和错误测试。
  - defaults。
  - custom values。
  - invalid negative/overflow conversion where applicable。
  - read-only/write contention case。
  - Rust error to .NET exception mapping。

验收标准:

- .NET config 覆盖当前 root `StoreConfig` / `DataSetConfig` 权威字段。
- Generated exception type does not leak through public facade.

---

## Phase DOTNET-4: Store 与 Dataset lifecycle

文件:

- Modify: `wrapper/dotnet/native/src/bridge.rs`
- Modify: `wrapper/dotnet/native/src/timslite.udl`
- Create: `wrapper/dotnet/src/Timslite/Store.cs`
- Create: `wrapper/dotnet/src/Timslite/Dataset.cs`
- Create: `wrapper/dotnet/src/Timslite/InspectTypes.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/LifecycleTests.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/InspectTests.cs`

任务:

- [ ] 实现 bridge `StoreBridge`。
  - `open(path, config)`。
  - `close()`。
  - `is_closed()`。
  - `is_read_only()`。
  - closed guard。
- [ ] 实现 Store dataset lifecycle。
  - `create_dataset`。
  - `open_dataset`。
  - `open_dataset_by_identifier`。
  - `drop_dataset`。
- [ ] 实现 listing and inspect。
  - `get_dataset_names`。
  - `get_dataset_types`。
  - `inspect_dataset`。
  - `tick_background_tasks`。
  - `next_background_delay_ms`。
- [ ] 实现 C# `Store` facade。
  - `IDisposable`。
  - `using var` 示例可运行。
  - close 后方法抛 stable closed exception。
- [ ] 实现 C# `Dataset` facade lifecycle shell。
  - 持有 internal `DatasetBridge`。
  - `Close()` / `Dispose()`。
  - getters: id, identifier, dataDir, latestTimestamp, isClosed。
- [ ] 添加 lifecycle tests。
  - open/close。
  - create/open/drop/recreate。
  - open by identifier。
  - inspect nullability。
  - close idempotency。

验收标准:

- .NET 可以完整管理 Store 和 Dataset 生命周期。
- 生命周期行为与 Python/Node/Java Store-managed public boundary 对齐。

---

## Phase DOTNET-5: 数据读写与查询

文件:

- Create: `wrapper/dotnet/native/src/query.rs`
- Modify: `wrapper/dotnet/native/src/bridge.rs`
- Modify: `wrapper/dotnet/native/src/timslite.udl`
- Modify: `wrapper/dotnet/src/Timslite/Dataset.cs`
- Create: `wrapper/dotnet/src/Timslite/QueryIterator.cs`
- Create: `wrapper/dotnet/src/Timslite/QueryLengthIterator.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/DatasetIoTests.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/QueryTests.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/PersistenceTests.cs`

任务:

- [ ] 实现 Dataset write APIs。
  - `Write(long, byte[])`。
  - `WriteNow(byte[])`。
  - `Append(long, byte[])`。
  - `AppendNow(byte[])`。
  - `Delete(long)`。
  - `Flush()`。
- [ ] 实现 Dataset read APIs。
  - `Read(long) -> Record?`。
  - `ReadLatest() -> Record?`。
  - Returned `byte[]` 独立拥有。
- [ ] 实现 query iterator bridge。
  - `query_iter(start, end) -> QueryIteratorBridge`。
  - `next() -> Record?`。
  - `reverse()`。
  - `skip(count)`。
  - `collect_all()` / `collect_take(count)`。
- [ ] 实现 C# query iterators。
  - Implement `IEnumerable<T>` / `IEnumerator<T>` where practical。
  - Implement `IDisposable`。
  - Exhaustion and explicit dispose both safe。
- [ ] 实现 lightweight reads。
  - `ReadExist`。
  - `QueryExist`。
  - `ReadLength`。
  - `QueryLength`。
  - `QueryLengthAll`。
- [ ] 添加数据和查询测试。
  - write/read。
  - write now and append now。
  - append forward and append latest。
  - delete then read returns null。
  - read latest exact max timestamp behavior。
  - query iterator partial consumption。
  - query all。
  - query exist bitmap。
  - read length and query length。
  - close/reopen persistence。

验收标准:

- .NET wrapper 能完成基础时序读写与查询工作流。
- Query iterator 不要求用户理解 Rust lifetime、IndexEntry 或 generated classes。

---

## Phase DOTNET-6: Queue 与 Journal API

文件:

- Create: `wrapper/dotnet/native/src/queue.rs`
- Modify: `wrapper/dotnet/native/src/bridge.rs`
- Modify: `wrapper/dotnet/native/src/timslite.udl`
- Create: `wrapper/dotnet/src/Timslite/Queue.cs`
- Create: `wrapper/dotnet/src/Timslite/QueueConsumer.cs`
- Create: `wrapper/dotnet/src/Timslite/JournalQueue.cs`
- Create: `wrapper/dotnet/src/Timslite/JournalQueueConsumer.cs`
- Create: `wrapper/dotnet/src/Timslite/JournalIndexInfo.cs`
- Optional Create: `wrapper/dotnet/src/Timslite/PollWakeCallback.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/QueueTests.cs`
- Create: `wrapper/dotnet/tests/Timslite.Tests/JournalTests.cs`

任务:

- [ ] 实现 Dataset queue。
  - `Store.OpenQueue(dataset)`。
  - `DatasetQueue.Push(byte[]) -> long`。
  - `DatasetQueue.OpenConsumer(groupName, options)`。
  - `DatasetQueue.GetConsumerGroupNames()`。
  - `DatasetQueue.DropConsumer(groupName)`。
  - `DatasetQueue.Close()`。
- [ ] 实现 queue consumer。
  - `Poll(TimeSpan) -> Record?`。
  - `PollAsync(TimeSpan, CancellationToken) -> Task<Record?>` in C# facade。
  - `Ack(long)`。
  - `Flush()`。
  - `Inspect()`。
- [ ] 实现 journal APIs。
  - `JournalLatestSequence()`。
  - `JournalRead(sequence)`。
  - `JournalQuery(start, end)`。
  - `ReadJournalSourceRecord(identifier, indexInfo)`。
- [ ] 实现 journal queue。
  - `OpenJournalQueue()`。
  - `JournalQueue.OpenConsumer(...)`。
  - `JournalQueueConsumer.Poll / PollAsync / Ack`。
- [ ] 实现 poll callback only after callback gate passes。
  - Set callback。
  - Clear callback。
  - Duplicate non-null registration raises stable exception。
  - Delegate remains alive while registered。
  - Callback invocation is safe from Rust notification path。
- [ ] 添加 queue/journal tests。
  - open consumer before push, then poll。
  - ack prevents redelivery。
  - timeout returns null。
  - `PollAsync` does not block caller thread。
  - consumer inspect exposes info/state。
  - journal create/write/delete/append creates sequences when dataset journaling is enabled。
  - journal source record dereference works while source data exists。
  - journal queue consumes subsequent records。
  - poll callback tests only if callback gate is implemented。

验收标准:

- .NET queue and journal behavior matches current Rust/Python/Node/Java public contract。
- Poll callback is either fully verified or explicitly left out of MVP docs and status table。

---

## Phase DOTNET-7: Native loading 与 NuGet packaging

文件:

- Modify: `wrapper/dotnet/src/Timslite/Timslite.csproj`
- Modify: `wrapper/dotnet/src/Timslite/NativeLibraryLoader.cs`
- Modify: `wrapper/dotnet/native/Cargo.toml`
- Create: `wrapper/dotnet/scripts/prepare-publish.ps1`
- Create: `wrapper/dotnet/scripts/prepare-publish.sh`
- Create: `wrapper/dotnet/tests/Timslite.Tests/PackagingTests.cs`
- Create: `wrapper/dotnet/README.md`

任务:

- [ ] Configure NuGet metadata。
  - `PackageId=Timslite` unless final package ID changes。
  - `TargetFramework=net8.0`。
  - XML docs included。
  - Source link / repository metadata included if consistent with existing release setup。
- [ ] Configure native asset packaging。
  - `runtimes/win-x64/native/timslite_dotnet.dll`。
  - `runtimes/win-arm64/native/timslite_dotnet.dll`。
  - `runtimes/linux-x64/native/libtimslite_dotnet.so`。
  - `runtimes/linux-arm64/native/libtimslite_dotnet.so`。
  - `runtimes/linux-musl-x64/native/libtimslite_dotnet.so`。
  - `runtimes/linux-musl-arm64/native/libtimslite_dotnet.so`。
  - `runtimes/osx-arm64/native/libtimslite_dotnet.dylib`。
- [ ] Add native library loader tests。
  - Current RID selects native library。
  - Missing native library produces actionable error。
  - `TIMSLITE_NATIVE_LIBRARY_PATH` override works。
- [ ] Add publish preparation scripts。
  - Verify root crate and wrapper versions match。
  - Rewrite `timslite = { path = "../../.." }` to exact crates.io dependency in a release copy.
  - Do not mutate the live development manifest except in an explicit release workspace.
- [ ] Verify local NuGet package。
  - Run: `dotnet pack wrapper/dotnet/src/Timslite/Timslite.csproj -c Release`
  - Create temporary consumer project.
  - Add local `.nupkg`.
  - Call `TimsliteInfo.Version()`.

验收标准:

- Local .NET consumers can depend on the package from a local NuGet source。
- Native loading works from normal NuGet restore/build output, not only from the repo tree。

---

## Phase DOTNET-8: 集成测试与回归验证

文件:

- Modify/Create: `wrapper/dotnet/tests/Timslite.Tests/*.cs`
- Modify: `wrapper/dotnet/src/Timslite/Timslite.csproj`
- Modify: `wrapper/dotnet/Timslite.sln`

任务:

- [ ] 完成 .NET integration tests。
  - Smoke。
  - Config。
  - Lifecycle。
  - Dataset IO。
  - Query。
  - Persistence。
  - Inspect。
  - Queue。
  - Journal。
  - Errors。
  - Packaging/load。
- [ ] 完成 Rust bridge tests。
  - Config conversion。
  - Error conversion。
  - Closed guard。
  - Iterator exhaustion。
- [ ] 运行 .NET wrapper verification。
  - `cargo check --manifest-path wrapper/dotnet/native/Cargo.toml`
  - `cargo test --manifest-path wrapper/dotnet/native/Cargo.toml`
  - `dotnet build wrapper/dotnet/Timslite.sln`
  - `dotnet test wrapper/dotnet/Timslite.sln`
  - `dotnet pack wrapper/dotnet/src/Timslite/Timslite.csproj -c Release`
- [ ] 运行 root regression ladder。
  - `cargo fmt -- --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test -- --test-threads=1`
  - `git diff --check`

验收标准:

- .NET wrapper tests pass on local platform。
- Root crate regression checks pass or any environment limitation is explicitly documented。

---

## Phase DOTNET-9: CI/native 发布准备

文件:

- Create/Modify: `.github/workflows/dotnet-release.yml`
- Modify: `wrapper/dotnet/src/Timslite/Timslite.csproj`
- Modify: `wrapper/dotnet/native/Cargo.toml`
- Modify: `wrapper/dotnet/scripts/prepare-publish.*`
- Modify: `wrapper/dotnet/README.md`

任务:

- [x] Add CI matrix。
  - Windows x64。
  - Windows ARM64。
  - Linux x64。
  - Linux ARM64。
  - Linux x64 musl。
  - Linux ARM64 musl。
  - macOS ARM64。
  - .NET SDK 8.0。
- [x] Build native release artifacts。
  - Rust cdylib per RID。
  - Gather into `runtimes/{rid}/native/` package layout。
- [x] Add NuGet package dry-run。
  - `dotnet pack`。
  - Package content inspection。
  - Temporary consumer project restore/build/run。
- [x] Add publication configuration。
  - NuGet API key or trusted publishing path, depending on repository release policy。
  - Version sync check with root `Cargo.toml`。
- [x] Document unsupported platforms。
  - Throw actionable error for unsupported RID。
  - Keep macOS x64/NativeAOT as not shipped unless explicitly approved。

验收标准:

- Release workflow has an explicit native artifact strategy before public publication。
- NuGet package content can be inspected and loaded before publishing。

---

## Phase DOTNET-10: 跨层文档同步

文件:

- Modify: `README.md`
- Modify: `design.md`
- Modify: `plan.md`
- Optional Modify: `docs/design/store-and-ffi.md`
- Optional Create: `docs/plan/phase-dotnet-wrapper.md`
- Modify: `wrapper/dotnet/README.md`
- Modify: `wrapper/dotnet/design.md`
- Modify: `wrapper/dotnet/plan.md`

任务:

- [x] 在 root README 增加 .NET wrapper 状态和入口。
- [x] 在 root `design.md` 的 wrapper 索引中增加 .NET wrapper。
- [x] 在 root `plan.md` 增加 DOTNET wrapper phase 状态。
- [x] 如 .NET wrapper 暴露边界影响 Store/FFI 说明, 同步更新 `docs/design/store-and-ffi.md`。
- [x] 如项目需要独立 phase 文档, 创建 `docs/plan/phase-dotnet-wrapper.md`。
- [x] 在 `wrapper/dotnet/README.md` 增加:
  - Installation from local NuGet / future NuGet.org。
  - Basic write/read/query example。
  - Queue example。
  - Journal example。
  - Native artifact loading notes。
  - .NET 8 requirement。

验收标准:

- 项目入口文档可以发现 .NET wrapper。
- .NET wrapper 文档与 root 状态文档不冲突。

---

## 回归验证清单

实现完成前不得声称完成, 除非以下验证已跑通或明确记录无法运行原因:

```powershell
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
cargo check --manifest-path wrapper/dotnet/native/Cargo.toml
cargo test --manifest-path wrapper/dotnet/native/Cargo.toml
dotnet build wrapper/dotnet/Timslite.sln
dotnet test wrapper/dotnet/Timslite.sln
dotnet pack wrapper/dotnet/src/Timslite/Timslite.csproj -c Release
git diff --check
```

如实现过程中修改 Python wrapper、Node.js wrapper、Java wrapper、C ABI/header 或 root public API, 必须追加对应语言/FFI 的验证命令。

---

## 风险跟踪

| 风险 | 优先级 | 应对 |
|------|--------|------|
| `uniffi-bindgen-cs` 是第三方 generator | 高 | DOTNET-0 锁定版本并用 smoke UDL 验证 |
| generator Rust toolchain 要求高于当前开发环境 | 高 | 单独记录 wrapper-specific toolchain; CI 先验证安装 |
| Generated C# API 对 public facade 不够自然 | 高 | generated namespace internal-by-convention, facade 是唯一稳定 API |
| Native asset 加载失败 | 高 | RID packaging tests + custom path override + actionable errors |
| Blocking poll 被误用在 request thread | 中 | 文档说明 + `PollAsync` convenience |
| poll callback delegate lifetime/thread semantics complex | 中 | 单独 gate, 未验证前不标记 MVP 完成 |
| Eager query-all 命中 generated list size limit | 中 | 推荐 iterator, 大结果避免 `QueryAll` |
| .NET wrapper 与 Python/Node/Java API 漂移 | 中 | 共享测试场景和 Store-managed boundary 术语 |

---

## 后续扩展

- `ReadOnlySpan<byte>` / `ReadOnlyMemory<byte>` overload。
- `IAsyncEnumerable<Record>` queue/journal helpers。
- Journal payload decode helper。
- macOS x64 packaged native artifact, only if explicitly required。
- NativeAOT compatibility investigation。
- Benchmark and throughput examples。
