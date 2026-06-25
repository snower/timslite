# timslite Java Wrapper - UniFFI 设计

> 目标: 在 `wrapper/java` 中提供基于 UniFFI 的 JVM 绑定, 让 Java 项目可以使用当前 timslite Rust library。最低运行目标为 Java 8。

---

## 1. 设计目标

Java wrapper 应当是 timslite 的薄包装层, 不重新实现存储、索引、queue、journal 或磁盘格式逻辑。核心行为继续由 Rust crate 的 public API 负责, Java 层只处理:

- UniFFI 绑定生成、JVM native library 加载和 Maven 包装。
- Java 8 友好的 public facade: `AutoCloseable`, builder, `byte[]`, `long`, `List`, `CompletableFuture`。
- Java/Kotlin 类型与 Rust 类型之间的安全转换。
- timslite 错误到 Java exception 层次的稳定映射。
- Store/Dataset/Queue/Journal 对象生命周期和 close 后访问保护。
- Java 侧测试、文档、发布产物和 native artifact 分发。

不改变当前 Rust、C ABI、Python wrapper、Node.js wrapper 或磁盘格式契约。

## 2. 技术方案

### 2.0 工具链确认 (JAVA-0)

**UniFFI 版本约束:**

| 组件 | 版本 | 说明 |
|------|------|------|
| `uniffi` (Rust crate) | `0.31` | Rust-side scaffolding 和 runtime |
| `uniffi_bindgen` (CLI) | `0.31` | Kotlin/JVM binding 生成 |
| `uniffi_build` (build dep) | `0.31` | Cargo build script 集成 |

**Java 8 兼容性配置:**

- Maven compiler `source`/`target`: `1.8`
- Kotlin Maven plugin `jvmTarget`: `1.8`
- UniFFI Kotlin backend: 使用 `kotlin.system.exitProcess` 替代 Java 9 `Cleaner`
- UniFFI config: `bindings.kotlin.disable_java_cleaner = true` (禁用 Java 9 Cleaner, 依赖显式 `close()`)

**Maven 坐标:**

- `groupId`: `io.github.snower`
- `artifactId`: `timslite`
- `version`: `0.1.1` (与 Rust crate 同步)

**运行依赖:**

- `org.jetbrains.kotlin:kotlin-stdlib:2.2.0`
- `net.java.dev.jna:jna:5.14.0`

### 2.1 推荐方案: UniFFI Kotlin/JVM 绑定 + Java facade

UniFFI 当前 JVM 绑定主线是 Kotlin backend。Java wrapper 采用双层结构:

```text
Java applications
        |
        v
io.github.snower.timslite Java facade classes
        |
        v
io.github.snower.timslite.uniffi generated Kotlin/JVM bindings
        |
        v
wrapper/java/native Rust UniFFI bridge cdylib
        |
        v
timslite Rust public API
```

Java 用户只依赖 `io.github.snower.timslite.*` facade。UniFFI 生成的 Kotlin classes 放在内部包 `io.github.snower.timslite.uniffi`, 不作为长期稳定 API 承诺。这样可以继续使用 UniFFI 负责 native binding、object handle、JNA loading 和类型桥接, 同时避免 Java 调用方直接面对 Kotlin nullable、companion object、generated naming 等细节。

最低 Java 8 的约束写入构建配置:

- Maven compiler source/target 使用 `1.8`。
- Kotlin Maven plugin `jvmTarget` 使用 `1.8`。
- UniFFI config 禁用依赖 Java 9 `Cleaner` 的清理策略, wrapper public contract 要求显式 `close()`。
- 测试矩阵至少包含一个 Java 8 runtime 或 toolchain 验证。

### 2.2 备选方案

| 方案 | 优点 | 主要问题 | 结论 |
|------|------|----------|------|
| UniFFI Kotlin/JVM + Java facade | 复用 Rust public API, 生成大部分 native 桥接, Java public API 可保持整洁 | 需要 Kotlin stdlib/JNA 运行依赖, 构建链比纯 Java 多一层 | 推荐 |
| Java/JNA 直接调用 C ABI | 可避开 Kotlin, 与 `include/timslite.h` 直接对应 | 需要复制 FFI handle registry、malloc/free、err_buf、iterator close 规则, 容易和 Rust public API 分叉 | 不作为主线 |
| 手写 JNI | 可做极致 Java API 和 native loader 控制 | 维护成本高, 错误和生命周期边界手写, 与 UniFFI 目标不符 | 不采用 |

## 3. 支持范围

### 3.1 MVP 必须覆盖

- Store lifecycle: open / close / read-only config / manual background tick。
- Dataset lifecycle: create / open / open by identifier / drop / inspect。
- Dataset data operations: write / append / delete / read / read latest / flush。
- Query operations: query iterator / query all / read exist / query exist / read length / query length。
- DatasetQueue: open queue / push / open consumer / drop consumer / close。
- DatasetQueueConsumer: blocking poll / async poll facade / ack。
- Journal read/query/latest and JournalQueue consumer。
- Java exception mapping、Javadoc、Java integration tests。
- Local build and Maven-local package verification。

### 3.2 MVP 不覆盖

- 解析 encoded journal payload 的高级 Java helper。MVP 先返回 raw `byte[]`, 与 Python/Node wrapper 保持一致。
- Android AAR。Java 8 desktop/server JVM 是首要目标。
- GraalVM native-image。
- Spring、Reactor、Flow、Kotlin coroutine 等上层集成。
- Queue poll callback 可放在 queue 基础能力之后实现, 需要单独验证 JVM callback 线程附着和清理语义。

## 4. 目录结构

目标实现阶段的目录结构:

```text
wrapper/java/
├── design.md                         # 本文件
├── plan.md                           # 开发计划
├── pom.xml                           # Maven build, binding generation, tests
├── README.md                         # Java usage and packaging notes
├── native/                           # Rust UniFFI bridge crate
│   ├── Cargo.toml
│   ├── uniffi.toml                   # UniFFI Kotlin/JVM package and Java 8 config
│   ├── src/
│   │   ├── lib.rs                    # UniFFI scaffolding root
│   │   ├── timslite.udl              # Java-facing UniFFI interface
│   │   ├── bridge.rs                 # Store/Dataset/Queue wrapper objects
│   │   ├── config.rs                 # Java-facing config records
│   │   ├── errors.rs                 # TmslError -> UniFFI error conversion
│   │   ├── query.rs                  # Query iterator bridge objects
│   │   ├── queue.rs                  # Dataset/journal queue bridge objects
│   │   └── bin/uniffi_bindgen.rs     # UniFFI bindgen CLI
│   └── target/                       # Rust build output
├── src/main/java/io/github/snower/timslite/
│   ├── Timslite.java                 # version and loader helpers
│   ├── Store.java
│   ├── StoreConfig.java
│   ├── CreateDatasetOptions.java
│   ├── Dataset.java
│   ├── QueryIterator.java
│   ├── QueryLengthIterator.java
│   ├── Queue.java
│   ├── QueueConsumer.java
│   ├── JournalQueue.java
│   ├── JournalQueueConsumer.java
│   ├── Record.java
│   ├── LengthEntry.java
│   ├── InspectResult.java
│   └── errors/
│       └── *.java
├── src/test/java/io/github/snower/timslite/
│   └── *.java
├── scripts/
│   └── prepare-publish.sh            # Maven Central release prep
└── generated/
    └── uniffi/                       # generated Kotlin sources, build artifact only
```

`Cargo.toml`, `pom.xml`, Java/Rust sources, tests, README and generated bindings are implementation-stage files. This design stage only creates `design.md` and `plan.md`.

## 5. Public Java API

### 5.1 Module entry

```java
package io.github.snower.timslite;

public final class Timslite {
    public static String version();
}
```

### 5.2 Store

```java
try (Store store = Store.open("/data/timslite", StoreConfig.builder().build())) {
    store.createDataset("sensor_001", "events");
    try (Dataset ds = store.openDataset("sensor_001", "events")) {
        ds.write(1L, "hello".getBytes(StandardCharsets.UTF_8));
    }
}
```

`Store` implements `AutoCloseable` so Java 8 users can use try-with-resources. Closing is explicit and should be idempotent at the facade level. Operations after close throw `TmslStoreClosedException`.

Core methods:

```java
public final class Store implements AutoCloseable {
    public static Store open(String dataDir);
    public static Store open(String dataDir, StoreConfig config);

    public void close();
    public boolean isClosed();
    public boolean isReadOnly();

    public void createDataset(String name, String datasetType);
    public void createDataset(String name, String datasetType, CreateDatasetOptions options);
    public Dataset openDataset(String name, String datasetType);
    public Dataset openDatasetByIdentifier(long identifier);
    public void dropDataset(String name, String datasetType);

    public Queue openQueue(Dataset dataset);
    public JournalQueue openJournalQueue();

    public Long journalLatestSequence();
    public Record journalRead(long sequence);
    public List<Record> journalQuery(long startSequence, long endSequence);
    public Record readJournalSourceRecord(long identifier, JournalIndexInfo indexInfo);

    public TickResult tickBackgroundTasks();
    public long nextBackgroundDelayMillis();
    public List<String> getDatasetNames();
    public List<String> getDatasetTypes(String name);
    public InspectResult inspectDataset(String name, String datasetType);
}
```

### 5.3 Config builders

Java public API uses immutable config objects with builders. Generated UniFFI records stay internal.

```java
StoreConfig config = StoreConfig.builder()
    .enableBackgroundThread(false)
    .enableJournal(true)
    .readOnly(StoreReadOnly.AUTO)
    .build();

CreateDatasetOptions options = CreateDatasetOptions.builder()
    .indexContinuous(true)
    .retentionWindow(0L)
    .build();
```

`StoreReadOnly` is a Java enum:

| Java value | Rust semantic |
|------------|---------------|
| `AUTO` | `None`: try writable `.lock`, fall back to read-only |
| `WRITABLE_REQUIRED` | `Some(false)`: writable lock required |
| `READ_ONLY` | `Some(true)`: force read-only |

### 5.4 Dataset

```java
public final class Dataset implements AutoCloseable {
    public void write(long timestamp, byte[] data);
    public void append(long timestamp, byte[] data);
    public void delete(long timestamp);

    public Record read(long timestamp);
    public Record readLatest();

    public QueryIterator query(long startTs, long endTs);
    public List<Record> queryAll(long startTs, long endTs);

    public boolean readExist(long timestamp);
    public byte[] queryExist(long startTs, long endTs);
    public Integer readLength(long timestamp);
    public QueryLengthIterator queryLength(long startTs, long endTs);
    public List<LengthEntry> queryLengthAll(long startTs, long endTs);

    public void flush();
    public InspectResult inspect();
    public void close();

    public long id();
    public long identifier();
    public String dataDir();
    public Long latestTimestamp();
    public boolean isClosed();
}
```

`read` / `readLatest` return `null` when no visible record exists. `Record` is an immutable Java value object:

```java
public final class Record {
    public long timestamp();
    public byte[] data();
}
```

Returned `byte[]` must be owned by the caller. The facade should defensively copy arrays at public boundaries unless tests prove a safe no-copy path.

### 5.5 Query iterators

`QueryIterator` and `QueryLengthIterator` are Java facade objects over UniFFI bridge iterator handles:

```java
try (QueryIterator iter = ds.query(1L, 100L)) {
    while (iter.hasNext()) {
        Record record = iter.next();
    }
}
```

They implement `Iterator<T>` and `AutoCloseable`. Internally the Rust bridge may use the same source-cursor or index-snapshot/lazy-record strategy as the current public Rust/FFI/Python boundary, but Java users only see a normal Java iterator. Exhaustion closes the native iterator best-effort; explicit close remains recommended for early exit.

### 5.6 Queue and journal queue

```java
try (Queue queue = store.openQueue(ds)) {
    QueueConsumer consumer = queue.openConsumer("worker");
    long ts = queue.push(payload);
    Record record = consumer.poll(1000L);
    consumer.ack(record.timestamp());
}
```

Core methods:

```java
public final class Queue implements AutoCloseable {
    public long push(byte[] data);
    public QueueConsumer openConsumer(String groupName);
    public QueueConsumer openConsumer(String groupName, QueueConsumerOptions options);
    public void dropConsumer(String groupName);
    public void close();
}

public final class QueueConsumer {
    public Record poll(long timeoutMillis);
    public CompletableFuture<Record> pollAsync(long timeoutMillis);
    public void ack(long timestamp);
}

public final class JournalQueue implements AutoCloseable {
    public JournalQueueConsumer openConsumer(String groupName);
    public JournalQueueConsumer openConsumer(String groupName, QueueConsumerOptions options);
    public void close();
}

public final class JournalQueueConsumer {
    public Record poll(long timeoutMillis);
    public CompletableFuture<Record> pollAsync(long timeoutMillis);
    public void ack(long sequence);
}
```

`poll(timeoutMillis)` blocks the calling Java thread. `pollAsync(timeoutMillis)` is a Java facade convenience using `CompletableFuture` and an executor so Java applications can avoid blocking hot request threads. Timeout returns `null`; operational errors throw `TmslException`.

`pollCallback` is a separate implementation gate. If implemented, it should expose a Java 8 compatible callback interface, for example:

```java
public interface PollWakeCallback {
    void onWake();
}
```

Callback support must include tests for set, clear, duplicate registration errors, and JVM-safe invocation from Rust notification paths.

## 6. Type mapping

| Rust / UniFFI concept | Java public type | Notes |
|-----------------------|------------------|-------|
| `i64` timestamp / journal sequence | `long` | Signed timestamp contract maps directly |
| `u64` identifier / sizes | `long` | Wrapper validates `0..Long.MAX_VALUE`, no silent wrap |
| `u32` length | `int` | Record length remains below Java signed int max in current contract |
| `Vec<u8>` / `ByteArray` | `byte[]` | Copy at boundary unless safe ownership is proven |
| `String` | `String` | UTF-8 through UniFFI/JVM |
| `Option<i64>` | boxed `Long` / `null` | For latest timestamp and empty journal |
| `Vec<T>` | `List<T>` | Facade returns immutable or defensive-copy lists |
| Rust `Result<T, TmslError>` | return `T` or throw `TmslException` | Stable Java exception hierarchy |

Java public API avoids unsigned JVM types and Kotlin-specific `ULong`. All non-negative values that are `u64` in Rust are accepted only when they fit in signed Java `long`.

## 7. Error mapping

Generated UniFFI errors are wrapped into Java exceptions under `io.github.snower.timslite.errors`. The stable catching boundary is Java exception type plus an error code enum.

| Rust error | Java exception | Error code |
|------------|----------------|------------|
| `Io` | `TmslIoException` | `TMSL_IO` |
| `NotFound` | `TmslNotFoundException` | `TMSL_NOT_FOUND` |
| `AlreadyExists` | `TmslAlreadyExistsException` | `TMSL_ALREADY_EXISTS` |
| `InvalidData`, `InvalidMagic`, `InvalidVersion` | `TmslInvalidDataException` | `TMSL_INVALID_DATA` |
| `SegmentFull` | `TmslSegmentFullException` | `TMSL_SEGMENT_FULL` |
| `MmapError` | `TmslMmapException` | `TMSL_MMAP` |
| `CompressionError` | `TmslCompressionException` | `TMSL_COMPRESSION` |
| `DecompressionError` | `TmslDecompressionException` | `TMSL_DECOMPRESSION` |
| `Expired` | `TmslExpiredException` | `TMSL_EXPIRED` |
| Queue errors | matching `TmslQueue*Exception` | matching queue code |
| Wrapper closed state | `TmslStoreClosedException` / `TmslDatasetClosedException` | `TMSL_STORE_CLOSED` / `TMSL_DATASET_CLOSED` |

Exceptions should be unchecked (`RuntimeException`) unless implementation testing shows checked exceptions create a materially better Java API. Existing Python/Node wrappers expose runtime-style errors, so unchecked exceptions keep wrapper behavior aligned.

## 8. Lifecycle and ownership

- `Store` is the root object; `Dataset`, `Queue`, and journal queue objects are child handles.
- Java facade objects keep generated UniFFI objects private and guard closed state before delegation.
- `close()` should be explicit, idempotent where practical, and safe to call from try-with-resources.
- Finalizers are not part of the public contract. Java 8 compatibility means the wrapper must not rely on Java 9 `Cleaner`.
- Store close invalidates child facade objects. Child objects throw stable closed exceptions after invalidation.
- Native library loading should happen once per classloader through `Timslite` initialization, with a documented override for custom native path if implementation needs it.

## 9. Build and distribution

### 9.1 Local development

Implementation should support:

```bash
cargo check --manifest-path wrapper/java/native/Cargo.toml
mvn -f wrapper/java/pom.xml test
mvn -f wrapper/java/pom.xml install
```

On Windows:

```powershell
cargo check --manifest-path wrapper/java/native/Cargo.toml
mvn -f wrapper/java/pom.xml test
mvn -f wrapper/java/pom.xml install
```

### 9.2 Maven coordinates

Recommended coordinates:

```text
groupId: io.github.snower
artifactId: timslite
```

### 9.3 Native artifacts

MVP can start with local build and Maven-local validation. Release packaging should then add native artifacts for:

- Windows x86_64 MSVC
- Windows aarch64 MSVC
- Linux x86_64 GNU
- Linux aarch64 GNU
- macOS x86_64
- macOS aarch64 (Apple Silicon)

The preferred Maven layout is a single JAR containing all platform native libraries under platform-specific resource directories. Dynamic library filenames keep the standard OS convention:

```xml
<dependency>
    <groupId>io.github.snower</groupId>
    <artifactId>timslite</artifactId>
    <version>0.1.1</version>
</dependency>
```

The JAR includes native libraries at:

- `META-INF/native/macos-x86_64/libtimslite_java.dylib`
- `META-INF/native/macos-aarch64/libtimslite_java.dylib`
- `META-INF/native/linux-x86_64/libtimslite_java.so`
- `META-INF/native/linux-aarch64/libtimslite_java.so`
- `META-INF/native/windows-x86_64/timslite_java.dll`
- `META-INF/native/windows-aarch64/timslite_java.dll`

The `NativeLibraryLoader` detects the current OS/architecture and loads the correct library.

### 9.4 Release dependency strategy

Development checkout should use:

```toml
timslite = { path = "../../..", version = "=0.1.1" }
```

Before publishing source/native artifacts, release automation should rewrite the dependency to the exact crates.io version:

```toml
timslite = { version = "=0.1.1" }
```

This mirrors the Python and Node release model and prevents published Java source builds from depending on repository-relative paths.

## 10. Testing strategy

Java tests should use JUnit 4 or JUnit 5 with Java 8-compatible configuration. Each integration test must use an isolated temporary directory.

Test categories:

| Category | Focus |
|----------|-------|
| Smoke | load native library, `Timslite.version()`, open/close store |
| Config | defaults, Java builders, read-only tri-state |
| Lifecycle | create/open/drop/recreate datasets |
| Dataset IO | write, append, delete, read, read latest, flush |
| Query | iterator, queryAll, queryExist, readLength, queryLength |
| Persistence | close/reopen and data survives |
| Inspect | `DatasetInfo` and `DatasetState` nullability and numeric fields |
| Queue | push, poll timeout, ack, retry config |
| Journal | latest sequence, read/query raw payload, journal queue |
| Errors | each stable Java exception type and code |
| Packaging | Maven-local install, classpath load, native artifact selection |

Full implementation verification must include root crate checks because the bridge crate directly depends on the Rust public API.

## 11. Risks and constraints

| Risk | Impact | Mitigation |
|------|--------|------------|
| UniFFI JVM backend is Kotlin-first | Java API could feel awkward | Keep generated package internal and expose Java facade |
| Java 8 lacks `Cleaner` | Native object cleanup could rely on newer API | Require explicit `close()`, disable Java 9 cleaner path, test Java 8 runtime |
| `u64` values exceed Java `long` | Silent overflow or negative values | Validate all non-negative values fit `Long.MAX_VALUE` |
| Blocking queue poll | Application request threads can stall | Provide `pollAsync` facade and document blocking behavior |
| Callback from Rust notification thread | JVM attach/cleanup bugs | Gate `pollCallback` behind dedicated callback tests |
| Native artifact matrix | Release complexity | Build native libraries on all platforms, collect into single JAR |
| Wrapper API drift from Python/Node | User-facing behavior differs by language | Keep Store-managed public boundary and shared regression scenarios |

## 12. External references

- UniFFI user guide: <https://mozilla.github.io/uniffi-rs/latest/>
- UniFFI Kotlin/JVM configuration: <https://mozilla.github.io/uniffi-rs/latest/kotlin/configuration.html>
