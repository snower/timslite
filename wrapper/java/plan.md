# timslite Java Wrapper - 开发计划

> 基于 [design.md](design.md)
> 目标: 在 `wrapper/java` 中实现基于 UniFFI 的 JVM 绑定, 通过 Java 8 友好的 facade 暴露 timslite Store/Dataset/Queue/Journal API。

---

## 计划状态总览

| Phase | 描述 | 状态 | 产物 |
|-------|------|------|------|
| JAVA-0 | UniFFI 与 Java 8 工具链确认 | 待开始 | UniFFI 版本、Maven/Kotlin/JDK 约束 |
| JAVA-1 | Rust UniFFI bridge crate 骨架 | 待开始 | `Cargo.toml`, `src/lib.rs`, `src/timslite.udl` |
| JAVA-2 | Maven 项目与绑定生成 | 待开始 | `pom.xml`, generated Kotlin |
| JAVA-3 | 类型、配置、错误层 | 待开始 | config records/builders, Java exceptions |
| JAVA-4 | Store 与 Dataset lifecycle | 待开始 | `Store`, `Dataset`, inspect/listing |
| JAVA-5 | 数据读写与查询 | 待开始 | write/read/query/length/exist APIs |
| JAVA-6 | Queue 与 Journal API | 待开始 | queue poll/ack, journal read/query/queue |
| JAVA-7 | Java facade、README 与 Javadoc | 待开始 | Java public API, usage docs |
| JAVA-8 | 集成测试与回归验证 | 待开始 | Java/Rust tests and verification ladder |
| JAVA-9 | Maven/CI/native 发布准备 | 待开始 | Maven-local, native classifiers, release workflow plan |
| JAVA-10 | 跨层文档同步 | 待开始 | root README/design/plan/docs updates |

当前阶段只完成设计与计划文档, 不创建 Rust、Maven、Java 或测试源码。

---

## 开发原则

- 直接调用 timslite Rust public API, 不通过 C ABI 裸指针接口。
- UniFFI 生成 Kotlin/JVM binding, Java public API 通过 facade 暴露。
- Java 最低运行目标为 Java 8; 不依赖 Java 9 `Cleaner`。
- 所有 public lifecycle 类型实现 `AutoCloseable`, 示例使用 try-with-resources。
- 时间戳和 journal sequence 使用 Java `long`; 非负 `u64` 值只接受 `0..Long.MAX_VALUE`。
- payload 输入输出使用 `byte[]`; public 边界默认做防御性复制。
- Queue `poll()` 是阻塞调用; Java facade 额外提供 `CompletableFuture` 形式的 `pollAsync()`。
- 完成后先不要 git commit, 等审核确认。

---

## 目标目录结构

```text
wrapper/java/
├── Cargo.toml
├── uniffi.toml
├── pom.xml
├── README.md
├── design.md
├── plan.md
├── src/
│   ├── lib.rs
│   ├── timslite.udl
│   ├── bridge.rs
│   ├── config.rs
│   ├── errors.rs
│   ├── query.rs
│   └── queue.rs
├── src/main/java/io/github/snower/timslite/
│   └── *.java
├── src/main/java/io/github/snower/timslite/errors/
│   └── *.java
├── src/test/java/io/github/snower/timslite/
│   └── *.java
└── generated/uniffi/
    └── *.kt
```

---

## Phase JAVA-0: UniFFI 与 Java 8 工具链确认

文件:

- Modify: `wrapper/java/design.md`
- Modify: `wrapper/java/plan.md`

任务:

- [ ] 确认当前 UniFFI stable 版本和 Kotlin/JVM backend 配置项。
  - 记录 `uniffi`, `uniffi_bindgen` 或 Maven plugin 的精确版本。
  - 验证该版本支持 Java 8 目标字节码和禁用 Java 9 cleaner path。
- [ ] 确认 Maven/Kotlin/JDK 组合。
  - Maven compiler source/target 使用 1.8。
  - Kotlin Maven plugin `jvmTarget` 使用 1.8。
  - 测试运行至少覆盖 Java 8 或等效 Java 8 toolchain。
- [ ] 确认 Maven coordinates。
  - 固定使用 `groupId=io.github.snower`, `artifactId=timslite`。
  - Java package 固定使用 `io.github.snower.timslite`。
- [ ] 确认 local verification 命令。
  - `cargo check --manifest-path wrapper/java/Cargo.toml`
  - `cargo test --manifest-path wrapper/java/Cargo.toml`
  - `mvn -f wrapper/java/pom.xml test`
  - `mvn -f wrapper/java/pom.xml install`

验收标准:

- 后续实现不需要重新决定 UniFFI、Maven、Kotlin、JDK 核心版本。
- `design.md` 中 Java 8 兼容策略与实际工具链一致。

---

## Phase JAVA-1: Rust UniFFI bridge crate 骨架

文件:

- Create: `wrapper/java/Cargo.toml`
- Create: `wrapper/java/uniffi.toml`
- Create: `wrapper/java/src/lib.rs`
- Create: `wrapper/java/src/timslite.udl`
- Create: `wrapper/java/src/bridge.rs`
- Create: `wrapper/java/src/errors.rs`

任务:

- [ ] 创建 Rust bridge crate。
  - `[lib] crate-type = ["cdylib", "rlib"]`。
  - 依赖 root crate: `timslite = { path = "../..", version = "=0.1.1" }`。
  - 添加 UniFFI 运行时和 bindgen/build 依赖。
- [ ] 创建最小 UniFFI interface。
  - namespace 使用 `timslite`。
  - 暴露 `version() -> string` smoke function。
  - 暴露基础 `TimsliteError` error 类型。
- [ ] 创建 `src/lib.rs` scaffolding root。
  - 注册 UniFFI scaffolding。
  - 只接入 smoke function, 不暴露 Store/Dataset。
- [ ] 创建 `uniffi.toml`。
  - Kotlin package 使用 `io.github.snower.timslite.uniffi`。
  - 配置 Java 8 兼容清理策略。
- [ ] 验证 Rust bridge 编译。
  - `cargo check --manifest-path wrapper/java/Cargo.toml`

验收标准:

- bridge crate 可独立 `cargo check`。
- UniFFI 可从 bridge crate 生成 JVM binding。

---

## Phase JAVA-2: Maven 项目与绑定生成

文件:

- Create: `wrapper/java/pom.xml`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/Timslite.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/SmokeTest.java`
- Modify: `wrapper/java/uniffi.toml`

任务:

- [ ] 创建 Maven 项目。
  - 配置 `maven-compiler-plugin` source/target 1.8。
  - 配置 Kotlin Maven plugin 仅用于编译 generated UniFFI Kotlin sources。
  - 配置 Surefire 运行 Java integration tests。
- [ ] 添加 binding generation task。
  - 构建 Rust cdylib。
  - 调用 UniFFI bindgen 生成 Kotlin sources 到 `generated/uniffi`。
  - 将 generated Kotlin 加入 Maven compile sources。
- [ ] 添加 native library copy/load 任务。
  - 将当前平台 native library 复制到 test runtime resources。
  - Java smoke test 能加载本地构建产物。
- [ ] 创建 Java facade smoke API。
  - `Timslite.version()` 委托 generated binding。
- [ ] 添加 smoke test。
  - 验证 classpath 加载。
  - 验证 `Timslite.version()` 非空。
- [ ] 验证 Maven 测试。
  - `mvn -f wrapper/java/pom.xml test`

验收标准:

- Java 测试能通过 generated UniFFI binding 调用 Rust smoke function。
- 生成的 Kotlin/JVM binding 不作为手写源码提交, 除非发布策略要求。

---

## Phase JAVA-3: 类型、配置、错误层

文件:

- Create: `wrapper/java/src/config.rs`
- Modify: `wrapper/java/src/errors.rs`
- Modify: `wrapper/java/src/timslite.udl`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/StoreConfig.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/CreateDatasetOptions.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/QueueConsumerOptions.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/StoreReadOnly.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/errors/*.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/ConfigTest.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/ErrorTest.java`

任务:

- [ ] 设计 Java-facing UniFFI records。
  - Store config 覆盖当前 `StoreConfig` 字段。
  - Dataset options 覆盖当前 `DataSetConfigBuilder` 字段。
  - Queue consumer options 覆盖 retry config 字段。
- [ ] 实现 Rust config conversion。
  - 缺省字段使用 root crate defaults。
  - `readOnly` 三态映射到 Rust `Option<bool>`。
  - 非负 long 字段验证后转换为 `u64`。
- [ ] 实现 Rust error conversion。
  - 覆盖所有 `TmslError` 变体。
  - closed-state wrapper errors 有稳定 code。
- [ ] 实现 Java config builders。
  - Java facade builder 生成 internal UniFFI record。
  - Builder 默认值与 Rust default 语义一致。
- [ ] 实现 Java exception hierarchy。
  - 基类 `TmslException extends RuntimeException`。
  - 每个子类提供 `TmslErrorCode code()`。
- [ ] 添加配置和错误测试。
  - defaults。
  - custom values。
  - invalid negative size。
  - invalid read-only/write contention case。
  - Rust error 到 Java exception 映射。

验收标准:

- Java config 覆盖当前 root `StoreConfig` / `DataSetConfig` 权威字段。
- Java exception 捕获边界稳定, 不泄漏 generated exception 到用户 API。

---

## Phase JAVA-4: Store 与 Dataset lifecycle

文件:

- Modify: `wrapper/java/src/bridge.rs`
- Modify: `wrapper/java/src/timslite.udl`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/Store.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/Dataset.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/InspectResult.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/DatasetInfo.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/DatasetState.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/TickResult.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/LifecycleTest.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/InspectTest.java`

任务:

- [ ] 实现 bridge `StoreBridge`。
  - `open(path, config)`。
  - `close()`。
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
  - `next_background_delay`。
- [ ] 实现 Java `Store` facade。
  - `AutoCloseable`。
  - try-with-resources 示例可运行。
  - close 后方法抛 stable closed exception。
- [ ] 实现 Java `Dataset` facade lifecycle shell。
  - 持有 internal `DatasetBridge`。
  - `close()`。
  - getters: id, identifier, dataDir, latestTimestamp, closed。
- [ ] 添加 lifecycle tests。
  - open/close。
  - create/open/drop/recreate。
  - open by identifier。
  - inspect nullability。
  - close idempotency。

验收标准:

- Java 可以完整管理 Store 和 Dataset 生命周期。
- 生命周期行为与 Python/Node Store-managed public boundary 对齐。

---

## Phase JAVA-5: 数据读写与查询

文件:

- Create: `wrapper/java/src/query.rs`
- Modify: `wrapper/java/src/bridge.rs`
- Modify: `wrapper/java/src/timslite.udl`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/Record.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/LengthEntry.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/QueryIterator.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/QueryLengthIterator.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/DatasetIoTest.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/QueryTest.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/PersistenceTest.java`

任务:

- [ ] 实现 Dataset write APIs。
  - `write(long, byte[])`。
  - `append(long, byte[])`。
  - `delete(long)`。
  - `flush()`。
- [ ] 实现 Dataset read APIs。
  - `read(long) -> Record | null`。
  - `readLatest() -> Record | null`。
  - Returned `byte[]` 独立拥有。
- [ ] 实现 query iterator bridge。
  - `query(start, end) -> QueryIteratorBridge`。
  - `next() -> Record | null`。
  - `close()`。
- [ ] 实现 Java `QueryIterator` facade。
  - Implements `Iterator<Record>`。
  - Implements `AutoCloseable`。
  - Exhaustion and explicit close both safe。
- [ ] 实现 lightweight reads。
  - `readExist`。
  - `queryExist`。
  - `readLength`。
  - `queryLength`。
  - `queryLengthAll`。
- [ ] 添加数据和查询测试。
  - write/read。
  - append forward and append latest。
  - delete then read returns null。
  - readLatest exact max timestamp behavior。
  - query iterator partial consumption。
  - queryAll。
  - queryExist bitmap。
  - readLength and queryLength。
  - close/reopen persistence。

验收标准:

- Java wrapper 能完成基础时序读写与查询工作流。
- Query iterator 不要求用户理解 Rust lifetime 或 UniFFI generated classes。

---

## Phase JAVA-6: Queue 与 Journal API

文件:

- Create: `wrapper/java/src/queue.rs`
- Modify: `wrapper/java/src/bridge.rs`
- Modify: `wrapper/java/src/timslite.udl`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/Queue.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/QueueConsumer.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/JournalQueue.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/JournalQueueConsumer.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/JournalIndexInfo.java`
- Create: `wrapper/java/src/main/java/io/github/snower/timslite/PollWakeCallback.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/QueueTest.java`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/JournalTest.java`

任务:

- [ ] 实现 Dataset queue。
  - `Store.openQueue(dataset)`。
  - `Queue.push(byte[]) -> long`。
  - `Queue.openConsumer(groupName, options)`。
  - `Queue.dropConsumer(groupName)`。
  - `Queue.close()`。
- [ ] 实现 queue consumer。
  - `poll(timeoutMillis) -> Record | null`。
  - `pollAsync(timeoutMillis) -> CompletableFuture<Record>` in Java facade。
  - `ack(timestamp)`。
- [ ] 实现 journal APIs。
  - `journalLatestSequence()`。
  - `journalRead(sequence)`。
  - `journalQuery(start, end)`。
  - `readJournalSourceRecord(identifier, indexInfo)`。
- [ ] 实现 journal queue。
  - `openJournalQueue()`。
  - `JournalQueue.openConsumer(...)`。
  - `JournalQueueConsumer.poll / pollAsync / ack`。
- [ ] 实现 poll callback only after callback gate passes。
  - Set callback。
  - Clear callback。
  - Duplicate non-null registration raises stable exception。
  - Callback invocation is JVM-safe from Rust notification path。
- [ ] 添加 queue/journal tests。
  - open consumer before push, then poll。
  - ack prevents redelivery。
  - timeout returns null。
  - pollAsync does not block caller thread。
  - journal create/write/delete/append creates sequences。
  - journal queue consumes subsequent records。
  - poll callback set/clear/duplicate behavior when implemented。

验收标准:

- Java queue and journal behavior matches current Rust/Python/Node public contract。
- poll callback is either fully verified or explicitly left out of MVP docs and status table。

---

## Phase JAVA-7: Java facade、README 与 Javadoc

文件:

- Modify: `wrapper/java/src/main/java/io/github/snower/timslite/*.java`
- Modify: `wrapper/java/src/main/java/io/github/snower/timslite/errors/*.java`
- Create: `wrapper/java/README.md`
- Modify: `wrapper/java/pom.xml`

任务:

- [ ] 完成 Java public API polish。
  - Method names use Java lowerCamelCase。
  - Value objects are immutable。
  - Public `byte[]` returns are defensive copies。
  - Generated UniFFI types do not appear in public signatures。
- [ ] 添加 Javadoc。
  - Store lifecycle and try-with-resources。
  - read-only mode。
  - timestamp/long numeric constraints。
  - queue blocking poll and async poll。
  - journal raw payload contract。
- [ ] 编写 README。
  - Installation from Maven-local and future Maven Central。
  - Basic write/read/query example。
  - Queue example。
  - Journal example。
  - Native artifact loading notes。
- [ ] 添加 Javadoc generation verification。
  - Maven `javadoc:javadoc` goal passes。

验收标准:

- Java 用户无需阅读 generated Kotlin source 即可使用 wrapper。
- README 示例可直接复制到 Java 8 项目中运行。

---

## Phase JAVA-8: 集成测试与回归验证

文件:

- Modify/Create: `wrapper/java/src/test/java/io/github/snower/timslite/*.java`
- Modify: `wrapper/java/pom.xml`
- Optional Create: `wrapper/java/src/test/resources/*`

任务:

- [ ] 完成 Java integration tests。
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
- [ ] 运行 Java wrapper verification。
  - `cargo check --manifest-path wrapper/java/Cargo.toml`
  - `cargo test --manifest-path wrapper/java/Cargo.toml`
  - `mvn -f wrapper/java/pom.xml test`
  - `mvn -f wrapper/java/pom.xml javadoc:javadoc`
- [ ] 运行 root regression ladder。
  - `cargo fmt -- --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test -- --test-threads=1`
  - `git diff --check`

验收标准:

- Java wrapper tests pass on local platform。
- Root crate regression checks pass or any environment limitation is explicitly documented。

---

## Phase JAVA-9: Maven/CI/native 发布准备

文件:

- Modify: `wrapper/java/pom.xml`
- Modify: `wrapper/java/Cargo.toml`
- Create: `wrapper/java/scripts/prepare-publish.*`
- Create/Modify: `.github/workflows/java-release.yml`
- Modify: `wrapper/java/README.md`
- Create: `wrapper/java/src/test/java/io/github/snower/timslite/PackagingTest.java`

任务:

- [ ] Add Maven publication config。
  - Main Java facade jar。
  - Sources jar。
  - Javadoc jar。
  - Native classifier artifacts。
- [ ] Add native artifact loader tests。
  - Current OS/arch selects one library。
  - Missing library produces actionable error。
  - Custom native path override works if provided。
- [ ] Add publish preparation script。
  - Rewrites `timslite = { path = "../.." }` to exact crates.io dependency。
  - Verifies root crate and wrapper versions match。
- [ ] Add CI matrix。
  - Windows x86_64。
  - Linux x86_64。
  - Linux aarch64 when runner/tooling is available。
  - macOS x86_64。
  - macOS aarch64。
  - Java 8 runtime/toolchain verification。
- [ ] Add release dry-run。
  - `mvn -f wrapper/java/pom.xml install`。
  - Package content inspection。
  - Native library load from Maven-local dependency。

验收标准:

- Local Maven consumers can depend on the wrapper from Maven-local。
- Release workflow has an explicit native artifact strategy before public publication。

---

## Phase JAVA-10: 跨层文档同步

文件:

- Modify: `README.md`
- Modify: `design.md`
- Modify: `plan.md`
- Optional Modify: `docs/design/store-and-ffi.md`
- Optional Create: `docs/plan/phase-java-wrapper.md`

任务:

- [ ] 在 root README 增加 Java wrapper 状态和入口。
- [ ] 在 root `design.md` 的 wrapper/FFI 索引中增加 Java wrapper。
- [ ] 在 root `plan.md` 增加 Java wrapper phase 状态。
- [ ] 如 Java wrapper 暴露边界影响 Store/FFI 说明, 同步更新 `docs/design/store-and-ffi.md`。
- [ ] 如项目需要独立 phase 文档, 创建 `docs/plan/phase-java-wrapper.md`。

验收标准:

- 项目入口文档可以发现 Java wrapper。
- Java wrapper 文档与 root 状态文档不冲突。

---

## 回归验证清单

实现完成前不得声称完成, 除非以下验证已跑通或明确记录无法运行原因:

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
cargo check --manifest-path wrapper/java/Cargo.toml
cargo test --manifest-path wrapper/java/Cargo.toml
mvn -f wrapper/java/pom.xml test
mvn -f wrapper/java/pom.xml javadoc:javadoc
mvn -f wrapper/java/pom.xml install
git diff --check
```

Windows 等价命令:

```powershell
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
cargo check --manifest-path wrapper/java/Cargo.toml
cargo test --manifest-path wrapper/java/Cargo.toml
mvn -f wrapper/java/pom.xml test
mvn -f wrapper/java/pom.xml javadoc:javadoc
mvn -f wrapper/java/pom.xml install
git diff --check
```

如实现过程中修改 Python wrapper、Node wrapper、C ABI/header 或 root public API, 必须追加对应语言/FFI 的验证命令。

---

## 风险跟踪

| 风险 | 优先级 | 应对 |
|------|--------|------|
| UniFFI Kotlin generated API 对 Java 不够自然 | 高 | generated package internal, Java facade 是唯一稳定 API |
| Java 8 runtime cleanup 能力有限 | 高 | 显式 close + try-with-resources, 不依赖 Java 9 Cleaner |
| `u64` 到 Java `long` 溢出 | 高 | 范围验证, 超出直接抛 `TmslInvalidDataException` |
| Native artifact 加载失败 | 高 | loader tests + actionable error + Maven-local packaging test |
| Blocking poll 被误用在 request thread | 中 | 文档说明 + `pollAsync` convenience |
| poll callback JVM 线程语义复杂 | 中 | 单独 gate, 未验证前不标记 MVP 完成 |
| UniFFI 版本升级破坏生成代码 | 中 | JAVA-0 锁定版本, CI 跑 generated binding build |
| Java wrapper 与 Python/Node API 漂移 | 中 | 共享测试场景和 Store-managed boundary 术语 |

---

## 后续扩展

- Android AAR 包。
- `JournalRecord.decode(byte[])` Java helper。
- Reactive Streams / Flow adapter, 作为 Java 9+ 或独立 artifact。
- Kotlin-first facade artifact。
- Benchmark and throughput examples。
