# timslite Node.js Wrapper - Node-API Design

> 目标: 在 `wrapper/nodejs` 中提供基于 Node 官方 Node-API 的原生包装, 让 Node.js/TypeScript 用户可以直接使用当前 timslite Rust library。

---

## 1. 设计目标

Node.js wrapper 应当是 timslite 的薄包装层, 不重新实现存储、索引、queue 或 journal 逻辑。核心行为继续由 Rust crate 的 public API 负责, Node 层只处理:

- Node-API 模块导出和对象生命周期。
- JavaScript/TypeScript 类型到 Rust 类型的转换。
- `Buffer` / `Uint8Array` 与 `Vec<u8>` / `&[u8]` 的数据边界。
- `bigint` 与 Rust `i64` / `u64` 的无损转换。
- timslite 错误到 Node `Error` 的映射。
- queue/journal poll 的异步化, 避免阻塞 Node 事件循环。
- npm 包结构、类型声明、本地构建和预编译产物发布。

不改变当前 Rust、C ABI、Python wrapper 或磁盘格式契约。

## 2. 技术方案

### 2.1 推荐方案: Rust + Node-API binding

使用 Rust 原生 addon 直接依赖项目根 crate:

```text
Node.js / TypeScript
        |
        v
wrapper/nodejs native addon (.node)
        |
        v
timslite Rust public API
```

实现层建议使用 `napi-rs` 生成 Node-API addon。这里的稳定 ABI 边界是 Node 官方 Node-API, 不是 V8 私有 ABI, 也不是 C ABI 裸指针接口。`napi-rs` 只作为 Rust 侧绑定生成和构建工具, wrapper 不暴露 `include/timslite.h` 的 C handle 模型。

选择原因:

- 与 Python wrapper 一样直接调用 Rust public API, 避免 C ABI 的 malloc/free 和全局 handle registry 细节泄漏到 Node。
- Node-API 提供跨 Node 版本的 ABI 稳定性, 适合发布 npm 预编译 `.node` 二进制。
- Rust `Arc<DataSet>`、`DatasetQueue`、`JournalQueue` 等 Clone-safe handle 可以自然映射到 JS class。
- queue `poll` 可以用 Node-API async work / Promise 封装, 不阻塞事件循环。
- poll callback 可以用 Node-API threadsafe function 安全回到 JS 线程。

### 2.2 备选方案

| 方案 | 优点 | 主要问题 | 结论 |
|------|------|----------|------|
| Rust + Node-API binding | 复用 Rust public API, 类型和生命周期最接近 Python wrapper | 需要引入 Node wrapper 构建工具链 | 推荐 |
| C++ `node-addon-api` + C ABI | 更贴近官方 C++ wrapper 生态, 可以复用 `include/timslite.h` | 需要复制 FFI handle 生命周期、malloc/free、err_buf、iterator close 等规则 | 不作为主线 |
| 纯 JS FFI package | 初期代码少 | 依赖运行时动态加载, 类型安全弱, 分发复杂, 不符合官方 Node-API wrapper 目标 | 不采用 |

## 3. 支持范围

### 3.1 MVP 必须覆盖

- Store lifecycle: open / close / read-only config / manual background tick。
- Dataset lifecycle: create / open / open by identifier / drop / inspect。
- Dataset data operations: write / append / delete / read / read latest / query / flush。
- Lightweight reads: readExist / queryExist / readLength / queryLength。
- DatasetQueue: open queue / push / open consumer / drop consumer / close。
- DatasetQueueConsumer: async poll / sync non-blocking poll / ack / poll callback。
- Journal read/query/latest and JournalQueue consumer。
- TypeScript declarations and Node tests。

### 3.2 MVP 不覆盖

- 独立解析 JournalRecord payload 的高级 JS helper。MVP 先返回 encoded journal payload `Buffer`, 与当前 Python wrapper 保持一致。
- 浏览器/WASM build。
- Electron ABI 专属 build。Electron 用户可在后续通过 Node-API ABI 或专门预构建支持。
- 流式 Node `Readable` / `AsyncIterable` queue 消费封装。MVP 提供 Promise poll, 上层可自行封装。

## 4. 目录结构

```text
wrapper/nodejs/
├── design.md                      # 本文件
├── plan.md                        # 开发计划
├── Cargo.toml                     # Node-API native addon crate
├── build.rs                       # napi build hook
├── package.json                   # npm package metadata and scripts
├── README.md                      # Node 使用说明
├── binding-target.js              # runtime platform -> native binding filename mapping
├── scripts/
│   ├── install.js                  # npm postinstall source-build fallback
│   └── prepare-publish.js          # release-time crates.io dependency rewrite
├── index.d.ts                     # TypeScript public types
├── src/
│   ├── lib.rs                     # Node-API module root
│   ├── types.rs                   # bigint / Buffer / option conversion helpers
│   ├── errors.rs                  # TmslError -> Node Error mapping
│   ├── config.rs                  # StoreConfig / DatasetConfig option decoding
│   ├── store.rs                   # Store class
│   ├── dataset.rs                 # Dataset class
│   ├── query.rs                   # QueryIterator and QueryLengthIterator
│   └── queue.rs                   # DatasetQueue / JournalQueue and consumers
└── tests/
    ├── basic.test.ts              # import, open, close
    ├── config.test.ts             # config defaults and validation
    ├── dataset.test.ts            # lifecycle, write/read/query
    ├── queue.test.ts              # queue push/poll/ack/callback
    ├── journal.test.ts            # journal read/query/queue
    └── package.test.ts            # npm package layout, loader, install fallback
```

`Cargo.toml`, `package.json`, `src/`, `tests/` 等文件只在实现阶段创建。本设计阶段不创建代码骨架。

## 5. Public API

### 5.1 Module exports

```ts
export class Store;
export class Dataset;
export class QueryIterator implements Iterable<[bigint, Buffer]>;
export class QueryLengthIterator implements Iterable<[bigint, number]>;
export class DatasetQueue;
export class DatasetQueueConsumer;
export class JournalQueue;
export class JournalQueueConsumer;

export interface StoreConfig;
export interface CreateDatasetOptions;
export interface QueueConsumerOptions;
export interface DataSetInfo;
export interface DataSetState;
export interface DataSetInspectResult;
export interface JournalIndexInfo;

export type TmslErrorCode;
```

### 5.2 Numeric conventions

Node wrapper 必须避免 timestamp/sequence/identifier 精度丢失:

| Rust 类型 | JS/TS 输入 | JS/TS 输出 | 说明 |
|-----------|------------|------------|------|
| `i64` timestamp / journal sequence | `number | bigint` | `bigint` | 输入为 `number` 时必须是 safe integer |
| `u64` identifier / segment size | `number | bigint` | `bigint` for identifiers and inspect sizes | 配置输入允许 safe integer number |
| `u32` data length | `number` | `number` | 小于 JS safe integer |
| `usize` memory size | `number | bigint` | `number | bigint` by field | 配置中常用 number |
| `Duration` | `number` milliseconds | `number` milliseconds | Node API 使用毫秒 |
| bytes | `Buffer | Uint8Array` | `Buffer` | 输出 Buffer 由 Node/Rust 边界拥有 |

输入 `number` 超出 `Number.isSafeInteger()` 时返回 `TMSL_INVALID_DATA`, 要求调用方使用 `bigint`。

### 5.3 Store

```ts
class Store {
  static open(dataDir: string, config?: StoreConfig): Store;

  close(): void;

  createDataset(name: string, datasetType: string, options?: CreateDatasetOptions): Dataset;
  openDataset(name: string, datasetType: string): Dataset;
  openDatasetByIdentifier(identifier: number | bigint): Dataset;
  dropDataset(name: string, datasetType: string): void;

  openQueue(dataset: Dataset): DatasetQueue;
  openQueue(datasetId: number | bigint): DatasetQueue;
  openJournalQueue(): JournalQueue;

  journalLatestSequence(): bigint | null;
  journalRead(sequence: number | bigint): [bigint, Buffer] | null;
  journalQuery(startSequence: number | bigint, endSequence: number | bigint): Array<[bigint, Buffer]>;
  readJournalSourceRecord(datasetIdentifier: number | bigint, indexInfo: JournalIndexInfo): [bigint, Buffer];

  tickBackgroundTasks(): { executedTasks: number; nextDelayMs: number };
  nextBackgroundDelay(): number;

  getDatasetNames(): string[];
  getDatasetTypes(name: string): string[];
  inspectDataset(name: string, datasetType: string): DataSetInspectResult;

  readonly closed: boolean;
  readonly readOnly: boolean;
}
```

`Store` 内部持有 `Option<timslite::Store>`。`close()` 使用 `take()` 防止 use-after-close, 并对已追踪 dataset/queue 做 best-effort flush/close。关闭后所有 Store 方法返回 `TMSL_STORE_CLOSED`。

### 5.4 StoreConfig

Node 使用 plain object, 不暴露 builder:

```ts
interface StoreConfig {
  flushIntervalMs?: number;
  idleTimeoutMs?: number;
  dataSegmentSize?: number | bigint;
  indexSegmentSize?: number | bigint;
  initialDataSegmentSize?: number | bigint;
  initialIndexSegmentSize?: number | bigint;
  compressLevel?: number;
  compressType?: 0 | 1;
  cacheMaxMemory?: number | bigint;
  cacheIdleTimeoutMs?: number;
  retentionCheckHour?: number;
  enableBackgroundThread?: boolean;
  enableJournal?: boolean;
  readOnly?: boolean | null;
}
```

默认值来自 `timslite::StoreConfig::default()`。`readOnly` 语义与 Rust 一致: `undefined/null` 表示 auto, `false` 表示要求可写, `true` 表示强制只读。

### 5.5 CreateDatasetOptions

```ts
interface CreateDatasetOptions {
  dataSegmentSize?: number | bigint;
  indexSegmentSize?: number | bigint;
  initialDataSegmentSize?: number | bigint;
  initialIndexSegmentSize?: number | bigint;
  compressLevel?: number;
  compressType?: 0 | 1;
  indexContinuous?: boolean;
  retentionWindow?: number | bigint;
  enableJournal?: boolean;
}
```

缺省值来自 Store config。`compressType` 使用当前 Rust contract: `0=zstd`, `1=deflate`。

### 5.6 Dataset

```ts
class Dataset {
  write(timestamp: number | bigint, data: Buffer | Uint8Array): void;
  append(timestamp: number | bigint, data: Buffer | Uint8Array): void;
  delete(timestamp: number | bigint): void;

  read(timestamp: number | bigint): [bigint, Buffer] | null;
  readLatest(): [bigint, Buffer] | null;

  query(startTs: number | bigint, endTs: number | bigint): QueryIterator;
  queryAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, Buffer]>;

  readExist(timestamp: number | bigint): boolean;
  queryExist(startTs: number | bigint, endTs: number | bigint): Buffer;
  readLength(timestamp: number | bigint): number | null;
  queryLength(startTs: number | bigint, endTs: number | bigint): QueryLengthIterator;
  queryLengthAll(startTs: number | bigint, endTs: number | bigint): Array<[bigint, number]>;

  flush(): void;
  close(): void;
  inspect(): DataSetInspectResult;

  readonly id: bigint;
  readonly identifier: bigint;
  readonly dataDir: string;
  readonly latestTimestamp: bigint | null;
  readonly closed: boolean;
}
```

`Dataset` 持有 `Arc<timslite::DataSet>`。所有读写继续走 public `DataSet` API, 由 Rust 内部 mutex 保护。普通用户不接触 `DataSetHandle` 或 `Arc`。

### 5.7 Query iterators

```ts
class QueryIterator implements Iterable<[bigint, Buffer]> {
  [Symbol.iterator](): QueryIterator;
  next(): IteratorResult<[bigint, Buffer]>;
  close(): void;
  readonly remaining: number;
}

class QueryLengthIterator implements Iterable<[bigint, number]> {
  [Symbol.iterator](): QueryLengthIterator;
  next(): IteratorResult<[bigint, number]>;
  close(): void;
  readonly remaining: number;
}
```

MVP 可以沿用当前 Python wrapper 的 eager result 模式: `Dataset.query()` 调用 `DataSet::query()` 取得 `Vec<(i64, Vec<u8>)>`, 再用 JS iterator 逐条返回。后续如需进一步降低峰值内存, 再迁移为 source-cursor / lazy header 读取的 Node 专用 iterator。

### 5.8 Queue

```ts
interface QueueConsumerOptions {
  runningExpiredMs?: number;
  maxRetryCount?: number;
}

class DatasetQueue {
  push(data: Buffer | Uint8Array): bigint;
  openConsumer(groupName: string, options?: QueueConsumerOptions): DatasetQueueConsumer;
  dropConsumer(groupName: string): void;
  close(): void;
}

class DatasetQueueConsumer {
  poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>;
  pollSync(timeoutMs?: number): [bigint, Buffer] | null;
  ack(timestamp: number | bigint): void;
  pollCallback(callback: (() => void) | null): void;
}
```

`poll(timeoutMs)` 是推荐 API, 通过 Node-API async work 在线程池中执行 Rust `consumer.poll(Duration)`, resolve 为 record 或 `null`。`pollSync()` 只用于测试、CLI 和 `timeoutMs=0` 的显式非阻塞调用; 文档应提醒用户不要在事件循环中长时间同步等待。

`pollCallback(callback)` 使用 Node-API threadsafe function。Rust 通知线程只能调用 threadsafe function, 不能直接执行 JS callback。callback 是 best-effort wake hook, 不携带数据, 不保证每条记录精确触发一次, 数据消费仍必须以 `poll/ack` 为准。

### 5.9 Journal queue

```ts
class JournalQueue {
  openConsumer(groupName: string, options?: QueueConsumerOptions): JournalQueueConsumer;
  close(): void;
}

class JournalQueueConsumer {
  poll(timeoutMs?: number): Promise<[bigint, Buffer] | null>;
  pollSync(timeoutMs?: number): [bigint, Buffer] | null;
  ack(sequence: number | bigint): void;
  pollCallback(callback: (() => void) | null): void;
}
```

Journal payload 是 encoded `JournalRecord` 原始 bytes。Node wrapper 不把 `.journal/logs` 暴露为普通 Dataset。

## 6. 错误映射

Node wrapper 把 `timslite::TmslError` 转成 JS `Error`, 并设置稳定 `code` 与 `name`:

| Rust error | `name` | `code` |
|------------|--------|--------|
| `Io` | `TmslIoError` | `TMSL_IO` |
| `NotFound` | `TmslNotFoundError` | `TMSL_NOT_FOUND` |
| `AlreadyExists` | `TmslAlreadyExistsError` | `TMSL_ALREADY_EXISTS` |
| `InvalidData`, `InvalidMagic`, `InvalidVersion` | `TmslInvalidDataError` | `TMSL_INVALID_DATA` |
| `SegmentFull` | `TmslSegmentFullError` | `TMSL_SEGMENT_FULL` |
| `MmapError` | `TmslMmapError` | `TMSL_MMAP` |
| `CompressionError` | `TmslCompressionError` | `TMSL_COMPRESSION` |
| `DecompressionError` | `TmslDecompressionError` | `TMSL_DECOMPRESSION` |
| `Expired` | `TmslExpiredError` | `TMSL_EXPIRED` |
| `QueueAlreadyOpen` | `TmslQueueAlreadyOpenError` | `TMSL_QUEUE_ALREADY_OPEN` |
| `QueueNotOpen` | `TmslQueueNotOpenError` | `TMSL_QUEUE_NOT_OPEN` |
| `ConsumerGroupNotFound` | `TmslConsumerGroupNotFoundError` | `TMSL_CONSUMER_GROUP_NOT_FOUND` |
| `ConsumerGroupExists` | `TmslConsumerGroupExistsError` | `TMSL_CONSUMER_GROUP_EXISTS` |
| `QueueClosed` | `TmslQueueClosedError` | `TMSL_QUEUE_CLOSED` |
| `PendingFull` | `TmslPendingFullError` | `TMSL_PENDING_FULL` |
| wrapper lifecycle errors | `TmslInvalidDataError` or `TmslStoreClosedError` | `TMSL_STORE_CLOSED` / `TMSL_INVALID_DATA` |

MVP 以 `err.code` 作为稳定捕获边界。是否额外提供 JS subclass `instanceof TmslInvalidDataError` 由实现阶段评估; 不得牺牲 native method 的简单性和错误码稳定性。

## 7. 生命周期与所有权

- `Store` 是根对象, `Dataset` / queue / journal queue 是子对象。
- `Dataset` 持有 `Arc<DataSet>`, 可在 JS 中独立于 `Store` 对象引用存在, 但 `Store.close()` 后不应继续允许写入。
- wrapper 内部维护 dataset id -> handle/Arc 的追踪表, 用于 `openQueue(datasetId)` 和关闭时 best-effort cleanup。
- `close()` 必须幂等或返回稳定的 already-closed 错误; 不允许 panic。
- N-API finalizer 只能做 best-effort flush/drop, 用户可见文档必须要求显式 `close()`。
- `Dataset.close()` 走 Rust public lifecycle close; `Store.close()` 会 drain Store registry 并关闭 journal。
- queue consumer 没有单独 Rust close API; JS 对象释放时 drop handle。删除 consumer group 使用 `queue.dropConsumer(groupName)`。

## 8. 并发与 Node 事件循环

- Dataset 普通读写方法同步执行, 由 Rust 内部锁保证安全。调用方如果在热路径中担心事件循环延迟, 应放入 Worker Thread。
- `queue.poll()` / `journalConsumer.poll()` 默认异步, 避免 condvar wait 阻塞事件循环。
- `pollSync(timeoutMs)` 保留给 `timeoutMs=0`、测试和脚本使用。
- `pollCallback` 的 JS callback 必须通过 threadsafe function 调度到 JS 线程; Rust 通知线程不得直接持有 JS env 调用函数。
- callback 中不应做耗时工作, 只用于唤醒外部调度; 可靠消费仍以 `poll/ack` 为准。

## 9. 构建与发布

### 9.1 Local build

实现阶段应提供:

```bash
cd wrapper/nodejs
npm install
npm run build
npm test
```

Rust 验证:

```bash
cargo check --manifest-path wrapper/nodejs/Cargo.toml
cargo test --manifest-path wrapper/nodejs/Cargo.toml
cargo clippy --manifest-path wrapper/nodejs/Cargo.toml --all-targets -- -D warnings
```

### 9.2 Node support

以当前仍受 Node.js Release Working Group 支持的 LTS/Current release line 为目标。实现阶段必须在 `package.json engines.node` 中选择仍处于 supported release lines 的最低版本, 并记录选择原因。由于本 wrapper 依赖 BigInt、Node-API async work 和 threadsafe function, 不支持过旧 Node。

### 9.3 Package layout

npm 包名建议:

```text
timslite
```

发布前如 Python/Rust crate 已占用同名生态约定, 可使用 scoped name:

```text
@timslite/node
```

预编译产物至少覆盖:

- Windows x64 MSVC
- Windows ARM64 MSVC
- Linux x64 GNU
- Linux ARM64 GNU
- macOS ARM64

发布包采用单一 root package, 将支持平台的 `.node` 文件直接放在根包中, 不发布 `timslite-<platform>` optional packages。runtime loader 只加载根包内的 `timslite.<target>.node`, 不再读取平台子包的 `package.json` 做 `bindingPackageVersion` 检查。

开发 checkout 中 `wrapper/nodejs/Cargo.toml` 使用 `timslite = { path = "../..", version = "=x.y.z" }`, 以便本地开发复用当前源码并校验版本一致。npm 发布前, release workflow 将该依赖改写为 crates.io 上同版本的 `timslite = { version = "=x.y.z" }`, 生成发布包内的 `Cargo.lock`, 并把 `Cargo.toml`、`Cargo.lock`、`build.rs`、`src/**`、`scripts/**`、README 和预编译 `.node` 一起放入 npm 包。

如果当前平台没有预编译 `.node`, `postinstall` 会尝试使用本机 Rust toolchain 从 crates.io 依赖构建源码 fallback。`TIMSLITE_SKIP_SOURCE_BUILD=1` 可跳过源码构建, `TIMSLITE_BUILD_FROM_SOURCE=1` 可强制源码构建。

## 10. 测试策略

测试分层:

- Native unit tests: conversion helper、error mapping、closed-state guard。
- Node integration tests: 使用临时目录, 运行真实 Store/Dataset/Queue/Journal 流程。
- TypeScript compile tests: 确认 `index.d.ts` 暴露类型与实际 API 一致。
- Event loop tests: `poll()` 等待期间 `setTimeout` 仍能触发, 证明未阻塞主线程。
- Callback tests: 同一 consumer 重复设置非空 callback 报错, `null` 清除后可重新设置。

全仓验证仍需保留:

```bash
cargo test -- --test-threads=1
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

Node wrapper 修改完成后追加:

```bash
npm test --prefix wrapper/nodejs
cargo check --manifest-path wrapper/nodejs/Cargo.toml
cargo test --manifest-path wrapper/nodejs/Cargo.toml
cargo clippy --manifest-path wrapper/nodejs/Cargo.toml --all-targets -- -D warnings
```

## 11. 风险与约束

| 风险 | 影响 | 约束/应对 |
|------|------|-----------|
| JS `number` 精度丢失 | timestamp/sequence 错读 | 返回统一使用 `bigint`; number 输入必须 safe integer |
| 长时间同步 poll 阻塞事件循环 | Node 服务卡顿 | 默认 `poll()` 为 Promise; `pollSync()` 明确标注风险 |
| JS callback 从 Rust 通知线程触发 | 未定义行为或崩溃 | 只使用 threadsafe function 调度回 JS 线程 |
| Store close 后仍有 Dataset 引用 | use-after-close 或语义漂移 | wrapper 跟踪 closed state, 子对象检查状态 |
| C ABI 与 Rust public API 语义分叉 | wrapper 行为不一致 | Node wrapper 不走 C ABI, 直接复用 Rust public API |
| npm 预构建矩阵维护成本 | 发布复杂 | 单 root package 携带预编译 `.node`; 未覆盖平台通过 postinstall 源码 fallback |
| npm 与 crates.io 版本不同步 | 源码 fallback 无法解析同版本 crate | release workflow 校验 package/root/wrapper 版本一致, 并在 npm 发布前等待 crates.io 同版本可解析 |

## 12. 与现有 wrapper 的关系

- Python wrapper 是行为参考, 但 Node API 使用 Node/TypeScript 习惯命名: `readLatest`, `queryAll`, `openDatasetByIdentifier`。
- Node wrapper 仍以 Store-managed public boundary 为准, 不暴露内部 `DataSetInner`、`IndexEntry`、`QuerySource` 或 C ABI handle。
- C ABI header 仍用于 C/C++/Go 等宿主; Node wrapper 不复用其裸指针接口。
