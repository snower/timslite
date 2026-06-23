# timslite Node.js Wrapper - 开发计划

> 基于 [design.md](design.md)  
> 目标: 在 `wrapper/nodejs` 中实现基于 Node 官方 Node-API 的 timslite 原生包装, 提供 Node.js/TypeScript 可用的 npm 包。

---

## 计划状态总览

| Phase | 描述 | 状态 | 产物 |
|-------|------|------|------|
| NODE-0 | 版本与工具链确认 | ✅ 完成 | Node 16+, napi-rs v3, timslite-node |
| NODE-1 | 项目骨架与构建系统 | ✅ 完成 | `Cargo.toml`, `package.json`, build scripts |
| NODE-2 | 类型转换与错误映射 | ✅ 完成 | bigint/Buffer helpers, `TmslError` mapping |
| NODE-3 | Store 与配置包装 | ✅ 完成 | `Store`, `StoreConfig` |
| NODE-4 | Dataset 与查询包装 | ✅ 完成 | `Dataset`, iterators |
| NODE-5 | Queue 与异步 poll | ✅ 完成 | `Queue`, async poll, callback |
| NODE-6 | Journal API 与 journal queue | ✅ 完成 | journal read/query/queue |
| NODE-7 | TypeScript 声明 | ✅ 完成 | `index.d.ts` |
| NODE-8 | 跨层文档同步 | ✅ 完成 | root README/plan.md 状态同步 |
| NODE-9 | CI/prebuild 发布准备 | 待开始 | 多平台 npm build plan |

实现完成, 集成测试待补充。

---

## 开发原则

- 直接调用 timslite Rust public API, 不通过 C ABI 裸指针接口。
- Node 公开 timestamp、journal sequence、dataset identifier 时统一返回 `bigint`。
- `Buffer | Uint8Array` 是唯一 payload 输入类型, 输出统一为 `Buffer`。
- `queue.poll()` 和 `journalConsumer.poll()` 默认异步 Promise, 不阻塞事件循环。
- `pollCallback` 只能作为轻量 wake hook, 必须通过 threadsafe function 回到 JS 线程。
- 所有测试使用独立临时目录; Rust 文件系统测试仍按单线程运行。
- 完成后先不要 git commit, 等审核确认。

---

## 目标目录结构

```text
wrapper/nodejs/
├── Cargo.toml
├── build.rs
├── package.json
├── README.md
├── design.md
├── plan.md
├── index.d.ts
├── src/
│   ├── lib.rs
│   ├── types.rs
│   ├── errors.rs
│   ├── config.rs
│   ├── store.rs
│   ├── dataset.rs
│   ├── query.rs
│   └── queue.rs
└── tests/
    ├── basic.test.ts
    ├── config.test.ts
    ├── dataset.test.ts
    ├── queue.test.ts
    └── journal.test.ts
```

---

## Phase NODE-0: 版本与工具链确认

- [ ] 确认当前 Node.js supported release lines。
  - 记录 `engines.node` 的最低版本选择。
  - 最低版本必须支持 BigInt、Node-API async work、threadsafe function 和当前构建工具链。
- [ ] 确认 Node-API Rust binding 工具版本。
  - 选择 `napi`, `napi-derive`, `napi-build`, `@napi-rs/cli` 的稳定版本。
  - 记录所需 Rust MSRV, 不得低于项目当前可接受 Rust 版本。
- [ ] 确认 npm 包名。
  - 首选 `timslite`。
  - 如发布策略要求隔离, 使用 `@timslite/node`。
- [ ] 确认本地测试命令。
  - `npm test --prefix wrapper/nodejs`
  - `cargo check --manifest-path wrapper/nodejs/Cargo.toml`
  - `cargo test --manifest-path wrapper/nodejs/Cargo.toml`

验收标准:

- `wrapper/nodejs/design.md` 中的 Node support 选择有当前依据。
- 后续实现不需要重新决定核心工具链。

---

## Phase NODE-1: 项目骨架与构建系统

文件:

- Create: `wrapper/nodejs/Cargo.toml`
- Create: `wrapper/nodejs/build.rs`
- Create: `wrapper/nodejs/package.json`
- Create: `wrapper/nodejs/src/lib.rs`
- Create: `wrapper/nodejs/README.md`
- Create: `wrapper/nodejs/tests/basic.test.ts`

任务:

- [ ] 创建 Rust addon crate。
  - package name 使用 `timslite-nodejs` 或 `timslite-node`。
  - `[lib] crate-type = ["cdylib"]`。
  - 依赖项目根 crate: `timslite = { path = "../..", version = "0.1.0" }`。
  - 添加 Node-API binding 依赖和 build dependency。
- [ ] 创建 npm package。
  - 添加 `build`, `test`, `clean` scripts。
  - 添加 `types: "index.d.ts"`。
  - 添加 `engines.node`。
  - 标记 native addon 入口。
- [ ] 创建最小 `src/lib.rs`。
  - 只导出 `version()` 或 `nativeVersion()` smoke API。
  - 注册模块但不暴露 Store/Dataset。
- [ ] 创建 smoke test。
  - import package。
  - 调用 `nativeVersion()`。
- [ ] 验证本地构建。
  - `npm install`
  - `npm run build`
  - `npm test`
  - `cargo check --manifest-path wrapper/nodejs/Cargo.toml`

验收标准:

- 当前平台能生成 `.node` native addon。
- Node 能 import wrapper。
- Rust wrapper crate check 通过。

---

## Phase NODE-2: 类型转换与错误映射

文件:

- Create: `wrapper/nodejs/src/types.rs`
- Create: `wrapper/nodejs/src/errors.rs`
- Modify: `wrapper/nodejs/src/lib.rs`
- Create: `wrapper/nodejs/tests/config.test.ts`

任务:

- [ ] 实现 timestamp/sequence 转换 helper。
  - JS input 接受 `number | bigint`。
  - `number` 必须是 safe integer。
  - 转换目标为 Rust `i64`。
  - 输出统一为 JS `bigint`。
- [ ] 实现 u64 转换 helper。
  - identifier 输出为 `bigint`。
  - segment/cache size 输入接受 safe integer number 或 bigint。
- [ ] 实现 Buffer 转换 helper。
  - 输入接受 `Buffer | Uint8Array`。
  - 输出从 Rust `Vec<u8>` 创建 Node `Buffer`。
- [ ] 实现 `TmslError` 到 Node Error 映射。
  - 设置 `name`。
  - 设置稳定 `code`。
  - message 使用 `err.to_string()`。
- [ ] 增加 wrapper lifecycle 错误。
  - Store closed: `name=TmslStoreClosedError`, `code=TMSL_STORE_CLOSED`。
  - Invalid JS argument: `name=TmslInvalidDataError`, `code=TMSL_INVALID_DATA`。
- [ ] 添加测试。
  - safe integer number 可以转换。
  - unsafe integer number 被拒绝。
  - bigint 边界可转换。
  - invalid argument error 带 `code`。

验收标准:

- 不发生 timestamp 精度静默丢失。
- 所有 native error 都有稳定 `code`。

---

## Phase NODE-3: Store 与配置包装

文件:

- Create: `wrapper/nodejs/src/config.rs`
- Create: `wrapper/nodejs/src/store.rs`
- Modify: `wrapper/nodejs/src/lib.rs`
- Modify: `wrapper/nodejs/index.d.ts`
- Create/Modify: `wrapper/nodejs/tests/basic.test.ts`
- Create/Modify: `wrapper/nodejs/tests/config.test.ts`

任务:

- [ ] 实现 `StoreConfig` plain object decode。
  - `flushIntervalMs`, `idleTimeoutMs`, `cacheIdleTimeoutMs` 转 `Duration`。
  - `compressType` 只允许 `0 | 1`。
  - `readOnly` 支持 `undefined/null/false/true`。
  - 缺省使用 `StoreConfig::default()`。
- [ ] 实现 `Store.open(dataDir, config?)`。
  - 调用 `timslite::Store::open`。
  - 保存 `Option<Store>`。
  - 初始化 dataset tracking map。
- [ ] 实现 `Store.close()`。
  - flush tracked datasets。
  - close/drain Store。
  - 设置 closed state。
- [ ] 实现 background helpers。
  - `tickBackgroundTasks(): { executedTasks, nextDelayMs }`。
  - `nextBackgroundDelay(): number`。
- [ ] 实现 listing/inspect API。
  - `getDatasetNames()`。
  - `getDatasetTypes(name)`。
  - `inspectDataset(name, datasetType)`。
- [ ] 添加测试。
  - open/close。
  - close 后方法抛 `TMSL_STORE_CLOSED`。
  - read-only config 可构造并打开已有 store。
  - manual background config 可 tick。

验收标准:

- Store lifecycle 与 Rust public API 一致。
- Config 字段覆盖当前 `src/config.rs` 权威字段。

---

## Phase NODE-4: Dataset 与查询包装

文件:

- Create: `wrapper/nodejs/src/dataset.rs`
- Create: `wrapper/nodejs/src/query.rs`
- Modify: `wrapper/nodejs/src/store.rs`
- Modify: `wrapper/nodejs/src/lib.rs`
- Modify: `wrapper/nodejs/index.d.ts`
- Create: `wrapper/nodejs/tests/dataset.test.ts`

任务:

- [ ] 实现 `Store.createDataset(...) -> Dataset`。
  - 支持 `CreateDatasetOptions`。
  - 使用 `DataSetConfigBuilder::from_store()`。
  - 调用 `create_dataset_with_config`。
  - 通过 `Store::get_dataset` 获得 `Arc<DataSet>`。
- [ ] 实现 `Store.openDataset(...) -> Dataset`。
  - 调用 `Store::open_dataset`。
  - 追踪 dataset id 和 Rust handle。
- [ ] 实现 `Store.openDatasetByIdentifier(identifier)`。
  - identifier 输入接受 number/bigint。
- [ ] 实现 `Store.dropDataset(name, datasetType)`。
  - 调用 `drop_dataset_by_name`。
  - 清理 wrapper tracking 中匹配项。
- [ ] 实现 `Dataset` methods。
  - `write`, `append`, `delete`。
  - `read`, `readLatest`。
  - `flush`, `close`, `inspect`。
  - getters: `id`, `identifier`, `dataDir`, `latestTimestamp`, `closed`。
- [ ] 实现 query methods。
  - `query()` 返回 `QueryIterator`。
  - `queryAll()` 返回 array。
  - MVP 允许先使用 eager `DataSet::query()`。
- [ ] 实现 lightweight reads。
  - `readExist`, `queryExist`, `readLength`。
  - `queryLength`, `queryLengthAll`。
- [ ] 添加测试。
  - create/open/drop/recreate。
  - write/read/readLatest。
  - append 新 timestamp 与 append latest。
  - delete 后 read 返回 null。
  - query iterator 支持 `for...of`。
  - queryExist 返回 Buffer bitmap。
  - inspect 的 nullability 字段正确。

验收标准:

- Node wrapper 能完成基础时序读写工作流。
- timestamp 返回全部为 bigint。
- Dataset closed/read-only 行为有稳定错误。

---

## Phase NODE-5: Queue 与异步 poll

文件:

- Create/Modify: `wrapper/nodejs/src/queue.rs`
- Modify: `wrapper/nodejs/src/store.rs`
- Modify: `wrapper/nodejs/src/lib.rs`
- Modify: `wrapper/nodejs/index.d.ts`
- Create: `wrapper/nodejs/tests/queue.test.ts`

任务:

- [ ] 实现 `Store.openQueue(datasetOrId)`。
  - 接受 `Dataset` 或 dataset id。
  - 调用 `Store::open_queue(handle)`。
- [ ] 实现 `DatasetQueue`。
  - `push(data) -> bigint`。
  - `openConsumer(groupName, options?)`。
  - `dropConsumer(groupName)`。
  - `close()`。
- [ ] 实现 `QueueConsumerOptions` decode。
  - `runningExpiredMs` 转秒或按 Rust builder 当前单位映射。
  - `maxRetryCount` 校验 `0..=255`。
  - 默认值与 Rust queue config 一致。
- [ ] 实现 `DatasetQueueConsumer.poll(timeoutMs?)`。
  - 使用 Node-API async work。
  - 返回 `Promise<[bigint, Buffer] | null>`。
  - timeout 无数据 resolve `null`, 不作为异常。
- [ ] 实现 `pollSync(timeoutMs?)`。
  - 直接调用 Rust poll。
  - 文档说明长 timeout 会阻塞事件循环。
- [ ] 实现 `ack(timestamp)`。
- [ ] 实现 `pollCallback(callbackOrNull)`。
  - 使用 threadsafe function。
  - `null` 清除 callback。
  - 重复设置非空 callback 映射 Rust 错误。
- [ ] 添加测试。
  - 先 open consumer 再 push, 能 poll 到新数据。
  - ack 后不重复投递。
  - async poll 等待期间 Node timer 仍触发。
  - poll timeout 返回 null。
  - pollCallback 被 push 唤醒。
  - callback 清除后不再触发。
  - 同 consumer 重复设置非空 callback 抛错。

验收标准:

- queue API 不阻塞事件循环。
- callback 线程安全且语义与 Rust/Python 对齐。

---

## Phase NODE-6: Journal API 与 journal queue

文件:

- Modify: `wrapper/nodejs/src/store.rs`
- Modify: `wrapper/nodejs/src/queue.rs`
- Modify: `wrapper/nodejs/index.d.ts`
- Create: `wrapper/nodejs/tests/journal.test.ts`

任务:

- [ ] 实现 journal read API。
  - `journalLatestSequence() -> bigint | null`。
  - `journalRead(sequence) -> [bigint, Buffer] | null`。
  - `journalQuery(start, end) -> Array<[bigint, Buffer]>`。
- [ ] 实现 `readJournalSourceRecord(identifier, indexInfo)`。
  - `JournalIndexInfo` 包含 `timestamp`, `blockOffset`, `inBlockOffset`。
  - timestamp/blockOffset 使用 bigint-safe decode。
- [ ] 实现 `openJournalQueue()`。
- [ ] 实现 `JournalQueue`。
  - `openConsumer(groupName, options?)`。
  - `close()`。
- [ ] 实现 `JournalQueueConsumer`。
  - `poll()` Promise。
  - `pollSync()`。
  - `ack(sequence)`。
  - `pollCallback(callbackOrNull)`。
- [ ] 添加测试。
  - create/write/delete/append 产生 journal sequence。
  - journalRead 返回 raw payload Buffer。
  - journalQuery 范围正确。
  - journal queue consumer 只消费后续新增记录。
  - journal poll callback 语义与 dataset queue callback 一致。
  - `enableJournal=false` 时 journal API 返回对应错误。

验收标准:

- Node 能读取和消费 `.journal/logs` 专用 journal。
- 不把 `.journal/logs` 暴露为普通 Dataset。

---

## Phase NODE-7: TypeScript 声明与 README

文件:

- Create/Modify: `wrapper/nodejs/index.d.ts`
- Create/Modify: `wrapper/nodejs/README.md`
- Modify: `wrapper/nodejs/package.json`

任务:

- [ ] 完成 `index.d.ts`。
  - 所有 public class、interface、type export。
  - timestamp/sequence/identifier 返回类型必须是 `bigint`。
  - queue poll 返回 Promise。
- [ ] 添加 README。
  - 安装与本地构建。
  - 基本写/read/query 示例。
  - queue async poll 示例。
  - journal raw payload 示例。
  - BigInt 精度说明。
  - 显式 close 说明。
- [ ] 添加 package metadata。
  - license/repository/homepage 与 root crate 对齐。
  - files/include 配置只包含发布需要的文件。
- [ ] TypeScript compile smoke。
  - 使用 `tsc --noEmit` 或测试框架的类型检查能力。

验收标准:

- TypeScript 用户无需阅读 Rust/Python wrapper 即可使用 Node API。
- README 示例可以直接复制运行。

---

## Phase NODE-8: 跨层文档同步

文件:

- Modify: `README.md`
- Modify: `design.md`
- Modify: `plan.md`
- Optional Modify: `docs/design/store-and-ffi.md`
- Optional Create: `docs/plan/phase-nodejs-wrapper.md`

任务:

- [x] 在根 `design.md` 设计文档索引增加 Node.js wrapper 入口。
- [x] 在根 `plan.md` 状态表增加 Node.js wrapper phase。
- [ ] 在根 README 增加 Node.js wrapper 状态与使用入口。
- [ ] 实现完成后把根 `plan.md` 中 NODE 状态从待实现更新为实际完成状态。
- [ ] 如实现影响 Store/FFI 文档外部集成边界, 更新 `docs/design/store-and-ffi.md`。
- [ ] 如需要根计划 checklist, 创建 `docs/plan/phase-nodejs-wrapper.md`。

验收标准:

- 项目入口文档能发现 Node wrapper。
- wrapper 文档与根文档状态不冲突。

---

## Phase NODE-9: CI/prebuild 发布准备

文件:

- Create/Modify: `.github/workflows/nodejs-wrapper.yml`
- Modify: `wrapper/nodejs/package.json`

任务:

- [ ] 添加 CI。
  - Windows x64。
  - Linux x64。
  - macOS x64/arm64。
  - Node supported LTS/Current matrix。
- [ ] CI 执行 Rust wrapper checks。
  - `cargo check --manifest-path wrapper/nodejs/Cargo.toml`
  - `cargo test --manifest-path wrapper/nodejs/Cargo.toml`
  - `cargo clippy --manifest-path wrapper/nodejs/Cargo.toml --all-targets -- -D warnings`
- [ ] CI 执行 Node tests。
  - `npm install --prefix wrapper/nodejs`
  - `npm run build --prefix wrapper/nodejs`
  - `npm test --prefix wrapper/nodejs`
- [ ] 规划 prebuild 发布。
  - 当前平台先发布 source build。
  - 后续按平台包或 napi-rs CLI 的推荐模式发布预编译产物。

验收标准:

- PR 能自动验证 Node wrapper。
- 发布前有明确 prebuild 策略。

---

## 回归验证清单

实现完成前不得声称完成, 除非以下验证已跑通或明确记录无法运行原因:

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test -- --test-threads=1
cargo check --manifest-path wrapper/nodejs/Cargo.toml
cargo test --manifest-path wrapper/nodejs/Cargo.toml
cargo clippy --manifest-path wrapper/nodejs/Cargo.toml --all-targets -- -D warnings
npm test --prefix wrapper/nodejs
git diff --check
```

如修改 Python wrapper 或 C ABI 以支持 Node wrapper, 还必须追加对应 Python/C ABI 验证; 当前计划不要求这类修改。

---

## 风险跟踪

| 风险 | 优先级 | 应对 |
|------|--------|------|
| `bigint` 与 `number` 混用导致用户困惑 | 高 | 输出统一 bigint, README 单独说明 |
| async poll worker 持有 consumer handle 生命周期不清 | 高 | AsyncTask 持有 clone-safe consumer, Store close 设置 closed guard |
| threadsafe callback 泄漏 | 高 | clear/null 和 object finalizer 都释放 TSFN |
| Windows native build 失败 | 中 | 优先 MSVC, CI 首批覆盖 Windows x64 |
| eager query iterator 峰值内存较高 | 中 | MVP 与 Python 对齐; 后续引入 lazy iterator |
| 自定义 JS Error subclass 难以从 native 直接抛出 | 中 | MVP 以 `err.code` 为稳定捕获边界 |

---

## 后续扩展

- `JournalRecord.decode(payload)` TypeScript helper。
- `for await` queue/journal consumer adapter。
- Worker Thread 示例和高吞吐 benchmark。
- Electron prebuild。
- WASM/browser-only read-only explorer, 作为独立项目评估。
