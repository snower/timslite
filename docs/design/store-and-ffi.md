# Store 与 C ABI Wrapper

## 十一、Store: 标准 Rust library 边界

### 11.1 Rust public API boundary

`timslite` 主 crate 是标准 Rust library。它不再导出 C ABI 符号, 也不再在主项目中维护 `DataSetHandle` 这类面向 FFI 的公开句柄类型。

`Store` 负责 Store 级生命周期和运行时上下文:

- `Store::open(data_dir, config) -> Result<Store>`
- `Store::create_dataset(...) -> Result<DataSet>`
- `Store::create_dataset_with_config(..., Option<DataSetConfigBuilder>) -> Result<DataSet>`
- `Store::open_dataset(name, dataset_type) -> Result<DataSet>`
- `Store::open_dataset_by_identifier(identifier) -> Result<DataSet>`
- `Store::drop_dataset(name, dataset_type) -> Result<()>`
- `Store::get_dataset_names() -> Result<Vec<String>>`
- `Store::get_dataset_types(name) -> Result<Vec<String>>`
- `Store::inspect_dataset(name, dataset_type) -> Result<DataSetInspectResult>`
- `Store::tick_background_tasks() -> Result<TickResult>`
- `Store::next_background_delay() -> Result<Duration>`
- `Store::journal_latest_sequence/read/query/open_journal_queue(...)`
- `Store::close(self) -> Result<()>`

普通 record 操作不再通过 Store facade 转发。调用方拿到 `DataSet` 后直接使用:

- `DataSet::write(timestamp, data)`
- `DataSet::write_now(data)`
- `DataSet::append(timestamp, data)`
- `DataSet::append_now(data)`
- `DataSet::delete(timestamp)`
- `DataSet::read(timestamp)`
- `DataSet::read_latest()`
- `DataSet::read_exist(timestamp)`
- `DataSet::query(start, end)`
- `DataSet::query_exist(start, end)`
- `DataSet::read_length(timestamp)`
- `DataSet::query_length(start, end)`
- `DataSet::query_length_iter(start, end)`
- `DataSet::flush()`
- `DataSet::close()`
- `DataSet::inspect()`
- `DataSet::identifier()`
- `DataSet::latest_written_timestamp()`
- `DataSet::open_queue()`
- `DataSet::close_queue()`

普通 queue 操作也不再通过 Store facade 转发。调用方使用 `DataSet::open_queue()` 得到 `DatasetQueue`, 再使用 `DatasetQueue::push/open_consumer/drop_consumer/close` 和 `DatasetQueueConsumer::poll/ack/poll_callback`。

> 核心原则: Store 管生命周期和全局上下文; DataSet 管数据操作; Queue 管消费语义。公开 Rust API 不暴露 FFI handle registry。

### 11.1.1 Store 管理的 DataSet

`Store::create_dataset*` 和 `Store::open_dataset*` 返回的 `DataSet` 是 Store 管理的操作视图。它内部持有共享 runtime context, 包括:

- 全局 `BlockCache`
- Journal sink
- dirty flush queue
- lifecycle invalidation hook
- Store read-only 状态

因此调用方直接调用 `DataSet::write/write_now/read/query/open_queue` 不会绕过 cache、journal、read-only、background flush 或 queue 通知语义。`write_now(data)` 和 `append_now(data)` 在 `DataSet` 内部 mutex 获取之后采样 Unix 秒级时间戳, 再复用普通 write/append 路径, 避免锁外取时间后因线程调度造成 timestamp 逆序。

低层 `DataSet::create/open/drop_dataset`、raw lock、`IndexEntry`、`QueryIterator` 内部状态、`QueueInner`、`ConsumerStateFile` 等仍是 crate-internal 实现细节。

### 11.1.2 Config

`StoreConfig` 和 `DataSetConfig` 的字段保持 crate-private。公开构造方式是 builder 和只读 getter:

```rust
let store_config = StoreConfig::builder()
    .enable_background_thread(true)
    .enable_journal(true)
    .build();

let dataset_config = DataSetConfigBuilder::from_store(&store_config)
    .index_continuous(0)
    .retention_window(0);

let mut store = Store::open("./data", store_config)?;
let dataset = store.create_dataset_with_config("sensor", "temperature", Some(dataset_config))?;
dataset.write(1, b"21.5")?;
```

`create_dataset_with_config` 接收 `Option<DataSetConfigBuilder>` 而不是已构造的 config, 这样 Store 可以统一套用 Store 默认值并执行 dataset config 校验。

### 11.2 Store 内部行为

| 操作 | 行为 |
|------|------|
| `Store::open` | 初始化 Store 根目录、`max_identifier`、BlockCache、JournalManager、BackgroundTasks; 不预扫描普通 dataset |
| `Store::create_dataset*` | 校验 public name/type, 分配 Store 级 identifier, 写入 meta/data/index/identifier, 注入 runtime context, 成功后按配置写 journal create record |
| `Store::open_dataset` | 读取并校验 dataset `identifier` 和 `meta`, 加载已有 segments, 注入 runtime context, 缓存到 registry |
| `Store::open_dataset_by_identifier` | 扫描合法 public dataset 目录查找 identifier, 找到后复用 `open_dataset` 语义 |
| `Store::drop_dataset` | 按 name/type 加载并关闭 dataset/queue, 删除 `{name}/{type}` 目录, 成功后按配置写 journal drop record |
| `DataSet::write/write_now/append/append_now/delete` | 执行 record 操作、cache invalidation、queue notify 和 journal hook |
| `DataSet::read/read_latest/query` | 使用 Store 注入的 cache 和 read-only context 执行读取 |
| `DataSet::open_queue` | 打开普通 dataset queue; read-only dataset 返回错误 |
| `Store::journal_*` | 通过 `JournalManager` 专用路径读取内置 journal, 不通过普通 dataset API |

### 11.2.1 Store read-only and lock contract

`Store::open` uses a sibling `.lock` file next to `max_identifier` only as the OS file-lock target. The contract must never treat `.lock` file existence as a lock signal; only the OS lock result matters. A stale unlocked `.lock` file must still allow writable open.

`StoreConfig::read_only() -> Option<bool>` controls the open mode:

- `None` (default): auto mode. `Store::open` creates the Store root if needed, tries to acquire the `.lock` OS lock, opens writable when it succeeds, and falls back to read-only when another instance holds the lock.
- `Some(false)`: explicit writable mode. `Store::open` creates the Store root if needed and returns an error when the `.lock` OS lock is already held.
- `Some(true)`: forced read-only mode. `Store::open` does not create, check, or lock `.lock`; it opens a read-only Store view directly.

The acquired lock handle is stored inside `Store` as `Option<File>` and is released by normal Store drop/close semantics. Read-only Stores store no lock handle.

Read-only Store mode rejects every mutating Store or Store-managed `DataSet` operation, including create/drop/write/write_now/append/append_now/delete, retention reclaim, queue open/close/push/poll/ack, and `open_journal_queue`. `read`, `read_latest`, `query`, inspect/list operations, and journal latest/read/query remain read-capable. Background tasks are not started in read-only mode, and manual background task APIs are unavailable for that Store.

The read-only view is an open-time persisted view, not a live reader. It only needs to read bytes already flushed to the Store files before the read-only Store opened.

## 十二、C ABI Wrapper

### 12.1 独立 crate

C ABI 已迁移到独立项目:

- crate: `wrapper/cffi`
- package name: `timslitecffi`
- header: `wrapper/cffi/include/timslite.h`
- Rust implementation: `wrapper/cffi/src/lib.rs`

`timslitecffi` 依赖主 `timslite` crate 的公开 Rust API。它不能访问 `timslite` 的 crate-private 模块, 也不再要求主 crate 以 `cdylib` 形式构建。

主 crate 的 `Cargo.toml` 只构建标准 Rust library; `libc` 只属于 `timslitecffi` 的内存分配/释放边界。

### 12.2 C wrapper ownership model

C wrapper 继续向 C 侧暴露 opaque store/dataset 指针和 queue/consumer 数值句柄:

- `void* store`: owns a `Store`
- `void* dataset`: owns a Store-managed `DataSet`
- `size_t queue_handle`: registry handle for `DatasetQueue` or `JournalQueue`
- `size_t consumer_handle`: registry handle for `DatasetQueueConsumer` or `JournalQueueConsumer`

这些都是 `timslitecffi` 内部资源, 不映射到主 crate 的 `DataSetHandle`。dataset record API 直接调用 `DataSet` public methods; queue API 直接调用 `DatasetQueue` / `JournalQueue` public methods。

### 12.3 C ABI API groups

主要函数组:

- Store lifecycle/config: `tmsl_store_config_default`, `tmsl_store_open`, `tmsl_store_open_with_config`, `tmsl_store_close`
- Store listing/background: `tmsl_store_get_dataset_names`, `tmsl_store_get_dataset_types`, `tmsl_store_tick_background_tasks`, `tmsl_store_next_background_delay`
- Dataset lifecycle: `tmsl_dataset_create`, `tmsl_dataset_create_with_config`, `tmsl_dataset_open`, `tmsl_dataset_open_by_identifier`, `tmsl_dataset_close`, `tmsl_dataset_drop`
- Dataset operations: `tmsl_dataset_write`, `tmsl_dataset_append`, `tmsl_dataset_delete`, `tmsl_dataset_read`, `tmsl_dataset_read_latest`, `tmsl_dataset_query`
- Lightweight reads: `tmsl_dataset_read_exist`, `tmsl_dataset_query_exist`, `tmsl_dataset_read_length`, `tmsl_dataset_query_length`, `tmsl_dataset_query_length_iter`
- Queue: `tmsl_queue_open`, `tmsl_queue_close`, `tmsl_queue_consumer_open`, `tmsl_queue_consumer_open_with_config`, `tmsl_queue_consumer_drop`, `tmsl_queue_push`, `tmsl_queue_poll`, `tmsl_queue_ack`, `tmsl_queue_consumer_poll_callback`
- Journal: `tmsl_journal_latest_sequence`, `tmsl_journal_read`, `tmsl_journal_query`, `tmsl_journal_queue_open`, `tmsl_journal_queue_poll`, `tmsl_journal_queue_ack`
- Inspect/free helpers: `tmsl_store_inspect_dataset`, `tmsl_free_inspect_result`, `tmsl_data_free`, `tmsl_free_string_array`

Buffers returned by read/query/queue/journal APIs are allocated by `timslitecffi` with `malloc` and must be released with `tmsl_data_free`. String arrays are released with `tmsl_free_string_array`.

### 12.4 Example

```c
char err[512] = {0};
void* store = tmsl_store_open("./data", err, sizeof(err));
void* dataset = tmsl_dataset_create(
    store, "sensor", "temperature",
    64 * 1024 * 1024, 4 * 1024 * 1024,
    6, 0, 0,
    err, sizeof(err)
);

tmsl_dataset_write(dataset, 1700000001, (const unsigned char*)"21.5", 4, err, sizeof(err));

int64_t ts = 0;
unsigned char* data = NULL;
size_t data_len = 0;
if (tmsl_dataset_read(dataset, 1700000001, &ts, &data, &data_len, err, sizeof(err)) == 0) {
    /* use data */
    tmsl_data_free(data);
}

tmsl_dataset_close(dataset, err, sizeof(err));
tmsl_store_close(store, err, sizeof(err));
```

## 十三、Wrapper 同步原则

Python、Node.js、Java 和 C ABI wrapper 都应依赖 `timslite` 的公开 Rust API:

- Store wrapper 可保留语言层自己的 object id, 但不得依赖主 crate 的 FFI handle registry。
- Dataset wrapper 应直接持有 `DataSet` 或 `Arc<DataSet>`。
- Queue wrapper 应通过 `DataSet::open_queue()` 打开普通 queue。
- Journal queue 仍通过 `Store::open_journal_queue()` 打开。
- 文档和测试应以 `DataSet` 直接读写为 public Rust contract。
