# Store 与 FFI API

## 十一、Store: 存储门面

### 11.1 Store API

> **核心原则**: `create_dataset` 与 `open_dataset` 分离。
> - `create_dataset`: 显式创建新数据集, 需传入 `data_segment_size`, `index_segment_size`, `compress_level`; 已存在返回错误
> - `open_dataset`: 仅打开已有数据集, 参数从 meta 文件读取
> - `drop_dataset`: 删除数据集并清除所有关联文件
> - Store 持有 `BlockCache` 和 JournalManager, 并把二者注入每个 Store 管理的业务 DataSet; DataSet public API 自行使用全局缓存和 journal hook

```rust
/// FFI 数据集句柄 (不透明指针)
pub struct DataSetHandle(pub u64);

/// 手动 tick 后台任务的返回结果
pub struct TickResult {
    /// 本次 tick 中实际被执行的任务数量 (0..=4)
    pub executed_tasks: usize,
    /// 距离下一次任一任务到期的剩余时间 (saturating, 不低于 0)
    pub next_delay: Duration,
}

pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,
    block_cache: Arc<BlockCache>,
    journal: Arc<JournalManager>,   // 内置 .journal/logs 变更日志, 可配置启用/禁用
    // 内部共享的执行器 (详见 §17.9), 由 BackgroundTasks 持有
    bg_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>;
    pub fn create_dataset(&self, name: &str, dataset_type: &str,
        data_segment_size: u64, index_segment_size: u64, compress_level: u8,
        retention_window: u64,
    ) -> Result<DataSetHandle>;
    pub fn create_dataset_with_config(&self, name: &str, dataset_type: &str,
        config_builder: Option<DataSetConfigBuilder>,
    ) -> Result<DataSetHandle>;
    pub fn open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>;
    pub fn write_dataset(&self, handle: DataSetHandle, timestamp: i64, data: &[u8]) -> Result<()>;
    pub fn append_dataset(&self, handle: DataSetHandle, timestamp: i64, data: &[u8]) -> Result<()>;
    pub fn delete_dataset_record(&self, handle: DataSetHandle, timestamp: i64) -> Result<()>;
    pub fn read_dataset(&self, handle: DataSetHandle, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn query_dataset(&self, handle: DataSetHandle, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub fn latest_written_timestamp(&self, handle: DataSetHandle) -> Result<i64>;
    pub fn open_queue(&self, handle: DataSetHandle) -> Result<DatasetQueue>;
    pub fn open_journal_queue(&self) -> Result<DatasetQueue>;
    pub fn open_consumer(&self, queue: &DatasetQueue, group_name: &str) -> Result<DatasetQueueConsumer>;
    pub fn drop_consumer(&self, queue: &DatasetQueue, group_name: &str) -> Result<()>;
    pub fn queue_push(&self, queue: &DatasetQueue, data: &[u8]) -> Result<i64>;
    pub fn queue_poll(&self, consumer: &DatasetQueueConsumer, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn queue_ack(&self, consumer: &DatasetQueueConsumer, timestamp: i64) -> Result<()>;
    pub fn close_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset_by_name(&self, name: &str, dataset_type: &str) -> Result<()>;
    pub fn block_cache(&self) -> &Arc<BlockCache>;
    pub fn config(&self) -> &StoreConfig;
    pub fn close(self) -> Result<()>;

    // 后台任务手动执行与查询 (详见 §17.10)
    pub fn tick_background_tasks(&self) -> Result<TickResult>;
    pub fn next_background_delay(&self) -> Duration;
}
```

### 11.2 Store 内部行为

| 操作 | 文件操作 | 目录操作 |
|------|---------|---------|
| `Store::open` | 初始化 `BlockCache`/JournalManager/runtime context; 若 `enable_journal=true`, 先单独 open/create 内置 `.journal/logs`; 再扫描 `{data_dir}/*/*` 加载已有普通数据集并注入 runtime context | `.journal/logs` 是内部保留 dataset; 普通扫描跳过它 |
| `Store::create_dataset` | 写入 `meta` 文件; 写入第一个空 data segment + index segment header; 注入 runtime context; journal 开启时成功后写 `0x01` | 创建 `{name}/{type}/data/` + `{name}/{type}/index/` |
| `Store::open_dataset` | 读取 `meta` 文件校验; 加载已有 segments; 注入 runtime context | 不创建新目录, 仅读取 |
| `Store::write_dataset` | 调用 DataSet public API; DataSet 自行应用 retention/cache/queue/journal hook; 成功后 journal 写 `0x11` | 普通正序写会通知 queue; correction/out-of-order 不通知 |
| `Store::append_dataset` | 调用 DataSet public API; DataSet 自行应用 retention/cache/queue/journal hook; 成功后 journal 写 `0x13` | 创建新 timestamp 时通知普通 queue; 修改已有 latest 不重新投递 |
| `Store::delete_dataset_record` | 调用 DataSet public API; DataSet 自行 invalidate 旧 cache key 并写 `0x12` journal | 不删除物理 record, 仅标记 filler/invalid |
| `Store::read_dataset` / `query_dataset` | 调用 DataSet public API; DataSet 自动使用 runtime context 中的全局 `BlockCache`; retention 统一生效 | `.journal/logs` read-only handle 也允许读取 |
| `Store::latest_written_timestamp` | 返回 dataset 已写入最大 timestamp | 删除 latest 后仍返回最大已写 timestamp |
| `Store::open_queue` / `open_journal_queue` | 打开普通 dataset queue 或内置 journal queue | journal queue producer 只允许 `JournalManager` |
| `Store::drop_dataset` | 删除 `{name}/{type}/` 整个目录树; journal 开启时成功后写 `0x02` | `remove_dir_all(base_dir)` |

### 11.3 Dataset name/type 校验

`name` 和 `dataset_type` 直接作为目录名使用, 不做转义。两者必须非空、最长 255 字节且整体匹配 `^[0-9A-Za-z_-]+$`。

允许字符:
- `0-9`
- `a-z`
- `A-Z`
- `-`
- `_`

不允许 `.`, `..`, `/`, `\`, 空格、控制字符、非 ASCII 字符或任何其它字符。所有 Store/FFI 创建、打开和按名称删除入口在拼接路径前执行同一校验; 校验失败返回 `InvalidData`。该 255 字节上限也用于 journal name/type TLV value, 避免主操作成功后才发现 journal 字段不可编码。

内部保留 dataset `.journal/logs` 不适用公共命名规则。`enable_journal=true` 时, public `open_dataset(".journal", "logs")` 允许返回只读 handle, 支持 `read/query/query_iter/latest_timestamp/open_queue`; public create/write/append/delete/drop/queue_push 仍必须拒绝 `.journal`。`enable_journal=false` 时, 所有 `.journal/logs` public open/read/query/open_queue 请求返回 `NotFound`。

### 11.4 StoreConfig: retention_check_hour

```rust
pub struct StoreConfig {
    // ... existing fields ...
    /// 每日保留回收执行时间点 (UTC hour, 0=UTC 00:00, 默认 0)
    pub retention_check_hour: u8,
}

impl StoreConfigBuilder {
    /// 设置每日保留回收执行时间 (0-23 UTC hour, 默认 0=UTC 00:00)
    pub fn retention_check_hour(mut self, hour: u8) -> Self {
        self.retention_check_hour = Some(hour.clamp(0, 23));
        self
    }
}
```

**调度逻辑**: 后台线程根据 `retention_check_hour` 计算下一次执行时间 (距 UTC 00:00 的小时偏移), 每日触发一次。触发时:
1. 读取每个 dataset 的 `retention_window` 和 `latest_written_timestamp` (写入过的最大 timestamp, 不要求该 timestamp 当前仍可读)
2. 若 `retention_window > 0`, 调用 `DataSet::reclaim_expired_segments()`

详见 [后台任务 §17.8](background-and-cache.md#十七后台任务)。

### 11.5 StoreConfig: enable_journal

```rust
pub struct StoreConfig {
    // ... existing fields ...
    /// 是否启用内置 journal (默认 true)
    pub enable_journal: bool,
}

impl StoreConfigBuilder {
    /// 设置是否启用内置 journal。
    ///
    /// - `true` (默认): `Store::open` 自动 open/create `.journal/logs`, 普通变更成功后同步追加 journal record
    /// - `false`: 不创建、不打开、不追加 journal; `.journal/logs` 的 open/read/query/open_queue 均不可用
    pub fn enable_journal(mut self, enable: bool) -> Self {
        self.enable_journal = Some(enable);
        self
    }
}
```

**默认值与兼容性**:
- `tmsl_store_open(data_dir)` 使用默认配置, journal 默认开启。
- 如果宿主希望完全避免 journal 写放大或不希望自动创建 `.journal/`, 必须使用 `tmsl_store_open_with_config` 并设置 `enable_journal=false`。
- 禁用 journal 不影响普通 dataset 的 create/write/delete/drop 成功语义, 对应 journal hook 为 no-op。

### 11.6 StoreConfig: enable_background_thread

```rust
pub struct StoreConfig {
    // ... existing fields ...
    /// 是否启用内置后台线程 (默认 true)
    pub enable_background_thread: bool,
}

impl StoreConfigBuilder {
    /// 设置是否启用内置后台线程。
    ///
    /// - `true` (默认): `Store::open` 自动启动单个后台线程, 按配置间隔执行 flush /
    ///   idle-close / cache-eviction / retention-reclaim 任务
    /// - `false`: 不启动后台线程, 由调用方通过 `Store::tick_background_tasks()` 主动驱动
    pub fn enable_background_thread(mut self, enable: bool) -> Self {
        self.enable_background_thread = Some(enable);
        self
    }
}
```

**使用场景 (enable_background_thread = false)**:
- 集成到外部事件循环 (如 C 端的 `select` / `epoll` / libuv loop)
- 严格控制线程数的宿主程序 (如嵌入式或单线程运行环境)
- 单元/集成测试中需要精确控制任务执行时机

**与 `tmsl_store_open` 的兼容性**:
- `tmsl_store_open(data_dir)` 使用全部默认配置 (含 `enable_background_thread=true`)
- 需要禁用线程时, 调用方应使用 `tmsl_store_open_with_config(data_dir, config_ptr, ...)`
- `tmsl_store_open_with_config` 使用版本化 `TmslStoreConfigFFI`; `config_ptr == NULL` 时等价于默认配置

### 11.7 Store: 后台任务手动执行与查询

```rust
impl Store {
    /// 同步执行一次后台任务到期检查。
    ///
    /// 根据配置间隔判断 flush / idle / cache / retention 各任务是否到期,
    /// 到期则立即执行。返回本次执行的任务数量 + 距离下一次任一任务到期的剩余时间。
    ///
    /// # 并发
    /// - `enable_background_thread=true` 下也可调用, 与后台线程通过 `Mutex` 串行执行
    /// - `enable_background_thread=false` 时, 必须由外部主动调用以驱动后台逻辑
    pub fn tick_background_tasks(&self) -> Result<TickResult>;

    /// 返回距离下一次后台任务执行应等待的时间。
    ///
    /// 仅计算, 不执行任何任务。后台线程或外部 tick 执行期间短暂阻塞 (等待状态锁释放)。
    pub fn next_background_delay(&self) -> Duration;
}

/// tick 返回结果
pub struct TickResult {
    pub executed_tasks: usize,   // 本次执行的任务数量 (0..=4)
    pub next_delay: Duration,    // 距离下一次任一任务到期的剩余时间
}
```

**调用示例 (Rust)**:
```rust
let config = StoreConfig::builder()
    .enable_background_thread(false)
    .build();
let store = Store::open("/data/timslite", config)?;

// 外部事件循环: 每次唤醒后调用 tick, 根据返回的 next_delay 调度下一次唤醒
loop {
    let result = store.tick_background_tasks()?;
    if result.executed_tasks > 0 {
        log::info!("[ext] executed {} tasks, next in {:?}",
                   result.executed_tasks, result.next_delay);
    }
    // 外部 select / epoll / sleep 使用 next_delay...
    std::thread::sleep(result.next_delay);
}
```

详见 [后台任务 §17.10](background-and-cache.md#1710-外部手动执行-external-manual-execution)。

## 十二、FFI API

```rust
#[repr(C)]
pub struct TmslStoreConfigFFI {
    pub version: u32,                    // 必须为支持 enable_journal 的 TMSL_STORE_CONFIG_FFI_VERSION
    pub flush_interval_ms: u64,
    pub idle_timeout_ms: u64,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub cache_max_memory: u64,
    pub cache_idle_timeout_ms: u64,
    pub compress_level: u8,
    pub retention_check_hour: u8,
    pub enable_background_thread: u8,    // 0=false, non-zero=true
    pub enable_journal: u8,              // 0=false, non-zero=true
}

// enable_journal 追加后必须提升 TMSL_STORE_CONFIG_FFI_VERSION。
// 若实现选择兼容旧版本 config, 旧版本缺失的 enable_journal 按默认 true 处理。

#[repr(C)]
pub struct TmslDatasetConfigFFI {
    pub version: u32,                    // 必须为 TMSL_DATASET_CONFIG_FFI_VERSION
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub retention_window: u64,
    pub compress_level: u8,
    pub index_continuous: u8,            // 0=false, non-zero=true
}

// Store 管理
#[no_mangle] pub extern "C" fn tmsl_store_config_default(out_config: *mut TmslStoreConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_store_open(data_dir: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_open_with_config(data_dir: *const c_char, config_ptr: *const TmslStoreConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_close(store: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// Store 后台任务控制 (手动模式 / 与后台线程共存; 详见 §11.5)
/// 手动执行一次后台任务 (flush / idle-close / cache-eviction / retention-reclaim)。
/// out_executed 写入本次实际执行的任务数量 (0..=4); out_next_delay_ms 写入下一次任务到期的 ms 数。
/// 返回 0=成功, -1=失败; 即使没有任何任务到期 (executed=0) 也返回 0。
/// 与后台线程通过 state Mutex 串行; enable_background_thread=false 时由 C 端驱动调用。
#[no_mangle] pub extern "C" fn tmsl_store_tick_background_tasks(store: *mut c_void,
    out_executed: *mut u32, out_next_delay_ms: *mut u64,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

/// 查询下一次后台任务应执行的剩余等待时间 (毫秒)。
/// 不会执行任何任务; 仅读取状态快照并计算 min(next_*) - now。
/// out_next_delay_ms 写入毫秒数; 返回 0=成功, -1=失败。
#[no_mangle] pub extern "C" fn tmsl_store_next_background_delay(store: *mut c_void,
    out_next_delay_ms: *mut u64,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集管理 — create/open/close/drop 分离
#[no_mangle] pub extern "C" fn tmsl_dataset_create(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    data_segment_size: u64, index_segment_size: u64,
    compress_level: u8, index_continuous: u8, retention_window: u64,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_create_with_config(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    config_ptr: *const TmslDatasetConfigFFI,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_open(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
// `name=".journal", dataset_type="logs"` 在 enable_journal=true 时打开只读 journal handle;
// 该 handle 可 read/query/open_queue, 但 write/append/delete/drop/queue_push 必须返回错误。
#[no_mangle] pub extern "C" fn tmsl_dataset_close(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_drop(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_flush(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集状态 — 写入过的最大时间戳 (0 = 空数据集; delete latest 不回退)
#[no_mangle] pub extern "C" fn tmsl_dataset_latest_timestamp(dataset: *mut c_void, out_ts: *mut c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据写入 (correction/out-of-order 会通过 Store 的 BlockCache invalidate 旧索引 key)
#[no_mangle] pub extern "C" fn tmsl_dataset_write(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据追加 (timestamp > latest 创建新 record; timestamp == latest 仅允许追加到未压缩末尾 record)
#[no_mangle] pub extern "C" fn tmsl_dataset_append(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据删除 (索引标记为哨兵, invalidate 旧缓存 key, 数据段 invalid_record_count++)
#[no_mangle] pub extern "C" fn tmsl_dataset_delete(dataset: *mut c_void, timestamp: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 单时间戳读取 (timestamp=-1 解析为最大已写 timestamp, 不反向搜索; malloc'd out_data, 0=成功/1=未找到/-1=错误)
#[no_mangle] pub extern "C" fn tmsl_dataset_read(dataset: *mut c_void, timestamp: c_longlong,
    out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 查询迭代器
#[no_mangle] pub extern "C" fn tmsl_dataset_query(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_iter_next(iter: *mut c_void, out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_data_free(data: *mut c_void);
#[no_mangle] pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar);
#[no_mangle] pub extern "C" fn tmsl_iter_close(iter: *mut c_void);
```

`block_max_size` 不在 Store/Dataset/FFI 配置中暴露。普通聚合 Block 的 payload 上限固定为 `BLOCK_MAX_SIZE=65536`, 是文件格式常量。`write` 与 `append` 的单条 record 纯数据上限固定为 4MiB, 也不作为运行期配置暴露。

> **内存所有权**:
> - `tmsl_iter_next` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_dataset_read` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_iter_free_data` 保留为兼容别名, 内部等价于 `tmsl_data_free`
> - `tmsl_iter_close` 释放迭代器本身 (Rust `Box::from_raw` + drop)
> - 所有 FFI 函数用 `catch_unwind` 包裹, panic 时返回 -1/null + err_buf 写错误信息

> **句柄生命周期**:
> - `store` 是父句柄; `dataset` 和 `iterator` 是子句柄。`tmsl_store_close` 在存在任一未关闭子句柄时返回 -1, 不释放 store。
> - `iterator` 必须先于创建它的 `dataset` 关闭; `tmsl_dataset_close` 在该 dataset 仍有活动 iterator 时返回 -1。
> - `tmsl_dataset_drop` 通过 FFI 调用时要求该 store 下没有活动 dataset/iterator 子句柄, 避免删除仍被 C 侧持有的对象。
> - `tmsl_dataset_close` / `tmsl_iter_close` 成功后对应指针立即失效, 之后不得再次传入任何 FFI 函数。

## 十三、C 侧调用示例

```c
char err_buf[512];

// 1. 打开存储
void* store = tmsl_store_open("/data/timslite", err_buf, sizeof(err_buf));

// 2. 创建数据集 (首次使用, 需指定分段大小、压缩等级、数据有效期)
void* ds = tmsl_dataset_create(store, "patient_001", "waveform",
    64ULL * 1024 * 1024,   // data_segment_size = 64MB
    4ULL * 1024 * 1024,    // index_segment_size = 4MB
    6,                     // compress_level
    0,                     // index_continuous (non-continuous)
    30ULL * 86400,         // retention_window = 30 days in timestamp units (seconds in this example)
    err_buf, sizeof(err_buf));

// 2b. 打开已有数据集 (参数从 meta 读取, 不可设置)
// void* ds = tmsl_dataset_open(store, "patient_001", "waveform", err_buf, sizeof(err_buf));

// 3. 写入
unsigned char d[] = {1,2,3,4};
tmsl_dataset_write(ds, 1700000000, d, 4, err_buf, sizeof(err_buf));

// 3b. 追加到最新记录或创建更新 timestamp 的新记录
unsigned char more[] = {5,6};
tmsl_dataset_append(ds, 1700000000, more, 2, err_buf, sizeof(err_buf));

// 4. 查询
void* iter = tmsl_dataset_query(ds, 1700000000, 1700000060, err_buf, sizeof(err_buf));
long ts; unsigned char* buf; size_t len;
while (tmsl_iter_next(iter, &ts, &buf, &len, err_buf, sizeof(err_buf)) == 0) {
    // 处理 buf[0..len]
    tmsl_data_free(buf);
}
tmsl_iter_close(iter);

// 5. 关闭
tmsl_dataset_close(ds, err_buf, sizeof(err_buf));
tmsl_store_close(store, err_buf, sizeof(err_buf));
```

---

**相关**: [架构概览](architecture.md) | [数据集操作](dataset-operations.md) | [内存与并发](memory-and-concurrency.md)
