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
    journal: Arc<JournalManager>,   // 内置 .journal/logs 专用 append log, 可配置启用/禁用
    // 内部共享的执行器 (详见 §17.9), 由 BackgroundTasks 持有
    bg_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>;

    // Handle registry / mutating facade operations require &mut self.
    pub fn create_dataset(&mut self, name: &str, dataset_type: &str,
        data_segment_size: u64, index_segment_size: u64, compress_level: u8,
        index_continuous: u8, retention_window: u64,
    ) -> Result<DataSetHandle>;
    pub fn create_dataset_with_config(&mut self, name: &str, dataset_type: &str,
        config_builder: Option<DataSetConfigBuilder>,
    ) -> Result<DataSetHandle>;
    pub fn open_dataset(&mut self, name: &str, dataset_type: &str) -> Result<DataSetHandle>;
    pub fn open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSetHandle>;
    pub fn write_dataset(&mut self, handle: DataSetHandle, timestamp: i64, data: &[u8]) -> Result<()>;
    pub fn append_dataset(&mut self, handle: DataSetHandle, timestamp: i64, data: &[u8]) -> Result<()>;
    pub fn delete_dataset_record(&mut self, handle: DataSetHandle, timestamp: i64) -> Result<()>;
    pub fn close_dataset(&mut self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset(&mut self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset_by_name(&mut self, name: &str, dataset_type: &str) -> Result<()>;
    pub fn open_queue(&mut self, handle: DataSetHandle) -> Result<DatasetQueue>;
    pub fn close_queue(&mut self, handle: DataSetHandle) -> Result<()>;
    pub fn journal_latest_sequence(&self) -> Result<Option<i64>>;
    pub fn journal_read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn journal_query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub fn open_journal_queue(&mut self) -> Result<JournalQueue>;
    pub fn open_consumer(&mut self, queue: &DatasetQueue, group_name: &str) -> Result<DatasetQueueConsumer>;
    pub fn drop_consumer(&mut self, queue: &DatasetQueue, group_name: &str) -> Result<()>;
    pub fn queue_push(&mut self, queue: &DatasetQueue, data: &[u8]) -> Result<i64>;

    // Read/query and executor-inspection operations use &self.
    pub fn read_dataset(&self, handle: DataSetHandle, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn read_dataset_latest(&self, handle: DataSetHandle) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn query_dataset(&self, handle: DataSetHandle, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>>;
    pub fn dataset_read_exist(&self, handle: DataSetHandle, timestamp: i64) -> Result<bool>;
    pub fn dataset_query_exist(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<u8>>;
    pub fn dataset_read_length(&self, handle: DataSetHandle, timestamp: i64) -> Result<Option<u32>>;
    pub fn dataset_query_length(&self, handle: DataSetHandle, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, u32)>>;
    pub fn latest_written_timestamp(&self, handle: DataSetHandle) -> Result<Option<i64>>;
    pub fn dataset_identifier(&self, handle: DataSetHandle) -> Result<u64>;
    pub fn queue_poll(&self, consumer: &DatasetQueueConsumer, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>>;
    pub fn queue_ack(&self, consumer: &DatasetQueueConsumer, timestamp: i64) -> Result<()>;
    pub fn block_cache(&self) -> &Arc<BlockCache>;
    pub fn config(&self) -> &StoreConfig;
    pub fn get_dataset_names(&self) -> Result<Vec<String>>;
    pub fn get_dataset_types(&self, name: &str) -> Result<Vec<String>>;
    pub fn inspect_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetInspectResult>;
    pub fn tick_background_tasks(&self) -> Result<TickResult>;
    pub fn next_background_delay(&self) -> Result<Duration>;

    pub fn close(self) -> Result<()>;
}
```

### 11.2 Store 内部行为

| 操作 | 文件操作 | 目录操作 |
|------|---------|---------|
| `Store::open` | 初始化 `BlockCache`/JournalManager/runtime context; 读取/校验 Store 根目录 `max_identifier`; 若 `StoreConfig.enable_journal=true`, 先单独 open/create 内置 `.journal/logs` 专用 append log; 再扫描 `{data_dir}/*/*` 加载已有普通数据集、读取每个 dataset 的 `identifier` 和 `DataSetMeta.enable_journal` 并注入有效 runtime context | `.journal/logs` 是内部保留 journal; 普通扫描跳过它; 若扫描最大 identifier 大于 `max_identifier`, 修正根目录文件 |
| `Store::create_dataset` | 分配 `next_identifier = max_identifier + 1`; 写入 `meta` 文件; 写入第一个空 data segment + index segment header; 写入 dataset 目录 `identifier`; 更新 Store 根目录 `max_identifier`; 按有效 journal 开关注入 runtime context; 有效 journal 开启时成功后写 `0x01` | 创建 `{name}/{type}/identifier` + `{name}/{type}/meta` + `{name}/{type}/data/` + `{name}/{type}/index/` |
| `Store::open_dataset` | 读取 `meta` 文件校验; 加载已有 segments; 按有效 journal 开关注入 runtime context | 不创建新目录, 仅读取 |
| `Store::open_dataset_by_identifier` | 通过 Store 内存中的 `identifier -> DataSetKey` 索引定位 `(name,type)`, 再复用 `open_dataset` 语义 | `identifier=0` 返回 `InvalidData`; 未找到返回 `NotFound`; `.journal/logs` 不支持 |
| `Store::write_dataset` | 调用 DataSet public API; DataSet 自行应用 retention/cache/queue/journal hook; 有效 journal 开启时成功后写 `0x11` | 普通正序写会通知 queue; correction/out-of-order 不通知 |
| `Store::append_dataset` | 调用 DataSet public API; DataSet 自行应用 retention/cache/queue/journal hook; 有效 journal 开启时成功后写 `0x13` | 创建新 timestamp 时通知普通 queue; 修改已有 latest 不重新投递 |
| `Store::delete_dataset_record` | 调用 DataSet public API; DataSet 自行 invalidate 旧 cache key; 有效 journal 开启时成功后写 `0x12` | 不删除物理 record, 仅标记 filler/invalid |
| `Store::read_dataset` / `read_dataset_latest` / `query_dataset` | 调用 DataSet public API; DataSet 自动使用 runtime context 中的全局 `BlockCache`; retention 统一生效; `read_dataset(-1)` 是精确读取, latest 必须走 `read_dataset_latest` | 仅适用于普通 dataset |
| `Store::journal_latest_sequence/read/query` | 调用 JournalManager 专用 API 读取 encoded journal record payload | `enable_journal=false` 时返回 `NotFound`; 不通过 DataSet handle |
| `Store::latest_written_timestamp` | 返回 dataset 已写入最大 timestamp | 空 dataset 返回 `None`; 删除 latest 后仍返回最大已写 timestamp |
| `Store::open_queue` / `open_journal_queue` | `open_queue` 打开普通 dataset queue; `open_journal_queue` 打开专用 JournalQueue | journal queue producer 只允许 `JournalManager` |
| `Store::drop_dataset` | 删除 `{name}/{type}/` 整个目录树; 有效 journal 开启时成功后写 `0x02` | `remove_dir_all(base_dir)` |

### 11.3 Dataset name/type 校验

`name` 和 `dataset_type` 直接作为目录名使用, 不做转义。两者必须非空、最长 255 字节且整体匹配 `^[0-9A-Za-z_-]+$`。

允许字符:
- `0-9`
- `a-z`
- `A-Z`
- `-`
- `_`

不允许 `.`, `..`, `/`, `\`, 空格、控制字符、非 ASCII 字符或任何其它字符。所有 Store/FFI 创建、打开和按名称删除入口在拼接路径前执行同一校验; 校验失败返回 `InvalidData`。该 255 字节上限也用于 journal create/drop 记录中的 name/type 字段, 避免主操作成功后才发现 journal 字段不可编码。

内部保留路径 `.journal/logs` 不适用公共命名规则。它不再作为普通 DataSet handle 暴露; `open_dataset(".journal", "logs")` 必须返回 `NotFound` 或 `InvalidData`。`enable_journal=true` 时, 调用方通过 `journal_read` / `journal_query` / `open_journal_queue` 等专用 API 读取和消费 journal; public create/write/append/delete/drop/queue_push 仍必须拒绝 `.journal`。`enable_journal=false` 时, 所有 journal 专用 API 返回 `NotFound`。

### 11.3.1 Dataset Identifier

每个普通 dataset 创建时分配一个 Store 内唯一 `u64 identifier`, 持久化在 `{data_dir}/{name}/{type}/identifier`, Store 根目录用 `{data_dir}/max_identifier` 保存已经分配过的最大值。

identifier 规则:

- `0` 为无效值, 第一个普通 dataset 从 `1` 开始。
- `identifier` 与 `max_identifier` 均使用十进制数字字符串保存。
- Store open 时读取所有普通 dataset 的 `identifier` 并构建 `identifier -> DataSetKey` 索引。
- 重复 identifier、非法数字、溢出均视为目录损坏并返回 `InvalidData`。
- `.journal/logs` 不参与 public identifier 分配, 也不能通过 identifier 打开。

详细磁盘格式、crash 边界和测试要求见 [Dataset Identifier](dataset-identifier.md)。

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
    /// - `true` (默认): `Store::open` 自动 open/create `.journal/logs` 专用 append log, 普通变更成功后同步追加 journal record
    /// - `false`: 不创建、不打开、不追加 journal; journal read/query/open_queue 专用 API 均不可用
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

### 11.5.1 DataSetConfig: enable_journal

```rust
pub struct DataSetConfig {
    // ... existing fields ...
    /// 是否记录本 dataset 的 journal (默认 true, 创建后不可变)
    pub enable_journal: bool,
}

impl DataSetConfigBuilder {
    /// 设置本 dataset 是否写入 journal。
    ///
    /// - `true` (默认): 当 StoreConfig.enable_journal=true 时, 本 dataset 的 create/drop/write/delete/append 写 journal
    /// - `false`: 即使 StoreConfig.enable_journal=true, 本 dataset 的所有 journal record 也跳过
    pub fn enable_journal(mut self, enable: bool) -> Self {
        self.enable_journal = Some(enable);
        self
    }
}
```

有效 journal 开关为 `StoreConfig.enable_journal && DataSetConfig.enable_journal`。`DataSetConfig.enable_journal` 写入 dataset meta, reopen 后以 meta 为准; 当前 Store 默认值不会覆盖已有 dataset。

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
    pub fn next_background_delay(&self) -> Result<Duration>;
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

C ABI 句柄内部同步:

- `FfiStore` 内部持有 `Arc<Mutex<Store>>`, 不直接暴露或保存可被多个入口恢复成 `&mut Store` 的 raw `Store*`。
- `FfiDataset`、`FfiIterator`、FFI queue handle、FFI consumer handle 均作为子句柄持有同一个 store `Arc<Mutex<Store>>` 或由 queue/consumer registry 间接关联。
- 所有需要访问 Store handle registry、read-only handle set 或 Store mutating API 的 FFI 入口必须先锁定该 Store mutex。
- `tmsl_store_close` 在存在任一 dataset、iterator、queue 或 consumer 子句柄时返回错误, 不释放 store。

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
    pub compress_type: u8,               // 0=zstd, 1=deflate
    pub index_continuous: u8,            // 0=false, non-zero=true
    pub enable_journal: u8,              // 0=false, non-zero=true; default true
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
#[no_mangle] pub extern "C" fn tmsl_dataset_open_by_identifier(store: *mut c_void,
    identifier: u64,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
// `.journal/logs` 不再通过 tmsl_dataset_open 暴露; 使用 journal 专用 C ABI。
#[no_mangle] pub extern "C" fn tmsl_dataset_close(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_drop(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_flush(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集状态 — 写入过的最大时间戳 (0=有值, 1=空数据集, -1=错误; delete latest 不回退)
#[no_mangle] pub extern "C" fn tmsl_dataset_latest_timestamp(dataset: *mut c_void, out_ts: *mut c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_identifier(dataset: *mut c_void, out_identifier: *mut u64, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据写入 (correction/out-of-order 会通过 Store 的 BlockCache invalidate 旧索引 key)
#[no_mangle] pub extern "C" fn tmsl_dataset_write(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据追加 (timestamp > latest 创建新 record; timestamp == latest 仅允许追加到未压缩末尾 record)
#[no_mangle] pub extern "C" fn tmsl_dataset_append(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据删除 (索引标记为哨兵, invalidate 旧缓存 key, 数据段 invalid_record_count++)
#[no_mangle] pub extern "C" fn tmsl_dataset_delete(dataset: *mut c_void, timestamp: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 单时间戳读取 (timestamp 为精确业务时间戳; -1 不再是 latest sentinel; malloc'd out_data, 0=成功/1=未找到/-1=错误)
#[no_mangle] pub extern "C" fn tmsl_dataset_read(dataset: *mut c_void, timestamp: c_longlong,
    out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// latest 读取 (使用 latest_written_timestamp: Option<i64>; 0=成功/1=未找到或空/-1=错误)
#[no_mangle] pub extern "C" fn tmsl_dataset_read_latest(dataset: *mut c_void,
    out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 查询迭代器
#[no_mangle] pub extern "C" fn tmsl_dataset_query(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_iter_next(iter: *mut c_void, out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_data_free(data: *mut c_void);
#[no_mangle] pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar);
#[no_mangle] pub extern "C" fn tmsl_iter_close(iter: *mut c_void);

// 轻量级读操作 (详见 dataset-read-operations.md §5 FFI 接口)
/// 检查 timestamp 当前是否存在可见数据。过期 timestamp 与 filler/deleted entry 返回 false。
/// 返回 0=false/1=true; 错误时返回 -1。
#[no_mangle] pub extern "C" fn tmsl_dataset_read_exist(dataset: *mut c_void, timestamp: c_longlong,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

/// 范围数据存在性快速检查，返回位图。位 i 代表 (start_ts + i) 当前是否存在可见数据。
/// 返回的 bitmap 由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
/// 过期 timestamp 和 filler/deleted entry 均返回 0；bitmap 最多 4MiB。
/// bitmap_len 写入字节数；出错时返回 NULL。
#[no_mangle] pub extern "C" fn tmsl_dataset_query_exist(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong,
    out_bitmap: *mut *mut c_uchar, out_bitmap_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

/// 读取单条记录的数据长度。timestamp 为精确业务时间戳。
/// 返回 0=成功(out_len 有效)/1=未找到/-1=错误。
#[no_mangle] pub extern "C" fn tmsl_dataset_read_length(dataset: *mut c_void, timestamp: c_longlong,
    out_len: *mut u32,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

#[repr(C)]
pub struct TmslLengthEntry {
    pub timestamp: i64,
    pub data_len: u32,
}

/// 范围查询数据长度数组。返回的数组由 libc::malloc 分配，调用方需通过 tmsl_data_free 释放。
/// out_array_len 写入 TmslLengthEntry 元素数量，而不是字节数；出错时返回 NULL。
/// TmslLengthEntry 使用 C struct 普通布局，非 packed；sizeof=16，alignment=8。
#[no_mangle] pub extern "C" fn tmsl_dataset_query_length(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong,
    out_array: *mut *mut TmslLengthEntry, out_array_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

/// 创建数据长度迭代器。返回迭代器句柄，出错时返回 NULL。
#[no_mangle] pub extern "C" fn tmsl_dataset_query_length_iter(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;

/// 迭代器 next。返回 0=成功/1=无更多数据/-1=错误。
#[no_mangle] pub extern "C" fn tmsl_length_iter_next(iter: *mut c_void,
    out_ts: *mut c_longlong, out_len: *mut u32,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// Queue C ABI
#[repr(C)]
pub struct TmslQueueConsumerConfigFFI {
    pub version: u32,
    pub running_expired_seconds: u32,
    pub max_retry_count: u32,
}

#[no_mangle] pub extern "C" fn tmsl_queue_open(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_queue_close(queue_handle: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_queue_consumer_open(queue_handle: usize, group_name: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_queue_consumer_open_with_config(queue_handle: usize, group_name: *const c_char, config: *const TmslQueueConsumerConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_queue_consumer_drop(queue_handle: usize, consumer_handle: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_queue_push(queue_handle: usize, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_longlong;
#[no_mangle] pub extern "C" fn tmsl_queue_poll(consumer_handle: usize, timeout_ms: c_longlong, out_timestamp: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_queue_ack(consumer_handle: usize, timestamp: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// Journal C ABI (dedicated append log, not DataSet)
#[no_mangle] pub extern "C" fn tmsl_journal_latest_sequence(store: *mut c_void,
    out_sequence: *mut c_longlong,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_journal_read(store: *mut c_void, sequence: c_longlong,
    out_sequence: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_journal_query(store: *mut c_void, start_sequence: c_longlong, end_sequence: c_longlong,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_journal_iter_next(iter: *mut c_void,
    out_sequence: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_journal_iter_close(iter: *mut c_void);
#[no_mangle] pub extern "C" fn tmsl_journal_queue_open(store: *mut c_void,
    err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_journal_queue_close(queue_handle: usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_journal_queue_consumer_open(queue_handle: usize, group_name: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_journal_queue_consumer_open_with_config(queue_handle: usize, group_name: *const c_char,
    config: *const TmslQueueConsumerConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> usize;
#[no_mangle] pub extern "C" fn tmsl_journal_queue_poll(consumer_handle: usize, timeout_ms: c_longlong,
    out_sequence: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_journal_queue_ack(consumer_handle: usize, sequence: c_longlong,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
```

`block_max_size` 不在 Store/Dataset/FFI 配置中暴露。普通聚合 Block 的 payload 上限固定为 `BLOCK_MAX_SIZE=65536`, 是文件格式常量。`write` 与 `append` 的单条 record 纯数据上限固定为 4MiB, 也不作为运行期配置暴露。

> **内存所有权**:
> - `tmsl_iter_next` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_dataset_read` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_queue_poll` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_journal_read` / `tmsl_journal_iter_next` / `tmsl_journal_queue_poll` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_dataset_query_exist` 返回的 `out_bitmap` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_dataset_query_length` 返回的 `out_array` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_data_free` 释放
> - `tmsl_iter_free_data` 保留为兼容别名, 内部等价于 `tmsl_data_free`
> - `tmsl_iter_close` 释放迭代器本身 (Rust `Box::from_raw` + drop)
> - 所有 FFI 函数用 `catch_unwind` 包裹, panic 时返回 -1/null + err_buf 写错误信息

> **句柄生命周期**:
> - `store` 是父句柄; `dataset`、`iterator`、普通 queue/consumer、journal iterator、journal queue/consumer 都是子句柄。`tmsl_store_close` 在存在任一未关闭子句柄时返回 -1, 不释放 store。
> - `iterator` 必须先于创建它的 `dataset` 关闭; `tmsl_dataset_close` 在该 dataset 仍有活动 iterator 时返回 -1。
> - 普通 `queue` 必须先于创建它的 `dataset` 关闭; `tmsl_dataset_close` 在该 dataset 仍有活动 queue handle 时返回 -1。
> - journal queue 是 store 级子句柄, 不依赖 dataset handle; 必须在 `tmsl_store_close` 前关闭。
> - `consumer` 必须先于或随所属 `queue` 关闭; `tmsl_queue_close` 会关闭并移除该 queue 下所有 FFI consumer handle。
> - `tmsl_dataset_drop` 通过 FFI 调用时要求该 store 下没有活动 dataset/iterator/queue/consumer 子句柄, 避免删除仍被 C 侧持有的对象。
> - `tmsl_dataset_close` / `tmsl_iter_close` / `tmsl_queue_close` / `tmsl_queue_consumer_drop` / journal close/drop 函数成功后对应句柄立即失效, 之后不得再次传入任何 FFI 函数。

> **Queue FFI 语义**:
> - `tmsl_queue_open(dataset)` 以 FFI dataset 句柄为入口, 内部使用该 dataset 对应的 Store handle id 调用 `Store::open_queue`。C 侧不直接持有或传入 `DataSetHandle` 数值。
> - `tmsl_queue_close(queue_handle)` 对普通 dataset queue 调用 `Store::close_queue` 并移除 registry entry。
> - `tmsl_queue_push` 对普通 queue 自动分配 `latest_written_timestamp.map_or(1, |ts| ts + 1)`。
> - `tmsl_queue_consumer_open` 使用默认 consumer 配置; `tmsl_queue_consumer_open_with_config` 接受 `TmslQueueConsumerConfigFFI { version=1, running_expired_seconds<=65535, max_retry_count<=255 }`。
> - 同一 queue/group 的活动 consumer 必须使用一致配置; 不一致时 open 返回错误。
> - `tmsl_queue_poll` 返回值: `0=成功并写出数据`, `-2=超时无数据`, `-1=错误`。成功返回的数据必须用 `tmsl_data_free` 释放。
> - `tmsl_queue_consumer_drop(queue_handle, consumer_handle)` 删除对应消费组状态并使同一 queue/group 下的 FFI consumer handle 全部失效。

> **Journal FFI 语义**:
> - journal C ABI 以 `store` 为入口, 不需要也不能先打开 `.journal/logs` DataSet handle。
> - `tmsl_journal_latest_sequence` 在空 journal 时写出 `0`, 有记录时写出最新 sequence。
> - `tmsl_journal_read` 返回值: `0=成功并写出数据`, `1=未找到`, `-1=错误`。
> - `tmsl_journal_query` 返回专用 iterator, `tmsl_journal_iter_next` 返回 `0=成功`, `1=结束`, `-1=错误`。
> - `tmsl_journal_queue_*` 使用专用 JournalQueue, consumer group name 复用 queue 路径安全规则, consumer retry 配置与普通 queue 相同。

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

## P1-2 Active Contract: FFI Compression Type

`TmslStoreConfigFFI` and `TmslDatasetConfigFFI` include `compress_type:u8`.

- `compress_type = 0` selects zstd and is the default returned by `tmsl_store_config_default`.
- `compress_type = 1` selects deflate.
- Unknown values are rejected during FFI config decode.
- `compress_level` remains `u8`, defaults to `6`, and is interpreted by the selected algorithm. Current builders clamp values greater than `9` to `9`; on-disk meta values greater than `9` are invalid.
- `tmsl_dataset_create(...)` without an explicit config uses the store default compression type.
- `tmsl_dataset_create_with_config(...)` stores the supplied dataset compression type in dataset meta, and new segment headers copy it into their immutable TLV meta.
- Read paths must use the owning segment header `compress_type` for decompression, not the caller's current config.

## Flush Runtime Context

Store 持有一个全局共享 dirty flush queue。Store 管理的每个 `DataSet` 都持有 `DataSetRuntimeContext`, context 除 cache/journal/read-only 状态外, 还持有该全局队列的 `Arc` 引用:

```rust
enum SegmentFlushTarget {
    Data { file_offset: u64 },
    Index { start_timestamp: i64 },
    QueueState { group_name: String },
    DatasetState,
}

struct DataSetFlushTarget {
    dataset: DataSetKey,
    segment: SegmentFlushTarget,
}

struct DataSetRuntimeContext {
    block_cache: Option<Arc<BlockCache>>,
    journal: Option<Arc<dyn DataSetJournalSink>>,
    flush_queue: Option<Arc<Mutex<VecDeque<DataSetFlushTarget>>>>,
    read_only: bool,
}
```

普通 Store facade 写入、通过 Store 获取的 `DataSet` 直接写入、普通 queue consumer state 变更和 dataset inspect state 缓存变更复用同一个全局 dirty queue。后台 flush 任务 drain 队列后按 dataset key 精确定位普通 dataset, 再执行 `Data`、`Index`、`QueueState`、`DatasetState` target, 不遍历所有 dataset。Journal 使用专用 append log, 不把 journal segment 加入该 dirty queue; 后台 flush 到期时直接调用 `JournalManager::flush_dirty()`。低层 `DataSet::create/open` 如果没有 runtime context, `DataSet::flush()` 退化为同步所有打开 segment、queue state files 和 dataset state file。

> Rust API mutability note: methods that allocate or remove handles, mutate the handle registry, mutate dataset contents, open/close queue producer state, or push queue data require &mut self. Read/query, queue poll/ack, config/cache access, dataset listing, inspect, and background executor tick/query use internal synchronization and keep &self.
