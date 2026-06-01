# Store 与 FFI API

## 十一、Store: 存储门面

### 11.1 Store API

> **核心原则**: `create_dataset` 与 `open_dataset` 分离。
> - `create_dataset`: 显式创建新数据集, 需传入 `data_segment_size`, `index_segment_size`, `compress_level`; 已存在返回错误
> - `open_dataset`: 仅打开已有数据集, 参数从 meta 文件读取
> - `drop_dataset`: 删除数据集并清除所有关联文件
> - Store 持有 `BlockCache` (全局共享; 读取 compressed block 时自动使用缓存, correction/delete/out-of-order 写入负责失效旧 key)

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
    // 内部共享的执行器 (详见 §17.9), 由 BackgroundTasks 持有
    bg_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>;
    pub fn create_dataset(&self, name: &str, dataset_type: &str,
        data_segment_size: u64, index_segment_size: u64, compress_level: u8,
        retention_ms: u64,
    ) -> Result<DataSetHandle>;
    pub fn open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>;
    pub fn close_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn close(self) -> Result<()>;

    // 后台任务手动执行与查询 (详见 §17.10)
    pub fn tick_background_tasks(&self) -> Result<TickResult>;
    pub fn next_background_delay(&self) -> Duration;
}
```

### 11.2 Store 内部行为

| 操作 | 文件操作 | 目录操作 |
|------|---------|---------|
| `Store::open` | 扫描 `{data_dir}/*/*` 加载已有数据集 | 不创建新目录, 仅读取 |
| `Store::create_dataset` | 写入 `meta` 文件; 写入第一个空 data segment + index segment header | 创建 `{name}/{type}/data/` + `{name}/{type}/index/` |
| `Store::open_dataset` | 读取 `meta` 文件校验; 加载已有 segments | 不创建新目录, 仅读取 |
| `Store::drop_dataset` | 删除 `{name}/{type}/` 整个目录树 | `remove_dir_all(base_dir)` |

### 11.3 Dataset name/type 校验

`name` 和 `dataset_type` 直接作为目录名使用, 不做转义。两者必须非空且整体匹配 `^[0-9A-Za-z_-]+$`。

允许字符:
- `0-9`
- `a-z`
- `A-Z`
- `-`
- `_`

不允许 `.`, `..`, `/`, `\`, 空格、控制字符、非 ASCII 字符或任何其它字符。所有 Store/FFI 创建、打开和按名称删除入口在拼接路径前执行同一校验; 校验失败返回 `InvalidData`。

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
1. 读取每个 dataset 的 `retention_ms` 和 `latest_written_timestamp` (写入过的最大 timestamp, 不要求该 timestamp 当前仍可读)
2. 若 `retention_ms > 0`, 调用 `DataSet::reclaim_expired_segments()`

详见 [后台任务 §17.8](background-and-cache.md#十七后台任务)。

### 11.5 StoreConfig: enable_background_thread

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

### 11.5 Store: 后台任务手动执行与查询

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
    pub version: u32,                    // 必须为 TMSL_STORE_CONFIG_FFI_VERSION
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
}

#[repr(C)]
pub struct TmslDatasetConfigFFI {
    pub version: u32,                    // 必须为 TMSL_DATASET_CONFIG_FFI_VERSION
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub retention_ms: u64,
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
    compress_level: u8, index_continuous: u8, retention_ms: u64,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_create_with_config(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    config_ptr: *const TmslDatasetConfigFFI,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_open(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_close(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_drop(store: *mut c_void,
    name: *const c_char, dataset_type: *const c_char,
    err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_flush(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集状态 — 写入过的最大时间戳 (0 = 空数据集; delete latest 不回退)
#[no_mangle] pub extern "C" fn tmsl_dataset_latest_timestamp(dataset: *mut c_void, out_ts: *mut c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据写入 (correction/out-of-order 会通过 Store 的 BlockCache invalidate 旧索引 key)
#[no_mangle] pub extern "C" fn tmsl_dataset_write(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

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

`block_max_size` 不在 Store/Dataset/FFI 配置中暴露。普通聚合 Block 的 payload 上限固定为 `BLOCK_MAX_SIZE=65536`, 是文件格式常量。

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
    30ULL * 86400 * 1000,  // retention_ms = 30 days (ms timestamps)
    err_buf, sizeof(err_buf));

// 2b. 打开已有数据集 (参数从 meta 读取, 不可设置)
// void* ds = tmsl_dataset_open(store, "patient_001", "waveform", err_buf, sizeof(err_buf));

// 3. 写入
unsigned char d[] = {1,2,3,4};
tmsl_dataset_write(ds, 1700000000, d, 4, err_buf, sizeof(err_buf));

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
