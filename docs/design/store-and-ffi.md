# Store 与 FFI API

## 十一、Store: 存储门面

### 11.1 Store API

> **核心原则**: `create_dataset` 与 `open_dataset` 分离。
> - `create_dataset`: 显式创建新数据集, 需传入 `data_segment_size`, `index_segment_size`, `compress_level`; 已存在返回错误
> - `open_dataset`: 仅打开已有数据集, 参数从 meta 文件读取
> - `drop_dataset`: 删除数据集并清除所有关联文件
> - Store 持有 `BlockCache` (全局共享, 所有 DataSet 查询自动使用缓存)

```rust
/// FFI 数据集句柄 (不透明指针)
pub struct DataSetHandle(pub u64);

pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    config: StoreConfig,
    block_cache: Arc<BlockCache>,
    bg_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(data_dir: P, config: StoreConfig) -> Result<Self>;
    pub fn create_dataset(&self, name: &str, dataset_type: &str,
        data_segment_size: u64, index_segment_size: u64, compress_level: u8,
    ) -> Result<DataSetHandle>;
    pub fn open_dataset(&self, name: &str, dataset_type: &str) -> Result<DataSetHandle>;
    pub fn close_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn drop_dataset(&self, handle: DataSetHandle) -> Result<()>;
    pub fn close(self) -> Result<()>;
}
```

### 11.2 Store 内部行为

| 操作 | 文件操作 | 目录操作 |
|------|---------|---------|
| `Store::open` | 扫描 `{data_dir}/*/*` 加载已有数据集 | 不创建新目录, 仅读取 |
| `Store::create_dataset` | 写入 `meta` 文件; 写入第一个空 data segment + index segment header | 创建 `{name}/{type}/data/` + `{name}/{type}/index/` |
| `Store::open_dataset` | 读取 `meta` 文件校验; 加载已有 segments | 不创建新目录, 仅读取 |
| `Store::drop_dataset` | 删除 `{name}/{type}/` 整个目录树 | `remove_dir_all(base_dir)` |

## 十二、FFI API

```rust
// Store 管理
#[no_mangle] pub extern "C" fn tmsl_store_open(data_dir: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_open_with_config(data_dir: *const c_char, config_ptr: *const StoreConfigFFI, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_store_close(store: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据集管理 — create/open/close/drop 分离
#[no_mangle] pub extern "C" fn tmsl_dataset_create(store: *mut c_void, name: *const c_char, dataset_type: *const c_char, data_segment_size: u64, index_segment_size: u64, compress_level: u8, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_open(store: *mut c_void, name: *const c_char, dataset_type: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_dataset_close(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_drop(store: *mut c_void, name: *const c_char, dataset_type: *const c_char, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_dataset_flush(dataset: *mut c_void, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 数据写入
#[no_mangle] pub extern "C" fn tmsl_dataset_write(dataset: *mut c_void, timestamp: c_longlong, data: *const c_uchar, data_len: usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;

// 查询迭代器
#[no_mangle] pub extern "C" fn tmsl_dataset_query(dataset: *mut c_void, start_ts: c_longlong, end_ts: c_longlong, err_buf: *mut c_char, err_buf_len: usize) -> *mut c_void;
#[no_mangle] pub extern "C" fn tmsl_iter_next(iter: *mut c_void, out_ts: *mut c_longlong, out_data: *mut *mut c_uchar, out_data_len: *mut usize, err_buf: *mut c_char, err_buf_len: usize) -> c_int;
#[no_mangle] pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar);
#[no_mangle] pub extern "C" fn tmsl_iter_close(iter: *mut c_void);
```

> **内存所有权**:
> - `tmsl_iter_next` 返回的 `out_data` 用 `libc::malloc` 分配 → C 侧必须调用 `tmsl_iter_free_data` 释放
> - `tmsl_iter_close` 释放迭代器本身 (Rust `Box::from_raw` + drop)
> - 所有 FFI 函数用 `catch_unwind` 包裹, panic 时返回 -1/null + err_buf 写错误信息

## 十三、C 侧调用示例

```c
char err_buf[512];

// 1. 打开存储
void* store = tmsl_store_open("/data/timslite", err_buf, sizeof(err_buf));

// 2. 创建数据集 (首次使用, 需指定分段大小和压缩等级)
void* ds = tmsl_dataset_create(store, "patient_001", "waveform",
    64ULL * 1024 * 1024,   // data_segment_size = 64MB
    4ULL * 1024 * 1024,    // index_segment_size = 4MB
    6,                     // compress_level
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
    tmsl_iter_free_data(buf);
}
tmsl_iter_close(iter);

// 5. 关闭
tmsl_dataset_close(ds, err_buf, sizeof(err_buf));
tmsl_store_close(store, err_buf, sizeof(err_buf));
```

---

**相关**: [架构概览](architecture.md) | [数据集操作](dataset-operations.md) | [内存与并发](memory-and-concurrency.md)
