# timslite - Rust 时序数据存储库详细设计

> 参考: monitorcare-orbit TimeStore (Java)  
> 目标: Rust 动态库(`dylib`), 提供 FFI 可调用 C ABI  
> 核心: 按数据集名称+类型分段 + 内存映射 + 时间索引

---

## 一、整体架构

```
libtimslite (CDylib)
│
├── Store           (门面 - data_dir 级别)
│   │
│   └── DataSet     (数据集 - (name, type) 级别)
│       │
│       ├───DataSegment       (单个数据文件, Mmap-backed)
│       ├───DataSegmentSet    (同类型数据文件集合)
│       │
│       └───TimeIndex         (当前数据集的专属时间索引)
│           │
│           └───IndexSegment  (单个索引文件, Mmap-backed)
└── FFI           (extern "C" API)
```

---

## 二、目录结构

```
{data_dir}/
├── {dataset_name_1}/
│   ├── {dataset_type_A}/
│   │   ├── .index/
│   │   │   ├── 00000000000000000000              # 起始秒级时间戳 (20位,0填充)
│   │   │   └── 0000000000001700000000
│   │   ├── 00000000000000000000                  # data segment, 起始offset (20位,0填充)
│   │   ├── 00000000000067108864                  # offset = 64MB
│   │   └── 000000000000134217728
│   │
│   └── {dataset_type_B}/
│       ├── .index/
│       │   └── 0000000000001700000000
│       └── 00000000000000000000
│
└── {dataset_name_2}/
    └── {dataset_type_C}/
        ├── .index/
        └── 00000000000000000000
```

### 2.1 命名规则

| 文件类型 | 目录 | 命名格式 | 示例 |
|---------|------|---------|------|
| 数据段(DataSegment) | `{name}/{type}/` | 20位十进制, 起始字节offset, 零填充 | `00000000000000000000` |
| 索引段(IndexSegment) | `{name}/{type}/.index/` | 20位十进制, 起始秒级timestamp, 零填充 | `0000000000001700000000` |

### 2.2 隔离保证

- 每个 `(dataset_name, dataset_type)` 拥有完全独立的 `.index/` 目录
- 索引文件只包含对应 `(name, type)` 的时间戳→偏移量映射
- 不同数据集名称之间文件物理隔离
- 同一名称不同类型之间文件物理隔离

---

## 三、核心数据模型

### 3.1 Record (数据记录)

每条数据记录由两部分组成:

```
┌─────────────────┬──────────────────────────────┐
│ timestamp       │ data                         │
│ i64 (8 bytes)   │ bytes (可变长度)              │
└─────────────────┴──────────────────────────────┘
```

写入到磁盘时的实际布局:
```
[data payload]
┌──────┬──────────────────────────────┬──────┬────────┐
│ size │ timestamp (i64, 8 bytes)     │ size │ data   │
│ 4B   │                              │ 4B   │ 可变   │
└──────┴──────────────────────────────┴──────┴────────┘
```

> `size` 为后续数据的总字节数 (8 + data.len()), 使用 little-endian 编码。

### 3.2 IndexEntry (索引条目)

每个索引条目固定 **16字节**:

```
┌──────────────────────┬──────────────────────────┐
│ timestamp (i64)      │ data_offset (u64)        │
│ 8 bytes              │ 8 bytes                  │
└──────────────────────┴──────────────────────────┘
```

- `timestamp`: 秒级时间戳
- `data_offset`: 对应数据在该数据段中的绝对字节偏移量

### 3.3 FileMetadata

每个数据段和索引段的头部元数据 (固定 64 字节):

```
0-3:    magic = b"TMSL"  (4 bytes)
4-5:    version (u16, little-endian) = 1
6-7:    flags (u16)
         bit 0: compressed (deflate)
         bit 1: sealed (不再写入)
8-15:   file_type (i64)
         positive = data segment
         negative = index segment
         (绝对值标识逻辑类型)
16-23:  file_offset (i64)
         data segment: 起始字节offset
         index segment: 起始秒级timestamp
24-31:  file_size (i64)       # 文件总大小(字节)
32-39:  wrote_position (i64)  # 已写入位置(从数据区起始)
40-47:  created_at (i64)      # 创建时间(unix ms)
48-55:  record_count (i64)    # 记录条数
56-63:  reserved (8 bytes)    # 保留
```

---

## 四、核心类型定义

```rust
/// 存储实例句柄 (线程安全)
pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    // 后台任务
    flush_interval_sec: u64,
    idle_timeout_sec: u64,
    flush_handle: Option<JoinHandle<()>>,
    idle_handle: Option<JoinHandle<()>>,
}

/// 数据集句柄 (非 Send, 通过 Store 内部管理)
struct DataSet {
    id: DataSetKey,
    data_dir: PathBuf,
    config: DataSetConfig,
    // 数据段集合
    segments: DataSegmentSet,
    // 时间索引 (每个数据集独立)
    time_index: TimeIndex,
    // 最后使用时间
    last_used_at: Instant,
}

/// 数据集唯一标识
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct DataSetKey {
    name: String,
    dataset_type: String,
}

/// 数据集配置
struct DataSetConfig {
    /// 数据段单文件最大大小 (默认 64MB)
    data_segment_size: u64,
    /// 索引段单文件最大大小 (默认 4MB)
    index_segment_size: u64,
    /// 是否启用压缩 (默认 false)
    compress: bool,
    /// 压缩级别 (0-9, 默认 6)
    compress_level: u8,
}
```

---

## 五、DataSegmentSet: 数据段集合

### 5.1 职责

- 管理同一数据集下的多个 DataSegment 文件
- 按 offset 路由到正确的数据段
- 自动创建新文件 (当前文件满时)
- 数据读取时跨段迭代

### 5.2 结构

```rust
struct DataSegmentSet {
    base_dir: PathBuf,           // {data_dir}/{name}/{type}/
    segment_size: u64,           // 单文件最大大小
    segments: Vec<DataSegment>,  // 按 file_offset 升序
    compress: bool,
    compress_level: u8,
    next_offset: u64,
    total_records: u64,
}
```

### 5.3 写入流程

```rust
impl DataSegmentSet {
    /// 写入一条记录, 返回 (segment_file_offset, record_data_offset)
    fn append(&mut self, timestamp: i64, data: &[u8]) -> io::Result<(u64, u64)> {
        let segment = self.get_or_create_segment(self.next_offset)?;
        let data_offset = segment.append_data(timestamp, data, self.compress, self.compress_level)?;
        
        if segment.is_full() {
            self.next_offset += self.segment_size;
        }
        self.total_records += 1;
        Ok((segment.file_offset, data_offset))
    }

    /// 获取或创建当前可写的segment
    fn get_or_create_segment(&mut self, offset: u64) -> io::Result<&mut DataSegment> {
        let last = self.segments.iter().rev().find(|s| !s.is_sealed());
        match last {
            Some(s) if !s.is_full() => Ok(s),
            _ => {
                let new_seg = DataSegment::create(&self.base_dir, offset, self.segment_size)?;
                self.segments.push(new_seg);
                self.segments.sort_by_key(|s| s.file_offset);
                Ok(self.segments.last_mut().unwrap())
            }
        }
    }
}
```

---

## 六、DataSegment: 单个数据段

### 6.1 结构

```rust
struct DataSegment {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
    data_start: u64,  // = HEADER_SIZE (64)
    wrote_position: u64,  // 已写入位置(从data_start开始计算的偏移)
    record_count: u64,
    created_at: i64,
    // mmap 管理
    mmap: MmapMut,
    sealed: bool,
}
```

### 6.2 file layout

```
┌─────────────────────────────────────────┐
│ FileHeader (64 bytes)                   │
│ - magic "TMSL", version, flags, ...     │
├─────────────────────────────────────────┤
│ Data Area (variable size)               │
│ ┌──────┬──────────┬──────┬───────────┐  │
│ │ size │ timestamp│ size │ data bytes│  │  record 1
│ │ 4B   │ 8 bytes  │ 4B   │ N bytes   │  │
│ └──────┴──────────┴──────┴───────────┘  │
│ ┌──────┬──────────┬──────┬───────────┐  │
│ │ size │ timestamp│ size │ data bytes│  │  record 2
│ └──────┴──────────┴──────┴───────────┘  │
│ ...                                     │
└─────────────────────────────────────────┘
```

### 6.3 写入 (append_data)

```rust
impl DataSegment {
    fn append_data(
        &mut self,
        timestamp: i64,
        data: &[u8],
        compress: bool,
        compress_level: u8,
    ) -> io::Result<u64> {
        let record_size = 8 + data.len(); // timestamp + raw data
        let payload_size: u32 = record_size as u32;
        
        // 检查空间
        if self.wrote_position + 4 + payload_size as u64 > self.file_size - self.data_start {
            self.seal()?;
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "segment full"));
        }

        let mut pos = self.data_start + self.wrote_position;

        // 写入数据量头
        write_u32_le(&mut self.mmap, pos, payload_size)?;
        pos += 4;

        if compress {
            // deflate 压缩
            let compressed = deflate(data, compress_level);
            // 设置 compressed flag
            self.set_compressed_flag();
            // 写入 timestamp (不压缩)
            write_i64_le(&mut self.mmap, pos, timestamp)?;
            pos += 8;
            // 写入压缩后大小
            let comp_size = compressed.len() as u32;
            write_u32_le(&mut self.mmap, pos, comp_size)?;
            pos += 4;
            // 写入压缩数据
            self.mmap[pos as usize..(pos + comp_size as u64) as usize]
                .copy_from_slice(&compressed);
            self.wrote_position += 4 + payload_size as u64;
            Ok(self.data_start + self.wrote_position - payload_size as u64)
        } else {
            // 不压缩: 直接写入 timestamp + data
            write_i64_le(&mut self.mmap, pos, timestamp)?;
            pos += 8;
            self.mmap[pos as usize..(pos + data.len() as u64) as usize]
                .copy_from_slice(data);
            self.wrote_position += 4 + payload_size as u64;
            let data_offset = self.data_start + self.wrote_position - payload_size as u64;
            Ok(data_offset)
        }
    }
}
```

### 6.4 读取

```rust
impl DataSegment {
    /// 从指定 offset 读取一条记录
    fn read_record(&self, data_offset: u64) -> io::Result<(i64, Vec<u8>)> {
        let mut pos = data_offset;
        let payload_size = read_u32_le(&self.mmap, pos) as u64;
        pos += 4;
        
        let timestamp = read_i64_le(&self.mmap, pos);
        pos += 8;
        
        if self.is_compressed() {
            let comp_size = read_u32_le(&self.mmap, pos) as usize;
            pos += 4;
            let compressed = &self.mmap[pos as usize..(pos + comp_size as u64) as usize];
            let data = inflate(compressed)?;
            Ok((timestamp, data))
        } else {
            let data_len = (payload_size - 8) as usize;
            let data = self.mmap[pos as usize..(pos + data_len as u64) as usize].to_vec();
            Ok((timestamp, data))
        }
    }
}
```

---

## 七、TimeIndex: 时间索引

### 7.1 职责

- 维护 `{data_dir}/{name}/{type}/.index/` 目录下的索引文件
- 索引文件按时间戳分段, 每段固定大小
- 将 `(timestamp → data_segment_offset, data_record_offset)` 映射持久化
- 支持按时间范围快速定位
- 索引与数据文件同生命周期

### 7.2 结构

```rust
struct TimeIndex {
    base_dir: PathBuf,    // {data_dir}/{name}/{type}/.index/
    segment_size: u64,
    index_segments: Vec<IndexSegment>,
    // 内存缓存 (加速最近写入)
    in_memory_buffer: Vec<IndexEntry>,
    in_memory_flush_threshold: usize,  // 默认 1024
}
```

### 7.3 索引写入

```rust
impl TimeIndex {
    /// 添加索引条目
    fn add_entry(&mut self, timestamp: i64, data_offset: u64) -> io::Result<()> {
        // 写入内存缓冲
        self.in_memory_buffer.push(IndexEntry {
            timestamp,
            data_offset,
        });
        
        if self.in_memory_buffer.len() >= self.in_memory_flush_threshold {
            self.flush_to_disk()?;
        }
        Ok(())
    }

    /// 将内存缓冲刷新到磁盘
    fn flush_to_disk(&mut self) -> io::Result<()> {
        if self.in_memory_buffer.is_empty() {
            return Ok(());
        }

        // 按时间顺序排序
        self.in_memory_buffer.sort_by_key(|e| e.timestamp);

        for entry in self.in_memory_buffer.drain(..) {
            let segment = self.get_or_create_segment(entry.timestamp)?;
            segment.append_entry(&entry)?;
        }
        Ok(())
    }

    /// 根据时间戳获取或创建索引段
    fn get_or_create_segment(&mut self, timestamp: i64) -> io::Result<&mut IndexSegment> {
        // timestamp -> 段起始时间戳 = timestamp / entries_per_segment * entries_per_segment
        let entries_per_segment = (self.segment_size - HEADER_SIZE) / 16;
        let segment_start_ts = (timestamp / entries_per_segment as i64) * entries_per_segment as i64;

        // 查找已存在的段
        if let Some(seg) = self.index_segments.iter_mut().find(|s| s.start_timestamp == segment_start_ts) {
            return Ok(seg);
        }

        // 创建新段
        let new_seg = IndexSegment::create(&self.base_dir, segment_start_ts, self.segment_size)?;
        self.index_segments.push(new_seg);
        self.index_segments.sort_by_key(|s| s.start_timestamp);
        Ok(self.index_segments.last_mut().unwrap())
    }
}
```

### 7.4 索引文件布局 (IndexSegment)

```
┌─────────────────────────────────────────┐
│ FileHeader (64 bytes)                   │
│ - magic "TMSL", version, ...            │
│ - file_offset = start_timestamp         │
├─────────────────────────────────────────┤
│ Index Area (variable size)              │
│ ┌──────────────┬──────────────────────┐ │
│ │ timestamp    │ data_offset          │ │  entry 1
│ │ 8 bytes      │ 8 bytes              │ │
│ └──────────────┴──────────────────────┘ │
│ ┌──────────────┬──────────────────────┐ │
│ │ timestamp    │ data_offset          │ │  entry 2
│ └──────────────┴──────────────────────┘ │
│ ...                                     │
└─────────────────────────────────────────┘
```

### 7.5 时间范围查询

```rust
impl TimeIndex {
    /// 按时间范围查找相关索引条目
    fn query(&self, start_ts: i64, end_ts: i64) -> io::Result<Vec<IndexEntry>> {
        let mut results = Vec::new();

        // 从内存缓冲查找
        for entry in &self.in_memory_buffer {
            if entry.timestamp >= start_ts && entry.timestamp <= end_ts {
                results.push(*entry);
            }
        }

        // 从磁盘段查找
        for segment in &self.index_segments {
            // 快速排除: 段的时间范围与查询范围无交集
            if segment.start_timestamp + segment.entries_per_segment as i64 <= start_ts {
                continue;
            }
            if segment.start_timestamp > end_ts {
                break;
            }
            // 在段内查找
            let entries = segment.query_range(start_ts, end_ts)?;
            results.extend(entries);
        }

        // 按时间排序
        results.sort_by_key(|e| e.timestamp);
        Ok(results)
    }

    /// 查找指定时间戳最近的索引条目 (用于写入时定位)
    fn find_entry_by_timestamp(&self, timestamp: i64) -> io::Result<Option<IndexEntry>> {
        // 先在内存缓冲中精确查找
        if let Some(entry) = self.in_memory_buffer.iter().find(|e| e.timestamp == timestamp) {
            return Ok(Some(*entry));
        }

        // 在磁盘段中二分查找
        for segment in &self.index_segments {
            if segment.contains_timestamp(timestamp) {
                return segment.binary_search_timestamp(timestamp);
            }
        }
        Ok(None)
    }
}
```

### 7.6 IndexSegment: 单个索引段

```rust
struct IndexSegment {
    path: PathBuf,
    start_timestamp: i64,
    entries_per_segment: usize,
    wrote_count: u64,
    mmap: MmapMut,
    sealed: bool,
}

impl IndexSegment {
    fn append_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        if self.wrote_count >= self.entries_per_segment as u64 {
            self.seal()?;
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "index segment full"));
        }

        let pos = HEADER_SIZE + self.wrote_count as usize * 16;
        write_i64_le(&mut self.mmap, pos, entry.timestamp)?;
        write_u64_le(&mut self.mmap, pos + 8, entry.data_offset)?;
        self.wrote_count += 1;

        // 更新header中的wrote_position和record_count
        write_u64_le(&mut self.mmap, 32, self.wrote_count)?;  // wrote_position in entries
        write_i64_le(&mut self.mmap, 48, self.wrote_count as i64)?;  // record_count

        Ok(())
    }

    /// 二分查找时间戳
    fn binary_search_timestamp(&self, target_ts: i64) -> io::Result<Option<IndexEntry>> {
        let mut lo = 0usize;
        let mut hi = self.wrote_count as usize - 1;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE + mid * 16;
            let ts = read_i64_le(&self.mmap, pos);

            match ts.cmp(&target_ts) {
                Ordering::Equal => return Ok(Some(IndexEntry {
                    timestamp: ts,
                    data_offset: read_u64_le(&self.mmap, pos + 8),
                })),
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => {
                    if mid == 0 { break; }
                    hi = mid - 1;
                }
            }
        }
        Ok(None)
    }

    /// 范围查询: 返回时间戳在 [start, end] 内的条目
    fn query_range(&self, start_ts: i64, end_ts: i64) -> io::Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        let count = self.wrote_count as usize;

        // 二分查找起始位置
        let start_idx = self.lower_bound(start_ts);

        for i in start_idx..count {
            let pos = HEADER_SIZE + i * 16;
            let ts = read_i64_le(&self.mmap, pos);
            if ts > end_ts { break; }
            if ts >= start_ts {
                results.push(IndexEntry {
                    timestamp: ts,
                    data_offset: read_u64_le(&self.mmap, pos + 8),
                });
            }
        }
        Ok(results)
    }

    /// 查找第一个 >= target_ts 的位置 (lower_bound)
    fn lower_bound(&self, target_ts: i64) -> usize {
        let mut lo = 0usize;
        let mut hi = self.wrote_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = HEADER_SIZE + mid * 16;
            let ts = read_i64_le(&self.mmap, pos);

            if ts < target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}
```

---

## 八、DataSet: 数据集

```rust
struct DataSet {
    id: DataSetKey,
    base_dir: PathBuf,    // {data_dir}/{name}/{type}/
    config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    created_at: Instant,
    last_used_at: Instant,
}

impl DataSet {
    fn write(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()> {
        // 1. 写入数据段
        let (seg_offset, data_offset) = self.segments.append(timestamp, data)?;

        // 2. 写入索引
        self.time_index.add_entry(timestamp, seg_offset + data_offset)?;  // 绝对偏移

        self.last_used_at = Instant::now();
        Ok(())
    }

    /// 按时间范围读取
    fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<(i64, Vec<u8>)>> {
        // 1. 从索引查找时间范围
        let entries = self.time_index.query(start_ts, end_ts)?;

        // 2. 按索引条目读取数据
        let mut records = Vec::with_capacity(entries.len());
        for entry in &entries {
            let (timestamp, data) = self.segments.read_at(entry.data_offset)?;
            records.push((timestamp, data));
        }

        // 数据可能跨多个segment, 按timestamp去重和排序
        records.sort_by_key(|(ts, _)| *ts);
        Ok(records)
    }

    /// 按时间范围迭代读取 (不全部加载到内存)
    fn query_iter(&mut self, start_ts: i64, end_ts: i64) -> io::Result<DataSetIterator<'_>> {
        let entries = self.time_index.query(start_ts, end_ts)?;
        Ok(DataSetIterator {
            dataset: self,
            entries: entries.into_iter(),
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.segments.flush()?;
        self.time_index.flush_to_disk()?;
        Ok(())
    }

    fn is_idle(&self, timeout: Duration) -> bool {
        self.last_used_at.elapsed() >= timeout
    }
}
```

---

## 九、Store: 存储门面

```rust
pub struct Store {
    data_dir: PathBuf,
    datasets: RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>,
    flush_interval: Duration,
    idle_timeout: Duration,
    flush_handle: Option<JoinHandle<()>>,
    idle_handle: Option<JoinHandle<()>>,
}

impl Store {
    /// 打开存储
    pub fn open<P: AsRef<Path>>(data_dir: P, config: Option<StoreConfig>) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        // 加载已有数据集
        let mut datasets = HashMap::new();
        Self::load_existing_datasets(&data_dir, &mut datasets)?;

        let mut store = Self {
            data_dir,
            datasets: RwLock::new(datasets),
            flush_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(1800),  // 30min
            flush_handle: None,
            idle_handle: None,
        };

        store.start_background_tasks()?;
        Ok(store)
    }

    /// 打开或创建数据集
    pub fn open_dataset(
        &self,
        name: &str,
        dataset_type: &str,
        config: Option<DataSetConfig>,
    ) -> io::Result<DataSetHandle> {
        let key = DataSetKey {
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
        };

        // 先读锁查找
        {
            let datasets = self.datasets.read().unwrap();
            if let Some(ds) = datasets.get(&key) {
                ds.lock().unwrap().last_used_at = Instant::now();
                return Ok(DataSetHandle(Arc::clone(ds)));
            }
        }

        // 写锁创建
        let mut datasets = self.datasets.write().unwrap();
        // double-check
        if let Some(ds) = datasets.get(&key) {
            ds.lock().unwrap().last_used_at = Instant::now();
            return Ok(DataSetHandle(Arc::clone(ds)));
        }

        // 创建数据集
        let base_dir = self.data_dir.join(&name).join(&dataset_type);
        fs::create_dir_all(&base_dir)?;
        fs::create_dir_all(base_dir.join(".index"))?;

        let ds_cfg = config.unwrap_or_default();
        let mut dataset = DataSet::new(key.clone(), base_dir, ds_cfg);

        // 加载已有文件
        dataset.load_existing()?;

        let ds = Arc::new(Mutex::new(dataset));
        datasets.insert(key, Arc::clone(&ds));

        Ok(DataSetHandle(ds))
    }

    /// 关闭数据集 (释放资源)
    pub fn close_dataset(&self, handle: DataSetHandle) -> io::Result<()> {
        handle.0.lock().unwrap().flush()?;
        // 从map中移除
        let key = handle.0.lock().unwrap().id.clone();
        self.datasets.write().unwrap().remove(&key);
        Ok(())
    }

    /// 关闭存储 (flush所有, 停止后台线程)
    pub fn close(self) -> io::Result<()> {
        // 停止后台线程
        // flush所有数据集
        let datasets = self.datasets.write().unwrap();
        for ds in datasets.values() {
            ds.lock().unwrap().flush()?;
        }
        datasets.clear();
        Ok(())
    }

    // 后台: 定期flush (每5秒)
    fn flush_task(&mut self) { ... }

    // 后台: 检查空闲数据集 (每60秒, 超过30分钟未使用的自动flush+关闭mmap)
    fn idle_check_task(&mut self) { ... }

    // 启动时扫描已有目录, 加载已有的数据集
    fn load_existing_datasets(&self, data_dir: &Path, datasets: &mut HashMap<...>) -> io::Result<()> {
        // 扫描 {data_dir}/*/*/, 跳过 .index/
        for name_entry in fs::read_dir(data_dir)? {
            let name_dir = name_entry?.path();
            if !name_dir.is_dir() { continue; }

            for type_entry in fs::read_dir(&name_dir)? {
                let type_dir = type_entry?.path();
                let type_str = type_dir.file_name()?.to_string_lossy().to_string();
                if type_str.starts_with('.') { continue; } // 跳过.index
                if !type_dir.is_dir() { continue; }

                let name_str = name_dir.file_name()?.to_string_lossy().to_string();
                let key = DataSetKey { name: name_str, dataset_type: type_str };
                // ... 创建DataSet并加载
            }
        }
    }
}
```

---

## 十、DataSetHandle: C ABI 友好句柄

```rust
pub struct DataSetHandle(Arc<Mutex<DataSet>>);

impl DataSetHandle {
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> io::Result<()> {
        self.0.lock().unwrap().write(timestamp, data)
    }

    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> io::Result<Vec<(i64, Vec<u8>)>> {
        self.0.lock().unwrap().query(start_ts, end_ts)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}
```

---

## 十一、FFI API 设计

```rust
use std::os::raw::{c_char, c_int, c_long, c_void, c_uchar};

/// FFI: 打开存储
/// 返回存储句柄, NULL表示失败
#[no_mangle]
pub extern "C" fn tmsl_store_open(
    data_dir: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    catch_error(|| {
        let dir = CStr::from_ptr(data_dir).to_string_lossy().into_owned();
        let store = Box::new(Store::open(dir, None)?);
        Box::into_raw(store) as *mut c_void
    }, err_buf, err_buf_len)
}

/// FFI: 关闭存储
#[no_mangle]
pub extern "C" fn tmsl_store_close(
    store: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    catch_error(|| {
        let _store = Box::from_raw(store as *mut Store);  // 释放
        0
    }, err_buf, err_buf_len)
}

/// FFI: 打开数据集
/// 返回数据集句柄, NULL表示失败
#[no_mangle]
pub extern "C" fn tmsl_dataset_open(
    store: *mut c_void,
    name: *const c_char,
    dataset_type: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    catch_error(|| {
        let store_ref = unsafe { &*(store as *mut Store) };
        let name = CStr::from_ptr(name).to_string_lossy().into_owned();
        let ds_type = CStr::from_ptr(dataset_type).to_string_lossy().into_owned();
        let handle = store_ref.open_dataset(&name, &ds_type, None)?;
        Box::into_raw(Box::new(handle)) as *mut c_void
    }, err_buf, err_buf_len)
}

/// FFI: 写入数据
#[no_mangle]
pub extern "C" fn tmsl_dataset_write(
    dataset: *mut c_void,
    timestamp: c_long,
    data: *const c_uchar,
    data_len: usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    catch_error(|| {
        let ds = unsafe { &mut *(dataset as *mut DataSetHandle) };
        let data_slice = unsafe { std::slice::from_raw_parts(data, data_len) };
        ds.write(timestamp, data_slice)?;
        0
    }, err_buf, err_buf_len)
}

/// FFI: 查询 - 返回迭代器
#[no_mangle]
pub extern "C" fn tmsl_dataset_query(
    dataset: *mut c_void,
    start_ts: c_long,
    end_ts: c_long,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut c_void {
    catch_error(|| {
        let ds = unsafe { &mut *(dataset as *mut DataSetHandle) };
        let iter = ds.query_iter(start_ts, end_ts)?;
        Box::into_raw(Box::new(iter)) as *mut c_void
    }, err_buf, err_buf_len)
}

/// FFI: 迭代器 - 下一条
/// 返回0表示成功, 1表示无更多数据, -1表示错误
#[no_mangle]
pub extern "C" fn tmsl_iter_next(
    iter: *mut c_void,
    out_timestamp: *mut c_long,
    out_data: *mut *mut c_uchar,
    out_data_len: *mut usize,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    catch_error(|| {
        let iter_ref = unsafe { &mut *(iter as *mut QueryIterator) };
        match iter_ref.next() {
            Some((ts, data)) => {
                unsafe {
                    *out_timestamp = ts;
                    *out_data_len = data.len();
                    // caller应该分配足够空间, 或者返回data的副本
                    let out_ptr = libc::malloc(data.len()) as *mut c_uchar;
                    std::ptr::copy(data.as_ptr(), out_ptr, data.len());
                    *out_data = out_ptr;
                }
                0 // success
            }
            None => 1, // end
        }
    }, err_buf, err_buf_len)
}

/// FFI: 迭代器 - 释放数据
#[no_mangle]
pub extern "C" fn tmsl_iter_free_data(data: *mut c_uchar) {
    unsafe { libc::free(data as *mut c_void) };
}

/// FFI: 迭代器 - 关闭
#[no_mangle]
pub extern "C" fn tmsl_iter_close(iter: *mut c_void) {
    if !iter.is_null() {
        unsafe { Box::from_raw(iter as *mut QueryIterator) };
    }
}

/// FFI: 关闭数据集
#[no_mangle]
pub extern "C" fn tmsl_dataset_close(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    catch_error(|| {
        let ds = unsafe { Box::from_raw(dataset as *mut DataSetHandle) };
        ds.0.lock().unwrap().flush()?;
        0
    }, err_buf, err_buf_len)
}

/// FFI: flush数据集
#[no_mangle]
pub extern "C" fn tmsl_dataset_flush(
    dataset: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    catch_error(|| {
        let ds = unsafe { &mut *(dataset as *mut DataSetHandle) };
        ds.flush()?;
        0
    }, err_buf, err_buf_len)
}

/// 辅助: 统一的错误处理
fn catch_error<F>(f: F, err_buf: *mut c_char, err_buf_len: usize) -> i32
where
    F: FnOnce() -> io::Result<i32>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f())) {
        Ok(Ok(code)) => code,
        Ok(Err(e)) => {
            write_error(buf, buf_len, &format!("{}", e));
            -1
        }
        Err(panic) => {
            write_error(buf, buf_len, &format!("panic: {:?}", panic.downcast_ref::<String>()));
            -1
        }
    }
}
```

---

## 十二、内存管理

### 12.1 Mmap 策略

使用 `memmap2` crate:
- 数据段写入: `MmapMut` (可写映射)
- 数据段读取: `Mmap` (只读映射, 从 MmapMut `make_read_only()`)
- 索引段: 同数据段

### 12.2 Page Fault 优化

- 写入时通过 `madvise(MADV_SEQUENTIAL)` 提示顺序写入
- 读取时通过 `madvise(MADV_WILLNEED)` 预读
- `flush` 时调用 `mmap.msync(MsFlags::MS_SYNC)` 持久化

### 12.3 空闲回收

```
后台线程每60秒扫描:
  if ds.last_used_at.elapsed() >= idle_timeout (30min):
    ds.flush()
    ds.close_mmap()   // munmap + close file
    ds.mark_idle()    // 标记为空闲, 下次写入时重新mmap
```

---

## 十三、并发控制

```
Store:
  datasets: RwLock<HashMap<...>>    (read-heavy, 多写少读)

DataSet:
  Arc<Mutex<DataSet>>               (写独占保护)

DataSegmentSet:
  Vec<DataSegment>                  (通过DataSet mutex保护)

TimeIndex:
  Vec<IndexSegment> + in_memory     (通过DataSet mutex保护)

后台任务:
  通过Arc<Mutex<...>>获取锁后执行flush/idle-check
```

> **设计决策**: DataSet 级别的 Mutex 而非 file 级别, 简化并发逻辑。同一数据集的写入和读取互斥, 不同数据集完全并行。

---

## 十四、压缩支持

### 14.1 库选择

- `miniz_oxide`: Rust原生deflate实现, 纯Rust, 无C依赖
- 压缩级别: 0-9

### 14.2 记录级别的压缩

```
未压缩:
  [size:4][timestamp:8][data:N]

压缩:
  [size:4][timestamp:8][compressed_size:4][compressed_data:M]
```

> timestamp 不压缩, 方便索引查询后直接读取时解析时间。

### 14.3 header flags

```rust
const FLAG_COMPRESSED: u16 = 0x0001;
const FLAG_SEALED: u16     = 0x0002;
```

---

## 十五、后台任务

### 15.1 Flush Task (每5秒)

```rust
fn flush_loop(store: Weak<Store>) {
    loop {
        thread::sleep(flush_interval);
        if store.upgrade().is_none() { break; }

        let store = store.upgrade().unwrap();
        let datasets = store.datasets.read().unwrap();
        for ds in datasets.values() {
            // 仅flush有修改的
            if let Err(e) = ds.lock().unwrap().flush() {
                log::error!("flush error: {}", e);
            }
        }
    }
}
```

### 15.2 Idle Check (每60秒)

```rust
fn idle_check_loop(store: Weak<Store>) {
    loop {
        thread::sleep(Duration::from_secs(60));
        if store.upgrade().is_none() { break; }

        let store = store.upgrade().unwrap();
        let mut to_remove = Vec::new();
        {
            let datasets = store.datasets.read().unwrap();
            for (key, ds) in &*datasets {
                if ds.lock().unwrap().is_idle(30 * 60) {
                    to_remove.push(key.clone());
                }
            }
        }
        // 写锁移除
        for key in to_remove {
            if let Some(ds) = store.datasets.write().unwrap().remove(&key) {
                let _ = ds.lock().unwrap().flush();
            }
        }
    }
}
```

> **注意**: 空闲回收不等于数据删除。只是关闭mmap, 释放内存。下次写入时重新加载。

---

## 十六、数据加载 (启动时)

```rust
impl DataSet {
    /// 启动时扫描并加载已有文件
    fn load_existing(&mut self) -> io::Result<()> {
        // 1. 加载数据段: 扫描 {base_dir}/*/, 排除 .index/
        let data_dir = &self.base_dir;
        let mut segments = Vec::new();
        for entry in fs::read_dir(data_dir)? {
            let path = entry?.path();
            if !path.is_file() || path.file_name().map_or(false, |n| n.to_string_lossy().starts_with('.')) {
                continue;
            }
            let seg = DataSegment::open(&path)?;
            segments.push(seg);
        }
        segments.sort_by_key(|s| s.file_offset);
        self.segments = DataSegmentSet::from_loaded(segments, ...);

        // 2. 加载索引段: 扫描 {base_dir}/.index/*
        let index_dir = data_dir.join(".index");
        let mut index_segs = Vec::new();
        if index_dir.exists() {
            for entry in fs::read_dir(&index_dir)? {
                let path = entry?.path();
                if !path.is_file() { continue; }
                let seg = IndexSegment::open(&path)?;
                index_segs.push(seg);
            }
            index_segs.sort_by_key(|s| s.start_timestamp);
        }
        self.time_index = TimeIndex::from_loaded(index_segs, ...);

        Ok(())
    }
}
```

---

## 十七、Cargo.toml 配置

```toml
[package]
name = "timslite"
version = "0.1.0"
edition = "2021"

[lib]
name = "timslite"
crate-type = ["cdylib", "rlib"]

[dependencies]
memmap2 = "0.9"
miniz_oxide = "0.7"
log = "0.4"
libc = "0.2"
```

---

## 十八、与 TimeStore 的差异对比

| 对比项 | TimeStore (Java) | timslite (Rust) |
|--------|------------------|-----------------|
| 语言 | Java (JVM) | Rust (native) |
| 内存映射 | `MappedByteBuffer` | `memmap2::MmapMut` |
| 元数据 | Protobuf | 自定义64字节header |
| 索引目录 | 同级 (0/, 1/, 2/...) | 独立 `.index/` 子目录 |
| 数据集隔离 | 按recordId一级目录 | 按name/type两级目录 |
| 压缩 | deflate (JVM自带) | `miniz_oxide` |
| 元数据持久化 | 额外 `.pbb` 文件 | header内嵌 |
| WAL模块 | 有 (WAR预写日志) | 无 (简化) |
| FFI | 无 | 有 (`extern "C"`) |
| 后台线程 | Java ScheduledExecutor | Rust JoinHandle |
| 对象复用 | ObjectPool<Buffer> | 无 (Rust自有内存) |

---

## 十九、模块文件结构

```
timslite/
├── Cargo.toml
├── src/
│   ├── lib.rs              # 库入口, re-exports
│   ├── store.rs            # Store (门面)
│   ├── dataset.rs          # DataSet, DataSetKey, DataSetConfig
│   ├── segment/
│   │   ├── mod.rs          # DataSegmentSet
│   │   └── data.rs         # DataSegment (单个数据文件)
│   ├── index/
│   │   ├── mod.rs          # TimeIndex
│   │   └── segment.rs      # IndexSegment (单个索引文件)
│   ├── header.rs           # FileHeader (64字节) 序列化/反序列化
│   ├── ffi.rs              # extern "C" API
│   ├── error.rs            # 自定义错误类型
│   ├── compress.rs         # deflate/inflate 封装
│   ├── util.rs             # endian读写工具
│   └── bg/
│       ├── mod.rs          # 后台任务入口
│       ├── flush.rs        # Flush Task
│       └── idle.rs         # Idle Check Task
├── tests/
│   └── integration_test.rs
└── examples/
    └── basic_usage.rs
```

---

## 二十、使用示例 (伪FFI调用)

```c
// C 侧调用示例
char err_buf[512];
void* store = tmsl_store_open("/data/timslite", err_buf, sizeof(err_buf));
void* ds = tmsl_dataset_open(store, "patient_001", "waveform", err_buf, sizeof(err_buf));

// 写入
unsigned char data[] = {0x01, 0x02, 0x03, 0x04};
tmsl_dataset_write(ds, 1700000000, data, 4, err_buf, sizeof(err_buf));
tmsl_dataset_write(ds, 1700000001, data, 4, err_buf, sizeof(err_buf));
tmsl_dataset_write(ds, 1700000002, data, 4, err_buf, sizeof(err_buf));

// 查询
void* iter = tmsl_dataset_query(ds, 1700000000, 1700000002, err_buf, sizeof(err_buf));
long ts;
unsigned char* buf;
size_t len;
while (tmsl_iter_next(iter, &ts, &buf, &len, err_buf, sizeof(err_buf)) == 0) {
    // 处理数据...
    tmsl_iter_free_data(buf);
}
tmsl_iter_close(iter);

// 清理
tmsl_dataset_close(ds, err_buf, sizeof(err_buf));
tmsl_store_close(store, err_buf, sizeof(err_buf));
```

---

## 二十一、错误处理策略

- 所有 FFI 函数通过 `err_buf` 返回错误信息, 返回值 `0`=成功, `1`=结束, `-1`=错误
- Rust internal: 使用 `io::Error` 和自定义 `TmslError`
- `panic`: `catch_unwind` 捕获, 写入err_buf, 返回-1
- `Drop`: Store/Dataset/DataSetHandle 实现Drop, 自动flush

---

## 二十二、关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 索引目录位置 | `.index/` 子目录 | 与数据文件完全隔离, 方便单独管理 |
| 索引分段依据 | 时间戳范围 (entries数量固定) | 时间索引天然按时间分片, 查询高效 |
| 数据分段依据 | 字节偏移量 (固定大小) | 与mmap page对齐, IO高效 |
| header大小 | 64 bytes固定 | 简化offset计算, 可扩展reserva字段 |
| 时间戳精度 | 秒级 (index) + ms级 (record) | 索引按秒对齐节省空间, 记录保留毫秒 |
| 并发粒度 | DataSet级mutex | 简化逻辑, 不同数据集完全独立 |
| 压缩粒度 | 单条记录级 | 支持随机访问, 不需要全局解压 |
| WAL模块 | 不包含 | 简化设计, 可按需后续添加 |
| 删除/更新 | 不支持 | 追加写入, 通过过期清理回收空间 |
