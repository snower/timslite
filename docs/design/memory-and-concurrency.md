# 内存管理与并发控制

## 十四、内存管理

- `memmap2`: MmapMut (写入), Mmap (只读)
- `madvise`: SEQUENTIAL (写), WILLNEED (读)
- `flush`: mmap.flush() (MS_SYNC) — 仅同步到磁盘, **不改变任何 block 状态**
- 数据/索引 segment 均使用 mmap, 生命周期相同
- 空闲 30min → msync → munmap → close file (不改变 pending/block 状态)
- 下次访问 → on-demand open + mmap → 从 header 恢复 pending block
- 任意时刻只有活跃 segment 持有 mmap 文件句柄

## 十五、并发控制

```
Store: RwLock<HashMap>              (多读少写)
DataSet: Arc<DataSet>               (DataSet 内部 mutex 保护读写)
不同 DataSet: 完全并行
```

- 后台线程通过读锁遍历, 写锁获取后 double-check `last_used_at` 防止竞态
- 前台写操作更新 `last_used_at` 可自动"唤醒"即将 idle-close 的数据集

### FFI 句柄同步边界

C ABI 暴露的 `store` / `dataset` / `iterator` / `queue` / `consumer` 均是不透明句柄。FFI 层不能把同一个 raw `Store*` 在多个入口中直接恢复成多个 `&mut Store`。正式契约为:

- `FfiStore` 内部持有 `Arc<Mutex<Store>>`。
- `FfiDataset`、`FfiIterator`、FFI queue handle 均克隆同一个 `Arc<Mutex<Store>>`, 需要访问 Store registry 或 mutating Store API 时先获取该 mutex。
- 该 mutex 是 FFI facade 的入口串行化锁, 用于保护 Store handle registry、read-only handle set、queue open/close 等 Store 级状态, 也避免 Rust aliasing UB。
- 获取顺序为 `FfiStore.store_mutex` → `Store.datasets` → `DataSet mutex` → segment/index/queue 内部锁。不得在持有 DataSet/queue/state 锁时反向获取 `FfiStore.store_mutex`。
- Queue poll/push/ack 在拿到 `DatasetQueue` / `DatasetQueueConsumer` clone 后不需要持有 `FfiStore.store_mutex`; 它们只使用 queue 自身的 `Arc<DataSet>`、`QueueInner` 和 consumer state 锁。
- `tmsl_store_close` 在存在任何 dataset、iterator、queue 或 consumer 子句柄时返回错误; 子句柄必须先 close/drop。

## 十七.6 mmap 生命周期

```
┌─────────┐  write/read    ┌────────┐   idle 30min   ┌────────┐
│ closed  │ ─────────────→ │  open  │ ──────────────→ │ closed │
│         │ ←─ on-demand ──│(mmap) │                 │(unmap) │
└─────────┘                └────────┘                 └────────┘
    ↑                          │
    │      flush (15s default) │ msync only
    └──────────────────────────┘
```

## 十七.7 Pending Block 恢复详情

```
reopen 时 pending block 恢复流程:
   1. 读取 FileMetadata, 校验 magic/version
      - magic != "TMSL" → 返回 InvalidMagic (文件损坏/非本库文件)
      - version 不兼容 → 返回 InvalidVersion
   2. 检查 pending_block_offset != u64::MAX
   3. 恢复流程:
      a. 从 header 恢复 pending 状态
      b. 验证: pending_block_offset + header_len + pending_wrote_position <= file_size
      c. 保持 block.flags = 0, 仍作为 pending raw block 可继续追加或纠正写
      d. wrote_position 保持 header 中的已写位置
      e. 返回 OpenReady
```

> **恢复边界说明**:
> idle-close 时 msync 会尽力同步 header 和 block payload; reopen 时如果 pending 状态完整, 直接恢复为 pending raw block。
> 但本库不提供事务、WAL、checksum 或跨 data/index 文件的持久化顺序保证。crash 后允许丢失最近写入, 不再表述为"最多损失 flush 间隔内的数据"。

## 崩溃安全

### 设计取舍

timslite 面向高读写性能、可容忍最近数据丢失的场景, 不引入二阶段提交、WAL、commit marker、checksum 或额外事务状态。崩溃后的目标不是恢复每一次成功返回的写入, 而是:

1. 正常运行期间, 查询不会读取到尚未发布的 append 数据。
2. crash/reopen 后, 已损坏或不完整的 append 不能被当成另一条旧数据或错误数据返回。
3. 最近写入可以丢失; 可见性以索引条目为边界。

### append 发布顺序

append record 的逻辑发布顺序必须是:

1. 写入 record payload: `[data_len:u32][timestamp:i64][data]`。
2. 写入/更新 `BlockHeader` 和 data segment state: `payload_size`, `record_count`, `wrote_position`, pending state 等。
3. 最后写入或更新 `IndexEntry(timestamp, block_offset, in_block_offset)`, 其中 `block_offset` 是数据区逻辑全局偏移, 不含 header。

索引是唯一的查询入口。只要 index 在最后发布:

- crash 或错误发生在 index 写入前: payload/header 可能遗留在 data segment 中, 但没有索引指向它, 查询不可见, 按数据丢失处理。
- 正常运行期间 index 写入后: 同一 `DataSet` 由 mutex 串行保护, payload/header 已先写入内存映射, 查询不会看到未初始化的新 offset。
- crash 发生在 index 写入后: 不保证 data/index 两个 mmap 文件的落盘顺序。reopen 后如果 index 已持久化但 data payload/header 未完整持久化, 读取路径必须通过边界和 timestamp 校验识别异常, 将其视为缺失或损坏, 不能返回旧 timestamp 或错误 payload。

### 读取校验要求

通过索引读取 record 时, 不能只信任 `IndexEntry`。读取路径应至少校验:

- `block_offset` 能正确路由到对应 data segment, 且 `block_offset - segment.file_offset` 落在该段已写入范围内。
- 实际文件读取位置必须是 `segment.header_len + (block_offset - segment.file_offset)`, 不能把 `block_offset` 直接当作文件内物理 offset。
- `BlockHeader.payload_size` 与 `in_block_offset + record_header + data_len` 不越界。
- record 内嵌 `timestamp` 必须等于 `IndexEntry.timestamp`。
- filler/delete sentinel 必须在进入 data segment 前被跳过。

这些校验不恢复丢失写入, 只保证不会把部分写入或错位偏移解释成另一条有效数据。

### 其它边界

- reopen 时检测 pending block 并在状态完整时恢复为 pending raw block, 不做 seal/压缩。
- meta 文件创建时一次性写入; 若创建中断, 由 open/create 的 magic/version/TLV 校验处理。
- 索引和数据段独立文件; 单个文件损坏不应扩散到其他段。
- Header `file_size` 不随扩容更新, 打开时以磁盘实际大小为准, 消除扩容时 header/file_size 不一致的风险。

---

**相关**: [架构概览](architecture.md) | [后台任务与缓存](background-and-cache.md) | [懒分配与扩容](lazy-allocation.md)
