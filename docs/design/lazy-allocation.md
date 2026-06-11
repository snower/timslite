# 分段文件懒分配与倍增扩容

> 核心原则: 分段文件创建时只分配初始大小。写入过程中如果已分配空间不足, 按 2 倍速度扩容, 上限为配置的 `segment_size`。
> 优化目标: 减少小数据量场景下的磁盘空间浪费。

## 24.1 设计动机

全量预分配会让少量数据也占用完整 data/index segment 大小。例如只写入少量记录时, 仍可能占用 64MiB data segment + 4MiB index segment。

懒分配方案:

- data segment 初始 256KiB。
- index segment 初始 4KiB。
- 写入时逐步扩容, 最大到 `data_segment_size` / `index_segment_size`。
- 新分段文件名和逻辑路由仍基于 max segment size, 不基于当前物理文件大小。

## 24.2 配置参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---:|---:|---|
| `initial_data_segment_size` | `u64` | `256 * 1024` | data segment 初始物理文件大小 |
| `initial_index_segment_size` | `u64` | `4 * 1024` | index segment 初始物理文件大小 |

约束:

- `initial_data_segment_size >= data header_len + 最小可用空间`。
- `initial_index_segment_size >= 128 + INDEX_ENTRY_SIZE`。
- `initial_* <= 对应 segment_size`。
- `initial_* == segment_size` 时退化为全量预分配。

Dataset meta 持久化:

| Type | 名称 | 长度 | 类型 | 说明 |
|---|---|---:|---|---|
| `0x06` | `initial_data_segment_size` | 8 | `u64 LE` | data segment 初始大小 |
| `0x07` | `initial_index_segment_size` | 8 | `u64 LE` | index segment 初始大小 |

`initial_*` 是新分段的分配策略, 不是分段文件内不可变布局参数。

## 24.3 创建与扩容流程

创建分段:

```rust
file.set_len(initial_size)?;
let metadata = FileMetadata::create_default(file_type, file_offset, max_size);
```

Header meta `file_size` 记录的是 `max_size`, 不是 `initial_size`。

扩容流程:

```text
expand(current_file_size, max_size):
  1. target = min(current_file_size * 2, max_size)
  2. if target == current_file_size: return SegmentFull
  3. unmap current mmap
  4. file.set_len(target)
  5. remap mmap from the same file
  6. update in-memory current file size / entry capacity
```

扩容只要求 `set_len(target)` 成功返回后, 文件系统元数据中的文件长度变更对后续 open/remap 可见。扩容步骤本身不执行 `mmap.flush()`; `flush()` 只同步已经写入 mmap 的 data/header state 内容, 由后台 dirty flush、idle-close 或显式 `DataSet::flush()` 负责。

Crash 语义:

- 如果 crash 发生在 `set_len` 前, reopen 看到旧物理长度。
- 如果 crash 发生在 `set_len` 后但新区域未写入或未 flush, reopen 看到扩容后的物理长度, 新区域按 OS 文件系统语义处理。
- Header meta `file_size` 始终保持 `max_size`, 不随扩容改写, 因此不会出现 header 记录 initial/current size 的二次状态维护问题。

## 24.4 触发时机

Data segment:

```text
current_file_size - header_len < required_data_area_position
```

Index segment:

```text
wrote_count >= entries_capacity
```

Index entry area 固定从文件偏移 128 开始, 扩容后重新根据当前物理文件长度计算 `entries_capacity = (current_file_size - 128) / INDEX_ENTRY_SIZE`。

## 24.5 达到上限

当当前分段已经达到 max segment size 且仍放不下新写入:

1. 如果存在 pending block, 在 next write overflow 路径 seal + compress 当前 pending block。
2. 创建新分段, 新分段按 `initial_*` 大小创建。
3. 写入新分段。

新分段的 `file_offset` / 文件名仍按 max segment size 计算:

```text
segment_n_base = n * segment_size
```

## 24.6 打开已有分段

`DataSegment::open()` 和 `IndexSegment::open()`:

- 通过 `fs::metadata(path)?.len()` 读取当前物理文件长度。
- 通过 header meta `file_size` 校验/恢复 max segment size 契约。
- data segment 的可写位置来自 state `wrote_position`。
- index segment 的可写位置来自 state `wrote_position`, entry capacity 来自当前物理长度。

## 24.7 风险与边界

| 风险 | 影响 | 处理 |
|---|---|---|
| 扩容 unmap/remap 开销 | 写入延迟偶发增加 | 默认初始大小控制扩容次数 |
| initial size 过小 | 频繁扩容 | builder/meta 校验最小值, 默认值保守 |
| 扩容期间 crash | 可能看到旧长度或新长度 | 以 OS 文件长度为准, header 不记录 current size |
| 后台任务与扩容并发 | 可能竞争同一 mmap | DataSet mutex 串行保护 |

---

相关: [数据模型](data-model.md) | [数据段管理](data-segment.md) | [时间索引](time-index.md)
