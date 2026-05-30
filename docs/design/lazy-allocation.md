# 分段文件懒分配与倍率扩容

> **核心原则**: 分段文件创建时仅分配初始大小, 写入过程中如已分配空间不足, 按 2 倍速率扩容, 上限为配置的 `segment_size`。
> **优化目标**: 减少小数据量场景下的磁盘空间浪费。

## 24.1 设计动机

当前全量预分配问题:
- 仅写入 100 条记录 → 实际数据 < 10KB → 磁盘占用 64MB + 4MB = 68MB
- 创建 50 个小数据集 → 磁盘浪费 3.4GB

懒分配方案:
- 数据段初始 256KB, 索引段初始 4KB → 磁盘占用仅 260KB
- 写入时逐步扩容, 最多到 64MB/4MB
- 节省 99%+ 的磁盘空间

## 24.2 新增配置参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `initial_data_segment_size` | u64 | 256 * 1024 (256KB) | 数据分段文件初始大小 |
| `initial_index_segment_size` | u64 | 4 * 1024 (4KB) | 索引分段文件初始大小 |

**约束**:
- `initial_data_segment_size` 必须 ≥ data `header_len` + 最小可用空间 (当前 v1 默认 116 bytes)
- `initial_index_segment_size` 必须 ≥ index `header_len` + 最小可用空间 (当前 v1 默认 52 bytes)
- `initial_*` 必须 ≤ 对应的 `segment_size` (max)
- 若 `initial_* == segment_size` → 退化为全量预分配

### Meta 文件持久化 (TLV 扩展)

| Type (hex) | 名称 | 长度 | 数据类型 | 说明 |
|------------|------|------|---------|------|
| 0x06 | initial_data_segment_size | 8 | u64 LE | 数据分段初始大小 |
| 0x07 | initial_index_segment_size | 8 | u64 LE | 索引分段初始大小 |

> `initial_*` 不是分段文件的不可变布局参数, 而是新分段的初始分配策略。
> 旧版本库读取时跳过未知 TLV type。

## 24.3 分段创建与扩容

### 创建

```rust
// DataSegment::create - 仅分配初始大小
pub fn create(path: &Path, file_offset: u64, initial_size: u64, max_size: u64) -> Result<Self> {
    file.set_len(initial_size)?;  // ← 仅分配初始大小
    let metadata = DataFileMetadata::create_default(FILE_TYPE_DATA, file_offset as i64, max_size as u32);
    // header file_size = max, NOT initial_size
}

// IndexSegment::create - 同理
pub fn create(base_dir: &Path, start_timestamp: i64, initial_size: u64, max_size: u64) -> Result<Self>;
```

### 扩容算法

```
expand(current_file_size, max_size, mmap, path):
   │
   ├─ 1. target = min(current_file_size * 2, max_size)
   │     └─ target == current_size → 已达上限, 返回错误
   │
   ├─ 2. unmap: self.mmap = None
   ├─ 3. file.set_len(target)
   ├─ 4. remap: self.mmap = Some(MmapMut::map_mut(&file))
   ├─ 5. 更新内存字段: file_size = target
   └─ 6. flush (确保持久化)
```

### 触发时机

**DataSegment**: `file_size - header_len < wrote_position + 需要的空间` → 扩容
**IndexSegment**: `wrote_count >= entries_capacity` → 扩容 → 使用 `header_len` 重新计算 `entries_capacity`

### 扩容上限处理

当 `target == max_file_size` (已达上限) 时:
1. 密封当前段 (seal pending block)
2. 创建新段 (以 `initial_size` 创建)
3. 写入到新段

> **重要**: `next_offset` 计算仍然基于 `segment_size` (max), 确保新段文件名正确:
> - 第 1 段: offset = 0
> - 第 2 段: offset = 64MB (即使第 1 段只用了 1MB)

## 24.4 Header file_size 语义

文件头中的 `file_size` (meta TLV type 0x03) **不随扩容更新**:

| 时机 | file_size 值 | 说明 |
|------|-------------|------|
| 创建时 | `max_file_size` | 创建时写入, 记录标准分段大小 |
| 扩容时 | **不变** | 扩容仅修改磁盘文件, 不修改 header |
| 打开时 | **忽略** | 以 `fs::metadata(path)?.len()` 实际大小为准 |

> **Crash 安全**: header file_size 不更新 → 扩容中途 crash → header 与实际文件不会不一致。

## 24.5 打开已有分段 (兼容)

`DataSegment::open()` 和 `IndexSegment::open()`:
- 通过 `fs::metadata(path)?.len()` 读取实际文件大小
- 已全量预分配文件: 实际大小 = segment_size → 正常打开
- 懒分配+扩容后的文件: 实际大小 = 扩容后大小 → 正常打开
- `max_file_size` 由配置传入

## 24.6 磁盘空间优化效果

| 场景 | 传统 (全量预分配) | 懒分配 | 节省 |
|------|-------------------|--------|------|
| 100 条记录 (data ~1KB) | 68MB | 260KB | 99.6% |
| 1000 条记录 (data ~10KB) | 68MB | 260KB | 99.6% |
| 50000 条记录 (data ~500KB) | 68MB | 774KB | 98.9% |
| 满数据 (64MB) | 68MB | 68MB | 0% |

## 24.7 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 扩容时 unmap/remap 开销 | 写入延迟增加 | 64MB 从 256KB 仅需 9 次扩容 |
| initial_size 配置过小 | 频繁扩容 | 默认 256KB/4KB 已能容纳大量小写入 |
| 扩容 crash | header 损坏风险 | **零风险**: header 不变, 以磁盘实际大小为准 |
| 后台任务与扩容竞态 | 冲突 | DataSet 的 Mutex 保证互斥 |

---

**相关**: [数据模型](data-model.md) | [数据段管理](data-segment.md) | [时间索引](time-index.md)
