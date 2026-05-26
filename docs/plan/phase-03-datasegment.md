# Phase 3: DataSegment 写入/读取 (核心)

**目标**: DataSegment 完整的 Block 聚合写入、延迟压缩、懒加载生命周期、恢复逻辑

---

## 3.1 DataSegment 结构定义 (segment/data.rs)

```rust
pub struct DataSegment {
    path: PathBuf,
    file_offset: u64,
    file_size: u64,
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    created_at: i64,
    pub mmap: Option<MmapMut>,      // None = closed
    lifecycle: SegmentLifecycle,     // Closed / OpenReady
    last_accessed_at: Instant,       // 最近读写时间
    // Pending Block 状态 (从 header state 读取)
    pending_block_offset: Option<u64>,  // u64::MAX = no pending
    pending_wrote_position: u64,
    pending_record_count: u64,
}

pub enum SegmentLifecycle {
    Closed,          // 文件未打开
    OpenReady,       // 打开中, 可读写
    OpenIdle,        // 即将关闭 (idle timeout 触发)
}
```

## 3.2 DataSegment 创建与打开

- `fn create(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 创建/截断文件到 file_size, mmap (MmapMut)
  - 写入 FileMetadata (HEADER + data_start=100)
  - 初始化所有计数为 0, lifecycle = OpenReady
  - pending_block_offset = u64::MAX (无 pending)
- `fn open(path: &Path, file_offset: u64, file_size: u64) -> Result<Self>`
  - 打开文件 (不截断), mmap, 读取 FileMetadata, 校验 magic/version
  - 恢复 wrote_position, record_count, total_uncompressed_size, pending_block 状态
  - **pending 恢复**: 如果 `pending_block_offset != u64::MAX`:
    1. 在 pending_block_offset 处密封 block (flags = SEALED, 不压缩)
    2. 清除 header pending state: pending_block_offset=u64::MAX
    3. flush file header 到 mmap
    4. wrote_position 指向 sealed block 之后

## 3.3 DataSegment 生命周期管理

- `fn ensure_open(&mut self) -> Result<()>` — lazily open if closed
- `fn idle_close(&mut self) -> Result<()>` — idle timeout 触发:
  1. `mmap.flush()` (MS_SYNC)
  2. 如果 pending_block_offset.is_some(): 密封 pending block (flags = SEALED, 不压缩)
  3. 更新 header: clear pending fields
  4. `munmap` + close file
  5. Set lifecycle = Closed, mmap = None
- `fn sync(&mut self) -> Result<()>` — flush loop 调用:
  - 如果 mmap.is_some(): `mmap.flush()` (MS_SYNC)
  - **不密封 pending, 不压缩**

## 3.4–3.10 核心写入与读取

- `fn write_raw_record_to_pending`
- `fn create_pending_and_append`
- `fn seal_pending_block`
- `fn create_single_record_block`
- 读取逻辑
- sync 方法 (flush loop 调用)

## 验收标准

- [x] 集成测试: 创建 DataSegment → 写入 1000 条 record → 全部逐条读取, 数据一致
- [x] 集成测试: 写入 record 触发 block 切换 (>64KB) → 验证多 block 写入读取
- [x] 集成测试: 写入 record > 64KB → 独占 block → 读取验证
- [x] 集成测试: block 溢出 → 密封+压缩 → 验证 compression flag 正确
- [x] 集成测试: idle_close → 验证 pending block 密封 → munmap → reopen → pending 已密封
- [x] 集成测试: sync → 验证 mmap 内容同步到磁盘, pending block 不变
- [x] 集成测试: crash 模拟 (不 sync) → reopen → pending block 恢复+密封
- [x] 集成测试: create → 写入部分 → close → reopen → wrote_position 恢复, pending 状态恢复
- [x] 目录验证: 数据文件保存在 `data/` 子目录下
- [x] `cargo test --lib` all pass

---

**导航**: [← Phase 2](phase-02-header-block.md) | [→ Phase 4](phase-04-time-index.md)
