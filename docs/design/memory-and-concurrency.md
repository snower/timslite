# 内存管理与并发控制

## 十四、内存管理

- `memmap2`: MmapMut (写入), Mmap (只读)
- `madvise`: SEQUENTIAL (写), WILLNEED (读)
- `flush`: mmap.flush() (MS_SYNC) — 仅同步到磁盘, **不改变任何 block 状态**
- 数据/索引 segment 均使用 mmap, 生命周期相同
- 空闲 30min → msync → 密封 pending (不压缩) → munmap → close file
- 下次访问 → on-demand open + mmap → 检测/恢复 pending block
- 任意时刻只有活跃 segment 持有 mmap 文件句柄

## 十五、并发控制

```
Store: RwLock<HashMap>              (多读少写)
DataSet: Arc<Mutex<DataSet>>        (读写互斥)
不同 DataSet: 完全并行
```

- 后台线程通过读锁遍历, 写锁获取后 double-check `last_used_at` 防止竞态
- 前台写操作更新 `last_used_at` 可自动"唤醒"即将 idle-close 的数据集

## 十七.6 mmap 生命周期

```
┌─────────┐  write/read    ┌────────┐   idle 30min   ┌────────┐
│ closed  │ ─────────────→ │  open  │ ──────────────→ │ closed │
│         │ ←─ on-demand ──│(mmap) │                 │(unmap) │
└─────────┘                └────────┘                 └────────┘
    ↑                          │
    │      flush (10min)       │ msync only
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
      b. 验证: pending_block_offset + HEADER_SIZE + pending_wrote_position <= file_size
      c. 密封 pending block (FLAGS=SEALED, 不压缩)
      d. 清除 header pending state
      e. wrote_position = sealed block 末尾
      f. 返回 OpenReady
```

> **Crash 安全分析**:
> idle-close 时 msync 已确保 header 和 block payload 同步到磁盘。
> Reopen 时如果 pending 数据已写入但 header 未 seal → 恢复流程可以安全密封。
> 如果 crash 发生在 msync 前 → 部分数据丢失 (但 header 记录的是 msync 前的状态)。
> 这 10min flush 间隔内的 crash 损失可接受 (mmap 本身已有 OS page cache 保护)。

## 崩溃安全

- mmap 写入已有 OS page cache 保护, crash 时最多损失 10 分钟 (flush 间隔) 内未 sync 的数据
- reopen 时检测 pending block 并安全密封 (FLAGS=SEALED, 不压缩), 不会损坏已有数据
- meta 文件创建时一次性写入, 不存在部分写入问题
- 索引和数据段独立文件, 单个文件损坏不影响其他段
- Header `file_size` 不随扩容更新, 打开时以磁盘实际大小为准 — 消除扩容 crash 风险

---

**相关**: [架构概览](architecture.md) | [后台任务与缓存](background-and-cache.md) | [懒分配与扩容](lazy-allocation.md)
