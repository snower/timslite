# 压缩策略

## 十六、压缩

- `miniz_oxide`: 纯 Rust deflate。
- Block 级压缩, 不是 record 级。
- 延迟压缩: pending 时保持 raw, 仅在 pending overflow 或 exclusive/single-record block 创建时压缩。
- **强制压缩**: 一旦 block 从 pending 转为 sealed, 必须写入 deflate 后的 payload, 并同时设置 `SEALED | COMPRESSED`。不再根据压缩后是否缩小决定保留 raw。
- **idle-close 不改变 block 状态**: 只执行 sync + unmap/close, 不 seal、不 compress、不清 pending state。重新打开后最后一个 block 仍按 header 中的 pending state 恢复为 pending。
- exclusive/single-record block 不进入 pending, 创建时立即 deflate 并写为 `SEALED | COMPRESSED | SINGLE_RECORD`。它可能由 >64KB record 产生, 也可能由 append 修改已有 latest record 后超过 70% 聚合阈值产生。

### 状态机

```
flags = 0
pending raw block
    │
    ├─ append 且未超过 BLOCK_MAX_SIZE(65536)
    │     └─ 继续保持 pending raw, 更新 pending_wrote_position / pending_record_count
    │
    ├─ next write 导致 pending overflow
    │     ├─ 读取 raw payload
    │     ├─ deflate 压缩
    │     ├─ 写入 compressed payload
    │     ├─ header.flags = SEALED | COMPRESSED
    │     └─ 清除 file header pending state
    │
    └─ idle-close / reopen
          └─ 不改变 block header 或 pending state

exclusive/single-record block
    └─ 构造单条 record raw payload → deflate → SEALED | COMPRESSED | SINGLE_RECORD
```

**核心 invariant**:

- 新格式中 `SEALED` 与 `COMPRESSED` 必须同时存在或同时不存在。
- `SEALED && !COMPRESSED` 为非法状态。
- `COMPRESSED && !SEALED` 为非法状态。
- `flags = 0` 表示 pending raw block, 可追加、可纠正写、可读取, 但不得进入全局 BlockCache。

### 空间约束

deflate payload 可能略大于 raw payload, 实现不能依赖“压缩后一定更小”:

1. pending overflow seal 前, 需要按 `compressed.len()` 检查当前 segment 剩余空间。
2. 若当前 segment 初始分配不足但仍小于最大 segment size, 先扩容后再写 compressed payload。
3. 若扩容到最大 segment size 后仍无法容纳 compressed payload, 返回 `SegmentFull`/错误, 由上层创建新 segment 或调整写入策略。
4. exclusive/single-record block 创建时, 也以 `compressed.len()` 作为实际 payload 长度做空间检查与 `wrote_position` 更新。

### Block Flags

```rust
const BLOCK_FLAG_COMPRESSED: u16     = 0x0001;  // 已压缩
const BLOCK_FLAG_SEALED: u16         = 0x0002;  // 已密封 (不再写入)
const BLOCK_FLAG_SINGLE_RECORD: u16  = 0x0004;  // exclusive/single-record block
```

### 读取时解压

```
读取 block:
  1. 读取 BlockHeader, 检查 flags
  2. 若 flags = 0:
       → pending raw payload, 直接读取, 不进入全局 BlockCache
  3. 若 flags 同时包含 SEALED | COMPRESSED:
       → 先查询全局 BlockCache; 未命中则 inflate(payload), 再写入全局 BlockCache
  4. 若 flags 只包含 SEALED 或只包含 COMPRESSED:
       → 格式错误
```

全局缓存建立在 compressed block 不可变的前提上: 一旦 block 带有 `SEALED | COMPRESSED`, correction 写入不得原地修改该 block, 只能回退为乱序追加并更新索引。pending raw block 可读、可追加、可纠正写, 但不得进入全局缓存。

### 压缩配置

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `compress_level` | 6 | 1-9 | deflate 压缩级别, 1=最快/最低压缩, 9=最慢/最高压缩 |

---

**相关**: [数据模型](data-model.md) | [数据集操作](dataset-operations.md) | [设计决策](design-decisions.md)
