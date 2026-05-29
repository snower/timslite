# 压缩策略

## 十六、压缩

- `miniz_oxide`: 纯 Rust deflate
- Block 级压缩, 不是 record 级
- 延迟压缩: pending 时 raw, 溢出时 seal+压缩
- 如果压缩后不缩小, 保留 raw (不设 COMPRESSED flag)
- **idle-close 仅密封 pending, 不压缩** — 压缩延迟至 next write overflow
- 超大 record (独占 block) → 立即 seal+压缩 (因为不存在 pending); record payload 仍使用统一的 `data_len: u32 + timestamp: i64 + data` 编码

### 压缩数据流

```
写入 record → 追加到 pending block (raw 格式)
    │
    ├─ pending block 满了 (≥64KB) → 触发 seal
    │     1. 读取 raw payload
    │     2. deflate 压缩
    │     3. 比较: compressed.len() < raw.len()?
    │        ├─ Yes → 写入压缩数据 + set COMPRESSED flag
    │        └─ No  → 保留 raw 数据 + set SEALED flag (无 COMPRESSED)
    │
    ├─ idle-close → 密封 pending (不压缩)
    │     flags = SEALED (无 COMPRESSED)
    │
    └─ 超大 record (独占 block) → 立即 seal + 压缩
          flags = SEALED | COMPRESSED | SINGLE_RECORD
```

### Block Flags

```rust
const BLOCK_FLAG_COMPRESSED: u16     = 0x0001;  // 已压缩
const BLOCK_FLAG_SEALED: u16         = 0x0002;  // 已密封 (不再写入)
const BLOCK_FLAG_SINGLE_RECORD: u16  = 0x0004;  // 独占 record 的超大 block
```

### 读取时解压

```
读取 sealed block:
  1. 读取 BlockHeader, 检查 flags
  2. 如果 COMPRESSED flag 设置:
     → miniz_oxide::inflate::decompress_to_vec(payload)
  3. 如果未设置 COMPRESSED:
     → 直接使用 raw payload
  4. 解压后的数据存入 BlockCache (供后续查询复用)
```

### 压缩配置

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `compress_level` | 6 | 1-9 | deflate 压缩级别, 1=最快/最低压缩, 9=最慢/最高压缩 |

---

**相关**: [数据模型](data-model.md) | [数据集操作](dataset-operations.md) | [设计决策](design-decisions.md)
