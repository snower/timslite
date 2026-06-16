# 压缩策略

## 十六、压缩

### 16.1 Active Contract: 压缩算法选择

压缩算法由不可变的 `compress_type` 决定。`compress_type` 同时保存在 dataset meta 和 data/index segment header immutable meta 中。

| `compress_type` | 算法 | 默认 | 实现 |
|-----------------|------|------|------|
| `0` | zstd | 是 | `zstd` crate |
| `1` | deflate | 否 | `miniz_oxide` |

规则:

- 新建 Store / DataSet / Segment 默认使用 `compress_type = 0` (zstd)。
- `compress_type = 1` 保留为内置支持算法, 用于显式选择 deflate。
- 未知 `compress_type` 必须在 config decode、dataset/meta/header open、compress/decompress 前被拒绝。
- Data segment 写入 sealed block 时使用该 segment header 中的 `compress_type` 压缩 payload。
- 读取 compressed block 时必须使用所属 segment header 的 `compress_type` 解压, 不能使用当前 Store/DataSet 默认值。
- Pending block 始终保持 raw 且可变, 与 `compress_type` 无关。
- 当前项目仍处于首次开发阶段, 不需要旧单算法文件格式兼容分支。

### 16.2 `compress_level` 契约

`compress_level` 是 `u8`, 默认 `6`, 由当前选中的算法解释。

| 算法 | 当前公开范围 | 默认 | 说明 |
|------|--------------|------|------|
| zstd (`0`) | `0..=9` | `6` | 传给 zstd encoder; 新写出的 zstd frame 必须开启 content checksum。zstd 原生更高 level 当前不通过 public config 暴露。 |
| deflate (`1`) | `0..=9` | `6` | 传给 `miniz_oxide` deflate; `0` 表示最快/最低压缩, `9` 表示最慢/最高压缩。 |

非法值处理:

- Rust builder 当前会把传入的 `compress_level > 9` clamp 到 `9`。
- on-disk dataset meta 中 `compress_level > 9` 是非法值, open 必须拒绝。
- `compress_level` 本身不决定算法; 算法只由 `compress_type` 决定。

### 16.3 Block 级延迟压缩

- Block 级压缩, 不是 record 级。
- 延迟压缩: pending 时保持 raw, 仅在 pending overflow 或 exclusive/single-record block 创建时压缩。
- **强制压缩**: 一旦 block 从 pending 转为 sealed, 必须用 selected algorithm 写入 compressed payload, 并同时设置 `SEALED | COMPRESSED`。不再根据压缩后是否缩小决定保留 raw。
- **idle-close 不改变 block 状态**: 只执行 sync + unmap/close, 不 seal、不 compress、不清 pending state。重新打开后最后一个 block 仍按 header 中的 pending state 恢复为 pending。
- exclusive/single-record block 不进入 pending, 创建时立即用 selected algorithm 压缩并写为 `SEALED | COMPRESSED | SINGLE_RECORD`。它由编码后大小超过普通聚合 Block 上限的单条 record 产生。

### 16.4 状态机

```text
flags = 0
pending raw block
    │
    ├─ append 且未超过 BLOCK_MAX_SIZE(65536)
    │     └─ 继续保持 pending raw, 更新 pending_wrote_position / pending_record_count
    │
    ├─ next write 导致 pending overflow
    │     ├─ 读取 raw payload
    │     ├─ selected algorithm compress(payload, compress_level)
    │     ├─ 写入 compressed payload
    │     ├─ header.flags = SEALED | COMPRESSED
    │     └─ 清除 file header pending state
    │
    └─ idle-close / reopen
          └─ 不改变 block header 或 pending state

exclusive/single-record block
    └─ 构造单条 record raw payload
       → selected algorithm compress(payload, compress_level)
       → SEALED | COMPRESSED | SINGLE_RECORD
```

**核心 invariant**:

- 新格式中 `SEALED` 与 `COMPRESSED` 必须同时存在或同时不存在。
- `SEALED && !COMPRESSED` 为非法状态。
- `COMPRESSED && !SEALED` 为非法状态。
- `flags = 0` 表示 pending raw block, 可追加、可纠正写、可读取, 但不得进入全局 BlockCache。

### 16.5 空间约束

Selected algorithm 的 compressed payload 可能略大于 raw payload, 实现不能依赖“压缩后一定更小”:

1. pending overflow seal 前, 需要按 `compressed.len()` 检查当前 segment 剩余空间。
2. 若当前 segment 初始分配不足但仍小于最大 segment size, 先扩容后再写 compressed payload。
3. 若扩容到最大 segment size 后仍无法容纳 compressed payload, 返回 `SegmentFull`/错误, 由上层创建新 segment 或调整写入策略。
4. exclusive/single-record block 创建时, 也以 `compressed.len()` 作为实际 payload 长度做空间检查与 `wrote_position` 更新。

### 16.6 Block Flags

```rust
const BLOCK_FLAG_COMPRESSED: u16     = 0x0001;  // 已压缩
const BLOCK_FLAG_SEALED: u16         = 0x0002;  // 已密封 (不再写入)
const BLOCK_FLAG_SINGLE_RECORD: u16  = 0x0004;  // exclusive/single-record block
```

### 16.7 读取时解压

```text
读取 block:
  1. 读取所属 segment header 的 compress_type
  2. 读取 BlockHeader, 检查 flags
  3. 若 flags = 0:
       → pending raw payload, 直接读取, 不进入全局 BlockCache
  4. 若 flags 同时包含 SEALED | COMPRESSED:
       → 先查询全局 BlockCache
       → 未命中则使用 segment header compress_type 解压 payload
       → decoded payload 写入全局 BlockCache
  5. 若 flags 只包含 SEALED 或只包含 COMPRESSED:
       → 格式错误
```

全局缓存建立在 compressed block 不可变的前提上: 一旦 block 带有 `SEALED | COMPRESSED`, correction 写入不得原地修改该 block, 只能回退为乱序追加并更新索引。pending raw block 可读、可追加、可纠正写, 但不得进入全局缓存。

### 16.8 zstd Frame Checksum

对于 `compress_type = 0` (zstd), 每个新编码的 zstd frame 必须开启 zstd content checksum flag。

规则:

- checksum 是 zstd frame 内部属性, 不是 timslite block/header/meta 字段。
- Data segment 与 Journal segment 都必须调用 shared zstd compression helper, 让 checksum 行为在 helper 中统一实现。
- 旧 zstd frame 即使没有 checksum flag 也必须继续可读; zstd decompress 使用标准 decoder。
- 损坏的 zstd frame 应通过 zstd decoder 返回 `DecompressionError`。checksum 为当前代码写出的 frame 提供额外端到端完整性检查。
- Deflate (`compress_type = 1`) 行为保持不变, 不增加等价 checksum。

---

**相关**: [数据模型](data-model.md) | [数据集操作](dataset-operations.md) | [设计决策](design-decisions.md)
