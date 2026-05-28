# Phase 15: Header State 分化 — DataFileMetadata / IndexFileMetadata

> **目标**: 将统一的 100B FileMetadata 拆分为数据段 116B (DataFileMetadata, 9 state) 和索引段 52B (IndexFileMetadata, 1 state), 数据段额外维护 min/max_timestamp 用于段级范围过滤优化。

## 1. 背景与动机

### 1.1 现状问题

当前数据段和索引段共享同一个 `FileMetadata` 结构 (100B header), state 区包含 7×8B = 56 字节:

| State 字段 | 数据段用途 | 索引段用途 |
|-----------|-----------|-----------|
| `wrote_position` | ✅ 必需 | ✅ 必需 (计算 wrote_count) |
| `record_count` | ✅ 必需 | ⚠️ 冗余 (可从 wrote_position 计算) |
| `total_uncompressed_size` | ✅ 必需 | ❌ 无意义 |
| `invalid_record_count` | ⚠️ 未使用 | ❌ 无意义 |
| `pending_block_offset` | ✅ 必需 | ❌ 无意义 |
| `pending_wrote_position` | ✅ 必需 | ❌ 无意义 |
| `pending_record_count` | ✅ 必需 | ❌ 无意义 |

索引段只有 `wrote_position` 被实际使用, 其余 6 个字段全部浪费 48 字节。

### 1.2 优化收益

| 变化 | Before | After | 效果 |
|------|--------|-------|------|
| 索引段 header | 100B | 52B | 每条 entry 空间 +48B 容量 |
| 数据段 header | 100B | 116B | 增加 min/max timestamp 段级过滤 |
| 文件类型区分 | 隐式 (state 相同) | 显式 (state 大小不同) | 结构清晰 |

### 1.3 新增 min/max timestamp

数据段 state 新增 `min_timestamp` (i64) 和 `max_timestamp` (i64):
- 每次 `append_record` 时更新
- 用于 `DataSegmentSet` 查询路由: 跳过不在 `[start_ts, end_ts]` 范围内的段
- 空段初始值: `min_timestamp = i64::MAX`, `max_timestamp = i64::MIN`

## 2. 改动清单

### 2.1 `src/header.rs` — 核心重构

**删除**:
- `FileMetadata` 结构体
- `HEADER_SIZE` 常量 (= 100)
- `STATE_LENGTH_V1` 常量 (= 56)
- 所有 state 字段偏移常量 (`S_WROTE_POSITION`, `S_RECORD_COUNT` 等)

**新增**:
- `DataFileMetadata` 结构体 (9 state 字段)
- `IndexFileMetadata` 结构体 (1 state 字段)
- `DATA_HEADER_SIZE: u64 = 116`
- `INDEX_HEADER_SIZE: u64 = 52`
- `DataStateOffset` 偏移常量 (9 个)
- `IndexStateOffset` 偏移常量 (1 个)

**保留不动**:
- 固定前缀 (magic, version, fileType, meta_length)
- Meta TLV 区解析逻辑
- Meta TLV 序列化逻辑
- `parse_meta_tlv()` 方法

### 2.2 `src/segment/data.rs` — DataSegment

**字段变更**:
- 新增 `min_timestamp: i64`
- 新增 `max_timestamp: i64`

**写入逻辑**:
- `append_record()`: 每次写入后更新 min/max_timestamp 并写入 mmap state
- `create_pending_and_append()`: 同上
- `create_single_record_block()`: 同上
- `write_raw_record_to_pending()`: 同上

**文件头更新**:
- `update_file_wrote_position()`: 新增写入 min/max_timestamp 到 state 偏移
- 调整 state 偏移计算: 新增 min/max 后其他字段的偏移值

**生命周期**:
- `create()`: 初始化 `min_timestamp = i64::MAX`, `max_timestamp = i64::MIN`
- `open()`: 从 `DataFileMetadata` 读取 min/max_timestamp
- `ensure_open()`: 从 `DataFileMetadata` 恢复
- `idle_close()`: sync → seal → close (min/max 已在每次写入时持久化)

**硬编码偏移修正**:
- 当前代码使用硬编码偏移 (如 `mmap[44..52]`) 写入 state
- 需要根据新布局调整所有硬编码偏移值

### 2.3 `src/index/segment.rs` — IndexSegment

**字段变更**:
- 删除 `record_count` (从 `wrote_count` 派生的 `WROTE_POSITION` 计算)
- 保留 `wrote_count: usize` (从 `wrote_position` 反推)

**State 更新**:
- `append_entry()`: 仅写入 `wrote_position` (1 个 u64)
- `create()`: 初始 `wrote_position = INDEX_HEADER_SIZE`
- `open()`: 从 `IndexFileMetadata` 读取 `wrote_position`, 反推 `wrote_count`

**硬编码偏移修正**:
- 当前 `mmap[44..52]` 和 `mmap[52..60]` 需要改为 `mmap[44..52]` (仅 wrote_position)

**容量计算**:
- `entries_capacity = (current_file_size - INDEX_HEADER_SIZE) / INDEX_ENTRY_SIZE`

### 2.4 `src/segment/mod.rs` — DataSegmentSet

**时间范围过滤优化** (新增):
- `query()` / `read_at_index()` 等路由方法: 使用 `min_timestamp`/`max_timestamp` 跳过不在范围内的段
- 仅用于已关闭段的 `DataSegmentMeta` 过滤: 需从 meta 额外存储 min/max

**注意**: Closed segments 的 min/max_timestamp 信息:
- 方案A: `DataSegmentMeta` 新增 min/max 字段, idle_close 时记录
- 方案B: 每次都 lazy_open 读取 (性能差)
- 推荐方案A

### 2.5 `src/index/mod.rs` — TimeIndex

- 替换所有 `HEADER_SIZE` 引用为 `INDEX_HEADER_SIZE`
- `load_existing()`: 读取 index segment 时使用新常量

### 2.6 其他引用更新

| 文件 | 变更 |
|------|------|
| `src/dataset.rs` | `HEADER_SIZE` → `DATA_HEADER_SIZE` (数据段相关) / `INDEX_HEADER_SIZE` (索引段相关) |
| `src/block.rs` | 无变更 (BlockHeader 不涉及) |
| `src/cache.rs` | 无变更 |
| `src/lib.rs` | 导出新常量 |
| `tests/integration_test.rs` | 替换 `HEADER_SIZE` 引用 |

## 3. 数据段文件头新布局 (116 字节)

```
Offset  Size  Field                    Description
0       4     magic = b"TMSL"
4       2     version = 1
6       1     fileType = 2 (DATA)
7       2     meta_length = 33
9       33    Meta TLV (created_at, file_offset, file_size, compress_level)
42      2     state_length = 72
44      8     min_timestamp (i64 LE, i64::MAX=空段)
52      8     max_timestamp (i64 LE, i64::MIN=空段)
60      8     wrote_position (u64 LE)
68      8     record_count (u64 LE)
76      8     total_uncompressed_size (u64 LE)
84      8     pending_block_offset (u64 LE, u64::MAX=无)
92      8     pending_wrote_position (u64 LE)
100     8     pending_record_count (u64 LE)
108     8     reserved (u64 LE, 初始 0)
─────────────────────────────────────
Total: 116 bytes
```

## 4. 索引段文件头新布局 (52 字节)

```
Offset  Size  Field                    Description
0       4     magic = b"TMSL"
4       2     version = 1
6       1     fileType = 1 (INDEX)
7       2     meta_length = 33
9       33    Meta TLV (created_at, file_offset, file_size, compress_level)
42      2     state_length = 8
44      8     wrote_position (u64 LE)
─────────────────────────────────────
Total: 52 bytes
```

## 5. DataFileMetadata 结构体定义

```rust
pub struct DataFileMetadata {
    // === 固定前缀 (9 bytes) ===
    pub magic: [u8; 4],
    pub version: u16,
    pub file_type: u8,          // = 2 (DATA)

    // === Meta TLV (33 bytes, immutable) ===
    pub created_at: i64,
    pub file_offset: i64,
    pub file_size: u32,
    pub compress_level: u8,
    pub meta_length: u16,

    // === State (72 bytes, 9×8B) ===
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub wrote_position: u64,
    pub record_count: u64,
    pub total_uncompressed_size: u64,
    pub pending_block_offset: u64,
    pub pending_wrote_position: u64,
    pub pending_record_count: u64,
    pub reserved: u64,

    pub state_length: u16,      // = 72
}
```

## 6. IndexFileMetadata 结构体定义

```rust
pub struct IndexFileMetadata {
    // === 固定前缀 (9 bytes) ===
    pub magic: [u8; 4],
    pub version: u16,
    pub file_type: u8,          // = 1 (INDEX)

    // === Meta TLV (33 bytes, immutable) ===
    pub created_at: i64,
    pub file_offset: i64,
    pub file_size: u32,
    pub compress_level: u8,
    pub meta_length: u16,

    // === State (8 bytes, 1×8B) ===
    pub wrote_position: u64,

    pub state_length: u16,      // = 8
}
```

## 7. 迁移策略

### 7.1 旧文件兼容

旧文件 (100B header) 的 `file_type` 区分数据段和索引段:
- `file_type = 2` (DATA): 旧文件无 min/max_timestamp, open 时设置为当前段的 sentinel 值
- `file_type = 1` (INDEX): 旧文件有冗余 state, open 时只读取 `wrote_position`

**注意**: 当前版本号为 1, 变更 header 大小后建议保持版本号不变 (因为 state_length 字段自描述长度, 旧版本可通过 state_length 跳过未知字段)。

### 7.2 测试更新

- `header.rs` 单元测试: 更新所有 roundtrip 测试
- `segment/data.rs` 单元测试: 更新 header 偏移硬编码
- `index/segment.rs` 单元测试: 更新 state 写入逻辑
- 集成测试: 验证旧文件可正常打开 + 新文件状态完整

## 8. 验收标准

- [ ] `header.rs`: `FileMetadata` 拆分为 `DataFileMetadata` + `IndexFileMetadata`
- [ ] 常量: `DATA_HEADER_SIZE = 116`, `INDEX_HEADER_SIZE = 52` 替代 `HEADER_SIZE = 100`
- [ ] `DataSegment`: 新增 `min_timestamp`/`max_timestamp` 字段, 每次写入更新 + 持久化
- [ ] `IndexSegment`: state 仅写入 `wrote_position`, 删除冗余字段写入
- [ ] `DataSegmentSet`: closed segment meta 存储 min/max_timestamp 用于查询过滤
- [ ] 所有源文件中的 `HEADER_SIZE` 替换为对应类型常量
- [ ] `cargo clippy -- -D warnings` clean
- [ ] `cargo test -- --test-threads=1` 全部通过
- [ ] 旧文件 (100B header) 可正常打开 + 读写

---

**依赖**: 无前置依赖
**风险**: 文件头大小变更可能影响旧版本兼容性 (通过 state_length 自描述缓解)
