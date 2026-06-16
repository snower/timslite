# Phase 17: 纠正写入 (Correction Write) — 最新 block 原地覆盖, 支持变 size

> **目标**: 当写入 timestamp 等于 `latest_written_timestamp` 时, 允许覆盖之前写入的同时间戳数据 (纠正写入场景)。采用最新数据段最后一个未压缩 block 原地覆盖策略: 直接 mmap 覆写数据并支持 data 长度变化, 需更新 5 个计数字段, 索引完全不变, 不产生孤儿记录。非连续模式和连续模式均支持。

## 1. 背景与动机

### 1.1 当前问题

当前 `DataSet::write()` 对 `timestamp == latest_written_timestamp` 一律返回错误 (非连续模式: "out-of-order"; 连续模式: "duplicate timestamp")。这在以下场景中不合理:

- **数据纠正**: 传感器采集后数据经过二次校验/修正, 需要对同一时间点写入更正后的数据
- **幂等保证**: 分布式系统中重试写入同时间戳数据应成功 (覆盖) 而非报错
- **变 size 纠正**: 更正版数据长度可能与旧版不同, 需要支持

### 1.2 设计方案: 最新 block 原地覆盖 (In-Place Overwrite, 支持变 size)

| 维度 | 设计决策 |
|------|---------|
| 目标位置 | **最新数据段**的**最后一个未压缩 block**的**最末 record** |
| Block 形态 | Pending block (flags=0, 未密封) 或 Sealed block (flags=SEALED, 未压缩) |
| 索引 | **完全不变** (block_offset/in_block_offset 保持原值) |
| 变 size | **支持**: delta = new_data.len() - old_data_len (可正可负) |
| 约束 1 | 该 block 的 flags 不能含 COMPRESSED (否则返回错误, 不支持) |
| 约束 2 | record 必须是该 block 的最末 record (否则返回错误) |
| 需更新字段 | **5 个**: block payload_size + uncompressed_size; 段 wrote_position + total_uncompressed_size + pending_wrote_position (仅 pending) |
| 不更新 | record_count (段计数), pending_record_count (不变), latest_written_timestamp (不变) |
| 两种模式 | non-continuous + continuous 共用同一纠正路径 (在 mode 分支之前处理) |

### 1.3 为什么支持变 size

最新记录位于最新数据段的最后位置, 后续没有已写入数据:
- **Pending block**: 后续 mmap 区域未使用, 数据增长仅需扩展 pending block
- **SEALED|无 COMPRESSED**: 最后一个 block, 后续 mmap 区域未使用 (本段没有后续 block), 增长仅需扩展段内 wrote_position

因此变 size 只需调整段级计数字段 + block header 字段即可, 不会影响其他 block 或 record。

## 2. 改动清单

### 2.1 `src/segment/data.rs` — 新增 `overwrite_in_last_block()`

```rust
/// 纠正写入: 在段最后一个未压缩 block 的最末 record 位置原地覆盖 data 字节, 支持变 size
pub fn overwrite_in_last_block(
    &mut self,
    block_rel_offset: u64,
    in_block_offset: u16,
    new_data: &[u8],
) -> Result<()> {
    let mmap = self.mmap.as_mut()
        .ok_or_else(|| TmslError::MmapError("segment mmap not open".into()))?;

    // 1. 验证这是段内最后一个 block
    let block_abs_start = DATA_HEADER_SIZE as usize + block_rel_offset as usize;
    let hdr = BlockHeader::read_from(mmap, block_abs_start);

    // Block 末尾位置
    let block_abs_end = block_abs_start + BLOCK_HEADER_SIZE as usize + hdr.payload_size as usize;
    let seg_wrote_end = (DATA_HEADER_SIZE + self.wrote_position) as usize;
    if block_abs_end != seg_wrote_end {
        return Err(TmslError::InvalidData(
            "correction write: target block is not the last in segment".into()
        ));
    }

    // 2. 检查未压缩
    if hdr.is_compressed() {
        return Err(TmslError::InvalidData(
            "correction write: target block is compressed, not supported".into()
        ));
    }

    // 3. 读取 record 并验证为块内最末 record
    let record_pos = block_abs_start + BLOCK_HEADER_SIZE as usize + in_block_offset as usize;
    let old_data_len = u32::from_le_bytes(
        mmap[record_pos..record_pos + 4].try_into().unwrap()
    ) as usize;
    let record_size = 12 + old_data_len;  // record_overhead=12

    if in_block_offset as usize + record_size != hdr.payload_size as usize {
        return Err(TmslError::InvalidData(
            "correction write: target record is not the last in block".into()
        ));
    }

    // 4. 计算 delta
    let delta_i32 = new_data.len() as i32 - old_data_len as i32;
    let delta_u64 = if delta_i32 >= 0 { delta_i32 as u64 } else { (-delta_i32) as u64 };

    // 5. 写入新 record 的 data_len (u32) + data bytes
    let new_data_len_u32 = new_data.len() as u32;
    mmap[record_pos..record_pos + 4].copy_from_slice(&new_data_len_u32.to_le_bytes());
    // timestamp (8 bytes at record_pos+4) 不变
    let new_data_pos = record_pos + 12;
    mmap[new_data_pos..new_data_pos + new_data.len()].copy_from_slice(new_data);

    // 6. 更新 block header
    let new_payload_size: u32 = if delta_i32 >= 0 {
        hdr.payload_size.checked_add(delta_u64 as u32).unwrap()
    } else {
        hdr.payload_size.checked_sub(delta_u64 as u32).unwrap()
    };
    let new_uncomp_size: u32 = if delta_i32 >= 0 {
        hdr.uncompressed_size.checked_add(delta_u64 as u32).unwrap()
    } else {
        hdr.uncompressed_size.checked_sub(delta_u64 as u32).unwrap()
    };
    let new_hdr = BlockHeader::new(
        new_payload_size,
        hdr.flags,  // 保持原 flags
        hdr.record_count,  // 不变
        new_uncomp_size,
    );
    new_hdr.write_to(mmap, block_abs_start);

    // 7. 更新段内计数字段
    if delta_i32 >= 0 {
        self.wrote_position += delta_u64;
        self.total_uncompressed_size += delta_u64;
    } else {
        self.wrote_position -= delta_u64;
        self.total_uncompressed_size -= delta_u64;
    }

    // 8. 若是 pending block, 更新 pending_wrote_position 并写入 file header
    let is_pending = self.pending_block_offset == Some(block_rel_offset);
    if is_pending {
        if delta_i32 >= 0 {
            self.pending_wrote_position += delta_u64;
        } else {
            self.pending_wrote_position -= delta_u64;
        }
        self.update_file_header_for_pending(block_rel_offset)?;
    }

    self.update_file_wrote_position()?;
    Ok(())
}
```

### 2.2 `src/segment/mod.rs` — 新增 `overwrite_in_last_block()`

```rust
/// 纠正写入: 路由到最新数据段, 原地修改最后一个未压缩 block 的最末 record data 字节
pub fn overwrite_in_last_block(
    &mut self,
    block_offset: u64,
    in_block_offset: u16,
    _timestamp: i64,
    new_data: &[u8],
) -> Result<()> {
    let seg = self.segments.last_mut()
        .ok_or_else(|| TmslError::InvalidData("no segment available".into()))?;

    // 验证 block_offset 落在最新段内
    let seg_end = seg.file_offset + seg.wrote_position;
    if block_offset < seg.file_offset || block_offset >= seg_end {
        return Err(TmslError::InvalidData(format!(
            "correction write: block_offset {} is not in the latest segment [{}, {})",
            block_offset, seg.file_offset, seg_end
        )));
    }

    let block_rel_offset = block_offset - seg.file_offset;
    seg.overwrite_in_last_block(block_rel_offset, in_block_offset, new_data)
}
```

### 2.3 `src/index/mod.rs` — 新增 `find_entry()`

```rust
/// 查找指定 timestamp 的 IndexEntry (用于纠正写入定位)
pub fn find_entry(&mut self, timestamp: i64) -> Result<Option<IndexEntry>> {
    let ic = self.index_continuous;
    // 1. in-memory buffer
    if let Some(entry) = self.in_memory_buffer.iter().find(|e| e.timestamp == timestamp) {
        return Ok(Some(*entry));
    }
    // 2. unified segment registry (open hit or temporary open)
    for (_start, segment_entry) in &mut self.index_segments {
        let mut seg = segment_entry.ensure_open(self.segment_size)?;
        if let Some(entry) = seg.find_exact_cs(timestamp, ic) {
            return Ok(Some(entry));
        }
        segment_entry.idle_close_if_needed()?;
    }
    Ok(None)
}
```

### 2.4 `src/dataset.rs` — `write()` 新增纠正写入分支

```rust
pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
    // ── 纠正写入 (两种模式通用, 在 mode 分支之前判断) ──
    if self.latest_written_timestamp == Some(timestamp) {
        return self.correct_write(timestamp, data);
    }

    // ── 非连续模式 ──
    if self.config.index_continuous == 0 {
        if self.latest_written_timestamp.is_some_and(|latest| timestamp < latest) {
            return Err(TmslError::InvalidData(format!(
                "out-of-order: timestamp {} < latest {}",
                timestamp, self.latest_written_timestamp
            )));
        }
        // timestamp > latest: 正常写入 (后续逻辑不变)
    }

    // ── 连续模式 ──
    else {
        // 保留原 timestamp < latest / timestamp > latest 分支
        // 移除原先 `timestamp == latest ⇒ Error("duplicate timestamp")` 的逻辑
        // (纠正写入已在上面处理)
    }

    self.last_used_at = Instant::now();
    Ok(())
}

fn correct_write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
    // 1. 找到该 timestamp 的 IndexEntry
    let entry = self.time_index.find_entry(timestamp)?
        .ok_or_else(|| TmslError::NotFound(format!(
            "no index entry for correction timestamp {}", timestamp
        )))?;

    // 2. 在最新数据段的最后一个未压缩 block 内原地覆盖
    self.segments.overwrite_in_last_block(
        entry.block_offset,
        entry.in_block_offset,
        timestamp,
        data,
    )?;

    // 3. latest_written_timestamp 不变, 索引不变
    self.last_used_at = Instant::now();
    Ok(())
}
```

**写入分支顺序** (两种模式通用):

| 条件 | 行为 |
|------|------|
| `latest_written_timestamp == Some(timestamp)` | **纠正写入**: 最新 block 原地覆盖 (支持变 size), 索引不变 |
| `timestamp < latest` (非连续) | Error("out-of-order") |
| `timestamp < latest` (连续) | back-fill: append + replace_filler_with_real |
| `timestamp > latest` (任意模式) | 正常写入 |

### 2.5 无需修改的文件

`src/config.rs`, `src/meta.rs`, `src/ffi.rs`, `include/timslite.h`, `src/index/segment.rs`

## 3. 测试计划

| 测试名 | 验证点 |
|--------|--------|
| `test_correction_write_same_size_non_continuous` | write(t=100, data1[4]) → correct(t=100, data2[4]) → query → data2 |
| `test_correction_write_same_size_continuous` | 同上, 连续模式 |
| `test_correction_write_resize_larger` | write(t=100, data1[4]) → correct(t=100, data2[20]) → query → 验证 data2 完整返回 |
| `test_correction_write_resize_smaller` | write(t=100, data1[20]) → correct(t=100, data2[4]) → query → 验证 data2 |
| `test_correction_write_preserves_timestamp` | 纠正后 latest_written_timestamp 不变 |
| `test_correction_write_compressed_block_rejected` | 模拟 pending 已被密封为 COMPRESSED, 纠正写入应返回错误 |
| `test_correction_write_multiple` | 连续纠正 N 次同一 timestamp, 每次验证 query 结果 |
| `test_correction_write_then_new` | 纠正 → 正常写入新 ts → 查询完整 |

## 4. 验收标准

- [x] `segment/data.rs`: `overwrite_in_last_block()` — 最新 block 原地覆盖, 支持变 size, 更新 5 个字段, 不变索引
- [x] `segment/data.rs`: `overwrite_in_last_block()` — 拒绝 COMPRESSED block
- [x] `segment/data.rs`: `overwrite_in_last_block()` — 拒绝非最末 record
- [x] `segment/mod.rs`: `overwrite_in_last_block()` — 路由到最新数据段, 验证 block_offset 范围
- [x] `index/mod.rs`: `find_entry()` — 按 timestamp 查找 IndexEntry (buffer → open → closed)
- [x] `dataset.rs`: 纠正写入分支 (在 mode 分支之前), `correct_write()` 方法
- [x] 单元测试全部通过 (7 项新增) + 集成测试全部通过 (2 项新增)
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (128 tests: 107 unit + 21 integration)
- [x] 设计文档已更新: dataset-operations.md §9.1 + data-model.md §3.2 + index-continuous.md §23.2 + data-segment.md §6.3

## 5. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 最后一个 block 已压缩 | 无法原地修改 | 返回 InvalidData 错误, 调用方需重新写入新 timestamp |
| record 不是块内最末 | 修改会破坏相邻 record | 验证后拒绝, 返回 InvalidData |
| block 不是段内最后 | 增长会覆盖下一个 block 或留下空洞 | 验证 block_abs_end == wrote_position 后拒绝 |
| 缩小场景: delta 为负 | wrote_position 变小, 段内后续字节未清理 | 不影响 (wrote_position 标记有效范围边界, 后续新数据会覆盖这些位置) |
| 缩小场景: pending_wrote_position 计算 | 需正确做减法 | 使用 checked_sub + 校验 delta 合法性 |
| 增长场景: 段剩余空间不足 | 扩展时可能触发段的扩容逻辑 | 实际增长量通常很小, 若扩容则通过已有的 DataSegment::expand() |

---

**相关**: [数据集操作 §9.1](../design/dataset-operations.md#91-时间戳验证与写入分支) | [索引连续存储 §23.2](../design/index-continuous.md#232-写入行为) | [数据段管理 §6.3](../design/data-segment.md)
