# Phase 23: Record 长度编码升级为 u32

> 目标: 修复设计审查 P0-1。将 Block payload 内每条 record 的 `data_len` 从 `u16` 升级为 little-endian `u32`, 使超大独占 record 与文件格式长度字段一致, 同时保持普通聚合 Block 的 64KB 上限以保护 `in_block_offset: u16` 哨兵空间。

## 1. 设计决策

| 项目 | 决策 |
|------|------|
| Record header | `data_len: u32 LE` + `timestamp: i64 LE`, 固定 12 字节 |
| 超大 record | 继续走独占 Block; `data_len` 可表达 >64KB payload |
| 普通 Block 上限 | 64KB hard cap, 防止多 record 聚合时 `in_block_offset` 超过 `u16` 可表达范围或撞 `0xFFFF` 哨兵 |
| IndexEntry | 保持 18B 不变, `in_block_offset: u16` 不扩展 |
| BlockHeader | 保持 16B 不变, `payload_size` / `uncompressed_size` 继续使用 `u32` |
| 最大单条数据 | 受 `u32 data_len` 和 `BlockHeader.payload_size: u32` 共同约束 |

## 2. 改动范围

- `design.md` / `docs/design/*`: 统一 record 编码、record overhead、读取偏移和哨兵合法性说明
- `src/segment/data.rs`: 写入、独占 Block、纠正写入、读取路径全部改为 4 字节长度
- `src/cache.rs`: HotBlockCache 提取逻辑改为读取 `u32 data_len`
- `src/segment/mod.rs`: 新 Block 最小空间判断改为 12B record header
- `src/block.rs` / `src/config.rs`: 明确普通 Block payload hard cap 为 64KB
- 测试: 新增 `data.len() > u16::MAX` 的 roundtrip 回归测试

## 3. 验收标准

- [x] 设计文档已明确 `data_len: u32` 与 12B record header
- [x] 计划文档已记录 P0-1 修复范围
- [x] 新增 >64KB 单条 record roundtrip 测试, 修复前失败
- [x] 写入、读取、缓存和纠正写入路径均使用 `u32 data_len`
- [x] `block_max_size` 普通聚合上限保持 64KB
- [x] `cargo fmt -- --check` 通过
- [x] `cargo clippy --all-targets -- -D warnings` 通过
- [x] `cargo test -- --test-threads=1` 通过

---

**相关**: [数据模型](../design/data-model.md) | [数据段管理](../design/data-segment.md) | [查询迭代器](../design/query-iterator.md)
