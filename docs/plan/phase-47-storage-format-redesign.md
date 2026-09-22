# Phase 47: 不兼容存储格式重设计

## 目标

在代码变更前锁定新的不可兼容 data/index 存储格式：index segment 使用固定 32 字节 little-endian `IndexEntry`，普通聚合 Block 的未压缩 payload 上限为 256KiB，Block payload 内的 record stored span 按 4 字节对齐。

## 设计决策

| 项目 | 决策 |
|---|---|
| IndexEntry 大小 | 固定 32 字节 |
| IndexEntry timestamp | `timestamp: i64 LE`，保存完整业务时间戳，不使用 timestamp delta |
| IndexEntry block offset | `block_offset: u64 LE`，保持数据区逻辑全局偏移语义 |
| IndexEntry in-block offset | `in_block_offset_units: u16 LE`，单位为 4 字节，字节偏移为 `in_block_offset_units * 4` |
| IndexEntry reserved | 最后 14 字节固定为零，写入清零，读取拒绝非零值 |
| 普通聚合 Block 上限 | 未压缩 payload 最大 256KiB (262144 字节)，不代表每个 Block 在磁盘上固定预分配 256KiB |
| Record stored span | `align_up(12 + data_len, 4)` 字节，尾部 0 到 3 字节为零 padding |
| 兼容性 | 不兼容既有 data_dir、data/index segment 或 index entry；不提供读取、迁移或自动升级，已有数据必须删除后重建 |

`JournalIndexInfo` 不属于 index segment 物理格式，继续保持 `timestamp:i64 + block_offset:u64 + in_block_offset:u16` 的 18 字节格式。

## Checklist

- [x] `design.md` 与 `docs/design/*` 已定义 32 字节固定 index entry、256KiB 普通聚合 Block 未压缩 payload 上限及 4 字节 record stored span 对齐。
- [x] 已明确 index entry 的完整 `timestamp:i64 LE`、`block_offset:u64 LE`、4 字节单位的 `in_block_offset_units:u16 LE` 和 14 个零 reserved bytes。
- [x] 已明确字节偏移按 `in_block_offset_units * 4` 解码，record padding 必须为零。
- [x] 已明确本次格式不兼容，既有 data_dir 必须删除后重建，且不提供兼容读取、迁移或自动升级。
- [ ] 更新 `IndexEntry` 的内存字段、序列化、反序列化、filler/delete 标记、查询和恢复路径。
- [ ] 更新 `IndexSegment` 容量、连续模式槽位计算、二分查找和所有固定 entry 读写路径以使用 32 字节条目。
- [ ] 更新 record 写入、遍历、读取、纠正写入、append、压缩和缓存路径，写入并跳过 4 字节对齐零 padding。
- [ ] 更新普通聚合 Block overflow、append 和 single-record 判定以使用 256KiB 未压缩 payload 上限，且不引入 Block 固定预分配。
- [ ] 删除旧格式读取或 timestamp-delta 兼容分支，open 时拒绝不符合新格式的现有文件。
- [ ] 增加 32 字节 index entry、reserved 非零拒绝、offset units decode、record padding、256KiB 边界、reopen 和不兼容旧文件拒绝的回归测试。

## 验证

- `cargo test index:: -- --test-threads=1`
- `cargo test segment:: -- --test-threads=1`
- `cargo test -- --test-threads=1`
- `cargo fmt -- --check`
- `cargo clippy --all-targets -- -D warnings`
