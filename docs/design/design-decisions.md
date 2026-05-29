# 关键设计决策

## 二十、与 TimeStore 的差异

| 对比项 | TimeStore (Java) | timslite (Rust) |
|--------|------------------|-----------------|
| 存储单元 | 单条 record | Block (多条聚合, ≤64KB) |
| 压缩粒度 | record | Block |
| 压缩时机 | 立即 | 延迟 (pending→sealed, 溢出或 idle-close 时) |
| 内存映射 | MappedByteBuffer | memmap2::MmapMut, 懒加载/超时关闭(30min) |
| 元数据 | Protobuf | 100字节 header (meta/state 分离) |
| 索引目录 | 同级子目录 | `data/` + `index/` 独立子目录 |
| 索引条目 | 16B (ts+offset) | 18B (ts+block+in_block) |
| 文件头 | 64B | 100B (meta/state分离) |
| Record编码 | size+ts+data | data_len+ts+data |
| FFI | 无 | `extern "C"` |

## 二十二、关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储单元 | Block 聚合 | 提高压缩率, 减少 overhead |
| Block 上限 | 64KB | 适配 L1/L2 缓存 |
| 压缩时机 | 延迟 (pending→sealed) | 写入时零 CPU, 避免重复压缩 |
| 超大 record | 独占 block | 不截断数据 |
| Record 编码 | data_len(4)+ts(8)+data | 支持 block 内随机定位, `u32` 长度可表达超大独占 record |
| 索引条目 | 18 字节 | 精确定位到 block 内 record |
| 文件头 | 100 字节 | meta(不可变TLV)/state(可变7×8B)分离, 版本化扩展 |
| meta 扩展 | TLV {type:1}{len:2}{value:N} | 未知 type 通过 length 跳过, 向前兼容 |
| 索引目录 | `data/` + `index/` 独立子目录 | 数据与索引物理隔离 |
| 并发 | DataSet 级 Mutex | 不同数据集独立 |
| flush 行为 | 仅 msync (不 seal/不压缩) | 降低 flush CPU 开销 |
| flush 间隔 | 可配置, 默认 10min | 平衡数据持久化与性能 |
| segment 生命周期 | 懒打开/超时关闭 (30min) | 控制内存占用 |
| idle-close pending | 密封 (不压缩) | 保证 reopen 后一致 |
| **创建/打开分离** | `create` (带参数) / `open` (读 meta) | 防止误创建, 参数不可变 |
| **参数不可变** | 创建后 segment_size/compress_level 不可修改 | 影响文件布局 |
| **读缓存** | 解压后的 seal block payload | 跳过文件读取+解压, 价值最高 |
| **缓存规则** | 只缓存读取解压数据, 不缓存写入 | seal 前不可预测, seal 后不可变 |
| **LRU 水位** | 降至 max_memory × 0.85 | 留 15% 余量, 减少淘汰频率 |
| **缓存禁用** | `cache_max_memory=0` | 零额外开销 |
| **Filler 哨兵** | `block_offset=0xFFFFFFFFFFFFFFFF` | 远超任何合法偏移, 零成本识别 |

---

**相关**: [架构概览](architecture.md) | [压缩策略](compression.md) | [索引连续存储](index-continuous.md)
