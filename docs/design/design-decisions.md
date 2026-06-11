# 关键设计决策

## 二十、与 TimeStore 的差异

| 对比项 | TimeStore (Java) | timslite (Rust) |
|--------|------------------|-----------------|
| 存储单元 | 单条 record | Block (多条聚合, ≤64KB) |
| 压缩粒度 | record | Block |
| 压缩时机 | 立即 | 延迟 (pending overflow 或 exclusive/single-record block 创建时) |
| 内存映射 | MappedByteBuffer | memmap2::MmapMut, 懒加载/超时关闭(30min) |
| 元数据 | Protobuf | 可变长度 header (v1 data=116B, index=52B, meta/state 分离) |
| 索引目录 | 同级子目录 | `data/` + `index/` 独立子目录 |
| 索引条目 | 16B (ts+offset) | 18B (ts+block+in_block) |
| 文件头 | 64B | 可变长度 header (meta/state分离) |
| Record编码 | size+ts+data | data_len(u32)+ts(i64)+data |
| FFI | 无 | `extern "C"` |

## 二十二、关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储单元 | Block 聚合 | 提高压缩率, 减少 overhead |
| Block 上限 | 64KB | 适配 L1/L2 缓存 |
| 压缩时机 | 延迟 (pending→sealed, 仅 next write overflow 或 exclusive/single-record block) | 写入时零 CPU, 避免重复压缩 |
| exclusive/single-record block | 单条 record 独占 block | 支持编码后大小超过普通聚合 Block 上限的单条 record |
| Record 编码 | data_len(4)+ts(8)+data | 支持 block 内随机定位, `u32` 长度可表达超大独占 record |
| 索引条目 | 18 字节 | 精确定位到 block 内 record |
| 文件头 | 可变长度 | meta(不可变TLV)/state(可变)分离, 打开文件时按 header 中长度计算数据区起点 |
| meta 扩展 | TLV {type:1}{len:2}{value:N} | 未知 type 通过 length 跳过, 向前兼容 |
| 索引目录 | `data/` + `index/` 独立子目录 | 数据与索引物理隔离 |
| 并发 | DataSet 级 Mutex | 不同数据集独立 |
| flush 行为 | 仅 msync (不 seal/不压缩) | 降低 flush CPU 开销 |
| flush 间隔 | 可配置, 默认 10min | 平衡数据持久化与性能 |
| crash 模型 | 高性能、允许最近写入丢失, 不做事务恢复 | 目标场景对数据损失不敏感, 避免 WAL/二阶段提交带来的写放大 |
| append 可见性 | payload → block header/state → index | index 是查询发布点; index 前失败则不可见, index 后读取需校验 timestamp/边界 |
| segment 生命周期 | 懒打开/超时关闭 (30min) | 控制内存占用 |
| idle-close pending | 保持 pending raw | idle-close 只 sync+unmap, reopen 从 header 恢复 pending, 不引入 sealed raw 中间态 |
| **创建/打开分离** | `create` (带参数) / `open` (读 meta) | 防止误创建, 参数不可变 |
| **参数不可变** | 创建后 segment_size/compress_level 不可修改 | 影响文件布局 |
| **读缓存** | compressed block 的解压 payload | 跳过重复解压; compressed block 不允许原地修改, 具备全局缓存不可变性 |
| **缓存规则** | 只缓存读取 compressed block 后的解压数据, 不缓存 raw block 或写入数据 | pending raw block 可能追加、seal 或被 correction 原地修改; sealed raw 为非法状态 |
| **LRU 水位** | 降至 max_memory × 0.85 | 留 15% 余量, 减少淘汰频率 |
| **缓存禁用** | `cache_max_memory=0` | 零额外开销 |
| **Filler 哨兵** | `block_offset=0xFFFFFFFFFFFFFFFF` | `block_offset` 语义为数据区逻辑全局 offset, 合法全局偏移远低于该值, 零成本识别 |
| **目录名规则** | `^[0-9A-Za-z_-]+$` | name/type 直接作为目录名, 禁止转义和路径穿越字符 |
| **Journal** | 默认启用的内置 `.journal/logs` dataset 记录变更日志 | 复用现有 dataset/queue 存储能力; 可 read/query/open_queue 实时消费; 可通过 `enable_journal=false` 关闭; 不升级为事务 WAL |
| **retention 调度时区** | UTC hour | 与 UNIX epoch 日边界计算一致, 避免本地时区/DST 依赖 |
| **compaction** | 当前不支持 | `invalid_record_count` 只统计无效记录规模, 物理回收仅由 retention 整段删除 |

---

**相关**: [架构概览](architecture.md) | [压缩策略](compression.md) | [索引连续存储](index-continuous.md)
