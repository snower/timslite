# 总体概览

> timslite 开发计划 — 总体架构、里程碑、依赖关系与风险管理

---

## 总体里程碑

```
Phase 1:  项目骨架 + 基础工具                ✅
Phase 2:  文件头 + Block 核心                ✅
Phase 3:  DataSegment 写入/读取              ✅
Phase 4:  时间索引系统                       ✅
Phase 5:  DataSegmentSet + DataSet           ✅
Phase 6:  Store 门面 + 后台任务               ✅
Phase 7:  FFI 接口                           ⚠️ 部分完成 (C链接测试待完成)
Phase 8:  集成测试 + 性能调优                 ⚠️ 核心完成 (基准+Valgrind待完成)
Phase 9:  读缓存池 (BlockCache)              ✅
Phase 10: 索引连续存储                       ✅
Phase 11: 连续模式 O(1) 查询优化             ✅
Phase 12: 分段懒分配 + 倍率扩容              ✅
Phase 13: 查询迭代器 + HotBlockCache         ✅
Phase 14: create_dataset Builder 优化        ✅
Phase 15: Header State 分化                  ✅
Phase 16: 数据保留 (Retention)               ✅
Phase 17: 纠正写入 (Correction Write)        ✅
Phase 18: 乱序写入与删除                     ✅
Phase 19: 单时间戳读取                       ✅
Phase 20: 最新时间戳读取                     ✅
Phase 21: 后台任务手动执行                   ✅
Phase 22: Manual BG Python Wrapper           ✅
Phase 23: Record 长度编码升级为 u32          ✅ (P0-1 修复)
Phase 24: 连续索引稀疏 filler 分段            ✅ (P0-2 修复)
Phase 25: Header 可变长度                   ✅ (P0-3 修复)
Phase 26: GitHub Actions CI/CD               ✅
Phase 27: Queue 模块 (DatasetQueue + Consumer) ✅ 完成
Phase 28: Journal 变更日志 (.journal/logs)   ✅ 完成
Phase 29: Dataset Append API + Journal 0x13 ✅ 完成
Phase 30: Dataset 读操作优化                 ✅ 完成
Phase 31: Store API - Dataset 枚举           ✅ 完成
Phase 32: Dataset Inspect API              ✅ 完成
Phase 33: Dirty Segment Flush Queue        ✅ 完成
Phase 34: Ordered Segment Registry         ✅ 完成
Phase 35: Dataset Identifier               ✅ 完成
Phase 36: Journal 专用无索引存储             ✅ 完成
Phase 37: Journal Record TV Format         ✅ 完成
Phase 38: zstd Frame Checksum              ✅ 完成
Phase 39: Dataset Journal Toggle           ✅ 完成
Phase 40: Dataset Inspect State Cache      ⏳ 文档完成, 待实现
```

## 目录结构变更 (核心)

```
旧: {data_dir}/{name}/{type}/
    ├── {segment_files}     ← 数据段直接在 type/ 下
    ├── .index/             ← 索引目录带前导点
    └── ...

新: {data_dir}/{name}/{type}/
    ├── meta                ← 新增: TLV 元数据文件
    ├── data/               ← 新增: 数据段子目录
    │   └── {segment_files}
    └── index/              ← 重命名: 无前导点
        └── {segment_files}
```

## 依赖关系图

```
Phase 1 (骨架+工具+StoreConfig+meta.rs)
    │
    ├─────────────────────────────┐
    ▼                             ▼
Phase 2 (文件头+Block)       Phase 1 (util.rs)
    │                             │
    ▼                             ▼
Phase 3 (DataSegment + 生命周期) ◄──── Phase 2 (BlockHeader + compress)
    │                             │
    ├──────┐                      │
    ▼      ▼                      ▼
Phase 4 (索引 + 生命周期)  Phase 3
    │
    └──┬───┘
       ▼
Phase 5 (DataSet + DataSegmentSet + lazy open/close + meta file)
       │
       ▼
Phase 6 (Store + 单线程后台任务: flush 10min / idle 60s 统一循环)
       │
       ▼
Phase 7 (FFI 接口)
       │
       ▼
Phase 8 (集成测试 + 性能 + idle-close 恢复测试 + 目录结构验证)
       │
       ▼
Phase 9 (读缓存池: BlockCache LRU + idle 回收 + 读取集成)
        │
        ▼
Phase 10 (索引连续存储: filler 条目 + sentinel 值 + mmap 覆盖写 + meta TLV 扩展) ✅
        │
        ▼
Phase 11 (连续模式 O(1) 查询优化: 直接计算索引位置 + 消除二分查找)
        │
        ▼
Phase 12 (分段懒分配 + 倍率扩容: 初始大小创建, 2x 增长, max=segment_size)
         │
         ▼
Phase 13 (查询迭代器: Virtual Iterator 惰性遍历 + HotBlockCache 无锁局部缓存)
         │
         ▼
Phase 14 (create_dataset Builder: DataSetConfigBuilder + store 默认值继承)
         │
         ▼
Phase 15 (Header State 分化: Data 9 state / Index 1 state)
         │
         ▼
Phase 16 (数据保留: retention_ms TLV + 自动过期回收)
         │
         ▼
Phase 17 (纠正写入: overwrite_in_last_block + timestamp == latest)
         │
         ▼
Phase 18 (乱序写入与删除: update_entry/find_and_delete_entry + invalid_record_count)
         │
         ▼
Phase 19: 单时间戳读取 (read timestamp + FFI tmsl_dataset_read)
         │
         ▼
Phase 20: 最新时间戳读取 (latest_written_timestamp + read(-1) 快捷路径)
         │
         ▼
Phase 21: 后台任务手动执行 (ExecutorState Mutex + tick/next_delay API + FFI)
         │
         ▼
Phase 22: Manual BG Python Wrapper (tick_background_tasks + next_background_delay 绑定)
         │
         ▼
Phase 23 (Record 长度编码升级为 u32: data_len u32 + 12B record header)
         │
         ▼
Phase 24 (连续索引稀疏 filler 分段: base_timestamp + 逻辑空洞 + 边界分段填充)
         │
         ▼
Phase 25 (Header 可变长度: header_len = 9 + meta_length + 2 + state_length)
          │
          ▼
Phase 26 (GitHub Actions CI/CD: Rust 全层测试 + Python 3.9-3.13 矩阵)
          │
          ▼
Phase 27 (Queue 模块: DatasetQueue + Consumer + 4KB 状态文件 + Condvar 通知)
          │
          ▼
Phase 28 (Journal 变更日志: .journal/logs + Store hook + read/query/open_queue)
          │
          ▼
Phase 29 (Dataset Append API: latest tail append + 4MiB limit + journal 0x13)
          │
          ▼
Phase 30 (Dataset 读操作优化: exist/length 快速读取 + iterator)
          │
          ▼
Phase 31 (Store API - Dataset 枚举: list/open metadata)
          │
          ▼
Phase 32 (Dataset Inspect API: dataset runtime metadata inspection)
          │
          ▼
Phase 33 (Dirty Segment Flush Queue: 避免 flush 遍历全部 dataset/segment)
          │
          ▼
Phase 34 (Ordered Segment Registry: BTreeMap 段定位)
          │
          ▼
Phase 35 (Dataset Identifier: Store 分配数字 identifier)
          │
          ▼
Phase 36 (Journal 专用无索引存储: JournalSegment + JournalLog + JournalQueue)
          │
          ▼
Phase 37 (Journal Record TV Format: canonical identifier + log_type-scoped TV)
          │
          ▼
Phase 38 (zstd Frame Checksum: checksum-enabled zstd frames)
          │
          ▼
Phase 39 (Dataset Journal Toggle: per-dataset effective journal switch)
          │
          ▼
Phase 40 (Dataset Inspect State Cache: archived stats file + total/open segment counts)
```

## 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| memmap2 在 Windows 上行为差异 | Phase 3 延迟 | 提前在 Windows 上做 mmap 原型验证 |
| miniz_oxide 压缩率不足 | Phase 3 压缩效果差 | 预留切换 zstd 的能力 |
| FFI panic 跨语言 | 崩溃调用方 | 所有 FFI 函数必须 `catch_unwind` |
| 大量数据集同时打开 | Phase 6 OOM | Store open 时初始所有 segment → closed, 30min idle-close 释放 mmap |
| 索引 binary search 溢出 | 查询错误 | 边界条件充分测试 (0, 1, n entries) |
| pending block crash 恢复失败 | 数据丢失 | reopen 时完整校验 header 一致性, 密封 pending 但不压缩 |
| idle-close 后 reopen 性能 | 延迟增加 | mmap open 开销小 (<1ms), 可接受 |
| idle-check 竞态 | 错误关闭活跃 dataset | double-check last_used_at after write-lock acquired |
| index segment 查询时需遍历所有段 | 查询延迟 | 时间范围过滤: skip 段时间范围不在查询区间内的段 |
| 10min flush 间隔过长 | crash 损失数据 | mmap 写入已有 OS page cache 保护 |
| meta 文件与 config 不一致 | 数据损坏风险 | 不一致时拒绝打开; compress_level 不一致仅警告 |
| index 迁移到 index/ | 旧数据不可读 | 打开时自动重命名 `.index/` → `index/` |
| 数据文件迁移到 data/ | 旧数据不可读 | 打开时自动移动文件到 `data/` 子目录 |
| create vs open 混淆 | 误创建已存在数据集 | create 检查 meta 文件已存在则返回明确错误 |
| 误删数据集 (drop) | 数据丢失不可恢复 | FFI 层添加确认参数 |
| 单线程后台任务阻塞 | flush/idle/cache eviction 互相延迟 | 顺序执行, cache eviction 是内存操作 (毫秒级) |
| 缓存内存超限 | OOM | LRU 淘汰降至 85%, idle 回收每 60s 清理冷数据 |
| 缓存数据一致性 | 返回过期数据 | 只缓存已 seal 的 block, seal 后数据永不修改 |
| Filler 条目爆炸 | Index 体积、CPU、内存随真实 timestamp gap 线性增长 | Phase 24 改为稀疏逻辑空洞, 只填充前后边界分段 |
| Index segment 仅含 filler | 无效磁盘写入 | Phase 24 写入路径跳过中间完整空分段; 纯 filler segment 清理仅保留为兼容措施 |
| 连续模式逆序写入 | 目标可能是真实 entry、filler 或逻辑空洞 | 数据段追加; filler/real 覆盖索引; 逻辑空洞按需创建目标 segment |
| 连续/非连续切换 | 已有数据不兼容 | `index_continuous` 创建后不可变 |
| 扩容 crash | 无 header 损坏风险 | header file_size 不更新, 打开时以磁盘实际大小为准 |
| initial_size 过小 | 频繁扩容降低性能 | 默认 256KB/4KB, 64MB 仅需 9 次扩容 |
| timestamp=0 冲突 | index segment 命名歧义 | timestamp=0 保留为空位标记, 写入时拒绝 |
| 超大 record 长度截断 | `u16 data_len` 无法表达 >64KB 数据 | Record header 升级为 `u32 data_len`, 普通聚合 Block 保持 64KB 上限 |
| Header 扩展读歪数据区 | TLV/state 增长但数据/索引区仍按 116/52 固定起点访问 | Phase 25 改为运行时计算 `header_len`, 所有 Block/Entry 物理定位基于动态 header |
| Journal 写入放大 | create/drop/write/delete/append 会额外写 `.journal/logs` | `StoreConfig.enable_journal=false` 可关闭; Phase 36 移除 journal index, 改为专用 append log 降低磁盘放大 |
| Journal 被误认为事务 WAL | 调用方可能高估 crash 恢复保证 | Phase 28 明确 journal 是 change log, 主操作与 journal append 不回滚、不保证同步落盘 |
| Journal 递归写入 | `.journal/logs` 自身操作再次写 journal 导致无限递归 | JournalManager 内部路径不走 public hook, 普通扫描跳过 `.journal` |
| Journal queue 被外部伪造 | 热迁移/恢复消费者读到非系统日志 | `.journal/logs` queue 禁止外部 `push`, producer 仅允许 `JournalManager.append_*` |
| Journal 专用存储 API 断裂 | `open_dataset(".journal", "logs")` 不再返回 DataSet handle | Phase 36 提供 dedicated Rust/FFI/Python journal read/query/queue API, 文档明确 `.journal/logs` 不是普通 DataSet |
| Journal sequence off-by-one | queue 可能跳过第一条日志或重复消费 | sequence 从 `1` 开始, 统一使用 `next_sequence` 命名, latest 为 `next_sequence - 1` |
| Journal TV 无统一 length | 已知 log_type 内未知字段无法安全跳过 | Phase 37 改为 log_type-scoped fixed schema, known log_type 遇到 schema 外 type 直接 `InvalidData`; 扩展通过新增 log_type 或显式 length extension |
| Journal data record 只存 identifier | 离线 consumer 若没有 catalog 无法解析 name/type | Phase 37 保留 create/drop 中的 identifier + name + type + metadata, replay 工具先建立 identifier catalog |
| zstd frame 未带 checksum | compressed block 损坏可能只能被结构解码或上层大小校验发现 | Phase 38 对新写出的 zstd frame 开启 content checksum, 旧 frame 继续可读 |
| Dataset 级 journal 关闭 | 重要和不重要数据混写时 journal 写放大过高 | Phase 39 新增 `DataSetConfig.enable_journal`, 以全局和 dataset 开关取 AND 得到有效记录状态 |
| inspect 只统计打开分段 | `DataSetState.total_*` 不能反映整个 dataset | Phase 40 新增 `{dataset_dir}/state` 归档统计缓存, inspect 返回归档统计 + active tail 状态 |
| inspect 分段关闭数语义误导 | idle-close 状态被误解为数据覆盖范围 | Phase 40 改为返回 `data_segments` / `index_segments` 总数和 `open_*` 打开数, 不再返回关闭数 |
| Append 修改历史数据 | 旧 timestamp 可能位于 compressed block 或历史段中间, 无法稳定增长 | Phase 29 只允许 `timestamp > latest` 创建或 `timestamp == latest` 且位于未压缩段尾时追加 |
| Append 造成普通 block 过大 | 最新 record 追加后可能超过普通 pending block 容量 | 直接返回错误, 不迁移到独占 block |
| 单条 record 过大 | `data_len:u32` 可表达但资源消耗不可控 | `write` 和 `append` 均限制单条 record 纯数据长度不超过 4MiB |

## 开发规范

1. **原子提交**: 每个 Phase 内的小任务独立提交
2. **TDD**: 先写测试, 再实现 (Phase 2+)
3. **clippy**: `cargo clippy -- -D warnings` 作为 pre-commit check
4. **doc**: 所有 public API 必须有 doc comment
5. **log**: 关键操作 (open/close/flush/error/idle-close/reopen) 必须有日志
6. **no unsafe (except FFI)**: 除 ffi.rs 外, 禁止 unsafe
7. **error handling**: 不 unwrap, 不 expect, 返回 Error 或 Result
8. **mmap safety**: idle-close 必须先 munmap 再 close
9. **last_used_at**: 每次 write/query 操作必须更新

---

**详见各 Phase 文档:**

| 文档 | 内容 | 状态 |
|------|------|------|
| [phase-01-skeleton.md](phase-01-skeleton.md) | 项目骨架 + 基础工具 | ✅ |
| [phase-02-header-block.md](phase-02-header-block.md) | 文件头 + Block 核心 | ✅ |
| [phase-03-datasegment.md](phase-03-datasegment.md) | DataSegment 写入/读取 | ✅ |
| [phase-04-time-index.md](phase-04-time-index.md) | 时间索引系统 | ✅ |
| [phase-05-dataset.md](phase-05-dataset.md) | DataSegmentSet + DataSet | ✅ / ☐ |
| [phase-06-store-bg.md](phase-06-store-bg.md) | Store 门面 + 后台任务 | ✅ / ☐ |
| [phase-07-ffi.md](phase-07-ffi.md) | FFI 接口 | ☐ |
| [phase-08-tests-perf.md](phase-08-tests-perf.md) | 集成测试 + 性能调优 | ☐ |
| [phase-09-blockcache.md](phase-09-blockcache.md) | 读缓存池 | ✅ |
| [phase-10-continuous-storage.md](phase-10-continuous-storage.md) | 索引连续存储 | ✅ |
| [phase-11-o1-optimization.md](phase-11-o1-optimization.md) | 连续模式 O(1) 查询优化 | ✅ |
| [phase-12-lazy-allocation.md](phase-12-lazy-allocation.md) | 分段懒分配 + 倍率扩容 | ✅ / ☐ |
| [phase-13-query-iterator.md](phase-13-query-iterator.md) | 查询迭代器 + HotBlockCache | ☐ |
| [phase-23-record-length-u32.md](phase-23-record-length-u32.md) | Record 长度编码升级为 u32 | ✅ |
| [phase-24-sparse-continuous-index.md](phase-24-sparse-continuous-index.md) | 连续索引稀疏 filler 分段 | ✅ |
| [phase-25-header-variable-length.md](phase-25-header-variable-length.md) | Header 可变长度 | ✅ |
| [phase-26-github-actions-ci.md](phase-26-github-actions-ci.md) | GitHub Actions CI/CD | ✅ |
| [phase-27-queue-module.md](phase-27-queue-module.md) | Queue 模块 | ✅ |
| [phase-28-journal.md](phase-28-journal.md) | Journal 变更日志 | ✅ |
| [phase-29-dataset-append.md](phase-29-dataset-append.md) | Dataset Append API + Journal 0x13 | ✅ |
| [phase-30-dataset-read-operations.md](phase-30-dataset-read-operations.md) | Dataset 读操作优化 | ✅ |
| [phase-31-store-api-listing.md](phase-31-store-api-listing.md) | Store API - Dataset 枚举 | ✅ |
| [phase-32-dataset-inspect.md](phase-32-dataset-inspect.md) | Dataset Inspect API | ✅ |
| [phase-33-dirty-flush-queue.md](phase-33-dirty-flush-queue.md) | Dirty Segment Flush Queue | ✅ |
| [phase-34-ordered-segment-registry.md](phase-34-ordered-segment-registry.md) | Ordered Segment Registry | ✅ |
| [phase-35-dataset-identifier.md](phase-35-dataset-identifier.md) | Dataset Identifier | ✅ |
| [phase-36-journal-dedicated-storage.md](phase-36-journal-dedicated-storage.md) | Journal 专用无索引存储 | ✅ |
| [phase-37-journal-record-tv-format.md](phase-37-journal-record-tv-format.md) | Journal Record TV Format | ✅ |
| [phase-38-zstd-frame-checksum.md](phase-38-zstd-frame-checksum.md) | zstd Frame Checksum | ✅ |
| [phase-39-dataset-journal-toggle.md](phase-39-dataset-journal-toggle.md) | Dataset Journal Toggle | ✅ |
| [phase-40-dataset-inspect-state.md](phase-40-dataset-inspect-state.md) | Dataset Inspect State Cache | ⏳ |
