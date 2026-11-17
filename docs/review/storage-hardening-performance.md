# 存储加固与性能优化验收

日期：2026-11-17。对照版本：`b8e34e25d2f89780a4b332b47cae8226635ff97d`。改动未提交，公开 API 与磁盘格式保持兼容。

## 已实现

- 普通队列不再持有 state/QueueInner 锁进行 dataset I/O；领取采用锁外读取、锁内重检 processed/pending/closed 后提交。同组 Journal 竞争失败不返回重复记录。retry 读取错误不提前消耗重试次数。
- seek fallback 使用索引 cursor 而非物化全部剩余索引，并按 timestamp 重新读取当前可见内容，避免 seek 后删除/纠正导致旧 offset 被读取。
- 显式与后台 flush 共用失败恢复：失败和未执行 target 重新排队，保持重试能力。
- 当前与 rollover segment 均循环扩容；默认配置首写 700 KiB 随机记录无需调用方重试，非容量 I/O 错误不会被掩盖。
- block/record offset、长度、逻辑 wrote_position、timestamp、解压长度统一验证；raw block 直接借片解析，只复制返回记录。
- 解压过程限制输出；zstd 读取前限制 history window。segment 最大合法 block 使用 23-bit window 上限（8 MiB）；过大 window 描述符和输出均有回归测试。不自动截断损坏尾部。
- 缓存命中使用读锁和原子访问元数据；容量维护为 O(1)，拒绝超容量条目。淘汰改为 O(n) heapify 加 O(k log n) pop，按原 tick/sequence 顺序驱逐，并与排序 oracle 对照。
- Vec query/query_length 在原单次 dataset 锁内按 cursor 读取，不物化完整中间索引。iterator 每次结果的定位和读取共用一把 dataset 锁，仍为固定范围的弱快照。

## 最终验证

以下命令均退出 0：

```sh
cargo fmt -- --check
cargo clippy -- -D warnings
cargo test -- --test-threads=1
TIMSLITE_BENCH_SCALE=0.1 cargo bench --offline --bench bench_hotpaths
git diff --check
```

最终测试：969 passed、0 failed、2 ignored（30 个结果组）。独立 oracle 在补齐 zstd output/window 限额后通过安全/正确性 gate。并发测试有超时保护，但不等同于穷举所有交错；flush 错误用测试专用注入点验证，不代表实机断电试验。

## 本机热点基准

相同 benchmark 源码，release 模式，scale=0.1。原版从 `git archive HEAD` 在仓库外构建；所有结果校验断言通过。下表是短跑样本，不是统计置信区间，也不是生产容量承诺。

| Case | 原版 | 最终版 | 观察 |
|---|---:|---:|---|
| sparse queue poll+ack，300 条 | 3687.1 ns/op | 1037.8 ns/op | 约 3.6× |
| raw pending read_latest，32B record | 7360.0 ns/op | 261.7 ns/op | 约 28× |
| raw pending read_length | 4077.0 ns/op | 287.3 ns/op | 约 14× |
| compressed cache read，1 线程 | 150.4 ns/op | 283.6 ns/op | 约慢 89% |
| compressed cache read，4 线程 | 171.5 ns/op | 249.2 ns/op | 约慢 45% |
| Vec query，10000 条 | 9.434 ms | 0.975 ms | 约 9.7× |
| query iterator，10000 条 | 10.739 ms | 1.056 ms | 约 10× |

缓存归因对照：HEAD 复跑单线程 162.2 ns/op，单独替换 cache.rs 的两次结果为 155.4–159.2 ns/op；四线程为 157.6–175.8 ns/op。缓存本身没有复现完整读取路径的退化。新增 segment/dataset 校验与完整路径交互仍需进一步 profile；本轮保留安全校验，不以移除校验换性能。尚未建立 cache eviction 专项耗时基线，其复杂度改进由代码和等价性测试支持。

## 保留限制

- 压缩块缓存命中的完整 read 路径仍较原版慢，不能宣称所有读取场景都提速。
- 未新增 MVCC、严格 WAL、事务或损坏尾部自动修复；iterator 不是创建时的完整快照。
- QueueNotifier 的共享通知标志仍可能影响多 consumer 的唤醒延迟，超时重查仍为兜底；本轮未更换通知模型。
- 未运行独立 C ABI/各语言 wrapper 测试，未进行真实断电、跨平台或长期压力测试。

本次计划与检查项见 [storage-hardening-performance.md](../plan/storage-hardening-performance.md)。
