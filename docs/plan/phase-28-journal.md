# Phase 28: Journal 变更日志

> 目标: 基于内置 `.journal/logs` Dataset 实现可配置变更日志, 记录 dataset 创建/删除和数据写入/删除, 并支持普通 read/query 与 queue 实时 poll 消费。  
> 状态: 计划完成, 待实现

## 28.0 设计文档

- [Journal 变更日志](../design/journal.md)
- [Store 与 FFI API](../design/store-and-ffi.md)
- [Queue 模块 — 整体架构与 API](../design/queue-overview.md)
- [数据集操作](../design/dataset-operations.md)
- [数据模型](../design/data-model.md)

## 28.1 范围与非目标

### 实现范围

- [ ] 新增 `src/journal/mod.rs`, 实现 journal record 编码/解码、metadata snapshot、sequence timestamp 和 `JournalManager`
- [ ] `StoreConfig` 增加 `enable_journal: bool`, 默认开启, builder 与 FFI 配置同步支持
- [ ] `Store::open` 在 `enable_journal=true` 时单独 open/create 内置 `.journal/logs`
- [ ] 普通 dataset `create/drop/write/delete` 成功后追加四类 journal record
- [ ] `.journal/logs` 支持受控只读 open, 可执行 `read/query/query_iter/latest_timestamp/open_queue`
- [ ] `open_journal_queue()` 和 `.journal/logs` 的 queue poll 支持实时消费日志
- [ ] 禁止外部 create/write/delete/drop `.journal/logs`, 禁止 journal queue 外部 `push`
- [ ] Rust/FFI/header/Python wrapper 计划同步, 本阶段至少完成 Rust 与 C ABI 基础集成

### 非目标

- [ ] 不引入 WAL、事务、commit marker 或二阶段提交
- [ ] 不保证 journal 与业务 dataset 的 mmap 落盘顺序
- [ ] 不做 journal retention/checkpoint/compaction
- [ ] 不用 journal 替代全量扫描或严格故障恢复机制
- [ ] 不记录 `.journal/logs` 自身操作, 避免递归

## 28.2 核心数据结构与二进制编解码

**目标**: 先完成可独立测试的 journal payload codec, 后续 Store hook 只调用稳定接口。

### 实现任务

- [ ] **创建 `src/journal/mod.rs`**
  - [ ] 导出 `JournalManager`, `JournalRecord`, `JournalRecordKind`, `JournalIndexInfo`
  - [ ] 定义 `JOURNAL_DATASET_NAME = ".journal"` 和 `JOURNAL_DATASET_TYPE = "logs"`
  - [ ] 定义日志类型常量: `0x01`, `0x02`, `0x11`, `0x12`
  - [ ] 定义 TLV 常量: `0x01=name`, `0x02=type`, `0x03=metadata/index_info`
- [ ] **实现 record encoder**
  - [ ] payload 外层格式: `log_type:u8 + length:u16 LE + TLV bytes`
  - [ ] TLV 格式: `type:u8 + length:u16 LE + value`
  - [ ] `encode_create(name, dataset_type, meta_values)`
  - [ ] `encode_drop(name, dataset_type, meta_values)`
  - [ ] `encode_data_write(name, dataset_type, index_info)`
  - [ ] `encode_data_delete(name, dataset_type, index_info)`
- [ ] **实现 record decoder**
  - [ ] 校验 outer length 不越界
  - [ ] 未知 log type 可返回 `JournalRecordKind::Unknown(u8)` 或可跳过结构
  - [ ] 未知 TLV type 可跳过
  - [ ] 已知日志类型必须校验必需 TLV 存在且长度正确
  - [ ] `metadata` 长度必须能放入 `u16`
  - [ ] `index_info` 必须固定 18 字节
- [ ] **实现整数规则**
  - [ ] 所有 on-disk integer 使用 Little Endian
  - [ ] outer/TLV length 使用 `u16`
  - [ ] `index_info.timestamp` 使用 `i64 LE`
  - [ ] `index_info.block_offset` 使用 `u64 LE`
  - [ ] `index_info.in_block_offset` 使用 `u16 LE`
  - [ ] length 溢出返回 `TmslError::InvalidData`

### 测试策略

- [ ] **单元测试**: `test_journal_encode_decode_create`
  - encode `0x01`, decode 后校验 name/type/meta_values
- [ ] **单元测试**: `test_journal_encode_decode_drop`
  - encode `0x02`, decode 后校验 metadata 与 create 使用相同规则
- [ ] **单元测试**: `test_journal_encode_decode_data_write`
  - 校验 `timestamp/block_offset/in_block_offset` 的 LE 字节序
- [ ] **单元测试**: `test_journal_encode_decode_data_delete`
  - 校验删除记录保存旧真实 index entry, 不保存 filler sentinel
- [ ] **单元测试**: `test_journal_decode_rejects_truncated_outer_length`
- [ ] **单元测试**: `test_journal_decode_rejects_truncated_tlv`
- [ ] **单元测试**: `test_journal_decode_skips_unknown_tlv`
- [ ] **单元测试**: `test_journal_encode_rejects_oversized_tlv_value`

### 验收标准

- [ ] codec 单元测试全部通过
- [ ] decoder 对越界 length 无 panic, 返回明确错误
- [ ] 未知 TLV 不影响已知字段解析

---

## 28.3 StoreConfig 与 FFI 配置扩展

**目标**: 在不影响现有默认行为的前提下增加 journal 开关, 默认开启。

### 实现任务

- [ ] **修改 `src/config.rs`**
  - [ ] `StoreConfig` 增加 `enable_journal: bool`
  - [ ] `StoreConfig::default()` 设置 `enable_journal=true`
  - [ ] `StoreConfigBuilder` 增加 `enable_journal: Option<bool>`
  - [ ] 增加 `StoreConfigBuilder::enable_journal(enable: bool)`
  - [ ] `DataSetConfig::from_store()` 不从 store 派生 journal 字段, journal 是 Store 级能力
- [ ] **修改 `src/ffi.rs`**
  - [ ] `TmslStoreConfigFFI` 增加 `enable_journal: u8`
  - [ ] 提升 `TMSL_STORE_CONFIG_FFI_VERSION`
  - [ ] `store_config_to_ffi()` 写入 `enable_journal`
  - [ ] `store_config_from_ffi()` 读取 `enable_journal`
  - [ ] 若保留旧版本兼容, 旧版本缺失字段按默认 `true` 处理
- [ ] **修改 `include/timslite.h`**
  - [ ] 同步 `TmslStoreConfigFFI` 字段与版本
  - [ ] 注释说明 `enable_journal=0` 禁用 `.journal/logs`
- [ ] **修改 wrapper 配置绑定**
  - [ ] `wrapper/python` 若已有 StoreConfig wrapper, 同步暴露 `enable_journal`
  - [ ] 旧构造路径默认开启 journal

### 测试策略

- [ ] **单元测试**: `test_store_config_default_enable_journal`
- [ ] **单元测试**: `test_store_config_builder_disable_journal`
- [ ] **FFI 测试**: `test_ffi_store_config_default_enable_journal`
- [ ] **FFI 测试**: `test_ffi_store_config_disable_journal`

### 验收标准

- [ ] Rust 默认配置开启 journal
- [ ] FFI 默认配置开启 journal
- [ ] 显式禁用后 Store 不创建、不打开、不追加 journal

---

## 28.4 JournalManager 生命周期

**目标**: `Store::open` 单独管理 `.journal/logs`, 普通 dataset 扫描不把它当成可写业务 dataset。

### 实现任务

- [ ] **实现 `JournalManager`**
  - [ ] `Enabled { dataset: Arc<Mutex<DataSet>>, queue: Option<DatasetQueue> }`
  - [ ] `Disabled`
  - [ ] `open_or_create(data_dir, config) -> Result<JournalManager>`
  - [ ] `is_enabled() -> bool`
  - [ ] `open_readonly_dataset() -> Result<Arc<Mutex<DataSet>>>`
  - [ ] `open_queue() -> Result<DatasetQueue>`
  - [ ] `flush() -> Result<()>`
- [ ] **创建 journal dataset**
  - [ ] 路径固定为 `{data_dir}/.journal/logs`
  - [ ] 不走公共 name/type 校验
  - [ ] `index_continuous=false`
  - [ ] `retention_ms=0`
  - [ ] segment size / initial size / compress_level 继承 StoreConfig 默认 dataset 参数
  - [ ] 创建 `.journal/logs` 不写 `0x01` journal
- [ ] **打开 journal dataset**
  - [ ] 磁盘存在时从 meta 打开
  - [ ] 若 `enable_journal=false`, 即使磁盘存在 `.journal/logs` 也不打开
  - [ ] 打开失败返回 Store open 错误
- [ ] **修改 `src/store.rs`**
  - [ ] `Store` 增加 `journal: JournalManager`
  - [ ] `Store::open` 先初始化 journal, 再扫描普通 dataset
  - [ ] 普通扫描跳过 `.journal`
  - [ ] 后台任务 flush/idle-close 如需处理 journal, 通过 `JournalManager` 访问

### 测试策略

- [ ] **集成测试**: `test_store_open_creates_journal_by_default`
  - `Store::open` 后存在 `.journal/logs/meta`
- [ ] **集成测试**: `test_store_open_disable_journal_does_not_create_journal`
  - `enable_journal=false` 后不存在 `.journal`
- [ ] **集成测试**: `test_store_open_disable_journal_ignores_existing_journal`
  - 先默认创建, 再禁用打开, `.journal/logs` public open 返回 NotFound
- [ ] **集成测试**: `test_journal_not_listed_as_public_dataset`
  - 普通扫描不会把 `.journal` 当成可写业务 dataset

### 验收标准

- [ ] `.journal/logs` 生命周期由 `JournalManager` 独立管理
- [ ] `enable_journal=false` 下所有 journal public 入口不可用
- [ ] 内部 journal 操作不会递归写 journal

---

## 28.5 Dataset Create/Drop 日志

**目标**: dataset 创建和删除成功后追加 `0x01/0x02` 日志, metadata 使用 meta 文件除 header 外的 TLV bytes。

### 实现任务

- [ ] **实现 metadata snapshot**
  - [ ] 从 `{dataset}/meta` 读取完整 meta 文件
  - [ ] 校验 meta 固定 header: magic/version/meta_data_length
  - [ ] 提取 header 后 `meta_data_length` 字节作为 TLV bytes
  - [ ] meta 不存在或损坏时返回错误
- [ ] **集成 create hook**
  - [ ] `Store::create_dataset_with_config` 成功创建普通 dataset 后提取 metadata
  - [ ] 调用 `journal.append_create(key, meta_values)`
  - [ ] journal append 失败不回滚已创建 dataset, 但 API 返回错误
  - [ ] public create 拒绝 `.journal/logs`
- [ ] **集成 drop hook**
  - [ ] 删除前读取 metadata snapshot
  - [ ] 完成 registry 移除与目录删除后调用 `journal.append_drop(key, meta_values)`
  - [ ] journal append 不持有 datasets registry 写锁
  - [ ] public drop 拒绝 `.journal/logs`

### 测试策略

- [ ] **集成测试**: `test_journal_records_dataset_create`
  - 创建 dataset 后查询 `.journal/logs`, 解码最后一条为 `0x01`
- [ ] **集成测试**: `test_journal_records_dataset_drop`
  - 删除 dataset 后查询 `.journal/logs`, 解码最后一条为 `0x02`
- [ ] **集成测试**: `test_journal_create_drop_metadata_matches_meta_values`
  - 日志 metadata 与原 meta 文件 header 后 bytes 一致
- [ ] **集成测试**: `test_journal_does_not_record_internal_dataset_create`
  - Store 初始化 `.journal/logs` 时无递归 create 日志
- [ ] **集成测试**: `test_public_create_journal_dataset_rejected`
- [ ] **集成测试**: `test_public_drop_journal_dataset_rejected`

### 验收标准

- [ ] 普通 create 成功后有 `0x01`
- [ ] 普通 drop 成功后有 `0x02`
- [ ] create/drop 失败路径不写 journal
- [ ] metadata snapshot 二进制内容可用于后续迁移/恢复工具

---

## 28.6 Data Write/Delete 日志

**目标**: 数据写入和删除成功后追加 `0x11/0x12` 日志, 记录最终或旧真实 `IndexEntry`。

### 实现任务

- [ ] **修改 `src/dataset.rs` 写入返回值**
  - [ ] 新增 `WriteOutcome { index_entry: IndexEntry, branch: WriteBranch }`
  - [ ] `write_with_cache` 内部在 normal/correction/out-of-order/continuous_backfill 成功后返回最终 index entry
  - [ ] 保留 `write()` 兼容 wrapper, 或调整调用点后统一处理
  - [ ] 直接调用 `DataSet::write` 默认不写 journal
- [ ] **修改 `src/dataset.rs` 删除返回值**
  - [ ] 新增 `DeleteOutcome { old_index_entry: IndexEntry }`
  - [ ] `delete_with_cache` 成功删除真实 entry 后返回删除前 index entry
  - [ ] 不存在、filler、过期删除失败路径不返回 outcome
- [ ] **修改 `src/store.rs` 写入/删除门面**
  - [ ] 对所有 Store/FFI 可达写入路径, 写业务数据成功后调用 `journal.append_data_write`
  - [ ] 对所有 Store/FFI 可达删除路径, 删除成功后调用 `journal.append_data_delete`
  - [ ] `enable_journal=false` 时 hook 为 no-op
  - [ ] journal append 失败不回滚业务写入/删除, 但 API 返回错误
- [ ] **补齐 `JournalManager.append_*`**
  - [ ] 根据 `latest_written_timestamp()` 与当前时间生成严格递增 `journal_ts`
  - [ ] `journal_ts=max(now, last+1)`
  - [ ] `last==i64::MAX` 返回 `InvalidData`
  - [ ] append 成功后通知 journal queue consumer

### 测试策略

- [ ] **集成测试**: `test_journal_records_data_write_normal`
  - 普通写入后存在 `0x11`, index_info 指向最终 entry
- [ ] **集成测试**: `test_journal_records_data_write_correction`
  - correction 写入后仍存在 `0x11`
- [ ] **集成测试**: `test_journal_records_data_write_out_of_order`
  - 乱序写入后 `0x11` 使用更新后的 index entry
- [ ] **集成测试**: `test_journal_records_data_delete`
  - 删除真实 entry 后存在 `0x12`, index_info 为删除前旧 entry
- [ ] **集成测试**: `test_journal_does_not_record_failed_delete`
  - 不存在/filler/过期删除失败不写 `0x12`
- [ ] **集成测试**: `test_journal_disabled_no_write_delete_records`
  - 禁用 journal 后业务写入/删除成功但没有 journal append
- [ ] **集成测试**: `test_journal_sequence_timestamp_monotonic`
  - 连续 create/write/delete/drop 的 journal ts 严格递增

### 验收标准

- [ ] `0x11` 总是记录业务 timestamp 对应的最终 index entry
- [ ] `0x12` 总是记录删除前的旧真实 index entry
- [ ] correction/delete/out-of-order 与缓存失效、index 更新顺序保持一致
- [ ] journal queue consumer 可被 append 唤醒

---

## 28.7 只读 Journal Dataset 与 Queue

**目标**: journal 既能作为普通 dataset 查询历史, 也能通过 queue 实时 poll。

### 实现任务

- [ ] **只读 handle 设计**
  - [ ] 数据集 registry 或 handle metadata 增加 `read_only_internal` 标记
  - [ ] `.journal/logs` public open 仅在 `enable_journal=true` 时允许
  - [ ] 允许操作: `read/query/query_iter/latest_timestamp/open_queue/close`
  - [ ] 禁止操作: `write/delete/drop/create`
  - [ ] 禁止只读 handle 参与普通 drop_dataset
- [ ] **Store API**
  - [ ] `Store::open_dataset(".journal", "logs")` 返回只读 journal handle
  - [ ] `Store::open_journal_queue() -> Result<DatasetQueue>`
  - [ ] `Store::open_queue(handle)` 对 journal 只读 handle 返回 journal queue
  - [ ] `Store::queue_push(queue, data)` 对 journal queue 返回 `InvalidData`
- [ ] **Queue 集成**
  - [ ] `DatasetQueue` 或 `QueueInner` 增加 `producer_mode` / `read_only_producer` 标记
  - [ ] 普通 queue 保持 `push()` 可写
  - [ ] journal queue 的 producer 只能是 `JournalManager.append_*`
  - [ ] append 成功后复用 queue notify 唤醒 consumer

### 测试策略

- [ ] **集成测试**: `test_public_open_journal_dataset_readonly`
  - 能打开 `.journal/logs`, 能 `read(-1)` 读取最新日志
- [ ] **集成测试**: `test_public_journal_query_returns_raw_payloads`
  - `query` 返回 payload 可被 `JournalRecord::decode` 解码
- [ ] **集成测试**: `test_public_journal_write_rejected`
- [ ] **集成测试**: `test_public_journal_delete_rejected`
- [ ] **集成测试**: `test_public_journal_drop_rejected`
- [ ] **集成测试**: `test_open_journal_queue_poll_existing_records`
- [ ] **集成测试**: `test_open_journal_queue_poll_realtime_append`
  - 线程 A poll 等待, 线程 B create/write 普通 dataset, A 被唤醒
- [ ] **集成测试**: `test_journal_queue_push_rejected`
- [ ] **集成测试**: `test_journal_queue_ack_persists_checkpoint`

### 验收标准

- [ ] journal 历史可通过 read/query/query_iter 批量读取
- [ ] journal 实时变更可通过 queue poll 消费
- [ ] 外部无法伪造 journal record
- [ ] `enable_journal=false` 时所有 journal public open/queue 请求返回 NotFound

---

## 28.8 FFI 与 Wrapper API 集成

**目标**: 让 C ABI 可配置 journal, 可打开只读 journal dataset, 可消费 journal queue。

### 实现任务

- [ ] **复用现有 dataset FFI**
  - [ ] `tmsl_dataset_open(store, ".journal", "logs", ...)` 在 journal enabled 时返回只读 handle
  - [ ] `tmsl_dataset_read/query/latest_timestamp` 支持 journal handle
  - [ ] `tmsl_dataset_write/delete/drop` 对 journal handle 返回错误
- [ ] **复用现有 queue FFI**
  - [ ] `tmsl_queue_open(dataset, ...)` 支持 journal read-only handle
  - [ ] `tmsl_queue_push` 对 journal queue 返回错误
  - [ ] `tmsl_queue_consumer_poll/ack` 支持 journal queue
- [ ] **新增便利 FFI 可选项**
  - [ ] 如现有 FFI registry 结构允许, 增加 `tmsl_journal_queue_open(store, ...)`
  - [ ] 若不新增函数, 文档明确使用 `tmsl_dataset_open(".journal","logs") + tmsl_queue_open`
- [ ] **Python wrapper**
  - [ ] StoreConfig 暴露 `enable_journal`
  - [ ] 增加 `store.open_journal()` 或文档化 `open_dataset(".journal", "logs")`
  - [ ] 增加 journal queue 使用示例测试

### 测试策略

- [ ] **FFI 测试**: `test_ffi_open_journal_dataset_read_latest`
- [ ] **FFI 测试**: `test_ffi_journal_dataset_write_rejected`
- [ ] **FFI 测试**: `test_ffi_open_journal_queue_poll`
- [ ] **FFI 测试**: `test_ffi_disable_journal_open_not_found`
- [ ] **Python 测试**: `test_journal_enabled_by_default`
- [ ] **Python 测试**: `test_journal_query_decode`
- [ ] **Python 测试**: `test_journal_queue_poll`

### 验收标准

- [ ] C ABI 结构版本与 header 同步
- [ ] 旧默认入口 `tmsl_store_open` 默认开启 journal
- [ ] FFI 子句柄生命周期仍满足 store/dataset/iterator/queue/consumer 关闭规则

---

## 28.9 后台任务、缓存与 Retention

**目标**: journal 不破坏现有 flush/idle/cache/retention 语义。

### 实现任务

- [ ] **Flush**
  - [ ] 后台 flush 包含 journal dataset 的 data/index mmap
  - [ ] journal queue state files 纳入 queue flush 逻辑
  - [ ] 手动 `tick_background_tasks()` 与后台线程均覆盖 journal
- [ ] **Idle-Close**
  - [ ] journal dataset 可由 JournalManager 控制 close/reopen
  - [ ] 若 journal queue 打开, 不 idle-close journal dataset
  - [ ] idle-close 不写 journal
- [ ] **Retention**
  - [ ] journal dataset 默认 `retention_ms=0`
  - [ ] 普通 retention 扫描不把 `.journal/logs` 当业务 dataset 回收
- [ ] **BlockCache**
  - [ ] journal read/query 可复用全局 BlockCache
  - [ ] journal append 不缓存可变 pending block
  - [ ] 若 journal 内部发生 correction/delete, 按普通缓存失效规则处理; v1 append-only 正常不触发

### 测试策略

- [ ] **集成测试**: `test_background_flush_includes_journal`
- [ ] **集成测试**: `test_manual_tick_flushes_journal_queue_state`
- [ ] **集成测试**: `test_retention_does_not_reclaim_journal_by_default`
- [ ] **集成测试**: `test_journal_queue_blocks_idle_close`

### 验收标准

- [ ] 后台任务锁顺序不引入反向获取普通 dataset mutex 的路径
- [ ] journal dataset 不被普通 retention 误删
- [ ] 手动后台执行模式与自动后台线程模式行为一致

---

## 28.10 锁顺序与失败语义

**目标**: 明确实现时必须遵守的并发边界和错误返回策略。

### 锁顺序

```text
Store datasets registry lock
    -> target DataSet mutex
        -> JournalManager dataset mutex
            -> QueueInner
                -> ConsumerStateFile
```

### 实现要求

- [ ] journal append 内部不得调用 Store public create/open/drop/write/delete API
- [ ] 不允许持有 journal dataset mutex 后再获取普通 dataset mutex
- [ ] drop dataset 不得在持有 datasets registry 写锁时执行 journal append
- [ ] create/drop/write/delete 主操作成功但 journal append 失败时不回滚主操作
- [ ] API 返回 journal append 错误时, 文档和测试需体现“主操作可能已生效”
- [ ] append record 顺序为: 业务 payload/index 发布成功后, 再写 journal payload/header/index
- [ ] crash 前未落盘的 journal record 可丢失, 不作为恢复完整性保证

### 测试策略

- [ ] **集成测试**: `test_journal_append_failure_after_create_reports_error_without_rollback`
  - 可通过只读目录或注入测试 hook 模拟 journal 失败
- [ ] **集成测试**: `test_journal_drop_append_after_registry_unlock`
  - 通过并发 open/read 验证 drop 不长时间持有 registry 写锁
- [ ] **并发测试**: `test_concurrent_dataset_writes_serialized_journal_order`
  - 不同 dataset 并发写入, journal ts 严格递增且可解码

### 验收标准

- [ ] 无死锁风险路径
- [ ] journal 失败语义与设计文档一致
- [ ] 并发 append 后 journal record 完整、顺序单调

---

## 28.11 文件清单

### 新增文件

- `src/journal/mod.rs` — JournalManager + journal record encoder/decoder

### 修改文件

- `src/lib.rs` — 导出 `journal` 模块
- `src/config.rs` — `StoreConfig.enable_journal` 与 builder
- `src/error.rs` — 必要时增加 `ReadOnlyDataset` / `ReservedDataset` / journal 相关错误
- `src/store.rs` — journal 生命周期、普通操作 hook、只读 journal open、open_journal_queue
- `src/dataset.rs` — 写入/删除 outcome, 只读内部 handle 所需访问能力
- `src/queue/mod.rs` — journal queue 禁止外部 push, append notify 集成
- `src/bg/mod.rs` — flush/idle/retention 覆盖 journal
- `src/ffi.rs` — StoreConfig FFI、journal 只读 handle、queue FFI 行为
- `include/timslite.h` — FFI 结构版本与字段
- `wrapper/python/*` — Python 配置与 journal/queue wrapper
- `tests/integration_test.rs` 或 `tests/journal_test.rs` — journal 集成测试
- `docs/review/design-review-todo.md` — 若本阶段来自 review todo, 实现完成后更新状态
- `plan.md` / `docs/plan/overview.md` — Phase 状态同步

## 28.12 实施顺序

1. [ ] Codec TDD: 先完成 `src/journal/mod.rs` 的编码/解码测试与实现
2. [ ] Config/FFI TDD: 增加 `enable_journal` 默认值、builder、C ABI 结构版本测试
3. [ ] JournalManager 生命周期: Store open/create 内置 `.journal/logs`, 禁用路径测试
4. [ ] Create/Drop hook: metadata snapshot + `0x01/0x02` 日志测试
5. [ ] Write/Delete outcome: DataSet 返回最终/旧 index entry, Store hook 写 `0x11/0x12`
6. [ ] 只读 journal dataset: public open/read/query/latest, write/delete/drop reject
7. [ ] Journal queue: open_journal_queue, poll realtime append, push reject
8. [ ] FFI/Python: 配置、只读 handle、queue poll 端到端
9. [ ] 后台任务与并发: flush/idle/retention/lock order 场景测试
10. [ ] 文档与 todo: 更新设计审查 todo 状态, 同步 plan 状态

## 28.13 验证命令

本阶段实现完成后按以下顺序验证:

```powershell
cargo fmt -- --check
cargo test journal -- --test-threads=1
cargo test queue -- --test-threads=1
cargo test ffi -- --test-threads=1
cargo test -- --test-threads=1
cargo clippy --all-targets -- -D warnings
```

若更新 Python wrapper, 追加:

```powershell
python -m pytest wrapper/python/tests -q
```

## 28.14 验收总览

- [ ] `.journal/logs` 默认启用, 可通过 `enable_journal=false` 关闭
- [ ] 四类日志 `0x01/0x02/0x11/0x12` 均能编码、写入、读取、解码
- [ ] create/drop/write/delete 成功路径写 journal, 失败路径不写 journal
- [ ] `.journal/logs` 可 read/query/query_iter/latest_timestamp
- [ ] journal queue 可实时 poll, 可 ack checkpoint, 禁止外部 push
- [ ] FFI header 与 Rust 结构版本一致
- [ ] 后台 flush/idle/retention 不破坏 journal 语义
- [ ] 全量 Rust 测试、clippy、fmt 通过
