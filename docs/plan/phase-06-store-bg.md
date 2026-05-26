# Phase 6: Store 门面 + 后台任务

**目标**: Store 生命周期管理、数据集管理(create/open/drop)、后台 flush/idle (mmap 生命周期管理)

---

## 6.1 Store 结构

```rust
pub struct Store {
    config: StoreConfig,
    data_dir: PathBuf,
    datasets: HashMap<DataSetKey, Arc<Mutex<DataSet>>>,
    bg_tasks: Option<BackgroundTasks>,
    cache: BlockCache,  // Phase 9 加入
}
```

## 6.2 Store::open

- `fn open(data_dir: &Path, config: StoreConfig) -> Result<Self>`
  - 创建 data_dir (如不存在)
  - 启动后台任务循环

## 6.3 Store::create_dataset

- `fn create_dataset(&mut self, name, dataset_type, data_segment_size, index_segment_size, compress_level) -> Result<DataSetHandle>`
  - 检测 `{data_dir}/{name}/{type}/meta` 是否存在 → 存在返回 `AlreadyExists`
  - 调用 `DataSet::create(...)`
  - 注册到 datasets HashMap, 返回 handle

## 6.4 Store::open_dataset

- `fn open_dataset(&mut self, name, dataset_type) -> Result<DataSetHandle>`
  - 检查是否已在内存中
  - 调用 `DataSet::open(...)` 从 meta 读取参数
  - 注册到 datasets HashMap, 返回 handle

## 6.5–6.7 Store::close_dataset / drop_dataset / close

- `fn close_dataset(&mut self, handle) -> Result<()>` — close 并从 HashMap 移除
- `fn drop_dataset(&mut self, handle) -> Result<()>` — 调用 `DataSet::drop_dataset`, 删除目录树
- `fn close(self) -> Result<()>` — close 所有数据集, 停止后台任务

## 6.8 bg/mod.rs — 单线程后台循环 (合并 flush + idle)

- `pub struct BackgroundTasks { handle: Option<JoinHandle<()>>, shutdown_tx: Option<mpsc::Sender<()>> }`
- `fn start(datasets, flush_interval, idle_timeout) -> Self`:
  - 创建单一 mpsc channel
  - 启动单一线程, 内部循环:
    - 计算 `next_flush = last_flush + flush_interval`
    - 计算 `next_idle = last_idle_check + idle_check_interval` (默认 60s)
    - `recv_timeout(min(next_flush, next_idle) - now)` 等待
    - 超时后检查: 如果到了 flush 时间 → 执行 flush; 如果到了 idle 时间 → 执行 idle check
- `fn stop(self)` - 发送信号, join, 返回
- **删除 `bg/flush.rs` 和 `bg/idle.rs`** (逻辑合并到 mod.rs)

## 验收标准

- [x] 集成测试: `Store::create_dataset` → 创建成功, `create_dataset` 再次调用 → `AlreadyExists`
- [x] 集成测试: `Store::open_dataset` → 打开成功, `open_dataset` 对不存在数据集 → `NotFound`
- [x] 整合测试: `Store::create_dataset` × 2 → write data → flush 10min 触发 sync → close → reopen → 数据仍在
- [x] 整合测试: `Store::drop_dataset` → 删除后目录不可访问 → 重新 `create_dataset` 成功
- [x] 集成测试: Store flush 循环只执行 msync, pending block 保持 raw
- [x] 集成测试: Store idle check 在 30min 后关闭所有 segment, 释放 mmap
- [x] 集成测试: idle-close 后 → write/read 操作 → on-demand reopen → pending 已密封, 数据一致
- [x] 集成测试: Store::close 完整关闭所有资源, 无泄漏
- [x] 目录验证: 所有数据集的 `data/` 和 `index/` 子目录正确创建
- [x] `cargo test` all pass (94 tests: 81 unit + 13 integration)

---

**导航**: [← Phase 5](phase-05-dataset.md) | [→ Phase 7](phase-07-ffi.md)
