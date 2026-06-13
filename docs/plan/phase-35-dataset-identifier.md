# Phase 35: Dataset Identifier

## 目标

为每个普通 dataset 分配 Store 内唯一数字 `identifier`, 并支持通过 identifier 打开 dataset。

本阶段已完成代码实现与测试覆盖。

## 设计来源

- [Dataset Identifier](../design/dataset-identifier.md)
- [Store 与 FFI API](../design/store-and-ffi.md)
- [架构概览](../design/architecture.md)

## 磁盘格式

新增文件:

```text
{data_dir}/max_identifier
{data_dir}/{name}/{type}/identifier
```

格式:

- 十进制 `u64` 数字字符串。
- `0` 保留为无效值。
- 第一个普通 dataset 从 `1` 开始。
- 可接受末尾换行, 解析前 trim ASCII whitespace。
- 空文件、非数字、负号、溢出均返回 `InvalidData`。

## 实现任务

### 1. Store identifier 管理

- [x] `Store` 增加内存字段: `max_identifier: u64`。
- [x] `Store` 增加内存索引: `identifier_to_key: HashMap<u64, DataSetKey>` 或等价结构。
- [x] 新增 helper 读取/写入 `{data_dir}/max_identifier`。
- [x] 新增 helper 读取/写入 `{dataset_dir}/identifier`。
- [x] 所有数字文件解析必须统一边界校验。

### 2. Store::open

- [x] 打开 Store 根目录时读取 `max_identifier`, 缺失视为 `0`。
- [x] 扫描普通 dataset 时要求同时存在 `meta` 和 `identifier`。
- [x] 读取每个 dataset identifier 并建立 `identifier_to_key`。
- [x] 检测重复 identifier 并返回 `InvalidData`。
- [x] 如果扫描到的最大 identifier 大于根目录 `max_identifier`, 写回修正后的最大值。
- [x] `.journal/logs` 不参与 identifier 扫描和分配。

### 3. create_dataset

- [x] 创建普通 dataset 时分配 `next_identifier = max_identifier + 1`。
- [x] 溢出返回 `InvalidData`。
- [x] 创建 dataset 基础文件后写入 dataset `identifier` 文件。
- [x] 写入 dataset `identifier` 后再更新 Store 根目录 `max_identifier`。
- [x] 更新内存 `max_identifier` 和 `identifier_to_key`。
- [x] create 失败路径不得留下已注册的内存索引。

### 4. Rust API

- [x] 新增 `Store::open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSetHandle>`。
- [x] 新增 `Store::dataset_identifier(&self, handle: DataSetHandle) -> Result<u64>`。
- [x] `identifier == 0` 返回 `InvalidData`。
- [x] 未找到 identifier 返回 `NotFound`。
- [x] 已打开 dataset 的复用/新 handle 行为与 `open_dataset(name,type)` 保持一致。

### 5. Inspect / Listing

- [x] `DataSetInfo` 增加 `identifier: u64`。
- [x] `inspect_dataset(name,type)` 返回该字段。
- [x] `get_dataset_names()` / `get_dataset_types(name)` 语义保持不变。

### 6. FFI

- [x] `include/timslite.h` 增加 `tmsl_dataset_open_by_identifier`。
- [x] `include/timslite.h` 增加 `tmsl_dataset_identifier`。
- [x] `TmslDataSetInfo` 增加 `uint64_t identifier`。
- [x] `src/ffi.rs` 实现对应函数和错误处理。
- [x] FFI version 无需调整: 本阶段未修改 versioned config decode 结构。

### 7. Python Wrapper

- [x] `PyStore.open_dataset_by_identifier(identifier: int)`。
- [x] `PyDataset.identifier()` 或 inspect info 字段暴露。
- [x] Python tests 覆盖创建、reopen、按 id 打开。

## 测试计划

- [x] 创建多个 dataset 后 identifier 从 1 开始递增。
- [x] reopen 后通过 identifier 打开 dataset。
- [x] `max_identifier` 缺失时创建首个 dataset 得到 1。
- [x] `max_identifier` 落后于 dataset identifier 时, Store open 修正。
- [x] 重复 identifier 返回 `InvalidData`。
- [x] 非法 identifier 文件内容返回 `InvalidData`。
- [x] 缺少 dataset `identifier` 的目录不作为有效 dataset 加载。
- [x] `open_dataset_by_identifier(0)` 返回 `InvalidData`。
- [x] 不存在的 identifier 返回 `NotFound`。
- [x] `.journal/logs` 不分配 public identifier, 不能通过 id 打开。
- [x] FFI/Python wrapper 覆盖新 API。

## 验证命令

实现完成后至少执行:

```bash
cargo fmt -- --check
cargo check
cargo test -- --test-threads=1
cargo clippy --all-targets -- -D warnings
```

如修改 Python wrapper:

```bash
cd wrapper/python
maturin develop
python -m pytest tests/ -v
```
