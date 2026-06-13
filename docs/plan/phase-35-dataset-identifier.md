# Phase 35: Dataset Identifier

## 目标

为每个普通 dataset 分配 Store 内唯一数字 `identifier`, 并支持通过 identifier 打开 dataset。

本阶段只在后续实现时修改代码; 当前文档阶段先冻结设计契约和开发 checklist。

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

- [ ] `Store` 增加内存字段: `max_identifier: u64`。
- [ ] `Store` 增加内存索引: `identifier_to_key: HashMap<u64, DataSetKey>` 或等价结构。
- [ ] 新增 helper 读取/写入 `{data_dir}/max_identifier`。
- [ ] 新增 helper 读取/写入 `{dataset_dir}/identifier`。
- [ ] 所有数字文件解析必须统一边界校验。

### 2. Store::open

- [ ] 打开 Store 根目录时读取 `max_identifier`, 缺失视为 `0`。
- [ ] 扫描普通 dataset 时要求同时存在 `meta` 和 `identifier`。
- [ ] 读取每个 dataset identifier 并建立 `identifier_to_key`。
- [ ] 检测重复 identifier 并返回 `InvalidData`。
- [ ] 如果扫描到的最大 identifier 大于根目录 `max_identifier`, 写回修正后的最大值。
- [ ] `.journal/logs` 不参与 identifier 扫描和分配。

### 3. create_dataset

- [ ] 创建普通 dataset 时分配 `next_identifier = max_identifier + 1`。
- [ ] 溢出返回 `InvalidData`。
- [ ] 创建 dataset 基础文件后写入 dataset `identifier` 文件。
- [ ] 写入 dataset `identifier` 后再更新 Store 根目录 `max_identifier`。
- [ ] 更新内存 `max_identifier` 和 `identifier_to_key`。
- [ ] create 失败路径不得留下已注册的内存索引。

### 4. Rust API

- [ ] 新增 `Store::open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSetHandle>`。
- [ ] 新增 `Store::dataset_identifier(&self, handle: DataSetHandle) -> Result<u64>`。
- [ ] `identifier == 0` 返回 `InvalidData`。
- [ ] 未找到 identifier 返回 `NotFound`。
- [ ] 已打开 dataset 的复用/新 handle 行为与 `open_dataset(name,type)` 保持一致。

### 5. Inspect / Listing

- [ ] `DataSetInfo` 增加 `identifier: u64`。
- [ ] `inspect_dataset(name,type)` 返回该字段。
- [ ] `get_dataset_names()` / `get_dataset_types(name)` 语义保持不变。

### 6. FFI

- [ ] `include/timslite.h` 增加 `tmsl_dataset_open_by_identifier`。
- [ ] `include/timslite.h` 增加 `tmsl_dataset_identifier`。
- [ ] `TmslDataSetInfo` 增加 `uint64_t identifier`。
- [ ] `src/ffi.rs` 实现对应函数和错误处理。
- [ ] FFI version 如需调整, 同步更新 header 与 decode 逻辑。

### 7. Python Wrapper

- [ ] `PyStore.open_dataset_by_identifier(identifier: int)`。
- [ ] `PyDataset.identifier()` 或 inspect info 字段暴露。
- [ ] Python tests 覆盖创建、reopen、按 id 打开。

## 测试计划

- [ ] 创建多个 dataset 后 identifier 从 1 开始递增。
- [ ] reopen 后通过 identifier 打开 dataset。
- [ ] `max_identifier` 缺失时创建首个 dataset 得到 1。
- [ ] `max_identifier` 落后于 dataset identifier 时, Store open 修正。
- [ ] 重复 identifier 返回 `InvalidData`。
- [ ] 非法 identifier 文件内容返回 `InvalidData`。
- [ ] 缺少 dataset `identifier` 的目录不作为有效 dataset 加载。
- [ ] `open_dataset_by_identifier(0)` 返回 `InvalidData`。
- [ ] 不存在的 identifier 返回 `NotFound`。
- [ ] `.journal/logs` 不分配 public identifier, 不能通过 id 打开。
- [ ] FFI/Python wrapper 覆盖新 API。

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
