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
- [x] 不扫描普通 dataset、不加载 meta/identifier、不建立完整 `identifier_to_key`。
- [x] `identifier_to_key` 改为 create/open/open-by-id/drop 按需 cache。
- [x] `.journal/logs` 不参与 public identifier 分配。

### 3. create_dataset

- [x] 创建普通 dataset 时分配 `next_identifier = max_identifier + 1`。
- [x] 溢出返回 `InvalidData`。
- [x] 创建 dataset 前先推进 Store 根目录 `max_identifier`, crash 允许留下 identifier gap。
- [x] 创建 dataset 基础文件后写入 dataset `identifier` 文件。
- [x] 更新内存 `max_identifier` 和 `identifier_to_key`。
- [x] create 失败路径不得留下已注册的内存索引。

### 4. Rust API

- [x] 新增 `Store::open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSet>`。
- [x] 新增 `DataSet::identifier(&self) -> u64`。
- [x] `identifier == 0` 返回 `InvalidData`。
- [x] 未找到 identifier 返回 `NotFound`。
- [x] cache miss 时临时扫描合法 public dataset 目录并缓存匹配映射。
- [x] 扫描发现重复 identifier 返回 `InvalidData`。
- [x] 已打开 dataset 的复用行为与 `open_dataset(name,type)` 保持一致。

### 5. Inspect / Listing

- [x] `DataSetInfo` 增加 `identifier: u64`。
- [x] `inspect_dataset(name,type)` 返回该字段。
- [x] `get_dataset_names()` / `get_dataset_types(name)` 直接扫描合法 public 目录, 不打开 dataset。
- [x] `inspect_dataset(name,type)` 未打开时按 `open_dataset` 语义加载并保留在 registry。

### 6. FFI

- [x] `wrapper/cffi/include/timslite.h` 增加 `tmsl_dataset_open_by_identifier`。
- [x] `wrapper/cffi/include/timslite.h` 增加 `tmsl_dataset_identifier`。
- [x] `TmslDataSetInfo` 增加 `uint64_t identifier`。
- [x] `wrapper/cffi/src/lib.rs` 实现对应函数和错误处理。
- [x] FFI version 无需调整: 本阶段未修改 versioned config decode 结构。

### 7. Python Wrapper

- [x] `PyStore.open_dataset_by_identifier(identifier: int)`。
- [x] `PyDataset.identifier()` 或 inspect info 字段暴露。
- [x] Python tests 覆盖创建、reopen、按 id 打开。

## 测试计划

- [x] 创建多个 dataset 后 identifier 从 1 开始递增。
- [x] reopen 后通过 identifier 打开 dataset。
- [x] `max_identifier` 缺失时创建首个 dataset 得到 1。
- [x] `max_identifier` 落后于 dataset identifier 时, Store open 不修正, 访问该 dataset 返回 `InvalidData`。
- [x] `open_dataset_by_identifier` 扫描发现重复 identifier 返回 `InvalidData`。
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
