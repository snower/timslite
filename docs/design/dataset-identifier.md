# Dataset Identifier

## 目标

每个普通 dataset 在创建时分配一个 Store 内唯一的数字 `identifier`。该 identifier 用于后续通过 id 打开 dataset, 也可作为外部系统保存 dataset 引用时的稳定数字键。

identifier 不替代 `(dataset_name, dataset_type)`:

- `(name, type)` 仍是目录路径、日志记录和人工可读管理入口。
- `identifier` 是 Store 级唯一数字, 只在同一个 Store data directory 内保证唯一。
- `.journal/logs` 是内部保留 journal append log, 不参与 public identifier 分配和 `open_dataset_by_identifier`。

## 磁盘文件

Store 根目录新增:

```text
{data_dir}/
├── max_identifier
└── {dataset_name}/
    └── {dataset_type}/
        ├── identifier
        ├── meta
        ├── data/
        └── index/
```

文件格式:

| 文件 | 位置 | 内容 | 编码 |
|---|---|---|---|
| `max_identifier` | Store 根目录 | 已分配过的最大 identifier | ASCII/UTF-8 数字字符串 |
| `identifier` | 每个普通 dataset 目录, 与 `meta` 同级 | 当前 dataset 的 identifier | ASCII/UTF-8 数字字符串 |

约束:

- identifier 类型为 `u64`。
- `0` 保留为无效值; 第一个普通 dataset identifier 为 `1`。
- 文件内容只允许十进制数字, 可接受末尾单个换行; 解析时 trim ASCII whitespace 后必须非空。
- 解析溢出、空文件、负号、非数字字符均返回 `InvalidData`。
- `max_identifier` 不记录 `.journal/logs`。

## Store 打开流程

`Store::open(data_dir, config)`:

1. 确保 Store 根目录存在。
2. 读取 `{data_dir}/max_identifier`:
   - 不存在: 视为 `0`, 后续首次创建 dataset 时生成。
   - 存在: 解析为 `u64`, 失败则返回错误。
3. 扫描普通 dataset 目录 `{data_dir}/{name}/{type}`:
   - 跳过 `.journal` 和非法 public name/type。
   - 只加载同时存在 `meta` 和 `identifier` 的 dataset。
   - 读取并校验每个 dataset 的 identifier。
   - 建立 `identifier -> DataSetKey` 内存索引。
4. 若扫描到的最大 dataset identifier 大于 `max_identifier`, Store open 后应把 `max_identifier` 修正为扫描最大值。

重复 identifier 是数据目录损坏:

- 两个不同普通 dataset 读取到同一个 identifier 时, `Store::open` 返回 `InvalidData`。
- 同一个 `(name,type)` 只允许一个 identifier 文件。

## Dataset 创建流程

`Store::create_dataset*` 创建普通 dataset 时:

1. 校验 name/type 合法且不是 `.journal/logs`。
2. 读取 Store 内存中的 `max_identifier`。
3. `next_identifier = max_identifier.checked_add(1)`, 溢出返回 `InvalidData`。
4. 创建 dataset 目录、`meta`、初始 data/index segment。
5. 写入 `{dataset_dir}/identifier`, 内容为 `next_identifier.to_string()`。
6. 更新 `{data_dir}/max_identifier`, 内容为 `next_identifier.to_string()`。
7. 更新 Store 内存中的 `max_identifier` 和 `identifier -> DataSetKey` 索引。
8. 注入 runtime context, 注册 dataset handle, 执行 journal create hook。

Crash 边界:

- 如果 crash 发生在写入 dataset `identifier` 前, 该 dataset 创建不完整; reopen 时因缺少 `identifier` 不加载, 可由后续清理工具处理。
- 如果 crash 发生在写入 dataset `identifier` 后、更新 `max_identifier` 前, reopen 扫描 dataset 时会发现更大的 identifier 并修正 `max_identifier`。
- 因此 `identifier` 文件必须先于 `max_identifier` 推进写入; 不需要引入事务或二阶段状态文件。

## 打开与查询 API

Rust Store 新增:

```rust
impl Store {
    pub fn open_dataset_by_identifier(&mut self, identifier: u64) -> Result<DataSetHandle>;
    pub fn dataset_identifier(&self, handle: DataSetHandle) -> Result<u64>;
}
```

语义:

- `open_dataset_by_identifier(0)` 返回 `InvalidData`。
- 未找到 identifier 返回 `NotFound`。
- 找到后等价于按对应 `(name,type)` 调用 `open_dataset`。
- 如果目标 dataset 已经在 registry 中打开, 返回新的 handle 或复用现有 registry entry, 行为应与 `open_dataset(name,type)` 一致。
- `.journal/logs` 不支持通过 id 打开。

FFI 后续应同步增加:

```c
void* tmsl_dataset_open_by_identifier(void* store,
                                      uint64_t identifier,
                                      char* err_buf,
                                      size_t err_buf_len);

int tmsl_dataset_identifier(void* dataset,
                            uint64_t* out_identifier,
                            char* err_buf,
                            size_t err_buf_len);
```

Python wrapper 后续应同步暴露:

```python
store.open_dataset_by_identifier(identifier: int) -> DataSet
dataset.identifier() -> int
```

## 与 Inspect / Listing 的关系

`DataSetInfo` 应新增 `identifier: u64`, 便于外部管理界面和调用方展示 id 与 `(name,type)` 的映射。

`Store::get_dataset_names()` / `get_dataset_types(name)` 保持不变。identifier 是额外打开路径, 不改变按名称列举的语义。

## 与 Journal 的关系

Journal v1 create/drop 记录仍以 name/type 为主, 不强制在 TLV 中加入 identifier。identifier 文件属于 dataset 目录元数据, 热迁移工具可以通过 create 记录中的 name/type 打开源 dataset 后读取 identifier。

未来若需要跨 Store 保留相同 identifier, 应在 journal 或 snapshot 设计中显式定义迁移策略; v1 只保证单 Store data directory 内唯一。

## 测试要求

- 创建多个 dataset 后 identifier 从 1 开始递增。
- reopen 后可通过 identifier 打开 dataset。
- `max_identifier` 缺失时, 创建首个 dataset 生成 `1`。
- `max_identifier` 落后于 dataset identifier 时, reopen 扫描并修正。
- 重复 identifier 检测为 `InvalidData`。
- identifier 文件缺失的 dataset 不加载或按设计返回明确错误。
- FFI/Python wrapper 在实现阶段补充对应 API 测试。
