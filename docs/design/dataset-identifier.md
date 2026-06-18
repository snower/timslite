# Dataset Identifier

## 目标

每个普通 dataset 在创建时分配一个 Store 内唯一的数字 `identifier`。该 identifier 用于后续通过 id 打开 dataset, 也可作为外部系统保存 dataset 引用时的稳定数字键。

identifier 不替代 `(dataset_name, dataset_type)`:

- `(name, type)` 仍是目录路径、create/drop journal 记录和人工可读管理入口。
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
3. 初始化空的普通 dataset registry 和空的 `identifier -> DataSetKey` 按需 cache。
4. 不扫描普通 dataset 目录, 不打开普通 dataset, 不修正 `max_identifier`。

`max_identifier` 是 Store 分配 identifier 的权威 high-water mark。普通 dataset 的 `identifier`、`meta` 和 segment 校验延迟到对应访问入口:

- `open_dataset(name,type)` 读取该 dataset 的 `identifier` 和 `meta`。若 dataset identifier 大于权威 `max_identifier`, 返回 `InvalidData`。
- `open_dataset_by_identifier(id)` 在 cache miss 时临时扫描合法 public dataset 目录读取 `identifier`; 若两个不同普通 dataset 匹配同一个 id, 返回 `InvalidData`。
- 同一个 `(name,type)` 只允许一个 identifier 文件。

## Dataset 创建流程

`Store::create_dataset*` 创建普通 dataset 时:

1. 校验 name/type 合法且不是 `.journal/logs`。
2. 读取 Store 内存中的 `max_identifier`。
3. `next_identifier = max_identifier.checked_add(1)`, 溢出返回 `InvalidData`。
4. 确认目标 dataset `meta` 不存在。
5. 更新 `{data_dir}/max_identifier`, 内容为 `next_identifier.to_string()`。
6. 创建 dataset 目录、`meta`、初始 data/index segment。
7. 写入 `{dataset_dir}/identifier`, 内容为 `next_identifier.to_string()`。
8. 更新 Store 内存中的 `max_identifier` 和 `identifier -> DataSetKey` cache。
9. 注入 runtime context, 注册 dataset handle, 执行 journal create hook。

Crash 边界:

- 如果 crash 发生在推进 `max_identifier` 后、写入 dataset `identifier` 前, 该 identifier 号段保留为空洞; 后续 create 从更大的 `max_identifier` 继续分配。
- 如果 crash 留下存在 `meta` 但缺少 `identifier` 的 dataset, `open_dataset` 返回 `NotFound`, 可由后续清理工具处理。
- 如果人工或外部故障使 dataset `identifier` 大于权威 `max_identifier`, 访问该 dataset 时返回 `InvalidData`; Store open 不自动扫描修复。
- 因此 `max_identifier` 必须先于 dataset `identifier` 推进写入; 不需要引入事务或二阶段状态文件。

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
- cache miss 时临时扫描合法 public dataset 目录, 读取 `identifier` 并缓存匹配映射。
- 扫描发现重复 identifier 返回 `InvalidData`。
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

`Store::get_dataset_names()` / `get_dataset_types(name)` 直接扫描合法 public 目录返回名称和类型, 不打开 dataset, 不读取 meta/segments。`inspect_dataset(name,type)` 若目标未打开, 会按 `open_dataset` 语义加载并保留在 Store registry 中。

## 与 Journal 的关系

Journal record 使用 `identifier` 作为高频数据变更记录的紧凑 dataset 引用。

- `0x11`、`0x12`、`0x13` 只存 canonical identifier TV 和固定 payload 字段, 不再重复 `(name,type)` 字符串。
- `0x01` create 和 `0x02` drop 记录保留 identifier、`name`、`dataset_type` 和 metadata, 使审计、迁移和 dataset 删除后的历史解释仍然自描述。
- 处理数据变更记录的 consumer 可通过当前 Store 的按需 `identifier -> DataSetKey` cache 或 create/drop journal catalog 解析目标 dataset; 离线 replay 工具可通过 create/drop catalog 记录建立同样映射。
- `identifier` 仍是 Store-local。跨 Store 迁移若需要保留源 identifier, 必须显式定义源 id 到目标 dataset 的映射; journal record 格式本身不保证全局唯一。

## 测试要求

- 创建多个 dataset 后 identifier 从 1 开始递增。
- reopen 后可通过 identifier 打开 dataset。
- `max_identifier` 缺失时, 创建首个 dataset 生成 `1`。
- `max_identifier` 落后于 dataset identifier 时, Store open 不修正; 访问该 dataset 返回 `InvalidData`。
- `open_dataset_by_identifier` 扫描发现重复 identifier 时返回 `InvalidData`。
- identifier 文件缺失的 dataset 不加载或按设计返回明确错误。
- FFI/Python wrapper 在实现阶段补充对应 API 测试。
