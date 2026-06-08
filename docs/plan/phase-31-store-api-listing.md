# Phase 31: Store API - Dataset 枚举接口

## 概述

为 Store 添加两个数据集枚举 API：
- `get_dataset_names`: 获取所有 dataset 名称列表
- `get_dataset_types`: 获取指定 dataset 名称的所有类型列表

## 设计要点

### API 设计

```rust
// Store 公共 API
pub fn get_dataset_names(&self) -> Result<Vec<String>>;
pub fn get_dataset_types(&self, name: &str) -> Result<Vec<String>>;
```

### 实现方式

- 从 `Store.datasets: Arc<RwLock<HashMap<DataSetKey, Arc<Mutex<DataSet>>>>>` 中提取
- `get_dataset_names`: 遍历所有 key，收集 unique names，去重后排序返回
- `get_dataset_types`: 遍历所有 key，筛选匹配 name 的 types，排序后返回
- 线程安全：使用 `RwLock::read()` 获取读锁

### FFI 设计

```c
// 获取 dataset 名称列表
int tmsl_store_get_dataset_names(
    void *store,
    char **out_names,      // malloc'd array of malloc'd strings
    uint32_t *out_count,   // number of names returned
    char *err_buf,
    size_t err_buf_len
);

// 获取指定 name 的类型列表
int tmsl_store_get_dataset_types(
    void *store,
    const char *name,
    char **out_types,      // malloc'd array of malloc'd strings
    uint32_t *out_count,   // number of types returned
    char *err_buf,
    size_t err_buf_len
);

// 释放枚举结果
void tmsl_free_string_array(char **arr, uint32_t count);
```

### Python Wrapper 设计

```python
class PyStore:
    def get_dataset_names(self) -> list[str]: ...
    def get_dataset_types(self, name: str) -> list[str]: ...
```

## 实现任务

- [x] 设计文档更新 (docs/design/store-and-ffi.md)
- [x] 计划文档创建 (本文件)
- [x] plan.md 更新
- [x] Store 公共 API 实现
- [x] FFI 函数实现
- [x] C 头文件更新 (include/timslite.h)
- [x] Python wrapper 更新
- [x] 集成测试编写
- [x] 验证: cargo build + test + fmt + clippy

## 测试用例

### 集成测试

1. ✅ `test_get_dataset_names_empty`: 空 store 返回空列表
2. ✅ `test_get_dataset_names_after_create`: 创建多个 dataset 后返回正确名称列表
3. ✅ `test_get_dataset_names_dedup`: 同名不同类型的 dataset 名称去重
4. ✅ `test_get_dataset_types`: 获取指定 name 的所有类型
5. ✅ `test_get_dataset_types_not_found`: 不存在的 name 返回空列表
6. ✅ `test_get_dataset_types_after_drop`: drop 后类型列表更新

## 验收标准

- [x] `cargo build` 成功
- [x] `cargo test -- --test-threads=1` 全部通过
- [x] `cargo fmt -- --check` 无格式问题
- [x] `cargo clippy -- -D warnings` 无警告
- [x] Python wrapper 编译通过
