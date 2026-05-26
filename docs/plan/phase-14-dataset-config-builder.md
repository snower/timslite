# Phase 14: create_dataset Builder 优化

## 概述

优化 `Store::create_dataset` API, 引入 builder 模式替代多个独立参数。未设置的配置项自动继承 store 级别默认值, `index_continuous` 默认值为 0。config 参数可选 (`None` = 全用默认值)。

## 实现细节

### 1. `DataSetConfigBuilder::from_store(store_config)`

新增构造函数, 预填 store 级别的所有配置默认值, 允许用户仅覆盖需要的字段。

```rust
impl DataSetConfigBuilder {
    pub fn from_store(store: &StoreConfig) -> Self {
        Self {
            data_segment_size: Some(store.data_segment_size),
            index_segment_size: Some(store.index_segment_size),
            block_max_size: Some(store.block_max_size),
            compress_level: Some(store.compress_level),
            index_continuous: Some(0),
            initial_data_segment_size: Some(store.initial_data_segment_size),
            initial_index_segment_size: Some(store.initial_index_segment_size),
        }
    }
}
```

**字段默认规则**:
- `data_segment_size` → 继承 `store.data_segment_size`
- `index_segment_size` → 继承 `store.index_segment_size`
- `initial_data_segment_size` → 继承 `store.initial_data_segment_size`
- `initial_index_segment_size` → 继承 `store.initial_index_segment_size`
- `compress_level` → 继承 `store.compress_level`
- `index_continuous` → 默认 0

### 2. `Store::create_dataset_with_config(name, dataset_type, config_builder)`

新核心方法, 接受可选 builder:

```rust
pub fn create_dataset_with_config(
    &mut self,
    name: &str,
    dataset_type: &str,
    config_builder: Option<DataSetConfigBuilder>,
) -> Result<DataSetHandle>
```

- `None` → 使用 `DataSetConfigBuilder::from_store(&self.config).build()` (全部 store 默认值)
- `Some(builder)` → 使用 `builder.build()` (用户自定义 + store 默认值混合)

### 3. `Store::create_dataset(...)` 向后兼容

旧签名保持不变, 内部委托给新方法:

```rust
pub fn create_dataset(
    &mut self, name: &str, dataset_type: &str,
    data_segment_size: u64, index_segment_size: u64,
    compress_level: u8, index_continuous: u8,
) -> Result<DataSetHandle> {
    self.create_dataset_with_config(name, dataset_type, Some(
        DataSetConfigBuilder::from_store(&self.config)
            .data_segment_size(data_segment_size)
            .index_segment_size(index_segment_size)
            .compress_level(compress_level)
            .index_continuous(index_continuous)
    ))
}
```

### 4. 可见性提升

- `DataSetConfigBuilder`: `pub(crate)` → `pub` (Rust 用户需要)
- `DataSetConfig`: `pub(crate)` → `pub` (builder 返回值可见性)
- `lib.rs` 新增导出: `DataSetConfigBuilder`, `DataSetConfig`

### 5. FFI 保持不变

`extern "C"` 接口不支持 builder 模式, `tmsl_dataset_create` 继续使用显式参数, 内部通过旧 API 路径调用。

## 新增测试

### 单元测试
- `test_dataset_config_builder_from_store`: 验证所有字段从 store 正确继承
- `test_dataset_config_builder_from_store_with_overrides`: 验证部分覆盖 + 默认继承

### 集成测试
- `t14_1_create_with_none_config_uses_store_defaults`: `None` config 创建后读写正常
- `t14_2_create_with_builder_override`: 部分覆盖字段 (compress_level=9) 后读写正常
- `t14_3_backward_compat_existing_api`: 旧 API 调用与新 API 行为一致

## 验收标准

- [x] `DataSetConfigBuilder::from_store` 正确预填 store 默认值
- [x] `create_dataset_with_config` 接受 `None` 时使用全部默认值
- [x] `create_dataset_with_config` 接受 `Some(...)` 时仅覆盖指定字段
- [x] 旧 `create_dataset` API 保持向后兼容
- [x] FFI 接口不受影响
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test -- --test-threads=1` 全部通过 (110 tests)
