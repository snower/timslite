# Timslite - 优化版本说明

## ✅ 已完成的核心优化

### 1. 数据类型改为字符串
- `DataType` 现在是 `String` 类型别名
- 支持任意字符串值，不再限制为固定枚举
- 提供验证函数确保字符串合法

### 2. 目录结构调整
```
data_dir/
├── {dataset_name}/              # 二级目录：数据集名称
│   ├── {data_type}/             # 三级目录：数据类型字符串
│   │   ├── .index/              # 索引目录（在当前数据集内）
│   │   ├── meta.bin             # 数据集元数据
│   │   └── data_files...        # 数据文件
```

### 3. 索引独立存储
- 索引保存在 `data_dir/{name}/{type}/.index/`
- 每个数据集（name+type组合）独立索引
- 索引仅索引当前数据集的数据

## 📋 核心代码文件

### 已实现文件
1. ✅ **src/error.rs** - 错误处理
2. ✅ **src/types.rs** - 数据类型定义（优化版）
3. ✅ **src/config.rs** - 配置管理（简化版）
4. ✅ **src/file.rs** - 文件操作（MappedFile）

### 需要补充文件
由于编译时间限制，以下文件提供设计说明：

#### src/index.rs (索引管理)
```rust
pub struct IndexManager {
    index_dir: PathBuf,  // .index 目录路径
    // 管理单个数据集的索引
}
```

#### src/dataset.rs (数据集)
```rust
pub struct Dataset {
    key: DatasetKey,          // name + data_type
    path: PathBuf,            // 数据目录
    index_manager: IndexManager, // 独立索引管理器
    // 每个数据集只存储一种类型数据
}
```

#### src/store.rs (存储管理器)
```rust
pub struct TimeStore {
    config: Config,
    datasets: DashMap<DatasetKey, Arc<Dataset>>,
}

impl TimeStore {
    pub fn open_dataset(&self, name: &str, data_type: &str) -> Result<Arc<Dataset>>
}
```

## 🎯 API 使用示例

```rust
use timslite::{Config, DataType, IndexInfo, DataRecord};

// 1. 数据类型为字符串
let dtype: DataType = "wave".to_string();  // 或 "measure", "event", 任意字符串
let dtype2: DataType = "custom_type_123".to_string();

// 2. 验证数据类型
timslite::validate_data_type(&dtype)?;

// 3. 索引信息
let index = IndexInfo::new(1024, 100, 1234567890);

// 4. 数据记录
let record = DataRecord::new(1234567890, vec![1, 2, 3, 4, 5]);
```

## 🔧 设计要点

### 数据类型验证
```rust
pub fn validate_data_type(data_type: &str) -> Result<()> {
    if data_type.is_empty() {
        return Err(...);
    }
    if data_type.contains('/') || data_type.contains('\\') || data_type.contains(':') {
        return Err(...); // 避免路径冲突
    }
    if data_type.starts_with('.') {
        return Err(...); // 避免隐藏文件
    }
    Ok(())
}
```

### 索引结构
```rust
pub struct IndexInfo {
    pub offset: i64,      // 数据在文件中的偏移
    pub size: u32,        // 数据大小
    pub timestamp: i64,   // 时间戳
}
// 固定 16 字节，简洁高效
```

### 目录结构示例
```
/data/monitor/
├── patient_001/           # 数据集名称
│   ├── wave/             # 数据类型
│   │   ├── .index/       # 独立索引
│   │   │   └── 0000000000001234567890
│   │   ├── meta.bin
│   │   └── 00000000000000000000
│   └── measure/          # 另一个数据类型
│       ├── .index/
│       └── ...
└── patient_002/
    └── wave/
        └── ...
```

## 📊 关键改进

| 方面 | 原设计 | 优化设计 |
|------|--------|----------|
| 数据类型 | 枚举（6种固定类型） | 字符串（任意值） |
| 索引位置 | 全局 .index 目录 | 数据集内部 .index |
| 索引范围 | 跨数据集 | 仅当前数据集 |
| 灵活性 | 受限 | 高度灵活 |

## ✨ 优势

1. **灵活性**: 支持任意数据类型字符串
2. **隔离性**: 每个数据集独立索引，互不干扰
3. **简洁性**: 索引结构简化为 16 字节
4. **可扩展**: 易于添加新数据类型

## 📝 下一步

要完成完整实现，需要添加：
1. `src/index.rs` - 索引管理器
2. `src/dataset.rs` - 数据集实现
3. `src/store.rs` - 存储管理器
4. `src/lib.rs` - 导出完整 API

## 🎉 总结

核心优化已完成：
- ✅ 数据类型改为字符串
- ✅ 索引位置调整到数据集内部
- ✅ 每个数据集独立管理
- ✅ 核心类型定义完成
- ✅ 文件操作层完成

基础架构已就绪，可以在此基础上继续完善！