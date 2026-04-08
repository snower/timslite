use serde::{Deserialize, Serialize};

pub type DataType = String;

pub fn validate_data_type(data_type: &str) -> crate::Result<()> {
    if data_type.is_empty() {
        return Err(crate::Error::InvalidConfig("data_type cannot be empty".into()));
    }
    if data_type.contains('/') || data_type.contains('\') || data_type.contains(':') {
        return Err(crate::Error::InvalidConfig("data_type contains invalid chars".into()));
    }
    if data_type.starts_with('.') {
        return Err(crate::Error::InvalidConfig("data_type cannot start with .".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub offset: i64,
    pub size: u32,
    pub timestamp: i64,
}

impl IndexInfo {
    pub fn new(offset: i64, size: u32, timestamp: i64) -> Self {
        Self { offset, size, timestamp }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRecord {
    pub timestamp: i64,
    pub data: Vec<u8>,
}

impl DataRecord {
    pub fn new(timestamp: i64, data: Vec<u8>) -> Self {
        Self { timestamp, data }
    }
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub start_timestamp: i64,
    pub end_timestamp: i64,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self { start_timestamp: 0, end_timestamp: i64::MAX }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMeta {
    pub name: String,
    pub data_type: String,
    pub created_at: i64,
}

impl DatasetMeta {
    pub fn new(name: String, data_type: String) -> Self {
        Self { name, data_type, created_at: chrono::Utc::now().timestamp() }
    }
}
