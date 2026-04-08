/// Protobuf definitions (placeholder for now)
/// In production, these would be generated from .proto files

pub mod messages {
    use serde::{Deserialize, Serialize};

    /// Dataset metadata
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DatasetMetadata {
        pub version: u32,
        pub name: String,
        pub data_type: i32,
        pub created_at: i64,
        pub updated_at: i64,
        pub start_timestamp: i64,
        pub end_timestamp: i64,
        pub record_count: u64,
        pub total_size: u64,
    }

    /// Index entry
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct IndexEntry {
        pub wave_offset: i64,
        pub measure_offset: i64,
        pub timestamp: i64,
    }

    /// Data record
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DataRecord {
        pub timestamp: i64,
        pub data: Vec<u8>,
    }
}
