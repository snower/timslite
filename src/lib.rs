mod error;
mod types;
mod config;
mod file;

pub use error::{Error, Result};
pub use types::{DataType, IndexInfo, DataRecord, DatasetMeta, ReadOptions, validate_data_type};
pub use config::Config;
pub use file::{MappedFile, FileHeader, offset_to_filename};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
