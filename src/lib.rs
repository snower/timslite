//! timslite - Rust time-series data storage library.
//!
//! A high-performance, mmap-backed time-series data store with:
//! - Block-level aggregation (max 64KB per block)
//! - Delayed compression (seal on overflow)
//! - Lazy segment lifecycle (on-demand open, idle-close after 30min)
//! - Time-indexed queries with binary search
//! - C ABI FFI interface
//! - Explicit create/open/drop lifecycle for datasets

// Intentionally unused pub helpers / FFI-facing API — suppress dead_code warnings
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::wrong_self_convention)]
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use timslite::{Store, StoreConfig};
//!
//! let config = StoreConfig::default();
//! let mut store = Store::open("/data/timslite", config).unwrap();
//!
//! // Create a new dataset (specify segment sizes and compression)
//! store.create_dataset("my_data", "events",
//!     64 * 1024 * 1024,   // data_segment_size = 64MB
//!     4 * 1024 * 1024,    // index_segment_size = 4MB
//!     6,                  // compress_level
//!     0,                  // index_continuous
//!     0,                  // retention_ms
//! ).unwrap();
//!
//! // Open an existing dataset (parameters read from meta file)
//! store.open_dataset("my_data", "events").unwrap();
//! // ... write and query
//! ```

// ─── Module declarations ────────────────────────────────────────────────────
pub mod config;
pub mod error;
pub mod util;

mod bg;
mod block;
mod cache;
mod compress;
mod dataset;
mod ffi;
mod header;
mod index;
mod journal;
mod meta;
#[allow(clippy::module_inception)]
mod query;
pub mod queue;
mod segment;
mod store;

// ─── Public re-exports ──────────────────────────────────────────────────────
pub use bg::TickResult;
pub use config::{DataSetConfig, DataSetConfigBuilder, StoreConfig, StoreConfigBuilder};
pub use dataset::{DataSet, DataSetKey};
pub use error::{Result, TmslError};
pub use index::segment::{IndexEntry, BLOCK_OFFSET_FILLER};
pub use journal::{
    JournalAppendInfo, JournalIndexInfo, JournalRecord, JournalRecordKind, JOURNAL_DATASET_NAME,
    JOURNAL_DATASET_TYPE,
};
pub use query::hot_block::HotBlockCache;
pub use query::iter::{QueryIterator, QuerySource, SourceIndex};
pub use queue::{DatasetQueue, DatasetQueueConsumer, PendingEntry};
pub use store::{DataSetHandle, Store};

// ─── Queue constants (exported for FFI consumers) ───────────────────────────

/// Queue state file magic bytes ("QSTF").
pub const QUEUE_STATE_MAGIC: [u8; 4] = *queue::QUEUE_STATE_MAGIC;

/// Queue state file version.
pub const QUEUE_STATE_VERSION: u32 = queue::QUEUE_STATE_VERSION;

/// Queue state file size in bytes (4KB).
pub const QUEUE_STATE_FILE_SIZE: usize = queue::STATE_FILE_SIZE;

/// Maximum pending entries per consumer group.
pub const QUEUE_MAX_PENDING_ENTRIES: usize = queue::MAX_PENDING_ENTRIES;

// ─── Core constants (exported for FFI consumers) ────────────────────────────

/// Data segment v1 default header size in bytes.
pub const DATA_HEADER_SIZE: u64 = header::DATA_HEADER_SIZE;

/// Index segment v1 default header size in bytes.
pub const INDEX_HEADER_SIZE: u64 = header::INDEX_HEADER_SIZE;

/// Size of a block header in bytes.
pub const BLOCK_HEADER_SIZE: u64 = block::BLOCK_HEADER_SIZE;

/// Maximum payload size of a normal aggregated block in bytes.
pub const BLOCK_MAX_SIZE: u32 = block::BLOCK_MAX_SIZE;

/// Size of an index entry in bytes.
pub const INDEX_ENTRY_SIZE: usize = index::INDEX_ENTRY_SIZE;

/// Magic bytes identifying a timslite file.
pub const MAGIC: [u8; 4] = header::MAGIC;

/// Current file format version.
pub const VERSION: u16 = header::VERSION;

/// File type: data segment.
pub const FILE_TYPE_DATA: u8 = header::FILE_TYPE_DATA;

/// File type: index segment.
pub const FILE_TYPE_INDEX: u8 = header::FILE_TYPE_INDEX;

// ─── Default crate-level test ───────────────────────────────────────────────
#[cfg(test)]
mod tests {
    #[test]
    fn test_constants_nonzero() {
        assert_eq!(crate::DATA_HEADER_SIZE, 116);
        assert_eq!(crate::INDEX_HEADER_SIZE, 52);
        assert_eq!(crate::BLOCK_HEADER_SIZE, 16);
        assert_eq!(crate::BLOCK_MAX_SIZE, 65_536);
        assert_eq!(crate::INDEX_ENTRY_SIZE, 18);
        assert_eq!(&crate::MAGIC, b"TMSL");
        assert_eq!(crate::VERSION, 1);
        assert_eq!(crate::FILE_TYPE_DATA, 2);
        assert_eq!(crate::FILE_TYPE_INDEX, 1);
    }
}
