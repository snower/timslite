//! timslite - Rust time-series data storage library.
//!
//! A high-performance, mmap-backed time-series data store with:
//! - Block-level aggregation (max 64KB per block)
//! - Delayed compression (seal on overflow or idle-close)
//! - Lazy segment lifecycle (on-demand open, idle-close after 30min)
//! - Time-indexed queries with binary search
//! - C ABI FFI interface
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use timslite::{Store, StoreConfig};
//!
//! let config = StoreConfig::default();
//! let store = Store::open("/data/timslite", config).unwrap();
//! let dataset = store.open_dataset("my_data", "events").unwrap();
//! // ... write and query
//! ```

// ─── Module declarations ────────────────────────────────────────────────────
pub mod config;
pub mod error;
pub mod util;

mod bg;
mod block;
mod compress;
mod dataset;
mod ffi;
mod header;
mod index;
mod meta;
mod segment;
mod store;

// ─── Public re-exports ──────────────────────────────────────────────────────
pub use config::{StoreConfig, StoreConfigBuilder};
pub use error::{Result, TmslError};
pub use store::Store;
// pub use ffi::DataSetHandle;    // enabled in Phase 7

// ─── Core constants (exported for FFI consumers) ────────────────────────────

/// Size of the file header in bytes.
pub const HEADER_SIZE: u64 = header::HEADER_SIZE;

/// Size of a block header in bytes.
pub const BLOCK_HEADER_SIZE: u64 = block::BLOCK_HEADER_SIZE;

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
        assert_eq!(crate::HEADER_SIZE, 100);
        assert_eq!(crate::BLOCK_HEADER_SIZE, 16);
        assert_eq!(crate::INDEX_ENTRY_SIZE, 18);
        assert_eq!(&crate::MAGIC, b"TMSL");
        assert_eq!(crate::VERSION, 1);
        assert_eq!(crate::FILE_TYPE_DATA, 2);
        assert_eq!(crate::FILE_TYPE_INDEX, 1);
    }
}
