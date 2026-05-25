//! Configuration types for timslite.
//!
//! `StoreConfig` is the primary configuration at the Store level.
//! `DataSetConfig` is derived internally from `StoreConfig`.

use std::time::Duration;

/// Store-level configuration.
///
/// All datasets under a store share this configuration.
///
/// # Defaults
/// - `flush_interval`: 10 minutes (600s)
/// - `idle_timeout`: 30 minutes (1800s)
/// - `data_segment_size`: 64 MiB
/// - `index_segment_size`: 4 MiB
/// - `block_max_size`: 65536 bytes (64 KiB)
/// - `compress_level`: 6
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Interval between background flush cycles (mmap sync only).
    pub flush_interval: Duration,
    /// Time of inactivity before segments are idle-closed.
    pub idle_timeout: Duration,
    /// Size of each data segment file.
    pub data_segment_size: u64,
    /// Size of each index segment file.
    pub index_segment_size: u64,
    /// Maximum block payload size (excluding block header).
    pub block_max_size: u32,
    /// Deflate compression level (0-9).
    pub compress_level: u8,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(600), // 10 min
            idle_timeout: Duration::from_secs(1800),  // 30 min
            data_segment_size: 64 * 1024 * 1024,      // 64 MiB
            index_segment_size: 4 * 1024 * 1024,      // 4 MiB
            block_max_size: 65536,                    // 64 KiB
            compress_level: 6,
        }
    }
}

impl StoreConfig {
    /// Create a new builder.
    pub fn builder() -> StoreConfigBuilder {
        StoreConfigBuilder::default()
    }
}

/// Builder for `StoreConfig`.
#[derive(Clone, Debug, Default)]
pub struct StoreConfigBuilder {
    flush_interval: Option<Duration>,
    idle_timeout: Option<Duration>,
    data_segment_size: Option<u64>,
    index_segment_size: Option<u64>,
    block_max_size: Option<u32>,
    compress_level: Option<u8>,
}

impl StoreConfigBuilder {
    /// Set the flush interval (mmap sync only, no seal/compress).
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = Some(interval);
        self
    }

    /// Set the idle timeout for segment auto-close.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Set the data segment file size.
    pub fn data_segment_size(mut self, size: u64) -> Self {
        self.data_segment_size = Some(size);
        self
    }

    /// Set the index segment file size.
    pub fn index_segment_size(mut self, size: u64) -> Self {
        self.index_segment_size = Some(size);
        self
    }

    /// Set the maximum block payload size.
    pub fn block_max_size(mut self, size: u32) -> Self {
        self.block_max_size = Some(size);
        self
    }

    /// Set the deflate compression level (0-9).
    pub fn compress_level(mut self, level: u8) -> Self {
        self.compress_level = Some(level.min(9));
        self
    }

    /// Build the `StoreConfig`.
    pub fn build(self) -> StoreConfig {
        let defaults = StoreConfig::default();
        StoreConfig {
            flush_interval: self.flush_interval.unwrap_or(defaults.flush_interval),
            idle_timeout: self.idle_timeout.unwrap_or(defaults.idle_timeout),
            data_segment_size: self.data_segment_size.unwrap_or(defaults.data_segment_size),
            index_segment_size: self
                .index_segment_size
                .unwrap_or(defaults.index_segment_size),
            block_max_size: self.block_max_size.unwrap_or(defaults.block_max_size),
            compress_level: self.compress_level.unwrap_or(defaults.compress_level),
        }
    }
}

/// Dataset-level configuration (derived from `StoreConfig`).
///
/// This is an internal type, not exposed to users.
#[derive(Clone, Debug)]
pub(crate) struct DataSetConfig {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub block_max_size: u32,
    pub compress_level: u8,
}

#[allow(dead_code)]
impl DataSetConfig {
    pub fn from_store(config: &StoreConfig) -> Self {
        Self {
            data_segment_size: config.data_segment_size,
            index_segment_size: config.index_segment_size,
            block_max_size: config.block_max_size,
            compress_level: config.compress_level,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = StoreConfig::default();
        assert_eq!(cfg.flush_interval, Duration::from_secs(600));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(1800));
        assert_eq!(cfg.data_segment_size, 64 * 1024 * 1024);
        assert_eq!(cfg.index_segment_size, 4 * 1024 * 1024);
        assert_eq!(cfg.block_max_size, 65536);
        assert_eq!(cfg.compress_level, 6);
    }

    #[test]
    fn test_builder_all_fields() {
        let cfg = StoreConfig::builder()
            .flush_interval(Duration::from_secs(1200))
            .idle_timeout(Duration::from_secs(3600))
            .data_segment_size(128 * 1024 * 1024)
            .index_segment_size(8 * 1024 * 1024)
            .block_max_size(32768)
            .compress_level(9)
            .build();

        assert_eq!(cfg.flush_interval, Duration::from_secs(1200));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(3600));
        assert_eq!(cfg.data_segment_size, 128 * 1024 * 1024);
        assert_eq!(cfg.index_segment_size, 8 * 1024 * 1024);
        assert_eq!(cfg.block_max_size, 32768);
        assert_eq!(cfg.compress_level, 9);
    }

    #[test]
    fn test_builder_partial() {
        let cfg = StoreConfig::builder()
            .flush_interval(Duration::from_secs(300))
            .build();

        let defaults = StoreConfig::default();
        assert_eq!(cfg.flush_interval, Duration::from_secs(300));
        assert_eq!(cfg.idle_timeout, defaults.idle_timeout);
        assert_eq!(cfg.block_max_size, defaults.block_max_size);
    }

    #[test]
    fn test_builder_compress_level_cap() {
        let cfg = StoreConfig::builder().compress_level(15).build();
        assert_eq!(cfg.compress_level, 9); // capped at 9
    }

    #[test]
    fn test_dataset_config_from_store() {
        let store = StoreConfig::builder()
            .data_segment_size(32 * 1024 * 1024)
            .compress_level(3)
            .build();
        let dataset = DataSetConfig::from_store(&store);
        assert_eq!(dataset.data_segment_size, 32 * 1024 * 1024);
        assert_eq!(dataset.compress_level, 3);
        assert_eq!(dataset.block_max_size, 65536); // default
    }
}
