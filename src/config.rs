//! Configuration types for timslite.
//!
//! `StoreConfig` combines Store runtime settings with defaults for newly
//! created datasets. Existing datasets reopen from their own meta files.

use std::time::Duration;

use crate::compress::COMPRESS_TYPE_ZSTD;

/// Store-level configuration.
///
/// Existing datasets do not compare against these defaults when reopened.
///
/// # Defaults
/// - `flush_interval`: 10 minutes (600s)
/// - `idle_timeout`: 30 minutes (1800s)
/// - `data_segment_size`: 64 MiB
/// - `index_segment_size`: 4 MiB
/// - `initial_data_segment_size`: 256 KiB
/// - `initial_index_segment_size`: 4 KiB
/// - `compress_level`: 6
/// - `compress_type`: 0 (zstd)
/// - `cache_max_memory`: 256 MiB (0 = disabled)
/// - `cache_idle_timeout`: 30 minutes (1800s)
/// - `retention_check_hour`: 0 (daily at UTC 00:00)
/// - `enable_background_thread`: true
/// - `enable_journal`: true
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Interval between background flush cycles (mmap sync only).
    pub flush_interval: Duration,
    /// Time of inactivity before segments are idle-closed.
    pub idle_timeout: Duration,
    /// Default data segment file size for newly created datasets.
    pub data_segment_size: u64,
    /// Default index segment file size for newly created datasets.
    pub index_segment_size: u64,
    /// Default initial data segment file size for newly created datasets.
    pub initial_data_segment_size: u64,
    /// Default initial index segment file size for newly created datasets.
    pub initial_index_segment_size: u64,
    /// Default deflate compression level for newly created datasets (0-9).
    pub compress_level: u8,
    /// Default compression algorithm for newly created datasets (0=zstd, 1=deflate).
    pub compress_type: u8,
    /// Maximum memory for the read block cache (bytes, 0 = disabled).
    pub cache_max_memory: usize,
    /// Idle timeout for cache entries (eviction by background thread).
    pub cache_idle_timeout: Duration,
    /// UTC hour (0-23) at which the daily retention reclamation runs.
    pub retention_check_hour: u8,
    /// Whether to launch a background thread. When false, callers must invoke
    /// `Store::tick_background_tasks()` periodically to drive flush/idle/cache/retention.
    pub enable_background_thread: bool,
    /// Whether to enable the built-in `.journal/logs` change log.
    pub enable_journal: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(600), // 10 min
            idle_timeout: Duration::from_secs(1800),  // 30 min
            data_segment_size: 64 * 1024 * 1024,      // 64 MiB
            index_segment_size: 4 * 1024 * 1024,      // 4 MiB
            initial_data_segment_size: 256 * 1024,    // 256 KiB
            initial_index_segment_size: 4 * 1024,     // 4 KiB
            compress_level: 6,
            compress_type: COMPRESS_TYPE_ZSTD,
            cache_max_memory: 256 * 1024 * 1024, // 256 MiB
            cache_idle_timeout: Duration::from_secs(1800), // 30 min
            retention_check_hour: 0,             // UTC 00:00
            enable_background_thread: true,      // default: auto thread
            enable_journal: true,                // default: change log enabled
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
    initial_data_segment_size: Option<u64>,
    initial_index_segment_size: Option<u64>,
    compress_level: Option<u8>,
    compress_type: Option<u8>,
    cache_max_memory: Option<usize>,
    cache_idle_timeout: Option<Duration>,
    retention_check_hour: Option<u8>,
    enable_background_thread: Option<bool>,
    enable_journal: Option<bool>,
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

    /// Set the index segment file size (max/expansion limit).
    pub fn index_segment_size(mut self, size: u64) -> Self {
        self.index_segment_size = Some(size);
        self
    }

    /// Set the initial data segment file size (expanded up to data_segment_size).
    pub fn initial_data_segment_size(mut self, size: u64) -> Self {
        self.initial_data_segment_size = Some(size);
        self
    }

    /// Set the initial index segment file size (expanded up to index_segment_size).
    pub fn initial_index_segment_size(mut self, size: u64) -> Self {
        self.initial_index_segment_size = Some(size);
        self
    }

    /// Set the deflate compression level (0-9).
    pub fn compress_level(mut self, level: u8) -> Self {
        self.compress_level = Some(level.min(9));
        self
    }

    /// Set the compression algorithm (0=zstd, 1=deflate).
    pub fn compress_type(mut self, compress_type: u8) -> Self {
        self.compress_type = Some(compress_type);
        self
    }

    /// Set the maximum memory for the read block cache (bytes, 0 = disabled).
    pub fn cache_max_memory(mut self, size: usize) -> Self {
        self.cache_max_memory = Some(size);
        self
    }

    /// Set the idle timeout for cache entries.
    pub fn cache_idle_timeout(mut self, timeout: Duration) -> Self {
        self.cache_idle_timeout = Some(timeout);
        self
    }

    /// Set the daily retention reclamation UTC hour (0-23, default 0 = UTC 00:00).
    pub fn retention_check_hour(mut self, hour: u8) -> Self {
        self.retention_check_hour = Some(hour.clamp(0, 23));
        self
    }

    /// Whether to launch a background thread (default true).
    ///
    /// When `false`, callers must invoke `Store::tick_background_tasks()` periodically
    /// to drive flush, idle-close, cache eviction, and retention reclaim.
    pub fn enable_background_thread(mut self, enable: bool) -> Self {
        self.enable_background_thread = Some(enable);
        self
    }

    /// Whether to enable the built-in `.journal/logs` change log (default true).
    pub fn enable_journal(mut self, enable: bool) -> Self {
        self.enable_journal = Some(enable);
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
            initial_data_segment_size: self
                .initial_data_segment_size
                .unwrap_or(defaults.initial_data_segment_size),
            initial_index_segment_size: self
                .initial_index_segment_size
                .unwrap_or(defaults.initial_index_segment_size),
            compress_level: self.compress_level.unwrap_or(defaults.compress_level),
            compress_type: self.compress_type.unwrap_or(defaults.compress_type),
            cache_max_memory: self.cache_max_memory.unwrap_or(defaults.cache_max_memory),
            cache_idle_timeout: self
                .cache_idle_timeout
                .unwrap_or(defaults.cache_idle_timeout),
            retention_check_hour: self
                .retention_check_hour
                .unwrap_or(defaults.retention_check_hour),
            enable_background_thread: self
                .enable_background_thread
                .unwrap_or(defaults.enable_background_thread),
            enable_journal: self.enable_journal.unwrap_or(defaults.enable_journal),
        }
    }
}

/// Dataset-level creation/open configuration.
#[derive(Clone, Debug)]
pub struct DataSetConfig {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub index_continuous: u8,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    /// Data validity period in same unit as timestamps. 0 = no limit.
    pub retention_window: u64,
}

#[allow(dead_code)]
impl DataSetConfig {
    pub fn from_store(config: &StoreConfig) -> Self {
        Self {
            data_segment_size: config.data_segment_size,
            index_segment_size: config.index_segment_size,
            compress_level: config.compress_level,
            compress_type: config.compress_type,
            index_continuous: 0,
            initial_data_segment_size: config.initial_data_segment_size,
            initial_index_segment_size: config.initial_index_segment_size,
            retention_window: 0,
        }
    }

    /// Create a new builder.
    pub fn builder() -> DataSetConfigBuilder {
        DataSetConfigBuilder::default()
    }
}

/// Builder for `DataSetConfig`.
///
/// Use `DataSetConfigBuilder::from_store(store_config)` to pre-fill with store-level defaults,
/// then override specific fields. Unset fields inherit store defaults; `index_continuous` defaults to 0.
///
/// # Example
/// ```ignore
/// let config = StoreConfig::default();
/// let mut store = Store::open("/data/timslite", config)?;
///
/// // Use store defaults for everything
/// store.create_dataset_with_config("sensor", "temp", None)?;
///
/// // Override specific fields
/// store.create_dataset_with_config("sensor", "temp", Some(
///     DataSetConfigBuilder::from_store(&config)
///         .compress_level(9)
///         .index_continuous(1)
/// ))?;
/// ```
#[derive(Clone, Debug, Default)]
pub struct DataSetConfigBuilder {
    data_segment_size: Option<u64>,
    index_segment_size: Option<u64>,
    compress_level: Option<u8>,
    compress_type: Option<u8>,
    index_continuous: Option<u8>,
    initial_data_segment_size: Option<u64>,
    initial_index_segment_size: Option<u64>,
    retention_window: Option<u64>,
}

impl DataSetConfigBuilder {
    /// Create a builder pre-filled with store-level defaults.
    /// Unset fields will fall back to the store config; `index_continuous` defaults to 0.
    pub fn from_store(store: &StoreConfig) -> Self {
        Self {
            data_segment_size: Some(store.data_segment_size),
            index_segment_size: Some(store.index_segment_size),
            compress_level: Some(store.compress_level),
            compress_type: Some(store.compress_type),
            index_continuous: Some(0),
            initial_data_segment_size: Some(store.initial_data_segment_size),
            initial_index_segment_size: Some(store.initial_index_segment_size),
            retention_window: Some(0),
        }
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

    /// Set the deflate compression level (0-9).
    pub fn compress_level(mut self, level: u8) -> Self {
        self.compress_level = Some(level.min(9));
        self
    }

    /// Set the compression algorithm (0=zstd, 1=deflate).
    pub fn compress_type(mut self, compress_type: u8) -> Self {
        self.compress_type = Some(compress_type);
        self
    }

    /// Sets the index_continuous flag (0=non-continuous, 1=continuous storage).
    pub fn index_continuous(mut self, value: u8) -> Self {
        self.index_continuous = Some(value.clamp(0, 1));
        self
    }

    /// Set the initial data segment file size.
    pub fn initial_data_segment_size(mut self, size: u64) -> Self {
        self.initial_data_segment_size = Some(size);
        self
    }

    /// Set the initial index segment file size.
    pub fn initial_index_segment_size(mut self, size: u64) -> Self {
        self.initial_index_segment_size = Some(size);
        self
    }

    /// Set the data retention period in timestamp units (0 = no limit).
    pub fn retention_window(mut self, units: u64) -> Self {
        self.retention_window = Some(units);
        self
    }

    /// Build the `DataSetConfig`.
    pub fn build(self) -> DataSetConfig {
        let defaults = DataSetConfig::from_store(&StoreConfig::default());
        DataSetConfig {
            data_segment_size: self.data_segment_size.unwrap_or(defaults.data_segment_size),
            index_segment_size: self
                .index_segment_size
                .unwrap_or(defaults.index_segment_size),
            compress_level: self.compress_level.unwrap_or(defaults.compress_level),
            compress_type: self.compress_type.unwrap_or(defaults.compress_type),
            index_continuous: self.index_continuous.unwrap_or(0),
            initial_data_segment_size: self
                .initial_data_segment_size
                .unwrap_or(defaults.initial_data_segment_size),
            initial_index_segment_size: self
                .initial_index_segment_size
                .unwrap_or(defaults.initial_index_segment_size),
            retention_window: self.retention_window.unwrap_or(0),
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
        assert_eq!(cfg.initial_data_segment_size, 256 * 1024);
        assert_eq!(cfg.initial_index_segment_size, 4 * 1024);
        assert_eq!(cfg.compress_level, 6);
        assert_eq!(cfg.compress_type, crate::compress::COMPRESS_TYPE_ZSTD);
        assert_eq!(cfg.cache_max_memory, 256 * 1024 * 1024);
        assert_eq!(cfg.cache_idle_timeout, Duration::from_secs(1800));
        assert_eq!(cfg.retention_check_hour, 0);
        assert!(cfg.enable_background_thread);
        assert!(cfg.enable_journal);
    }

    #[test]
    fn test_builder_all_fields() {
        let cfg = StoreConfig::builder()
            .flush_interval(Duration::from_secs(1200))
            .idle_timeout(Duration::from_secs(3600))
            .data_segment_size(128 * 1024 * 1024)
            .index_segment_size(8 * 1024 * 1024)
            .initial_data_segment_size(512 * 1024)
            .initial_index_segment_size(8 * 1024)
            .compress_level(9)
            .cache_max_memory(128 * 1024 * 1024)
            .cache_idle_timeout(Duration::from_secs(600))
            .retention_check_hour(2)
            .enable_journal(false)
            .build();

        assert_eq!(cfg.flush_interval, Duration::from_secs(1200));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(3600));
        assert_eq!(cfg.data_segment_size, 128 * 1024 * 1024);
        assert_eq!(cfg.index_segment_size, 8 * 1024 * 1024);
        assert_eq!(cfg.initial_data_segment_size, 512 * 1024);
        assert_eq!(cfg.initial_index_segment_size, 8 * 1024);
        assert_eq!(cfg.compress_level, 9);
        assert_eq!(cfg.cache_max_memory, 128 * 1024 * 1024);
        assert_eq!(cfg.cache_idle_timeout, Duration::from_secs(600));
        assert_eq!(cfg.retention_check_hour, 2);
        assert!(!cfg.enable_journal);
    }

    #[test]
    fn test_builder_partial() {
        let cfg = StoreConfig::builder()
            .flush_interval(Duration::from_secs(300))
            .build();

        let defaults = StoreConfig::default();
        assert_eq!(cfg.flush_interval, Duration::from_secs(300));
        assert_eq!(cfg.idle_timeout, defaults.idle_timeout);
        assert_eq!(cfg.retention_check_hour, defaults.retention_check_hour);
    }

    #[test]
    fn test_builder_compress_level_cap() {
        let cfg = StoreConfig::builder().compress_level(15).build();
        assert_eq!(cfg.compress_level, 9); // capped at 9
    }

    #[test]
    fn test_builder_retention_check_hour_clamp() {
        let cfg = StoreConfig::builder().retention_check_hour(99).build();
        assert_eq!(cfg.retention_check_hour, 23);
        let cfg2 = StoreConfig::builder().retention_check_hour(12).build();
        assert_eq!(cfg2.retention_check_hour, 12);
    }

    #[test]
    fn test_builder_disable_background_thread() {
        let cfg = StoreConfig::builder()
            .enable_background_thread(false)
            .build();
        assert!(!cfg.enable_background_thread);
        // default is true
        let default = StoreConfig::default();
        assert!(default.enable_background_thread);
    }

    #[test]
    fn test_builder_disable_journal() {
        let cfg = StoreConfig::builder().enable_journal(false).build();
        assert!(!cfg.enable_journal);
        assert!(StoreConfig::default().enable_journal);
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
        assert_eq!(dataset.retention_window, 0);
    }

    #[test]
    fn test_dataset_config_builder_from_store() {
        let store = StoreConfig::builder()
            .data_segment_size(32 * 1024 * 1024)
            .index_segment_size(8 * 1024 * 1024)
            .compress_level(3)
            .initial_data_segment_size(512 * 1024)
            .initial_index_segment_size(8 * 1024)
            .build();

        let config = DataSetConfigBuilder::from_store(&store).build();
        assert_eq!(config.data_segment_size, 32 * 1024 * 1024);
        assert_eq!(config.index_segment_size, 8 * 1024 * 1024);
        assert_eq!(config.compress_level, 3);
        assert_eq!(config.index_continuous, 0);
        assert_eq!(config.initial_data_segment_size, 512 * 1024);
        assert_eq!(config.initial_index_segment_size, 8 * 1024);
        assert_eq!(config.retention_window, 0);
    }

    #[test]
    fn test_dataset_config_builder_from_store_with_overrides() {
        let store = StoreConfig::builder()
            .data_segment_size(64 * 1024 * 1024)
            .compress_level(6)
            .build();

        let config = DataSetConfigBuilder::from_store(&store)
            .compress_level(9)
            .index_continuous(1)
            .retention_window(30 * 86400)
            .build();

        // Override takes effect
        assert_eq!(config.compress_level, 9);
        assert_eq!(config.index_continuous, 1);
        assert_eq!(config.retention_window, 30 * 86400);
        // Store default is inherited
        assert_eq!(config.data_segment_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_dataset_config_builder_retention_window() {
        let config = DataSetConfigBuilder::default().retention_window(30).build();

        assert_eq!(config.retention_window, 30);
    }
}
