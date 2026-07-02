//! Configuration types for timslite.
//!
//! `StoreConfig` combines Store runtime settings with defaults for newly
//! created datasets. Existing datasets reopen from their own meta files.

use std::time::Duration;

use crate::compress::COMPRESS_TYPE_ZSTD;
use crate::error::{Result, TmslError};

fn validate_nonzero_size(field: &str, value: u64) -> Result<()> {
    if value == 0 {
        return Err(TmslError::InvalidData(format!("{field} must be > 0")));
    }
    Ok(())
}

pub(crate) fn validate_compress_level(compress_level: u8) -> Result<()> {
    if compress_level > 9 {
        return Err(TmslError::InvalidData(format!(
            "compress_level must be <= 9, got {compress_level}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_index_continuous(index_continuous: u8) -> Result<()> {
    if index_continuous > 1 {
        return Err(TmslError::InvalidData(format!(
            "index_continuous must be 0 or 1, got {index_continuous}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_retention_window(retention_window: u64) -> Result<()> {
    if retention_window > i64::MAX as u64 {
        return Err(TmslError::InvalidData(format!(
            "retention_window must be <= i64::MAX, got {retention_window}"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_dataset_config_values(
    data_segment_size: u64,
    index_segment_size: u64,
    compress_level: u8,
    compress_type: u8,
    index_continuous: u8,
    initial_data_segment_size: u64,
    initial_index_segment_size: u64,
    retention_window: u64,
) -> Result<()> {
    validate_nonzero_size("data_segment_size", data_segment_size)?;
    validate_nonzero_size("index_segment_size", index_segment_size)?;
    validate_nonzero_size("initial_data_segment_size", initial_data_segment_size)?;
    validate_nonzero_size("initial_index_segment_size", initial_index_segment_size)?;
    if initial_data_segment_size > data_segment_size {
        return Err(TmslError::InvalidData(format!(
            "initial_data_segment_size must be <= data_segment_size, got {initial_data_segment_size} > {data_segment_size}"
        )));
    }
    if initial_index_segment_size > index_segment_size {
        return Err(TmslError::InvalidData(format!(
            "initial_index_segment_size must be <= index_segment_size, got {initial_index_segment_size} > {index_segment_size}"
        )));
    }
    validate_compress_level(compress_level)?;
    crate::compress::validate_compress_type(compress_type)?;
    validate_index_continuous(index_continuous)?;
    validate_retention_window(retention_window)?;
    Ok(())
}

/// Store-level configuration.
///
/// Existing datasets do not compare against these defaults when reopened.
///
/// # Defaults
/// - `flush_interval`: 15 seconds
/// - `idle_timeout`: 30 minutes (1800s)
/// - `data_segment_size`: 64 MiB
/// - `index_segment_size`: 16 MiB
/// - `initial_data_segment_size`: 256 KiB
/// - `initial_index_segment_size`: 16 KiB
/// - `compress_level`: 6
/// - `compress_type`: 0 (zstd)
/// - `cache_max_memory`: 256 MiB (0 = disabled)
/// - `cache_idle_timeout`: 30 minutes (1800s)
/// - `retention_check_hour`: 0 (daily at UTC 00:00)
/// - `enable_background_thread`: true
/// - `enable_journal`: true
/// - `read_only`: None (auto: writable if the Store lock can be acquired)
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Interval between background flush cycles (mmap sync only).
    pub(crate) flush_interval: Duration,
    /// Time of inactivity before segments are idle-closed.
    pub(crate) idle_timeout: Duration,
    /// Default data segment file size for newly created datasets.
    pub(crate) data_segment_size: u64,
    /// Default index segment file size for newly created datasets.
    pub(crate) index_segment_size: u64,
    /// Default initial data segment file size for newly created datasets.
    pub(crate) initial_data_segment_size: u64,
    /// Default initial index segment file size for newly created datasets.
    pub(crate) initial_index_segment_size: u64,
    /// Default compression level for newly created datasets (0-9).
    pub(crate) compress_level: u8,
    /// Default compression algorithm for newly created datasets (0=zstd, 1=deflate).
    pub(crate) compress_type: u8,
    /// Maximum memory for the read block cache (bytes, 0 = disabled).
    pub(crate) cache_max_memory: usize,
    /// Idle timeout for cache entries (eviction by background thread).
    pub(crate) cache_idle_timeout: Duration,
    /// UTC hour (0-23) at which the daily retention reclamation runs.
    pub(crate) retention_check_hour: u8,
    /// Whether to launch a background thread. When false, callers must invoke
    /// `Store::tick_background_tasks()` periodically to drive flush/idle/cache/retention.
    pub(crate) enable_background_thread: bool,
    /// Whether to enable the built-in `.journal/logs` change log.
    pub(crate) enable_journal: bool,
    /// Store open mode. None = auto, Some(false) = require writable,
    /// Some(true) = force read-only.
    pub(crate) read_only: Option<bool>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(1800), // 30 min
            data_segment_size: 64 * 1024 * 1024,     // 64 MiB
            index_segment_size: 16 * 1024 * 1024,    // 16 MiB
            initial_data_segment_size: 256 * 1024,   // 256 KiB
            initial_index_segment_size: 16 * 1024,   // 16 KiB
            compress_level: 6,
            compress_type: COMPRESS_TYPE_ZSTD,
            cache_max_memory: 256 * 1024 * 1024, // 256 MiB
            cache_idle_timeout: Duration::from_secs(1800), // 30 min
            retention_check_hour: 0,             // UTC 00:00
            enable_background_thread: true,      // default: auto thread
            enable_journal: true,                // default: change log enabled
            read_only: None,                     // default: auto lock detection
        }
    }
}

impl StoreConfig {
    /// Create a new builder.
    pub fn builder() -> StoreConfigBuilder {
        StoreConfigBuilder::default()
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub fn data_segment_size(&self) -> u64 {
        self.data_segment_size
    }

    pub fn index_segment_size(&self) -> u64 {
        self.index_segment_size
    }

    pub fn initial_data_segment_size(&self) -> u64 {
        self.initial_data_segment_size
    }

    pub fn initial_index_segment_size(&self) -> u64 {
        self.initial_index_segment_size
    }

    pub fn compress_level(&self) -> u8 {
        self.compress_level
    }

    pub fn compress_type(&self) -> u8 {
        self.compress_type
    }

    pub fn cache_max_memory(&self) -> usize {
        self.cache_max_memory
    }

    pub fn cache_idle_timeout(&self) -> Duration {
        self.cache_idle_timeout
    }

    pub fn retention_check_hour(&self) -> u8 {
        self.retention_check_hour
    }

    pub fn enable_background_thread(&self) -> bool {
        self.enable_background_thread
    }

    pub fn enable_journal(&self) -> bool {
        self.enable_journal
    }

    pub fn read_only(&self) -> Option<bool> {
        self.read_only
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_dataset_config_values(
            self.data_segment_size,
            self.index_segment_size,
            self.compress_level,
            self.compress_type,
            0,
            self.initial_data_segment_size,
            self.initial_index_segment_size,
            0,
        )?;
        if self.retention_check_hour > 23 {
            return Err(TmslError::InvalidData(format!(
                "retention_check_hour must be <= 23, got {}",
                self.retention_check_hour
            )));
        }
        Ok(())
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
    read_only: Option<Option<bool>>,
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

    /// Set the compression level (0-9, interpreted by the selected algorithm).
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

    /// Set the Store read-only mode.
    ///
    /// - `None`: auto-detect. Open writable when the Store lock is free,
    ///   otherwise open read-only.
    /// - `Some(false)`: require writable Store lock.
    /// - `Some(true)`: force read-only without checking or taking the Store lock.
    pub fn read_only(mut self, read_only: Option<bool>) -> Self {
        self.read_only = Some(read_only);
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
            read_only: self.read_only.unwrap_or(defaults.read_only),
        }
    }
}

/// Dataset-level creation/open configuration.
#[derive(Clone, Debug)]
pub struct DataSetConfig {
    pub(crate) data_segment_size: u64,
    pub(crate) index_segment_size: u64,
    pub(crate) compress_level: u8,
    pub(crate) compress_type: u8,
    pub(crate) index_continuous: u8,
    pub(crate) initial_data_segment_size: u64,
    pub(crate) initial_index_segment_size: u64,
    /// Data validity period in same unit as timestamps. 0 = no limit.
    pub(crate) retention_window: u64,
    /// Whether this dataset records journal entries when the Store journal is enabled.
    pub(crate) enable_journal: bool,
    /// Dataset creation time (Unix milliseconds).
    pub(crate) create_time: i64,
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
            enable_journal: false,
            create_time: 0,
        }
    }

    /// Create a new builder.
    pub fn builder() -> DataSetConfigBuilder {
        DataSetConfigBuilder::default()
    }

    pub fn data_segment_size(&self) -> u64 {
        self.data_segment_size
    }

    pub fn index_segment_size(&self) -> u64 {
        self.index_segment_size
    }

    pub fn compress_level(&self) -> u8 {
        self.compress_level
    }

    pub fn compress_type(&self) -> u8 {
        self.compress_type
    }

    pub fn index_continuous(&self) -> u8 {
        self.index_continuous
    }

    pub fn initial_data_segment_size(&self) -> u64 {
        self.initial_data_segment_size
    }

    pub fn initial_index_segment_size(&self) -> u64 {
        self.initial_index_segment_size
    }

    pub fn retention_window(&self) -> u64 {
        self.retention_window
    }

    pub fn enable_journal(&self) -> bool {
        self.enable_journal
    }

    pub fn create_time(&self) -> i64 {
        self.create_time
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_dataset_config_values(
            self.data_segment_size,
            self.index_segment_size,
            self.compress_level,
            self.compress_type,
            self.index_continuous,
            self.initial_data_segment_size,
            self.initial_index_segment_size,
            self.retention_window,
        )
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
    enable_journal: Option<bool>,
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
            enable_journal: Some(false),
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

    /// Set the compression level (0-9, interpreted by the selected algorithm).
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

    /// Set whether this dataset records journal entries when the Store journal is enabled.
    pub fn enable_journal(mut self, enable: bool) -> Self {
        self.enable_journal = Some(enable);
        self
    }

    /// Build the `DataSetConfig`.
    pub fn build(self) -> Result<DataSetConfig> {
        let defaults = DataSetConfig::from_store(&StoreConfig::default());
        let retention_window = self.retention_window.unwrap_or(0);
        validate_retention_window(retention_window)?;
        let config = DataSetConfig {
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
            retention_window,
            enable_journal: self.enable_journal.unwrap_or(false),
            create_time: 0, // Set at dataset creation
        };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = StoreConfig::default();
        assert_eq!(cfg.flush_interval, Duration::from_secs(15));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(1800));
        assert_eq!(cfg.data_segment_size, 64 * 1024 * 1024);
        assert_eq!(cfg.index_segment_size, 16 * 1024 * 1024);
        assert_eq!(cfg.initial_data_segment_size, 256 * 1024);
        assert_eq!(cfg.initial_index_segment_size, 16 * 1024);
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
        assert!(!dataset.enable_journal);
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

        let config = DataSetConfigBuilder::from_store(&store).build().unwrap();
        assert_eq!(config.data_segment_size, 32 * 1024 * 1024);
        assert_eq!(config.index_segment_size, 8 * 1024 * 1024);
        assert_eq!(config.compress_level, 3);
        assert_eq!(config.index_continuous, 0);
        assert_eq!(config.initial_data_segment_size, 512 * 1024);
        assert_eq!(config.initial_index_segment_size, 8 * 1024);
        assert_eq!(config.retention_window, 0);
        assert!(!config.enable_journal);
    }

    #[test]
    fn test_dataset_config_builder_default_disables_journal() {
        let config = DataSetConfigBuilder::default().build().unwrap();
        assert!(!config.enable_journal);
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
            .enable_journal(false)
            .build()
            .unwrap();

        // Override takes effect
        assert_eq!(config.compress_level, 9);
        assert_eq!(config.index_continuous, 1);
        assert_eq!(config.retention_window, 30 * 86400);
        assert!(!config.enable_journal);
        // Store default is inherited
        assert_eq!(config.data_segment_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_dataset_config_builder_retention_window() {
        let config = DataSetConfigBuilder::default()
            .retention_window(30)
            .build()
            .unwrap();

        assert_eq!(config.retention_window, 30);
    }

    #[test]
    fn test_dataset_config_builder_rejects_retention_above_i64_max() {
        let result = DataSetConfigBuilder::default()
            .retention_window(i64::MAX as u64 + 1)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_validate_nonzero_size_zero() {
        let result = validate_nonzero_size("test_field", 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("test_field"));
        assert!(msg.contains("> 0"));
    }

    #[test]
    fn test_validate_compress_level_over_9() {
        let result = validate_compress_level(10);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("compress_level"));
        assert!(msg.contains("10"));
    }

    #[test]
    fn test_validate_index_continuous_over_1() {
        let result = validate_index_continuous(2);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("index_continuous"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn test_validate_retention_window_over_i64_max() {
        let ret = i64::MAX as u64 + 1;
        let result = validate_retention_window(ret);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("retention_window"));
        assert!(msg.contains("i64::MAX"));
    }

    #[test]
    fn test_validate_dataset_config_values_initial_over_max() {
        let result = validate_dataset_config_values(1024, 2048, 6, 0, 0, 2048, 1024, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_data_segment_size"));
        assert!(msg.contains("data_segment_size"));
    }

    #[test]
    fn test_builder_read_only_auto() {
        let cfg = StoreConfig::builder().read_only(None).build();
        assert!(cfg.read_only().is_none());
    }

    #[test]
    fn test_builder_read_only_true() {
        let cfg = StoreConfig::builder().read_only(Some(true)).build();
        assert_eq!(cfg.read_only(), Some(true));
    }

    #[test]
    fn test_builder_read_only_false() {
        let cfg = StoreConfig::builder().read_only(Some(false)).build();
        assert_eq!(cfg.read_only(), Some(false));
    }

    #[test]
    fn test_validate_nonzero_size_valid() {
        assert!(validate_nonzero_size("field", 1).is_ok());
        assert!(validate_nonzero_size("field", 1024).is_ok());
        assert!(validate_nonzero_size("field", u64::MAX).is_ok());
    }

    #[test]
    fn test_validate_compress_level_valid_range() {
        for level in 0..=9 {
            assert!(
                validate_compress_level(level).is_ok(),
                "level {level} should be valid"
            );
        }
    }

    #[test]
    fn test_validate_compress_level_boundary_9_is_valid() {
        assert!(validate_compress_level(9).is_ok());
    }

    #[test]
    fn test_validate_compress_level_10_is_invalid() {
        let result = validate_compress_level(10);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("<= 9"));
    }

    #[test]
    fn test_validate_compress_level_max_u8() {
        let result = validate_compress_level(u8::MAX);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_index_continuous_zero_valid() {
        assert!(validate_index_continuous(0).is_ok());
    }

    #[test]
    fn test_validate_index_continuous_one_valid() {
        assert!(validate_index_continuous(1).is_ok());
    }

    #[test]
    fn test_validate_index_continuous_two_invalid() {
        let result = validate_index_continuous(2);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("0 or 1"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn test_validate_index_continuous_max_u8() {
        assert!(validate_index_continuous(u8::MAX).is_err());
    }

    #[test]
    fn test_validate_retention_window_zero_valid() {
        assert!(validate_retention_window(0).is_ok());
    }

    #[test]
    fn test_validate_retention_window_i64_max_valid() {
        assert!(validate_retention_window(i64::MAX as u64).is_ok());
    }

    #[test]
    fn test_validate_retention_window_i64_max_plus_one_invalid() {
        let val = i64::MAX as u64 + 1;
        let result = validate_retention_window(val);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("i64::MAX"));
    }

    #[test]
    fn test_validate_retention_window_u64_max_invalid() {
        assert!(validate_retention_window(u64::MAX).is_err());
    }

    #[test]
    fn test_validate_dataset_config_values_valid() {
        let result = validate_dataset_config_values(
            64 * 1024 * 1024,
            16 * 1024 * 1024,
            6,
            0,
            0,
            256 * 1024,
            16 * 1024,
            0,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_dataset_config_values_zero_index_segment() {
        let result = validate_dataset_config_values(1024, 0, 6, 0, 0, 512, 512, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("index_segment_size"));
    }

    #[test]
    fn test_validate_dataset_config_values_zero_initial_data_segment() {
        let result = validate_dataset_config_values(1024, 1024, 6, 0, 0, 0, 512, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_data_segment_size"));
    }

    #[test]
    fn test_validate_dataset_config_values_zero_initial_index_segment() {
        let result = validate_dataset_config_values(1024, 1024, 6, 0, 0, 512, 0, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_index_segment_size"));
    }

    #[test]
    fn test_validate_dataset_config_values_initial_index_over_max() {
        // initial_index_segment_size > index_segment_size
        let result = validate_dataset_config_values(1024, 512, 6, 0, 0, 512, 1024, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_index_segment_size"));
        assert!(msg.contains("index_segment_size"));
    }

    #[test]
    fn test_validate_dataset_config_values_invalid_compress_type() {
        let result = validate_dataset_config_values(1024, 1024, 6, 99, 0, 512, 512, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("compress_type"));
    }

    #[test]
    fn test_validate_dataset_config_values_invalid_compress_level() {
        let result = validate_dataset_config_values(1024, 1024, 10, 0, 0, 512, 512, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_dataset_config_values_invalid_index_continuous() {
        let result = validate_dataset_config_values(1024, 1024, 6, 0, 5, 512, 512, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_dataset_config_values_retention_above_i64_max() {
        let result =
            validate_dataset_config_values(1024, 1024, 6, 0, 0, 512, 512, i64::MAX as u64 + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_store_config_validate_success() {
        let cfg = StoreConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_store_config_validate_retention_check_hour_over_23() {
        let cfg = StoreConfig {
            retention_check_hour: 24,
            ..Default::default()
        };
        let result = cfg.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("retention_check_hour"));
        assert!(msg.contains("23"));
    }

    #[test]
    fn test_store_config_validate_retention_check_hour_23_ok() {
        let cfg = StoreConfig::builder().retention_check_hour(23).build();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_dataset_builder_zero_data_segment_size() {
        let result = DataSetConfigBuilder::default().data_segment_size(0).build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("data_segment_size"));
    }

    #[test]
    fn test_dataset_builder_zero_index_segment_size() {
        let result = DataSetConfigBuilder::default()
            .index_segment_size(0)
            .build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("index_segment_size"));
    }

    #[test]
    fn test_dataset_builder_zero_initial_data_segment_size() {
        let result = DataSetConfigBuilder::default()
            .initial_data_segment_size(0)
            .build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_data_segment_size"));
    }

    #[test]
    fn test_dataset_builder_zero_initial_index_segment_size() {
        let result = DataSetConfigBuilder::default()
            .initial_index_segment_size(0)
            .build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_index_segment_size"));
    }

    #[test]
    fn test_dataset_builder_initial_data_over_max() {
        let result = DataSetConfigBuilder::default()
            .data_segment_size(512)
            .initial_data_segment_size(1024)
            .build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_data_segment_size"));
        assert!(msg.contains("data_segment_size"));
    }

    #[test]
    fn test_dataset_builder_initial_index_over_max() {
        let result = DataSetConfigBuilder::default()
            .index_segment_size(512)
            .initial_index_segment_size(1024)
            .build();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("initial_index_segment_size"));
        assert!(msg.contains("index_segment_size"));
    }

    #[test]
    fn test_dataset_builder_invalid_compress_type() {
        let result = DataSetConfigBuilder::default().compress_type(99).build();
        assert!(result.is_err());
    }

    #[test]
    fn test_dataset_config_validate_success() {
        let cfg = DataSetConfig::from_store(&StoreConfig::default());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_store_builder_compress_level_clamped_to_9() {
        let cfg = StoreConfig::builder().compress_level(255).build();
        assert_eq!(cfg.compress_level, 9);
    }

    #[test]
    fn test_store_builder_retention_check_hour_clamped() {
        assert_eq!(
            StoreConfig::builder()
                .retention_check_hour(0)
                .build()
                .retention_check_hour,
            0
        );
        assert_eq!(
            StoreConfig::builder()
                .retention_check_hour(23)
                .build()
                .retention_check_hour,
            23
        );
        assert_eq!(
            StoreConfig::builder()
                .retention_check_hour(24)
                .build()
                .retention_check_hour,
            23
        );
    }

    #[test]
    fn test_dataset_builder_compress_level_clamped_to_9() {
        let config = DataSetConfigBuilder::default()
            .compress_level(255)
            .build()
            .unwrap();
        assert_eq!(config.compress_level, 9);
    }

    #[test]
    fn test_dataset_builder_index_continuous_clamped() {
        let config = DataSetConfigBuilder::default()
            .index_continuous(5)
            .build()
            .unwrap();
        assert_eq!(config.index_continuous, 1);
    }

    #[test]
    fn test_store_config_accessors() {
        let cfg = StoreConfig::default();
        assert_eq!(cfg.flush_interval(), Duration::from_secs(15));
        assert_eq!(cfg.idle_timeout(), Duration::from_secs(1800));
        assert_eq!(cfg.data_segment_size(), 64 * 1024 * 1024);
        assert_eq!(cfg.index_segment_size(), 16 * 1024 * 1024);
        assert_eq!(cfg.initial_data_segment_size(), 256 * 1024);
        assert_eq!(cfg.initial_index_segment_size(), 16 * 1024);
        assert_eq!(cfg.compress_level(), 6);
        assert_eq!(cfg.compress_type(), 0);
        assert_eq!(cfg.cache_max_memory(), 256 * 1024 * 1024);
        assert_eq!(cfg.cache_idle_timeout(), Duration::from_secs(1800));
        assert_eq!(cfg.retention_check_hour(), 0);
        assert!(cfg.enable_background_thread());
        assert!(cfg.enable_journal());
        assert!(cfg.read_only().is_none());
    }

    #[test]
    fn test_dataset_config_accessors() {
        let store = StoreConfig::default();
        let cfg = DataSetConfig::from_store(&store);
        assert_eq!(cfg.data_segment_size(), store.data_segment_size);
        assert_eq!(cfg.index_segment_size(), store.index_segment_size);
        assert_eq!(cfg.compress_level(), store.compress_level);
        assert_eq!(cfg.compress_type(), store.compress_type);
        assert_eq!(cfg.index_continuous(), 0);
        assert_eq!(
            cfg.initial_data_segment_size(),
            store.initial_data_segment_size
        );
        assert_eq!(
            cfg.initial_index_segment_size(),
            store.initial_index_segment_size
        );
        assert_eq!(cfg.retention_window(), 0);
        assert!(!cfg.enable_journal());
        assert_eq!(cfg.create_time(), 0);
    }

    #[test]
    fn test_dataset_config_builder_entry_point() {
        let config = DataSetConfig::builder()
            .data_segment_size(1024)
            .index_segment_size(1024)
            .initial_data_segment_size(512)
            .initial_index_segment_size(512)
            .build()
            .unwrap();
        assert_eq!(config.data_segment_size, 1024);
        assert_eq!(config.index_segment_size, 1024);
    }

    #[test]
    fn test_validate_dataset_config_initial_equals_max_allowed() {
        let result = validate_dataset_config_values(1024, 1024, 6, 0, 0, 1024, 1024, 0);
        assert!(result.is_ok());
    }
}
