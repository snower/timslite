#[derive(Debug, Clone, Default)]
pub struct StoreConfig {
    pub flush_interval_secs: Option<u64>,
    pub idle_timeout_secs: Option<u64>,
    pub data_segment_size: Option<u64>,
    pub index_segment_size: Option<u64>,
    pub initial_data_segment_size: Option<u64>,
    pub initial_index_segment_size: Option<u64>,
    pub compress_level: Option<u8>,
    pub cache_max_memory: Option<u64>,
    pub cache_idle_timeout_secs: Option<u64>,
    pub retention_check_hour: Option<u8>,
    pub enable_background_thread: Option<bool>,
    pub enable_journal: Option<bool>,
    pub read_only: Option<bool>,
}

impl StoreConfig {
    pub fn to_inner(&self) -> timslite::StoreConfig {
        let mut builder = timslite::StoreConfig::builder();

        if let Some(v) = self.flush_interval_secs {
            builder = builder.flush_interval(std::time::Duration::from_secs(v));
        }
        if let Some(v) = self.idle_timeout_secs {
            builder = builder.idle_timeout(std::time::Duration::from_secs(v));
        }
        if let Some(v) = self.data_segment_size {
            builder = builder.data_segment_size(v);
        }
        if let Some(v) = self.index_segment_size {
            builder = builder.index_segment_size(v);
        }
        if let Some(v) = self.initial_data_segment_size {
            builder = builder.initial_data_segment_size(v);
        }
        if let Some(v) = self.initial_index_segment_size {
            builder = builder.initial_index_segment_size(v);
        }
        if let Some(v) = self.compress_level {
            builder = builder.compress_level(v);
        }
        if let Some(v) = self.cache_max_memory {
            builder = builder.cache_max_memory(v as usize);
        }
        if let Some(v) = self.cache_idle_timeout_secs {
            builder = builder.cache_idle_timeout(std::time::Duration::from_secs(v));
        }
        if let Some(v) = self.retention_check_hour {
            builder = builder.retention_check_hour(v);
        }
        if let Some(v) = self.enable_background_thread {
            builder = builder.enable_background_thread(v);
        }
        if let Some(v) = self.enable_journal {
            builder = builder.enable_journal(v);
        }
        if let Some(v) = self.read_only {
            builder = builder.read_only(Some(v));
        }

        builder.build()
    }
}

#[derive(Debug, Clone, Default)]
pub struct DatasetConfig {
    pub data_segment_size: Option<u64>,
    pub index_segment_size: Option<u64>,
    pub initial_data_segment_size: Option<u64>,
    pub initial_index_segment_size: Option<u64>,
    pub compress_level: Option<u8>,
    pub compress_type: Option<u8>,
    pub index_continuous: Option<u8>,
    pub retention_window: Option<u64>,
    pub enable_journal: Option<bool>,
}

impl DatasetConfig {
    pub fn apply_to_builder(
        &self,
        mut builder: timslite::DataSetConfigBuilder,
    ) -> timslite::DataSetConfigBuilder {
        if let Some(v) = self.data_segment_size {
            builder = builder.data_segment_size(v);
        }
        if let Some(v) = self.index_segment_size {
            builder = builder.index_segment_size(v);
        }
        if let Some(v) = self.initial_data_segment_size {
            builder = builder.initial_data_segment_size(v);
        }
        if let Some(v) = self.initial_index_segment_size {
            builder = builder.initial_index_segment_size(v);
        }
        if let Some(v) = self.compress_level {
            builder = builder.compress_level(v);
        }
        if let Some(v) = self.compress_type {
            builder = builder.compress_type(v);
        }
        if let Some(v) = self.index_continuous {
            builder = builder.index_continuous(v);
        }
        if let Some(v) = self.retention_window {
            builder = builder.retention_window(v);
        }
        if let Some(v) = self.enable_journal {
            builder = builder.enable_journal(v);
        }
        builder
    }
}

#[derive(Debug, Clone, Default)]
pub struct CreateDatasetOptions {
    pub config: Option<DatasetConfig>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueConsumerConfig {
    pub running_expired_seconds: Option<u64>,
    pub max_retry_count: Option<u16>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueConsumerOptions {
    pub config: Option<QueueConsumerConfig>,
}
