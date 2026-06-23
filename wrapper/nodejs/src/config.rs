use napi::bindgen_prelude::BigInt;
use napi_derive::napi;
use std::time::Duration;

#[napi(object)]
pub struct StoreConfigOptions {
    pub flush_interval_ms: Option<u32>,
    pub idle_timeout_ms: Option<u32>,
    pub data_segment_size: Option<BigInt>,
    pub index_segment_size: Option<BigInt>,
    pub initial_data_segment_size: Option<BigInt>,
    pub initial_index_segment_size: Option<BigInt>,
    pub compress_level: Option<u8>,
    pub compress_type: Option<u8>,
    pub cache_max_memory: Option<BigInt>,
    pub cache_idle_timeout_ms: Option<u32>,
    pub retention_check_hour: Option<u8>,
    pub enable_background_thread: Option<bool>,
    pub enable_journal: Option<bool>,
    pub read_only: Option<bool>,
}

#[napi(object)]
pub struct CreateDatasetOptions {
    pub data_segment_size: Option<BigInt>,
    pub index_segment_size: Option<BigInt>,
    pub initial_data_segment_size: Option<BigInt>,
    pub initial_index_segment_size: Option<BigInt>,
    pub compress_level: Option<u8>,
    pub compress_type: Option<u8>,
    pub index_continuous: Option<bool>,
    pub retention_window: Option<BigInt>,
    pub enable_journal: Option<bool>,
}

fn bigint_to_u64(v: &BigInt, field: &str) -> napi::Result<u64> {
    let (_sign, val, lossless) = v.get_u64();
    if !lossless {
        return Err(crate::errors::invalid_data(&format!(
            "{field} must be a non-negative u64"
        )));
    }
    Ok(val)
}

fn bigint_to_usize(v: &BigInt, field: &str) -> napi::Result<usize> {
    let (_sign, val, lossless) = v.get_u64();
    if !lossless || val > usize::MAX as u64 {
        return Err(crate::errors::invalid_data(&format!(
            "{field} must fit in usize"
        )));
    }
    Ok(val as usize)
}

pub fn decode_store_config(opts: &StoreConfigOptions) -> napi::Result<timslite::StoreConfig> {
    let mut builder = timslite::StoreConfig::builder();

    if let Some(ms) = opts.flush_interval_ms {
        builder = builder.flush_interval(Duration::from_millis(ms as u64));
    }
    if let Some(ms) = opts.idle_timeout_ms {
        builder = builder.idle_timeout(Duration::from_millis(ms as u64));
    }
    if let Some(ref v) = opts.data_segment_size {
        builder = builder.data_segment_size(bigint_to_u64(v, "dataSegmentSize")?);
    }
    if let Some(ref v) = opts.index_segment_size {
        builder = builder.index_segment_size(bigint_to_u64(v, "indexSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_data_segment_size {
        builder = builder.initial_data_segment_size(bigint_to_u64(v, "initialDataSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_index_segment_size {
        builder = builder.initial_index_segment_size(bigint_to_u64(v, "initialIndexSegmentSize")?);
    }
    if let Some(v) = opts.compress_level {
        builder = builder.compress_level(v);
    }
    if let Some(v) = opts.compress_type {
        builder = builder.compress_type(v);
    }
    if let Some(ref v) = opts.cache_max_memory {
        builder = builder.cache_max_memory(bigint_to_usize(v, "cacheMaxMemory")?);
    }
    if let Some(ms) = opts.cache_idle_timeout_ms {
        builder = builder.cache_idle_timeout(Duration::from_millis(ms as u64));
    }
    if let Some(v) = opts.retention_check_hour {
        builder = builder.retention_check_hour(v);
    }
    if let Some(v) = opts.enable_background_thread {
        builder = builder.enable_background_thread(v);
    }
    if let Some(v) = opts.enable_journal {
        builder = builder.enable_journal(v);
    }
    if let Some(v) = opts.read_only {
        builder = builder.read_only(Some(v));
    }

    Ok(builder.build())
}

pub fn build_dataset_config(
    store_config: &timslite::StoreConfig,
    opts: &CreateDatasetOptions,
) -> napi::Result<timslite::DataSetConfigBuilder> {
    let mut builder = timslite::DataSetConfigBuilder::from_store(store_config);

    if let Some(ref v) = opts.data_segment_size {
        builder = builder.data_segment_size(bigint_to_u64(v, "dataSegmentSize")?);
    }
    if let Some(ref v) = opts.index_segment_size {
        builder = builder.index_segment_size(bigint_to_u64(v, "indexSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_data_segment_size {
        builder = builder.initial_data_segment_size(bigint_to_u64(v, "initialDataSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_index_segment_size {
        builder = builder.initial_index_segment_size(bigint_to_u64(v, "initialIndexSegmentSize")?);
    }
    if let Some(v) = opts.compress_level {
        builder = builder.compress_level(v);
    }
    if let Some(v) = opts.compress_type {
        builder = builder.compress_type(v);
    }
    if let Some(v) = opts.index_continuous {
        builder = builder.index_continuous(if v { 1 } else { 0 });
    }
    if let Some(ref v) = opts.retention_window {
        builder = builder.retention_window(bigint_to_u64(v, "retentionWindow")?);
    }
    if let Some(v) = opts.enable_journal {
        builder = builder.enable_journal(v);
    }

    Ok(builder)
}
