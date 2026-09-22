use napi::bindgen_prelude::{BigInt, Either};
use napi_derive::napi;
use std::time::Duration;

pub type NumberOrBigInt = Either<BigInt, f64>;

#[napi(object)]
pub struct StoreConfigOptions {
    pub flush_interval_ms: Option<u32>,
    pub idle_timeout_ms: Option<u32>,
    pub data_segment_size: Option<Either<BigInt, f64>>,
    pub index_segment_size: Option<Either<BigInt, f64>>,
    pub initial_data_segment_size: Option<Either<BigInt, f64>>,
    pub initial_index_segment_size: Option<Either<BigInt, f64>>,
    pub compress_level: Option<u8>,
    pub compress_type: Option<u8>,
    pub cache_max_memory: Option<Either<BigInt, f64>>,
    pub cache_idle_timeout_ms: Option<u32>,
    pub retention_check_hour: Option<u8>,
    pub enable_background_thread: Option<bool>,
    pub enable_journal: Option<bool>,
    pub read_only: Option<bool>,
}

#[napi(object)]
pub struct CreateDatasetOptions {
    pub data_segment_size: Option<Either<BigInt, f64>>,
    pub index_segment_size: Option<Either<BigInt, f64>>,
    pub initial_data_segment_size: Option<Either<BigInt, f64>>,
    pub initial_index_segment_size: Option<Either<BigInt, f64>>,
    pub compress_level: Option<u8>,
    pub compress_type: Option<u8>,
    pub index_continuous: Option<bool>,
    pub retention_window: Option<Either<BigInt, f64>>,
    pub timestamp_units_per_seconds: Option<Either<BigInt, f64>>,
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

// Number.MAX_SAFE_INTEGER: largest value a JS `number` represents exactly.
// `u64::MAX as f64` rounds up to 2**64, so it cannot bound lossless number input.
const JS_MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

fn number_or_bigint_to_u64(v: &NumberOrBigInt, field: &str) -> napi::Result<u64> {
    match v {
        Either::A(big) => bigint_to_u64(big, field),
        Either::B(num) => {
            if !num.is_finite() || num.fract() != 0.0 || *num < 0.0 || *num > JS_MAX_SAFE_INTEGER {
                return Err(crate::errors::invalid_data(&format!(
                    "{field} must be a non-negative integer; use BigInt above Number.MAX_SAFE_INTEGER"
                )));
            }
            Ok(*num as u64)
        }
    }
}

fn number_or_bigint_to_usize(v: &NumberOrBigInt, field: &str) -> napi::Result<usize> {
    let val = number_or_bigint_to_u64(v, field)?;
    if val > usize::MAX as u64 {
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
        builder = builder.data_segment_size(number_or_bigint_to_u64(v, "dataSegmentSize")?);
    }
    if let Some(ref v) = opts.index_segment_size {
        builder = builder.index_segment_size(number_or_bigint_to_u64(v, "indexSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_data_segment_size {
        builder = builder
            .initial_data_segment_size(number_or_bigint_to_u64(v, "initialDataSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_index_segment_size {
        builder = builder
            .initial_index_segment_size(number_or_bigint_to_u64(v, "initialIndexSegmentSize")?);
    }
    if let Some(v) = opts.compress_level {
        builder = builder.compress_level(v);
    }
    if let Some(v) = opts.compress_type {
        builder = builder.compress_type(v);
    }
    if let Some(ref v) = opts.cache_max_memory {
        builder = builder.cache_max_memory(number_or_bigint_to_usize(v, "cacheMaxMemory")?);
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
        builder = builder.data_segment_size(number_or_bigint_to_u64(v, "dataSegmentSize")?);
    }
    if let Some(ref v) = opts.index_segment_size {
        builder = builder.index_segment_size(number_or_bigint_to_u64(v, "indexSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_data_segment_size {
        builder = builder
            .initial_data_segment_size(number_or_bigint_to_u64(v, "initialDataSegmentSize")?);
    }
    if let Some(ref v) = opts.initial_index_segment_size {
        builder = builder
            .initial_index_segment_size(number_or_bigint_to_u64(v, "initialIndexSegmentSize")?);
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
        builder = builder.retention_window(number_or_bigint_to_u64(v, "retentionWindow")?);
    }
    if let Some(ref v) = opts.timestamp_units_per_seconds {
        builder = builder
            .timestamp_units_per_seconds(number_or_bigint_to_u64(v, "timestampUnitsPerSeconds")?);
    }
    if let Some(v) = opts.enable_journal {
        builder = builder.enable_journal(v);
    }

    Ok(builder)
}
