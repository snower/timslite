use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

use crate::config::{self, CreateDatasetOptions, StoreConfigOptions};
use crate::dataset::Dataset;
use crate::errors;
use crate::types;

#[napi(object)]
pub struct TickResult {
    pub executed_tasks: u32,
    pub next_delay_ms: u32,
}

#[napi(object)]
pub struct DataSetInfoResult {
    pub name: String,
    pub dataset_type: String,
    pub base_dir: String,
    pub identifier: BigInt,
    pub data_segment_size: BigInt,
    pub index_segment_size: BigInt,
    pub initial_data_segment_size: BigInt,
    pub initial_index_segment_size: BigInt,
    pub compress_type: u8,
    pub compress_level: u8,
    pub index_continuous: u8,
    pub retention_window: BigInt,
    pub enable_journal: bool,
    pub create_time: BigInt,
}

#[napi(object)]
pub struct DataSetStateResult {
    pub latest_written_timestamp: Option<BigInt>,
    pub open_data_segments: u32,
    pub data_segments: u32,
    pub total_record_count: BigInt,
    pub total_data_size: BigInt,
    pub total_uncompressed_size: BigInt,
    pub total_invalid_record_count: BigInt,
    pub min_timestamp: Option<BigInt>,
    pub max_timestamp: Option<BigInt>,
    pub open_index_segments: u32,
    pub index_segments: u32,
    pub pending_index_entries: u32,
    pub base_timestamp: Option<BigInt>,
    pub read_only: bool,
    pub has_block_cache: bool,
    pub has_journal: bool,
    pub has_queue: bool,
    pub queue_consumer_groups: u32,
}

#[napi(object)]
pub struct InspectResult {
    pub info: DataSetInfoResult,
    pub state: DataSetStateResult,
}

pub fn info_to_result(info: &timslite::DataSetInfo) -> DataSetInfoResult {
    DataSetInfoResult {
        name: info.name.clone(),
        dataset_type: info.dataset_type.clone(),
        base_dir: info.base_dir.clone(),
        identifier: types::u64_to_bigint(info.identifier),
        data_segment_size: types::u64_to_bigint(info.data_segment_size),
        index_segment_size: types::u64_to_bigint(info.index_segment_size),
        initial_data_segment_size: types::u64_to_bigint(info.initial_data_segment_size),
        initial_index_segment_size: types::u64_to_bigint(info.initial_index_segment_size),
        compress_type: info.compress_type,
        compress_level: info.compress_level,
        index_continuous: info.index_continuous,
        retention_window: types::u64_to_bigint(info.retention_window),
        enable_journal: info.enable_journal,
        create_time: types::i64_to_bigint(info.create_time),
    }
}

pub fn state_to_result(state: &timslite::DataSetState) -> DataSetStateResult {
    DataSetStateResult {
        latest_written_timestamp: state.latest_written_timestamp.map(types::i64_to_bigint),
        open_data_segments: state.open_data_segments,
        data_segments: state.data_segments,
        total_record_count: types::u64_to_bigint(state.total_record_count),
        total_data_size: types::u64_to_bigint(state.total_data_size),
        total_uncompressed_size: types::u64_to_bigint(state.total_uncompressed_size),
        total_invalid_record_count: types::u64_to_bigint(state.total_invalid_record_count),
        min_timestamp: state.min_timestamp.map(types::i64_to_bigint),
        max_timestamp: state.max_timestamp.map(types::i64_to_bigint),
        open_index_segments: state.open_index_segments,
        index_segments: state.index_segments,
        pending_index_entries: state.pending_index_entries,
        base_timestamp: state.base_timestamp.map(types::i64_to_bigint),
        read_only: state.read_only,
        has_block_cache: state.has_block_cache,
        has_journal: state.has_journal,
        has_queue: state.has_queue,
        queue_consumer_groups: state.queue_consumer_groups,
    }
}

#[napi]
pub struct Store {
    inner: Option<timslite::Store>,
    config: timslite::StoreConfig,
}

#[napi]
impl Store {
    #[napi(factory)]
    pub fn open(data_dir: String, config: Option<StoreConfigOptions>) -> napi::Result<Self> {
        let cfg = match config {
            Some(ref opts) => config::decode_store_config(opts)?,
            None => timslite::StoreConfig::default(),
        };
        let store = errors::wrap(timslite::Store::open(&data_dir, cfg.clone()))?;
        Ok(Self {
            inner: Some(store),
            config: cfg,
        })
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        let store = self.inner.take().ok_or_else(errors::store_closed)?;
        errors::wrap(store.close())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.inner.is_none()
    }

    #[napi(getter)]
    pub fn read_only(&self) -> bool {
        self.inner.as_ref().is_none_or(|s| s.is_read_only())
    }

    #[napi]
    pub fn create_dataset(
        &mut self,
        name: String,
        dataset_type: String,
        options: Option<CreateDatasetOptions>,
    ) -> napi::Result<()> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        let builder = match options {
            Some(ref opts) => Some(config::build_dataset_config(&self.config, opts)?),
            None => None,
        };
        errors::wrap(store.create_dataset_with_config(&name, &dataset_type, builder))?;
        Ok(())
    }

    #[napi]
    pub fn open_dataset(
        &mut self,
        name: String,
        dataset_type: String,
    ) -> napi::Result<Dataset> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        let ds = errors::wrap(store.open_dataset(&name, &dataset_type))?;
        let identifier = ds.identifier();
        Ok(Dataset::new(Arc::new(ds), identifier, store.is_read_only()))
    }

    #[napi]
    pub fn open_dataset_by_identifier(&mut self, identifier: BigInt) -> napi::Result<Dataset> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        let (_sign, id_u64, lossless) = identifier.get_u64();
        if !lossless {
            return Err(errors::invalid_data("identifier must be a non-negative u64"));
        }
        let ds = errors::wrap(store.open_dataset_by_identifier(id_u64))?;
        Ok(Dataset::new(Arc::new(ds), id_u64, store.is_read_only()))
    }

    #[napi]
    pub fn drop_dataset(&mut self, name: String, dataset_type: String) -> napi::Result<()> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        errors::wrap(store.drop_dataset(&name, &dataset_type))
    }

    #[napi]
    pub fn tick_background_tasks(&self) -> napi::Result<TickResult> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let result = errors::wrap(store.tick_background_tasks())?;
        Ok(TickResult {
            executed_tasks: result.executed_tasks as u32,
            next_delay_ms: types::duration_to_ms(result.next_delay),
        })
    }

    #[napi]
    pub fn next_background_delay(&self) -> napi::Result<u32> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let delay = errors::wrap(store.next_background_delay())?;
        Ok(types::duration_to_ms(delay))
    }

    #[napi]
    pub fn get_dataset_names(&self) -> napi::Result<Vec<String>> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(store.get_dataset_names())
    }

    #[napi]
    pub fn get_dataset_types(&self, name: String) -> napi::Result<Vec<String>> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(store.get_dataset_types(&name))
    }

    #[napi]
    pub fn inspect_dataset(&self, name: String, dataset_type: String) -> napi::Result<InspectResult> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let result = errors::wrap(store.inspect_dataset(&name, &dataset_type))?;
        Ok(InspectResult {
            info: info_to_result(&result.info),
            state: state_to_result(&result.state),
        })
    }

    #[napi]
    pub fn open_queue(&mut self, dataset: &Dataset) -> napi::Result<crate::queue::Queue> {
        let queue = errors::wrap(dataset.inner.open_queue())?;
        Ok(crate::queue::Queue::new(queue))
    }

    #[napi]
    pub fn open_journal_queue(&mut self) -> napi::Result<crate::queue::JournalQueue> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        let jq = errors::wrap(store.open_journal_queue())?;
        Ok(crate::queue::JournalQueue::new(jq))
    }

    #[napi]
    pub fn journal_latest_sequence(&self) -> napi::Result<Option<BigInt>> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let result = errors::wrap(store.journal_latest_sequence())?;
        Ok(result.map(types::i64_to_bigint))
    }

    #[napi]
    pub fn journal_read(&self, sequence: BigInt) -> napi::Result<Option<(BigInt, Buffer)>> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let seq = types::bigint_to_i64(&sequence)?;
        let result = errors::wrap(store.journal_read(seq))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn journal_query(&self, start: BigInt, end: BigInt) -> napi::Result<Vec<(BigInt, Buffer)>> {
        let store = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let s = types::bigint_to_i64(&start)?;
        let e = types::bigint_to_i64(&end)?;
        let results = errors::wrap(store.journal_query(s, e))?;
        Ok(results
            .into_iter()
            .map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data)))
            .collect())
    }

    #[napi]
    pub fn read_journal_source_record(
        &mut self,
        identifier: BigInt,
        index_info: JournalIndexInfo,
    ) -> napi::Result<(BigInt, Buffer)> {
        let store = self.inner.as_mut().ok_or_else(errors::store_closed)?;
        let (_sign, id_u64, lossless) = identifier.get_u64();
        if !lossless {
            return Err(errors::invalid_data("identifier must be a non-negative u64"));
        }
        let info = timslite::JournalIndexInfo {
            timestamp: index_info.timestamp,
            block_offset: index_info.block_offset as u64,
            in_block_offset: index_info.in_block_offset,
        };
        let (ts, data) = errors::wrap(store.read_journal_source_record(id_u64, info))?;
        Ok((types::i64_to_bigint(ts), types::vec_to_buffer(data)))
    }
}

#[napi(object)]
pub struct JournalIndexInfo {
    pub timestamp: i64,
    pub block_offset: i64,
    pub in_block_offset: u16,
}
