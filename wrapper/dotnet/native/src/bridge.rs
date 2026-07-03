use std::sync::{Arc, Mutex};

use crate::config::{CreateDatasetOptions, QueueConsumerOptions, StoreConfig};
use crate::errors::TmslError;
use crate::query::{QueryIteratorBridge, QueryLengthIteratorBridge};
use crate::queue::QueueConsumerBridge;

#[derive(Debug, Clone)]
pub struct JournalRecord {
    pub sequence: i64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp: i64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct LengthEntry {
    pub timestamp: i64,
    pub length: u32,
}

#[derive(Debug, Clone)]
pub struct QueueConsumerInfo {
    pub group_name: String,
    pub running_expired_seconds: u64,
    pub max_retry_count: u16,
}

#[derive(Debug, Clone)]
pub struct QueueConsumerPendingEntry {
    pub timestamp: i64,
    pub start_time: i64,
    pub status: u8,
    pub retry_count: u8,
}

#[derive(Debug, Clone)]
pub struct QueueConsumerState {
    pub processed_ts: i64,
    pub pending_entries: Vec<QueueConsumerPendingEntry>,
}

#[derive(Debug, Clone)]
pub struct QueueConsumerInspectResult {
    pub info: QueueConsumerInfo,
    pub state: QueueConsumerState,
}

#[derive(Debug, Clone)]
pub struct DataSetInfo {
    pub name: String,
    pub dataset_type: String,
    pub base_dir: String,
    pub identifier: u64,
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub initial_data_segment_size: u64,
    pub initial_index_segment_size: u64,
    pub compress_type: u8,
    pub compress_level: u8,
    pub index_continuous: u8,
    pub retention_window: u64,
    pub enable_journal: bool,
    pub create_time: i64,
}

#[derive(Debug, Clone)]
pub struct DataSetState {
    pub latest_written_timestamp: Option<i64>,
    pub open_data_segments: u32,
    pub data_segments: u32,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub open_index_segments: u32,
    pub index_segments: u32,
    pub pending_index_entries: u32,
    pub base_timestamp: Option<i64>,
    pub read_only: bool,
    pub has_block_cache: bool,
    pub has_journal: bool,
    pub has_queue: bool,
    pub queue_consumer_groups: u32,
}

#[derive(Debug, Clone)]
pub struct DataSetInspectResult {
    pub info: DataSetInfo,
    pub state: DataSetState,
}

#[derive(Debug, Clone)]
pub struct TickResult {
    pub executed_tasks: u64,
    pub next_delay_ms: u64,
}

#[derive(Debug, Clone)]
pub struct JournalIndexInfo {
    pub timestamp: i64,
    pub block_offset: u64,
    pub in_block_offset: u16,
}

pub struct StoreBridge {
    inner: Mutex<Option<timslite::Store>>,
}

impl StoreBridge {
    pub fn open(path: String, config: StoreConfig) -> Result<Self, TmslError> {
        let store = timslite::Store::open(path, config.to_inner())?;
        Ok(Self {
            inner: Mutex::new(Some(store)),
        })
    }

    pub fn close(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        if let Some(store) = guard.take() {
            store.close()?;
        }
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().map(|g| g.is_none()).unwrap_or(true)
    }

    pub fn is_read_only(&self) -> Result<bool, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => Ok(store.is_read_only()),
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn create_dataset(
        &self,
        name: String,
        dataset_type: String,
        options: CreateDatasetOptions,
    ) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                let store_config = store.config().clone();
                if let Some(config) = options.config {
                    let builder = config
                        .apply_to_builder(timslite::DataSetConfigBuilder::from_store(&store_config));
                    store.create_dataset_with_config(&name, &dataset_type, Some(builder))?;
                } else {
                    let builder = timslite::DataSetConfigBuilder::from_store(&store_config);
                    store.create_dataset_with_config(&name, &dataset_type, Some(builder))?;
                }
                Ok(())
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn open_dataset(
        &self,
        name: String,
        dataset_type: String,
    ) -> Result<Arc<DatasetBridge>, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                let ds = store.open_dataset(&name, &dataset_type)?;
                Ok(Arc::new(DatasetBridge::new(Arc::new(ds))))
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn open_dataset_by_identifier(
        &self,
        identifier: u64,
    ) -> Result<Arc<DatasetBridge>, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                let ds = store.open_dataset_by_identifier(identifier)?;
                Ok(Arc::new(DatasetBridge::new(Arc::new(ds))))
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn drop_dataset(
        &self,
        name: String,
        dataset_type: String,
    ) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                store.drop_dataset(&name, &dataset_type)?;
                Ok(())
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn get_dataset_names(&self) -> Result<Vec<String>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => Ok(store.get_dataset_names()?),
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn get_dataset_types(&self, name: String) -> Result<Vec<String>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => Ok(store.get_dataset_types(&name)?),
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn inspect_dataset(
        &self,
        name: String,
        dataset_type: String,
    ) -> Result<DataSetInspectResult, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => {
                let result = store.inspect_dataset(&name, &dataset_type)?;
                Ok(DataSetInspectResult {
                    info: DataSetInfo {
                        name: result.info.name,
                        dataset_type: result.info.dataset_type,
                        base_dir: result.info.base_dir,
                        identifier: result.info.identifier,
                        data_segment_size: result.info.data_segment_size,
                        index_segment_size: result.info.index_segment_size,
                        initial_data_segment_size: result.info.initial_data_segment_size,
                        initial_index_segment_size: result.info.initial_index_segment_size,
                        compress_type: result.info.compress_type,
                        compress_level: result.info.compress_level,
                        index_continuous: result.info.index_continuous,
                        retention_window: result.info.retention_window,
                        enable_journal: result.info.enable_journal,
                        create_time: result.info.create_time,
                    },
                    state: DataSetState {
                        latest_written_timestamp: result.state.latest_written_timestamp,
                        open_data_segments: result.state.open_data_segments,
                        data_segments: result.state.data_segments,
                        total_record_count: result.state.total_record_count,
                        total_data_size: result.state.total_data_size,
                        total_uncompressed_size: result.state.total_uncompressed_size,
                        total_invalid_record_count: result.state.total_invalid_record_count,
                        min_timestamp: result.state.min_timestamp,
                        max_timestamp: result.state.max_timestamp,
                        open_index_segments: result.state.open_index_segments,
                        index_segments: result.state.index_segments,
                        pending_index_entries: result.state.pending_index_entries,
                        base_timestamp: result.state.base_timestamp,
                        read_only: result.state.read_only,
                        has_block_cache: result.state.has_block_cache,
                        has_journal: result.state.has_journal,
                        has_queue: result.state.has_queue,
                        queue_consumer_groups: result.state.queue_consumer_groups,
                    },
                })
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn tick_background_tasks(&self) -> Result<TickResult, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => {
                let result = store.tick_background_tasks()?;
                Ok(TickResult {
                    executed_tasks: result.executed_tasks as u64,
                    next_delay_ms: result.next_delay.as_millis() as u64,
                })
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn next_background_delay_ms(&self) -> Result<u64, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => {
                let delay = store.next_background_delay()?;
                Ok(delay.as_millis() as u64)
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn open_queue(&self, dataset: Arc<DatasetBridge>) -> Result<Arc<QueueBridge>, TmslError> {
        let queue = dataset.inner.open_queue()?;
        Ok(Arc::new(QueueBridge::new(queue)))
    }

    pub fn open_journal_queue(&self) -> Result<Arc<JournalQueueBridge>, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                let queue = store.open_journal_queue()?;
                Ok(Arc::new(JournalQueueBridge::new(queue)))
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn journal_latest_sequence(&self) -> Result<Option<i64>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => Ok(store.journal_latest_sequence()?),
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn journal_read(&self, sequence: i64) -> Result<Option<JournalRecord>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => {
                let result = store.journal_read(sequence)?;
                Ok(result.map(|(seq, data)| JournalRecord { sequence: seq, data }))
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn journal_query(
        &self,
        start_sequence: i64,
        end_sequence: i64,
    ) -> Result<Vec<JournalRecord>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(store) => {
                let results = store.journal_query(start_sequence, end_sequence)?;
                Ok(results
                    .into_iter()
                    .map(|(seq, data)| JournalRecord {
                        sequence: seq,
                        data,
                    })
                    .collect())
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }

    pub fn read_journal_source_record(
        &self,
        dataset_identifier: u64,
        index_info: JournalIndexInfo,
    ) -> Result<Record, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_mut() {
            Some(store) => {
                let info = timslite::JournalIndexInfo {
                    timestamp: index_info.timestamp,
                    block_offset: index_info.block_offset,
                    in_block_offset: index_info.in_block_offset,
                };
                let (ts, data) = store.read_journal_source_record(dataset_identifier, info)?;
                Ok(Record {
                    timestamp: ts,
                    data,
                })
            }
            None => Err(TmslError::StoreClosed {
                message: "store is closed".into(),
            }),
        }
    }
}

pub struct QueueBridge {
    inner: Mutex<Option<timslite::DatasetQueue>>,
}

impl QueueBridge {
    pub fn new(queue: timslite::DatasetQueue) -> Self {
        Self {
            inner: Mutex::new(Some(queue)),
        }
    }

    pub fn push(&self, data: Vec<u8>) -> Result<i64, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(queue) => Ok(queue.push(&data)?),
            None => Err(TmslError::QueueBridgeClosed {
                message: "queue is closed".into(),
            }),
        }
    }

    pub fn open_consumer(
        &self,
        group_name: String,
        options: QueueConsumerOptions,
    ) -> Result<Arc<QueueConsumerBridge>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(queue) => {
                let consumer = if let Some(opts) = options.config {
                    let mut builder = timslite::QueueConsumerConfig::builder();
                    if let Some(v) = opts.running_expired_seconds {
                        builder = builder.running_expired_seconds(v);
                    }
                    if let Some(v) = opts.max_retry_count {
                        builder = builder.max_retry_count(v);
                    }
                    queue.open_consumer_with_config(&group_name, builder.build()?)?
                } else {
                    queue.open_consumer(&group_name)?
                };
                Ok(Arc::new(QueueConsumerBridge::new(consumer)))
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "queue is closed".into(),
            }),
        }
    }

    pub fn get_consumer_group_names(&self) -> Result<Vec<String>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(queue) => Ok(queue.get_consumer_group_names()?),
            None => Err(TmslError::QueueBridgeClosed {
                message: "queue is closed".into(),
            }),
        }
    }

    pub fn drop_consumer(&self, group_name: String) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(queue) => {
                queue.drop_consumer(&group_name)?;
                Ok(())
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "queue is closed".into(),
            }),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        if let Some(queue) = guard.take() {
            queue.close()?;
        }
        Ok(())
    }
}

pub struct JournalQueueBridge {
    inner: Mutex<Option<timslite::JournalQueue>>,
}

impl JournalQueueBridge {
    pub fn new(queue: timslite::JournalQueue) -> Self {
        Self {
            inner: Mutex::new(Some(queue)),
        }
    }

    pub fn open_consumer(
        &self,
        group_name: String,
        options: QueueConsumerOptions,
    ) -> Result<Arc<JournalQueueConsumerBridge>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(queue) => {
                let consumer = if let Some(opts) = options.config {
                    let mut builder = timslite::QueueConsumerConfig::builder();
                    if let Some(v) = opts.running_expired_seconds {
                        builder = builder.running_expired_seconds(v);
                    }
                    if let Some(v) = opts.max_retry_count {
                        builder = builder.max_retry_count(v);
                    }
                    queue.open_consumer_with_config(&group_name, builder.build()?)?
                } else {
                    queue.open_consumer(&group_name)?
                };
                Ok(Arc::new(JournalQueueConsumerBridge::new(consumer)))
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "journal queue is closed".into(),
            }),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.take();
        Ok(())
    }
}

pub struct JournalQueueConsumerBridge {
    inner: Mutex<Option<timslite::JournalQueueConsumer>>,
}

impl JournalQueueConsumerBridge {
    pub fn new(consumer: timslite::JournalQueueConsumer) -> Self {
        Self {
            inner: Mutex::new(Some(consumer)),
        }
    }

    pub fn poll(&self, timeout_ms: u64) -> Result<Option<JournalRecord>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                let result = consumer.poll(std::time::Duration::from_millis(timeout_ms))?;
                Ok(result.map(|(seq, data)| JournalRecord {
                    sequence: seq,
                    data,
                }))
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "journal consumer is closed".into(),
            }),
        }
    }

    pub fn ack(&self, sequence: i64) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                consumer.ack(sequence)?;
                Ok(())
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "journal consumer is closed".into(),
            }),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.take();
        Ok(())
    }
}

pub struct DatasetBridge {
    inner: Arc<timslite::DataSet>,
    identifier: u64,
}

impl DatasetBridge {
    pub fn new(dataset: Arc<timslite::DataSet>) -> Self {
        let identifier = dataset.identifier();
        Self {
            inner: dataset,
            identifier,
        }
    }

    pub fn identifier(&self) -> u64 {
        self.identifier
    }

    pub fn is_closed(&self) -> bool {
        false
    }

    pub fn write(&self, timestamp: i64, data: Vec<u8>) -> Result<(), TmslError> {
        self.inner.write(timestamp, &data)?;
        Ok(())
    }

    pub fn append(&self, timestamp: i64, data: Vec<u8>) -> Result<(), TmslError> {
        self.inner.append(timestamp, &data)?;
        Ok(())
    }

    pub fn write_now(&self, data: Vec<u8>) -> Result<(), TmslError> {
        self.inner.write_now(&data)?;
        Ok(())
    }

    pub fn append_now(&self, data: Vec<u8>) -> Result<(), TmslError> {
        self.inner.append_now(&data)?;
        Ok(())
    }

    pub fn delete(&self, timestamp: i64) -> Result<(), TmslError> {
        self.inner.delete(timestamp)?;
        Ok(())
    }

    pub fn read(&self, timestamp: i64) -> Result<Option<Record>, TmslError> {
        let result = self.inner.read(timestamp)?;
        Ok(result.map(|(ts, data)| Record {
            timestamp: ts,
            data,
        }))
    }

    pub fn read_latest(&self) -> Result<Option<Record>, TmslError> {
        let result = self.inner.read_latest()?;
        Ok(result.map(|(ts, data)| Record {
            timestamp: ts,
            data,
        }))
    }

    pub fn read_exist(&self, timestamp: i64) -> Result<bool, TmslError> {
        Ok(self.inner.read_exist(timestamp)?)
    }

    pub fn read_length(&self, timestamp: i64) -> Result<Option<u32>, TmslError> {
        Ok(self.inner.read_length(timestamp)?)
    }

    pub fn query(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Record>, TmslError> {
        let results = self.inner.query(start_ts, end_ts)?;
        Ok(results
            .into_iter()
            .map(|(ts, data)| Record {
                timestamp: ts,
                data,
            })
            .collect())
    }

    pub fn query_exist(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<u8>, TmslError> {
        Ok(self.inner.query_exist(start_ts, end_ts)?)
    }

    pub fn query_length(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<LengthEntry>, TmslError> {
        let results = self.inner.query_length(start_ts, end_ts)?;
        Ok(results
            .into_iter()
            .map(|(ts, len)| LengthEntry {
                timestamp: ts,
                length: len,
            })
            .collect())
    }

    pub fn query_iter(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Arc<QueryIteratorBridge>, TmslError> {
        let iter = self.inner.query_iter(start_ts, end_ts)?;
        Ok(Arc::new(QueryIteratorBridge::new(iter)))
    }

    pub fn query_length_iter(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Arc<QueryLengthIteratorBridge>, TmslError> {
        let iter = self.inner.query_length_iter(start_ts, end_ts)?;
        Ok(Arc::new(QueryLengthIteratorBridge::new(iter)))
    }

    pub fn flush(&self) -> Result<(), TmslError> {
        self.inner.flush()?;
        Ok(())
    }
}
