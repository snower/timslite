//! Journal change log stored in the internal `.journal/logs` append log.

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::config::StoreConfig;
use crate::dataset::{DataSetJournalSink, DataSetKey, SegmentFlushQueue};
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;

pub const JOURNAL_DATASET_NAME: &str = ".journal";
pub const JOURNAL_DATASET_TYPE: &str = "logs";

mod log;
mod queue;
mod record;
mod segment;

pub use queue::{JournalQueue, JournalQueueConsumer};
pub(crate) use record::{meta_values_from_file, validate_create_drop_record_inputs};
pub use record::{JournalAppendInfo, JournalIndexInfo, JournalRecord, JournalRecordKind};

use log::JournalLog;

pub(crate) enum JournalManager {
    Enabled {
        log: Arc<Mutex<JournalLog>>,
        queue_dir: std::path::PathBuf,
        queue: Mutex<Option<JournalQueue>>,
    },
    ReadOnly {
        log: Option<Arc<Mutex<JournalLog>>>,
    },
    Disabled,
}

impl JournalManager {
    pub(crate) fn open_or_create(
        data_dir: &Path,
        config: &StoreConfig,
        _flush_queue: Option<SegmentFlushQueue>,
    ) -> Result<Self> {
        if !config.enable_journal {
            return Ok(Self::Disabled);
        }
        let base_dir = data_dir
            .join(JOURNAL_DATASET_NAME)
            .join(JOURNAL_DATASET_TYPE);
        let log = JournalLog::open_or_create(base_dir, config)?;
        Ok(Self::Enabled {
            log: Arc::new(Mutex::new(log)),
            queue_dir: data_dir
                .join(JOURNAL_DATASET_NAME)
                .join(JOURNAL_DATASET_TYPE)
                .join("queue"),
            queue: Mutex::new(None),
        })
    }

    pub(crate) fn open_read_only(data_dir: &Path, config: &StoreConfig) -> Result<Self> {
        if !config.enable_journal {
            return Ok(Self::Disabled);
        }
        let base_dir = data_dir
            .join(JOURNAL_DATASET_NAME)
            .join(JOURNAL_DATASET_TYPE);
        Ok(Self::ReadOnly {
            log: JournalLog::open_read_only(base_dir, config)?.map(|log| Arc::new(Mutex::new(log))),
        })
    }

    pub(crate) fn key() -> DataSetKey {
        DataSetKey {
            name: JOURNAL_DATASET_NAME.to_string(),
            dataset_type: JOURNAL_DATASET_TYPE.to_string(),
        }
    }

    pub(crate) fn is_journal_key(key: &DataSetKey) -> bool {
        key.name == JOURNAL_DATASET_NAME && key.dataset_type == JOURNAL_DATASET_TYPE
    }

    pub(crate) fn is_enabled(&self) -> bool {
        matches!(
            self,
            JournalManager::Enabled { .. } | JournalManager::ReadOnly { .. }
        )
    }

    pub(crate) fn open_queue(&self) -> Result<JournalQueue> {
        match self {
            JournalManager::Enabled {
                log,
                queue_dir,
                queue,
            } => {
                let mut cached = queue
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
                if let Some(existing) = cached.as_ref() {
                    return Ok(existing.clone());
                }
                let q = JournalQueue::new(Arc::clone(log), queue_dir.clone());
                *cached = Some(q.clone());
                Ok(q)
            }
            JournalManager::ReadOnly { .. } => Err(TmslError::InvalidData(
                "journal queue is not available in read-only store".into(),
            )),
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
        }
    }

    pub(crate) fn append_create(
        &self,
        identifier: u64,
        key: &DataSetKey,
        metadata: &[u8],
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::create(
            identifier,
            &key.name,
            &key.dataset_type,
            metadata.to_vec(),
        ))
    }

    pub(crate) fn append_drop(
        &self,
        identifier: u64,
        key: &DataSetKey,
        metadata: &[u8],
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::drop_dataset(
            identifier,
            &key.name,
            &key.dataset_type,
            metadata.to_vec(),
        ))
    }

    pub(crate) fn append_data_write(
        &self,
        identifier: u64,
        entry: IndexEntry,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_write(identifier, entry))
    }

    pub(crate) fn append_data_delete(
        &self,
        identifier: u64,
        entry: IndexEntry,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_delete(identifier, entry))
    }

    pub(crate) fn append_data_append(
        &self,
        identifier: u64,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_append(
            identifier,
            entry,
            data_offset,
            data_len,
        ))
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.flush_dirty()
    }

    pub(crate) fn flush_dirty(&self) -> Result<()> {
        match self {
            JournalManager::Enabled { log, queue, .. } => {
                log.lock()
                    .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                    .flush_dirty()?;
                if let Some(q) = queue
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?
                    .as_ref()
                {
                    q.flush_state_files()?;
                }
            }
            JournalManager::ReadOnly { .. } | JournalManager::Disabled => {}
        }
        Ok(())
    }

    pub(crate) fn close(&self) -> Result<()> {
        match self {
            JournalManager::Enabled { queue, .. } => {
                if let Some(q) = queue
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?
                    .as_ref()
                {
                    let _ = q.close();
                }
                self.flush_dirty()?;
            }
            JournalManager::ReadOnly { .. } | JournalManager::Disabled => {}
        }
        Ok(())
    }

    pub(crate) fn latest_sequence(&self) -> Result<Option<i64>> {
        match self {
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
            JournalManager::Enabled { log, .. } => Ok(log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .latest_sequence()),
            JournalManager::ReadOnly { log } => match log {
                Some(log) => Ok(log
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                    .latest_sequence()),
                None => Ok(None),
            },
        }
    }

    pub(crate) fn read(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>> {
        match self {
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
            JournalManager::Enabled { log, .. } => log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .read(sequence),
            JournalManager::ReadOnly { log } => match log {
                Some(log) => log
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                    .read(sequence),
                None => Ok(None),
            },
        }
    }

    pub(crate) fn query(&self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>> {
        match self {
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
            JournalManager::Enabled { log, .. } => log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .query(start, end),
            JournalManager::ReadOnly { log } => match log {
                Some(log) => log
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                    .query(start, end),
                None => Ok(Vec::new()),
            },
        }
    }

    fn append(&self, record: JournalRecord) -> Result<Option<i64>> {
        match self {
            JournalManager::Disabled => Ok(None),
            JournalManager::ReadOnly { .. } => Err(TmslError::InvalidData(
                "read-only journal cannot append records".into(),
            )),
            JournalManager::Enabled { log, queue, .. } => {
                let payload = record.encode()?;
                let sequence = log
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                    .append(&payload)?;
                if let Some(q) = queue
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?
                    .as_ref()
                {
                    q.notify_record_appended();
                }
                Ok(Some(sequence))
            }
        }
    }
}

impl DataSetJournalSink for JournalManager {
    fn record_write(&self, identifier: u64, entry: IndexEntry) -> Result<()> {
        self.append_data_write(identifier, entry).map(|_| ())
    }

    fn record_delete(&self, identifier: u64, entry: IndexEntry) -> Result<()> {
        self.append_data_delete(identifier, entry).map(|_| ())
    }

    fn record_append(
        &self,
        identifier: u64,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Result<()> {
        self.append_data_append(identifier, entry, data_offset, data_len)
            .map(|_| ())
    }
}
