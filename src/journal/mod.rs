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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use crate::index::segment::IndexEntry;

    fn journal_temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_journal_mgr_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn journal_config() -> StoreConfig {
        StoreConfig {
            data_segment_size: 420,
            initial_data_segment_size: 420,
            enable_journal: true,
            ..StoreConfig::default()
        }
    }

    fn disabled_config() -> StoreConfig {
        StoreConfig {
            enable_journal: false,
            ..StoreConfig::default()
        }
    }

    #[test]
    fn test_journal_manager_disabled() {
        let dir = journal_temp_dir("disabled");
        let mgr = JournalManager::open_or_create(&dir, &disabled_config(), None).unwrap();
        assert!(matches!(mgr, JournalManager::Disabled));

        // is_enabled returns false for Disabled
        assert!(!mgr.is_enabled());

        // append returns Ok(None) for Disabled
        let entry = IndexEntry::new(1, 100, 0);
        assert_eq!(mgr.append_data_write(1, entry).unwrap(), None);

        // read returns Err for Disabled
        assert!(mgr.read(1).is_err());

        // query returns Err for Disabled
        assert!(mgr.query(1, 5).is_err());

        // latest_sequence returns Err for Disabled
        assert!(mgr.latest_sequence().is_err());

        // open_queue returns Err for Disabled
        assert!(mgr.open_queue().is_err());

        // flush_dirty is a no-op for Disabled
        assert!(mgr.flush_dirty().is_ok());

        // close is a no-op for Disabled
        assert!(mgr.close().is_ok());
    }

    #[test]
    fn test_journal_manager_read_only() {
        let dir = journal_temp_dir("read_only");
        // First, create a journal with some data
        {
            let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();
            assert!(mgr.is_enabled());
            let entry = IndexEntry::new(1, 100, 0);
            assert_eq!(mgr.append_data_write(1, entry).unwrap().unwrap(), 1);
            assert_eq!(mgr.append_data_write(1, entry).unwrap().unwrap(), 2);
            mgr.flush_dirty().unwrap();
        }

        // Re-open as read-only
        let ro_mgr = JournalManager::open_read_only(&dir, &journal_config()).unwrap();
        assert!(matches!(ro_mgr, JournalManager::ReadOnly { .. }));
        assert!(ro_mgr.is_enabled());

        // read should work
        let record = ro_mgr.read(1).unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().0, 1);

        // query should work
        let records = ro_mgr.query(1, 2).unwrap();
        assert_eq!(records.len(), 2);

        // latest_sequence should work
        assert_eq!(ro_mgr.latest_sequence().unwrap(), Some(2));

        // append should fail for ReadOnly
        let entry = IndexEntry::new(3, 100, 0);
        assert!(ro_mgr.append_data_write(1, entry).is_err());

        // open_queue should fail for ReadOnly
        assert!(ro_mgr.open_queue().is_err());

        // flush_dirty is a no-op for ReadOnly
        assert!(ro_mgr.flush_dirty().is_ok());

        // close is a no-op for ReadOnly
        assert!(ro_mgr.close().is_ok());
    }

    #[test]
    fn test_journal_manager_read_only_no_data_dir() {
        let dir = journal_temp_dir("read_only_nodata");
        // No journal data exists yet
        let ro_mgr = JournalManager::open_read_only(&dir, &journal_config()).unwrap();
        assert!(matches!(ro_mgr, JournalManager::ReadOnly { .. }));
        assert!(ro_mgr.is_enabled());

        // read returns Ok(None) when no log
        assert_eq!(ro_mgr.read(1).unwrap(), None);

        // query returns empty when no log
        let records = ro_mgr.query(1, 5).unwrap();
        assert!(records.is_empty());

        // latest_sequence returns Ok(None) when no log
        assert_eq!(ro_mgr.latest_sequence().unwrap(), None);
    }

    #[test]
    fn test_journal_manager_key() {
        let key = JournalManager::key();
        assert_eq!(key.name, ".journal");
        assert_eq!(key.dataset_type, "logs");

        assert!(JournalManager::is_journal_key(&key));

        let not_journal = DataSetKey {
            name: "other".to_string(),
            dataset_type: "data".to_string(),
        };
        assert!(!JournalManager::is_journal_key(&not_journal));

        let partial_name = DataSetKey {
            name: "other".to_string(),
            dataset_type: "logs".to_string(),
        };
        assert!(!JournalManager::is_journal_key(&partial_name));

        let partial_type = DataSetKey {
            name: ".journal".to_string(),
            dataset_type: "other".to_string(),
        };
        assert!(!JournalManager::is_journal_key(&partial_type));
    }

    #[test]
    fn test_journal_manager_enabled_append_and_read() {
        let dir = journal_temp_dir("enabled_append_read");
        let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();
        assert!(matches!(mgr, JournalManager::Enabled { .. }));
        assert!(mgr.is_enabled());

        // Append data_write records
        let entry = IndexEntry::new(1, 100, 0);
        assert_eq!(mgr.append_data_write(1, entry).unwrap().unwrap(), 1);

        let entry2 = IndexEntry::new(1, 200, 0);
        assert_eq!(mgr.append_data_write(1, entry2).unwrap().unwrap(), 2);

        // Read back
        let record = mgr.read(1).unwrap().unwrap();
        assert_eq!(record.0, 1);

        // latest_sequence
        assert_eq!(mgr.latest_sequence().unwrap(), Some(2));

        // query range
        let records = mgr.query(1, 2).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 1);
        assert_eq!(records[1].0, 2);

        mgr.flush_dirty().unwrap();
        mgr.close().unwrap();
    }

    #[test]
    fn test_journal_manager_append_create_drop() {
        let dir = journal_temp_dir("create_drop");
        let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();

        let key = DataSetKey {
            name: "testds".to_string(),
            dataset_type: "testtype".to_string(),
        };

        // append_create
        let seq = mgr.append_create(42, &key, b"meta").unwrap().unwrap();
        assert_eq!(seq, 1);

        // append_drop
        let seq2 = mgr.append_drop(42, &key, b"meta").unwrap().unwrap();
        assert_eq!(seq2, 2);

        // Read back
        let record = mgr.read(1).unwrap().unwrap();
        assert_eq!(record.0, 1);

        mgr.close().unwrap();
    }

    #[test]
    fn test_journal_manager_append_data_delete() {
        let dir = journal_temp_dir("data_delete");
        let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();

        let entry = IndexEntry::new(1, 100, 0);
        let seq = mgr.append_data_delete(1, entry).unwrap().unwrap();
        assert_eq!(seq, 1);

        let record = mgr.read(1).unwrap().unwrap();
        assert_eq!(record.0, 1);

        mgr.close().unwrap();
    }

    #[test]
    fn test_journal_manager_append_data_append() {
        let dir = journal_temp_dir("data_append");
        let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();

        let entry = IndexEntry::new(1, 100, 0);
        let seq = mgr.append_data_append(1, entry, 10, 50).unwrap().unwrap();
        assert_eq!(seq, 1);

        let record = mgr.read(1).unwrap().unwrap();
        assert_eq!(record.0, 1);

        mgr.close().unwrap();
    }

    #[test]
    fn test_journal_manager_flush_dirty() {
        let dir = journal_temp_dir("flush_dirty");
        let mgr = JournalManager::open_or_create(&dir, &journal_config(), None).unwrap();

        let entry = IndexEntry::new(1, 100, 0);
        for i in 1..=5 {
            assert_eq!(mgr.append_data_write(1, entry).unwrap().unwrap(), i);
        }

        // flush_dirty should succeed
        assert!(mgr.flush_dirty().is_ok());

        // Re-read after flush
        let records = mgr.query(1, 5).unwrap();
        assert_eq!(records.len(), 5);

        mgr.close().unwrap();
    }
}
