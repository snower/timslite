//! Journal change log stored in the internal `.journal/logs` dataset.

use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::config::StoreConfig;
use crate::dataset::{DataSet, DataSetJournalSink, DataSetKey, DataSetRuntimeContext};
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;
use crate::queue::DatasetQueue;

pub const JOURNAL_DATASET_NAME: &str = ".journal";
pub const JOURNAL_DATASET_TYPE: &str = "logs";

const LOG_CREATE_DATASET: u8 = 0x01;
const LOG_DROP_DATASET: u8 = 0x02;
const LOG_DATA_WRITE: u8 = 0x11;
const LOG_DATA_DELETE: u8 = 0x12;
const LOG_DATA_APPEND: u8 = 0x13;

const TLV_NAME: u8 = 0x01;
const TLV_TYPE: u8 = 0x02;
const TLV_META_OR_INDEX: u8 = 0x03;
const TLV_APPEND_INFO: u8 = 0x04;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalRecordKind {
    CreateDataset,
    DropDataset,
    DataWrite,
    DataDelete,
    DataAppend,
    Unknown(u8),
}

impl JournalRecordKind {
    fn to_log_type(self) -> u8 {
        match self {
            JournalRecordKind::CreateDataset => LOG_CREATE_DATASET,
            JournalRecordKind::DropDataset => LOG_DROP_DATASET,
            JournalRecordKind::DataWrite => LOG_DATA_WRITE,
            JournalRecordKind::DataDelete => LOG_DATA_DELETE,
            JournalRecordKind::DataAppend => LOG_DATA_APPEND,
            JournalRecordKind::Unknown(t) => t,
        }
    }

    fn from_log_type(t: u8) -> Self {
        match t {
            LOG_CREATE_DATASET => JournalRecordKind::CreateDataset,
            LOG_DROP_DATASET => JournalRecordKind::DropDataset,
            LOG_DATA_WRITE => JournalRecordKind::DataWrite,
            LOG_DATA_DELETE => JournalRecordKind::DataDelete,
            LOG_DATA_APPEND => JournalRecordKind::DataAppend,
            other => JournalRecordKind::Unknown(other),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalIndexInfo {
    pub timestamp: i64,
    pub block_offset: u64,
    pub in_block_offset: u16,
}

impl From<IndexEntry> for JournalIndexInfo {
    fn from(entry: IndexEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalAppendInfo {
    pub data_offset: u32,
    pub data_len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    pub kind: JournalRecordKind,
    pub name: String,
    pub dataset_type: String,
    pub metadata: Option<Vec<u8>>,
    pub index_info: Option<JournalIndexInfo>,
    pub append_info: Option<JournalAppendInfo>,
}

impl JournalRecord {
    pub fn create(name: &str, dataset_type: &str, metadata: Vec<u8>) -> Self {
        Self {
            kind: JournalRecordKind::CreateDataset,
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
            metadata: Some(metadata),
            index_info: None,
            append_info: None,
        }
    }

    pub fn drop_dataset(name: &str, dataset_type: &str, metadata: Vec<u8>) -> Self {
        Self {
            kind: JournalRecordKind::DropDataset,
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
            metadata: Some(metadata),
            index_info: None,
            append_info: None,
        }
    }

    pub fn data_write(name: &str, dataset_type: &str, entry: IndexEntry) -> Self {
        Self {
            kind: JournalRecordKind::DataWrite,
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
            metadata: None,
            index_info: Some(entry.into()),
            append_info: None,
        }
    }

    pub fn data_delete(name: &str, dataset_type: &str, entry: IndexEntry) -> Self {
        Self {
            kind: JournalRecordKind::DataDelete,
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
            metadata: None,
            index_info: Some(entry.into()),
            append_info: None,
        }
    }

    pub fn data_append(
        name: &str,
        dataset_type: &str,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Self {
        Self {
            kind: JournalRecordKind::DataAppend,
            name: name.to_string(),
            dataset_type: dataset_type.to_string(),
            metadata: None,
            index_info: Some(entry.into()),
            append_info: Some(JournalAppendInfo {
                data_offset,
                data_len,
            }),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut tlv = Vec::new();
        push_tlv(&mut tlv, TLV_NAME, self.name.as_bytes())?;
        push_tlv(&mut tlv, TLV_TYPE, self.dataset_type.as_bytes())?;
        match self.kind {
            JournalRecordKind::CreateDataset | JournalRecordKind::DropDataset => {
                let metadata = self
                    .metadata
                    .as_ref()
                    .ok_or_else(|| TmslError::InvalidData("journal metadata is missing".into()))?;
                push_tlv(&mut tlv, TLV_META_OR_INDEX, metadata)?;
            }
            JournalRecordKind::DataWrite
            | JournalRecordKind::DataDelete
            | JournalRecordKind::DataAppend => {
                let index = self.index_info.ok_or_else(|| {
                    TmslError::InvalidData("journal index_info is missing".into())
                })?;
                let mut buf = [0u8; 18];
                buf[0..8].copy_from_slice(&index.timestamp.to_le_bytes());
                buf[8..16].copy_from_slice(&index.block_offset.to_le_bytes());
                buf[16..18].copy_from_slice(&index.in_block_offset.to_le_bytes());
                push_tlv(&mut tlv, TLV_META_OR_INDEX, &buf)?;
                if self.kind == JournalRecordKind::DataAppend {
                    let append = self.append_info.ok_or_else(|| {
                        TmslError::InvalidData("journal append_info is missing".into())
                    })?;
                    let mut append_buf = [0u8; 8];
                    append_buf[0..4].copy_from_slice(&append.data_offset.to_le_bytes());
                    append_buf[4..8].copy_from_slice(&append.data_len.to_le_bytes());
                    push_tlv(&mut tlv, TLV_APPEND_INFO, &append_buf)?;
                }
            }
            JournalRecordKind::Unknown(_) => {}
        }

        let len = u16::try_from(tlv.len())
            .map_err(|_| TmslError::InvalidData("journal TLV list too large".into()))?;
        let mut out = Vec::with_capacity(3 + tlv.len());
        out.push(self.kind.to_log_type());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&tlv);
        Ok(out)
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 3 {
            return Err(TmslError::InvalidData("journal record too short".into()));
        }
        let kind = JournalRecordKind::from_log_type(buf[0]);
        let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
        if buf.len() < 3 + len {
            return Err(TmslError::InvalidData(
                "journal record TLV list truncated".into(),
            ));
        }
        let mut off = 3;
        let end = 3 + len;
        let mut name = None;
        let mut dataset_type = None;
        let mut field3 = None;
        let mut append_field = None;
        while off < end {
            if off + 3 > end {
                return Err(TmslError::InvalidData(
                    "journal TLV header truncated".into(),
                ));
            }
            let t = buf[off];
            let tlv_len = u16::from_le_bytes([buf[off + 1], buf[off + 2]]) as usize;
            off += 3;
            if off + tlv_len > end {
                return Err(TmslError::InvalidData("journal TLV value truncated".into()));
            }
            let value = &buf[off..off + tlv_len];
            match t {
                TLV_NAME => {
                    name = Some(
                        std::str::from_utf8(value)
                            .map_err(|e| TmslError::InvalidData(e.to_string()))?
                            .to_string(),
                    );
                }
                TLV_TYPE => {
                    dataset_type = Some(
                        std::str::from_utf8(value)
                            .map_err(|e| TmslError::InvalidData(e.to_string()))?
                            .to_string(),
                    );
                }
                TLV_META_OR_INDEX => field3 = Some(value.to_vec()),
                TLV_APPEND_INFO => append_field = Some(value.to_vec()),
                _ => {}
            }
            off += tlv_len;
        }

        let name = name.ok_or_else(|| TmslError::InvalidData("journal name missing".into()))?;
        let dataset_type =
            dataset_type.ok_or_else(|| TmslError::InvalidData("journal type missing".into()))?;
        let mut metadata = None;
        let mut index_info = None;
        let mut append_info = None;
        match kind {
            JournalRecordKind::CreateDataset | JournalRecordKind::DropDataset => {
                metadata =
                    Some(field3.ok_or_else(|| {
                        TmslError::InvalidData("journal metadata missing".into())
                    })?);
            }
            JournalRecordKind::DataWrite
            | JournalRecordKind::DataDelete
            | JournalRecordKind::DataAppend => {
                let bytes = field3
                    .ok_or_else(|| TmslError::InvalidData("journal index_info missing".into()))?;
                if bytes.len() != 18 {
                    return Err(TmslError::InvalidData(
                        "journal index_info must be 18 bytes".into(),
                    ));
                }
                index_info = Some(JournalIndexInfo {
                    timestamp: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    block_offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                    in_block_offset: u16::from_le_bytes(bytes[16..18].try_into().unwrap()),
                });
                if kind == JournalRecordKind::DataAppend {
                    let append_bytes = append_field.ok_or_else(|| {
                        TmslError::InvalidData("journal append_info missing".into())
                    })?;
                    if append_bytes.len() != 8 {
                        return Err(TmslError::InvalidData(
                            "journal append_info must be 8 bytes".into(),
                        ));
                    }
                    append_info = Some(JournalAppendInfo {
                        data_offset: u32::from_le_bytes(append_bytes[0..4].try_into().unwrap()),
                        data_len: u32::from_le_bytes(append_bytes[4..8].try_into().unwrap()),
                    });
                }
            }
            JournalRecordKind::Unknown(_) => {}
        }

        Ok(Self {
            kind,
            name,
            dataset_type,
            metadata,
            index_info,
            append_info,
        })
    }
}

fn push_tlv(out: &mut Vec<u8>, t: u8, value: &[u8]) -> Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| TmslError::InvalidData("journal TLV value too large".into()))?;
    out.push(t);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

pub(crate) enum JournalManager {
    Enabled {
        dataset: Arc<Mutex<DataSet>>,
        queue: Mutex<Option<DatasetQueue>>,
    },
    Disabled,
}

impl JournalManager {
    pub(crate) fn open_or_create(data_dir: &Path, config: &StoreConfig) -> Result<Self> {
        if !config.enable_journal {
            return Ok(Self::Disabled);
        }
        let key = Self::key();
        let base_dir = data_dir
            .join(JOURNAL_DATASET_NAME)
            .join(JOURNAL_DATASET_TYPE);
        let mut ds = if base_dir.join("meta").exists() {
            DataSet::open(key, base_dir)?
        } else {
            DataSet::create(
                key,
                base_dir,
                config.data_segment_size,
                config.index_segment_size,
                config.compress_level,
                0,
                config.initial_data_segment_size,
                config.initial_index_segment_size,
                0,
            )?
        };
        ds.set_runtime_context(DataSetRuntimeContext::read_only());
        Ok(Self::Enabled {
            dataset: Arc::new(Mutex::new(ds)),
            queue: Mutex::new(None),
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
        matches!(self, JournalManager::Enabled { .. })
    }

    pub(crate) fn dataset(&self) -> Result<Arc<Mutex<DataSet>>> {
        match self {
            JournalManager::Enabled { dataset, .. } => Ok(Arc::clone(dataset)),
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
        }
    }

    pub(crate) fn open_queue(&self) -> Result<DatasetQueue> {
        match self {
            JournalManager::Enabled { dataset, queue } => {
                let mut cached = queue
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
                if let Some(existing) = cached.as_ref() {
                    return Ok(existing.clone());
                }
                let (inner, notify) = {
                    let mut ds = dataset
                        .lock()
                        .map_err(|_| TmslError::InvalidData("journal dataset poisoned".into()))?;
                    ds.open_queue()?
                };
                let q = DatasetQueue::new_readonly_producer(Arc::clone(dataset), inner, notify);
                *cached = Some(q.clone());
                Ok(q)
            }
            JournalManager::Disabled => Err(TmslError::NotFound("journal is disabled".into())),
        }
    }

    pub(crate) fn append_create(&self, key: &DataSetKey, metadata: &[u8]) -> Result<Option<i64>> {
        self.append(JournalRecord::create(
            &key.name,
            &key.dataset_type,
            metadata.to_vec(),
        ))
    }

    pub(crate) fn append_drop(&self, key: &DataSetKey, metadata: &[u8]) -> Result<Option<i64>> {
        self.append(JournalRecord::drop_dataset(
            &key.name,
            &key.dataset_type,
            metadata.to_vec(),
        ))
    }

    pub(crate) fn append_data_write(
        &self,
        key: &DataSetKey,
        entry: IndexEntry,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_write(
            &key.name,
            &key.dataset_type,
            entry,
        ))
    }

    pub(crate) fn append_data_delete(
        &self,
        key: &DataSetKey,
        entry: IndexEntry,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_delete(
            &key.name,
            &key.dataset_type,
            entry,
        ))
    }

    pub(crate) fn append_data_append(
        &self,
        key: &DataSetKey,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Result<Option<i64>> {
        self.append(JournalRecord::data_append(
            &key.name,
            &key.dataset_type,
            entry,
            data_offset,
            data_len,
        ))
    }

    pub(crate) fn flush(&self) -> Result<()> {
        if let JournalManager::Enabled { dataset, .. } = self {
            dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("journal dataset poisoned".into()))?
                .flush()?;
        }
        Ok(())
    }

    pub(crate) fn close(&self) -> Result<()> {
        if let JournalManager::Enabled { dataset, .. } = self {
            dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("journal dataset poisoned".into()))?
                .close()?;
        }
        Ok(())
    }

    fn append(&self, record: JournalRecord) -> Result<Option<i64>> {
        match self {
            JournalManager::Disabled => Ok(None),
            JournalManager::Enabled { dataset, .. } => {
                let payload = record.encode()?;
                let mut ds = dataset
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal dataset poisoned".into()))?;
                let ts = next_journal_ts(ds.latest_written_timestamp())?;
                ds.write_with_cache(ts, &payload, None)?;
                Ok(Some(ts))
            }
        }
    }
}

impl DataSetJournalSink for JournalManager {
    fn record_write(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()> {
        self.append_data_write(key, entry).map(|_| ())
    }

    fn record_delete(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()> {
        self.append_data_delete(key, entry).map(|_| ())
    }

    fn record_append(
        &self,
        key: &DataSetKey,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Result<()> {
        self.append_data_append(key, entry, data_offset, data_len)
            .map(|_| ())
    }
}

fn next_journal_ts(last: i64) -> Result<i64> {
    if last == i64::MAX {
        return Err(TmslError::InvalidData(
            "journal timestamp overflow at i64::MAX".into(),
        ));
    }
    Ok(last + 1)
}

pub(crate) fn meta_values_from_file(meta_path: &Path) -> Result<Vec<u8>> {
    let bytes = std::fs::read(meta_path)?;
    if bytes.len() < 8 {
        return Err(TmslError::InvalidData("meta file too short".into()));
    }
    let meta_len = u16::from_le_bytes([bytes[6], bytes[7]]) as usize;
    if bytes.len() < 8 + meta_len {
        return Err(TmslError::InvalidData("meta file truncated".into()));
    }
    Ok(bytes[8..8 + meta_len].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_encode_decode_data_write() {
        let entry = IndexEntry::new(42, 1234, 7);
        let record = JournalRecord::data_write("sensor", "events", entry);
        let payload = record.encode().unwrap();
        let decoded = JournalRecord::decode(&payload).unwrap();

        assert_eq!(decoded.kind, JournalRecordKind::DataWrite);
        assert_eq!(decoded.name, "sensor");
        assert_eq!(decoded.dataset_type, "events");
        assert_eq!(
            decoded.index_info.unwrap(),
            JournalIndexInfo {
                timestamp: 42,
                block_offset: 1234,
                in_block_offset: 7
            }
        );
    }

    #[test]
    fn test_journal_decode_rejects_truncated_outer_length() {
        let payload = [LOG_CREATE_DATASET, 10, 0, TLV_NAME];
        assert!(matches!(
            JournalRecord::decode(&payload),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_next_journal_ts_is_sequence() {
        assert_eq!(next_journal_ts(0).unwrap(), 1);
        assert_eq!(next_journal_ts(1).unwrap(), 2);
        assert_eq!(next_journal_ts(42).unwrap(), 43);
        assert!(matches!(
            next_journal_ts(i64::MAX),
            Err(TmslError::InvalidData(_))
        ));
    }
}
