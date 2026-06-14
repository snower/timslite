use std::path::Path;

use crate::dataset::DataSetKey;
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;
use crate::util::PATH_COMPONENT_MAX_LEN;

const LOG_CREATE_DATASET: u8 = 0x01;
const LOG_DROP_DATASET: u8 = 0x02;
const LOG_DATA_WRITE: u8 = 0x11;
const LOG_DATA_DELETE: u8 = 0x12;
const LOG_DATA_APPEND: u8 = 0x13;

const TV_ID_U8: u8 = 0x01;
const TV_ID_U16: u8 = 0x02;
const TV_ID_U32: u8 = 0x03;
const TV_ID_U64: u8 = 0x04;

const TV_NAME_OR_INDEX: u8 = 0x10;
const TV_TYPE_OR_APPEND_INFO: u8 = 0x11;
const TV_METADATA: u8 = 0x12;

pub(crate) const JOURNAL_FIELD_VALUE_MAX_LEN: usize = u16::MAX as usize;
pub(crate) const JOURNAL_TEXT_MAX_LEN: usize = PATH_COMPONENT_MAX_LEN;

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
    pub dataset_identifier: u64,
    pub name: Option<String>,
    pub dataset_type: Option<String>,
    pub metadata: Option<Vec<u8>>,
    pub index_info: Option<JournalIndexInfo>,
    pub append_info: Option<JournalAppendInfo>,
}

impl JournalRecord {
    pub fn create(
        dataset_identifier: u64,
        name: &str,
        dataset_type: &str,
        metadata: Vec<u8>,
    ) -> Self {
        Self {
            kind: JournalRecordKind::CreateDataset,
            dataset_identifier,
            name: Some(name.to_string()),
            dataset_type: Some(dataset_type.to_string()),
            metadata: Some(metadata),
            index_info: None,
            append_info: None,
        }
    }

    pub fn drop_dataset(
        dataset_identifier: u64,
        name: &str,
        dataset_type: &str,
        metadata: Vec<u8>,
    ) -> Self {
        Self {
            kind: JournalRecordKind::DropDataset,
            dataset_identifier,
            name: Some(name.to_string()),
            dataset_type: Some(dataset_type.to_string()),
            metadata: Some(metadata),
            index_info: None,
            append_info: None,
        }
    }

    pub fn data_write(dataset_identifier: u64, entry: IndexEntry) -> Self {
        Self {
            kind: JournalRecordKind::DataWrite,
            dataset_identifier,
            name: None,
            dataset_type: None,
            metadata: None,
            index_info: Some(entry.into()),
            append_info: None,
        }
    }

    pub fn data_delete(dataset_identifier: u64, entry: IndexEntry) -> Self {
        Self {
            kind: JournalRecordKind::DataDelete,
            dataset_identifier,
            name: None,
            dataset_type: None,
            metadata: None,
            index_info: Some(entry.into()),
            append_info: None,
        }
    }

    pub fn data_append(
        dataset_identifier: u64,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Self {
        Self {
            kind: JournalRecordKind::DataAppend,
            dataset_identifier,
            name: None,
            dataset_type: None,
            metadata: None,
            index_info: Some(entry.into()),
            append_info: Some(JournalAppendInfo {
                data_offset,
                data_len,
            }),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.push(self.kind.to_log_type());
        push_identifier_tv(&mut out, self.dataset_identifier)?;
        match self.kind {
            JournalRecordKind::CreateDataset | JournalRecordKind::DropDataset => {
                let name = self
                    .name
                    .as_deref()
                    .ok_or_else(|| TmslError::InvalidData("journal name is missing".into()))?;
                let dataset_type = self.dataset_type.as_deref().ok_or_else(|| {
                    TmslError::InvalidData("journal dataset type is missing".into())
                })?;
                validate_journal_text_field("journal name", name)?;
                validate_journal_text_field("journal dataset type", dataset_type)?;
                let metadata = self
                    .metadata
                    .as_ref()
                    .ok_or_else(|| TmslError::InvalidData("journal metadata is missing".into()))?;
                push_len_prefixed_tv(&mut out, TV_NAME_OR_INDEX, name.as_bytes())?;
                push_len_prefixed_tv(&mut out, TV_TYPE_OR_APPEND_INFO, dataset_type.as_bytes())?;
                push_len_prefixed_tv(&mut out, TV_METADATA, metadata)?;
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
                out.push(TV_NAME_OR_INDEX);
                out.extend_from_slice(&buf);
                if self.kind == JournalRecordKind::DataAppend {
                    let append = self.append_info.ok_or_else(|| {
                        TmslError::InvalidData("journal append_info is missing".into())
                    })?;
                    let mut append_buf = [0u8; 8];
                    append_buf[0..4].copy_from_slice(&append.data_offset.to_le_bytes());
                    append_buf[4..8].copy_from_slice(&append.data_len.to_le_bytes());
                    out.push(TV_TYPE_OR_APPEND_INFO);
                    out.extend_from_slice(&append_buf);
                }
            }
            JournalRecordKind::Unknown(_) => {
                return Err(TmslError::InvalidData(
                    "unknown journal log type cannot be encoded".into(),
                ));
            }
        }

        Ok(out)
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(TmslError::InvalidData("journal record too short".into()));
        }
        let kind = JournalRecordKind::from_log_type(buf[0]);
        let mut off = 1;
        let dataset_identifier = parse_identifier_tv(buf, &mut off)?;
        let mut name = None;
        let mut dataset_type = None;
        let mut metadata = None;
        let mut index_info = None;
        let mut append_info = None;
        match kind {
            JournalRecordKind::CreateDataset | JournalRecordKind::DropDataset => {
                expect_field(buf, &mut off, TV_NAME_OR_INDEX)?;
                name = Some(read_len_prefixed_utf8(buf, &mut off, "journal name")?);
                expect_field(buf, &mut off, TV_TYPE_OR_APPEND_INFO)?;
                dataset_type = Some(read_len_prefixed_utf8(
                    buf,
                    &mut off,
                    "journal dataset type",
                )?);
                expect_field(buf, &mut off, TV_METADATA)?;
                metadata = Some(read_len_prefixed_bytes(buf, &mut off, "journal metadata")?);
            }
            JournalRecordKind::DataWrite
            | JournalRecordKind::DataDelete
            | JournalRecordKind::DataAppend => {
                expect_field(buf, &mut off, TV_NAME_OR_INDEX)?;
                let bytes = read_fixed_bytes(buf, &mut off, 18, "journal index_info")?;
                index_info = Some(JournalIndexInfo {
                    timestamp: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    block_offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                    in_block_offset: u16::from_le_bytes(bytes[16..18].try_into().unwrap()),
                });
                if kind == JournalRecordKind::DataAppend {
                    expect_field(buf, &mut off, TV_TYPE_OR_APPEND_INFO)?;
                    let append_bytes = read_fixed_bytes(buf, &mut off, 8, "journal append_info")?;
                    append_info = Some(JournalAppendInfo {
                        data_offset: u32::from_le_bytes(append_bytes[0..4].try_into().unwrap()),
                        data_len: u32::from_le_bytes(append_bytes[4..8].try_into().unwrap()),
                    });
                }
            }
            JournalRecordKind::Unknown(_) => {
                return Err(TmslError::InvalidData("unknown journal log type".into()));
            }
        }
        if off != buf.len() {
            return Err(TmslError::InvalidData(
                "journal record contains schema-unknown fields or trailing bytes".into(),
            ));
        }

        Ok(Self {
            kind,
            dataset_identifier,
            name,
            dataset_type,
            metadata,
            index_info,
            append_info,
        })
    }
}

fn push_len_prefixed_tv(out: &mut Vec<u8>, t: u8, value: &[u8]) -> Result<()> {
    validate_field_value_len("journal field value", value.len())?;
    let len = value.len() as u16;
    out.push(t);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn push_identifier_tv(out: &mut Vec<u8>, identifier: u64) -> Result<()> {
    validate_identifier(identifier)?;
    if let Ok(value) = u8::try_from(identifier) {
        out.push(TV_ID_U8);
        out.push(value);
    } else if let Ok(value) = u16::try_from(identifier) {
        out.push(TV_ID_U16);
        out.extend_from_slice(&value.to_le_bytes());
    } else if let Ok(value) = u32::try_from(identifier) {
        out.push(TV_ID_U32);
        out.extend_from_slice(&value.to_le_bytes());
    } else {
        out.push(TV_ID_U64);
        out.extend_from_slice(&identifier.to_le_bytes());
    }
    Ok(())
}

fn parse_identifier_tv(buf: &[u8], off: &mut usize) -> Result<u64> {
    if *off >= buf.len() {
        return Err(TmslError::InvalidData(
            "journal identifier field missing".into(),
        ));
    }
    let t = buf[*off];
    *off += 1;
    let identifier = match t {
        TV_ID_U8 => read_u8(buf, off)? as u64,
        TV_ID_U16 => read_u16(buf, off)? as u64,
        TV_ID_U32 => read_u32(buf, off)? as u64,
        TV_ID_U64 => read_u64(buf, off)?,
        _ => {
            return Err(TmslError::InvalidData(
                "journal identifier field missing".into(),
            ));
        }
    };
    validate_identifier(identifier)?;
    if identifier_type(identifier) != t {
        return Err(TmslError::InvalidData(
            "journal identifier must use minimal encoding".into(),
        ));
    }
    Ok(identifier)
}

fn identifier_type(identifier: u64) -> u8 {
    if identifier <= u8::MAX as u64 {
        TV_ID_U8
    } else if identifier <= u16::MAX as u64 {
        TV_ID_U16
    } else if identifier <= u32::MAX as u64 {
        TV_ID_U32
    } else {
        TV_ID_U64
    }
}

fn validate_identifier(identifier: u64) -> Result<()> {
    if identifier == 0 {
        Err(TmslError::InvalidData(
            "journal dataset identifier must be > 0".into(),
        ))
    } else {
        Ok(())
    }
}

fn expect_field(buf: &[u8], off: &mut usize, expected: u8) -> Result<()> {
    if *off >= buf.len() {
        return Err(TmslError::InvalidData("journal field missing".into()));
    }
    let actual = buf[*off];
    *off += 1;
    if actual != expected {
        return Err(TmslError::InvalidData(format!(
            "journal field type 0x{actual:02x} does not match expected 0x{expected:02x}"
        )));
    }
    Ok(())
}

fn read_len_prefixed_utf8(buf: &[u8], off: &mut usize, label: &str) -> Result<String> {
    let bytes = read_len_prefixed_bytes(buf, off, label)?;
    let value = std::str::from_utf8(&bytes)
        .map_err(|e| TmslError::InvalidData(e.to_string()))?
        .to_string();
    validate_journal_text_field(label, &value)?;
    Ok(value)
}

fn read_len_prefixed_bytes(buf: &[u8], off: &mut usize, label: &str) -> Result<Vec<u8>> {
    let len = read_u16(buf, off)? as usize;
    Ok(read_fixed_bytes(buf, off, len, label)?.to_vec())
}

fn read_fixed_bytes<'a>(
    buf: &'a [u8],
    off: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    if *off + len > buf.len() {
        return Err(TmslError::InvalidData(format!("{label} truncated")));
    }
    let bytes = &buf[*off..*off + len];
    *off += len;
    Ok(bytes)
}

fn read_u8(buf: &[u8], off: &mut usize) -> Result<u8> {
    Ok(read_fixed_bytes(buf, off, 1, "journal u8")?[0])
}

fn read_u16(buf: &[u8], off: &mut usize) -> Result<u16> {
    let bytes = read_fixed_bytes(buf, off, 2, "journal u16")?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u32(buf: &[u8], off: &mut usize) -> Result<u32> {
    let bytes = read_fixed_bytes(buf, off, 4, "journal u32")?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(buf: &[u8], off: &mut usize) -> Result<u64> {
    let bytes = read_fixed_bytes(buf, off, 8, "journal u64")?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn validate_journal_text_field(label: &str, value: &str) -> Result<()> {
    if value.len() <= JOURNAL_TEXT_MAX_LEN {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "{label} must be at most {JOURNAL_TEXT_MAX_LEN} bytes"
        )))
    }
}

fn validate_field_value_len(label: &str, len: usize) -> Result<()> {
    if len <= JOURNAL_FIELD_VALUE_MAX_LEN {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "{label} must be at most {JOURNAL_FIELD_VALUE_MAX_LEN} bytes"
        )))
    }
}

pub(crate) fn validate_create_drop_record_inputs(
    identifier: u64,
    key: &DataSetKey,
    metadata_len: usize,
) -> Result<()> {
    validate_identifier(identifier)?;
    validate_journal_text_field("journal name", &key.name)?;
    validate_journal_text_field("journal dataset type", &key.dataset_type)?;
    validate_field_value_len("journal metadata", metadata_len)
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
    fn test_journal_data_write_encodes_identifier_tv_without_name_type() {
        let entry = IndexEntry::new(42, 1234, 7);
        let record = JournalRecord::data_write(255, entry);
        let payload = record.encode().unwrap();

        let mut expected = vec![LOG_DATA_WRITE, 0x01, 255, 0x10];
        expected.extend_from_slice(&42i64.to_le_bytes());
        expected.extend_from_slice(&1234u64.to_le_bytes());
        expected.extend_from_slice(&7u16.to_le_bytes());

        assert_eq!(payload, expected);

        let decoded = JournalRecord::decode(&payload).unwrap();
        assert_eq!(decoded.kind, JournalRecordKind::DataWrite);
        assert_eq!(decoded.dataset_identifier, 255);
        assert_eq!(decoded.name, None);
        assert_eq!(decoded.dataset_type, None);
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
    fn test_journal_create_encodes_identifier_name_type_and_metadata() {
        let record = JournalRecord::create(256, "sensor", "events", vec![1, 2, 3]);
        let payload = record.encode().unwrap();

        let mut expected = vec![LOG_CREATE_DATASET, 0x02];
        expected.extend_from_slice(&256u16.to_le_bytes());
        expected.push(0x10);
        expected.extend_from_slice(&6u16.to_le_bytes());
        expected.extend_from_slice(b"sensor");
        expected.push(0x11);
        expected.extend_from_slice(&6u16.to_le_bytes());
        expected.extend_from_slice(b"events");
        expected.push(0x12);
        expected.extend_from_slice(&3u16.to_le_bytes());
        expected.extend_from_slice(&[1, 2, 3]);

        assert_eq!(payload, expected);

        let decoded = JournalRecord::decode(&payload).unwrap();
        assert_eq!(decoded.kind, JournalRecordKind::CreateDataset);
        assert_eq!(decoded.dataset_identifier, 256);
        assert_eq!(decoded.name.as_deref(), Some("sensor"));
        assert_eq!(decoded.dataset_type.as_deref(), Some("events"));
        assert_eq!(decoded.metadata.as_deref(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_journal_decode_rejects_non_minimal_identifier_encoding() {
        let mut payload = vec![LOG_DATA_WRITE, 0x02];
        payload.extend_from_slice(&42u16.to_le_bytes());
        payload.push(0x10);
        payload.extend_from_slice(&42i64.to_le_bytes());
        payload.extend_from_slice(&1234u64.to_le_bytes());
        payload.extend_from_slice(&7u16.to_le_bytes());

        assert!(matches!(
            JournalRecord::decode(&payload),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_journal_identifier_encoding_boundaries_are_canonical() {
        let cases = [
            (1, vec![TV_ID_U8, 1]),
            (255, vec![TV_ID_U8, 255]),
            (256, {
                let mut bytes = vec![TV_ID_U16];
                bytes.extend_from_slice(&256u16.to_le_bytes());
                bytes
            }),
            (65535, {
                let mut bytes = vec![TV_ID_U16];
                bytes.extend_from_slice(&65535u16.to_le_bytes());
                bytes
            }),
            (65536, {
                let mut bytes = vec![TV_ID_U32];
                bytes.extend_from_slice(&65536u32.to_le_bytes());
                bytes
            }),
            (u32::MAX as u64, {
                let mut bytes = vec![TV_ID_U32];
                bytes.extend_from_slice(&u32::MAX.to_le_bytes());
                bytes
            }),
            (u32::MAX as u64 + 1, {
                let mut bytes = vec![TV_ID_U64];
                bytes.extend_from_slice(&(u32::MAX as u64 + 1).to_le_bytes());
                bytes
            }),
            (u64::MAX, {
                let mut bytes = vec![TV_ID_U64];
                bytes.extend_from_slice(&u64::MAX.to_le_bytes());
                bytes
            }),
        ];
        let entry = IndexEntry::new(42, 1234, 7);

        for (identifier, expected_identifier_bytes) in cases {
            let payload = JournalRecord::data_write(identifier, entry)
                .encode()
                .unwrap();
            assert_eq!(
                &payload[1..1 + expected_identifier_bytes.len()],
                expected_identifier_bytes
            );
            let decoded = JournalRecord::decode(&payload).unwrap();
            assert_eq!(decoded.dataset_identifier, identifier);
        }
    }

    #[test]
    fn test_journal_data_append_roundtrip_uses_identifier_and_append_info() {
        let entry = IndexEntry::new(42, 1234, 7);
        let payload = JournalRecord::data_append(65_536, entry, 3, 9)
            .encode()
            .unwrap();
        let decoded = JournalRecord::decode(&payload).unwrap();

        assert_eq!(decoded.kind, JournalRecordKind::DataAppend);
        assert_eq!(decoded.dataset_identifier, 65_536);
        assert_eq!(decoded.name, None);
        assert_eq!(decoded.dataset_type, None);
        assert_eq!(decoded.index_info, Some(entry.into()));
        assert_eq!(
            decoded.append_info,
            Some(JournalAppendInfo {
                data_offset: 3,
                data_len: 9
            })
        );
    }

    #[test]
    fn test_journal_decode_rejects_zero_duplicate_missing_unknown_and_wrong_order() {
        let entry = IndexEntry::new(42, 1234, 7);
        let valid = JournalRecord::data_write(1, entry).encode().unwrap();

        let mut zero = valid.clone();
        zero[2] = 0;
        assert!(matches!(
            JournalRecord::decode(&zero),
            Err(TmslError::InvalidData(_))
        ));

        let mut duplicate = vec![LOG_DATA_WRITE, TV_ID_U8, 1, TV_ID_U8, 1];
        duplicate.push(TV_NAME_OR_INDEX);
        duplicate.extend_from_slice(&42i64.to_le_bytes());
        duplicate.extend_from_slice(&1234u64.to_le_bytes());
        duplicate.extend_from_slice(&7u16.to_le_bytes());
        assert!(matches!(
            JournalRecord::decode(&duplicate),
            Err(TmslError::InvalidData(_))
        ));

        let missing_identifier = [LOG_DATA_WRITE, TV_NAME_OR_INDEX];
        assert!(matches!(
            JournalRecord::decode(&missing_identifier),
            Err(TmslError::InvalidData(_))
        ));

        let mut unknown = valid.clone();
        unknown.push(0xff);
        assert!(matches!(
            JournalRecord::decode(&unknown),
            Err(TmslError::InvalidData(_))
        ));

        let mut wrong_order = vec![LOG_CREATE_DATASET, TV_ID_U8, 1, TV_TYPE_OR_APPEND_INFO];
        wrong_order.extend_from_slice(&1u16.to_le_bytes());
        wrong_order.extend_from_slice(b"t");
        assert!(matches!(
            JournalRecord::decode(&wrong_order),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_journal_encode_decode_data_write() {
        let entry = IndexEntry::new(42, 1234, 7);
        let record = JournalRecord::data_write(1, entry);
        let payload = record.encode().unwrap();
        let decoded = JournalRecord::decode(&payload).unwrap();

        assert_eq!(decoded.kind, JournalRecordKind::DataWrite);
        assert_eq!(decoded.dataset_identifier, 1);
        assert_eq!(decoded.name, None);
        assert_eq!(decoded.dataset_type, None);
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
    fn test_journal_decode_rejects_truncated_identifier() {
        let payload = [LOG_CREATE_DATASET, TV_ID_U16, 1];
        assert!(matches!(
            JournalRecord::decode(&payload),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_journal_encode_rejects_text_field_over_255_bytes() {
        let long_name = "a".repeat(256);
        let record = JournalRecord::create(1, &long_name, "logs", vec![]);

        let err = record
            .encode()
            .expect_err("journal text fields over 255 bytes must be rejected");

        assert!(err.to_string().contains("at most 255 bytes"));
    }

    #[test]
    fn test_journal_encode_rejects_metadata_field_over_u16() {
        let record = JournalRecord::create(1, "a", "b", vec![0; JOURNAL_FIELD_VALUE_MAX_LEN + 1]);

        let err = record
            .encode()
            .expect_err("journal metadata field over u16 must be rejected");

        assert!(err.to_string().contains("at most 65535 bytes"));
    }
}
