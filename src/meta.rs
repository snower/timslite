//! DataSetMeta: immutable dataset configuration stored as TLV in a `meta` file.
//!
//! Written once at dataset creation, never updated. It is the sole persistent
//! source of dataset layout/configuration when reopening.

use std::path::Path;

use crate::compress::validate_compress_type;
use crate::config::validate_retention_window;
use crate::error::{Result, TmslError};
use crate::util::{read_i64_le, read_u64_le};

const META_MAGIC: [u8; 4] = *b"TMSM";
const META_VERSION: u16 = 2;

const META_DATA_SEGMENT_SIZE: u8 = 0x01; // u64 LE
const META_INDEX_SEGMENT_SIZE: u8 = 0x02; // u64 LE
const META_COMPRESS_LEVEL: u8 = 0x03; // u8
const META_CREATE_TIME: u8 = 0x04; // i64 LE (unix ms)
const META_INDEX_CONTINUOUS: u8 = 0x05; // u8
const META_INITIAL_DATA_SEGMENT_SIZE: u8 = 0x06; // u64 LE
const META_INITIAL_INDEX_SEGMENT_SIZE: u8 = 0x07; // u64 LE
const META_RETENTION_WINDOW: u8 = 0x08; // u64 LE (0 = no limit)
const META_COMPRESS_TYPE: u8 = 0x09; // u8
const META_ENABLE_JOURNAL: u8 = 0x0A; // u8 (0=false, 1=true)
const META_TIMESTAMP_UNITS_PER_SECONDS: u8 = 0x0B; // u64 LE (0 = legacy retention)
const META_INDEX_DATA_SIZE: u8 = 0x0C; // u32 LE (immutable per-entry custom bytes)

pub(crate) const META_VALUES_LEN_V1: usize = 82;
/// V1 entries plus the 11-byte `timestamp_units_per_seconds` TLV.
pub(crate) const META_VALUES_LEN_V2: usize = META_VALUES_LEN_V1 + 11;
/// V2 entries plus the 7-byte `index_data_size` TLV.
pub(crate) const META_VALUES_LEN_V3: usize = META_VALUES_LEN_V2 + 7;

/// Immutable dataset configuration. Written once at creation.
#[derive(Debug, Clone)]
pub struct DataSetMeta {
    pub data_segment_size: u64,
    pub index_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub create_time: i64, // unix ms
    pub index_continuous: u8,
    pub initial_data_segment_size: u64, // 0 = uninitialized (backward compat)
    pub initial_index_segment_size: u64, // 0 = uninitialized (backward compat)
    pub retention_window: u64,          // 0 = no limit (same unit as timestamp)
    pub enable_journal: bool,
    pub timestamp_units_per_seconds: u64, // 0 = legacy retention scale
    pub index_data_size: u32,             // immutable per-entry custom index bytes
}

impl DataSetMeta {
    /// Create a new meta (for new datasets, immutable after creation).
    pub fn new(
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        compress_type: u8,
        index_continuous: u8,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        retention_window: u64,
        enable_journal: bool,
        timestamp_units_per_seconds: u64,
        index_data_size: u32,
    ) -> Self {
        Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            compress_type,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
            enable_journal,
            timestamp_units_per_seconds,
            index_data_size,
            create_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    /// Serialize: magic(4) + version(2) + meta_data_length(2) + TLV values
    pub fn to_bytes(&self) -> Vec<u8> {
        // Calculate meta_data_length:
        // 11 TLV entries: data_seg_size(11) + idx_seg_size(11) + compress_level(4)
        //               + compress_type(4) + create_time(11)
        //               + index_continuous(4) + initial_data_seg_size(11) + initial_idx_seg_size(11)
        //               + retention_window(11) + enable_journal(4) + timestamp_units_per_seconds(11)
        //               + index_data_size(7) = 100
        // Each u64 TLV: type(1) + length(2) + value(8) = 11 bytes
        // Each u8 TLV:  type(1) + length(2) + value(1) = 4 bytes
        // Each u32 TLV: type(1) + length(2) + value(4) = 7 bytes
        let meta_data_length: u16 = META_VALUES_LEN_V3 as u16;
        debug_assert_eq!(
            META_VALUES_LEN_V3,
            (1 + 2 + 8)
                + (1 + 2 + 8)
                + (1 + 2 + 1)
                + (1 + 2 + 1)
                + (1 + 2 + 8)
                + (1 + 2 + 1)
                + (1 + 2 + 8)
                + (1 + 2 + 8)
                + (1 + 2 + 8)
                + (1 + 2 + 1)
                + (1 + 2 + 8)
                + (1 + 2 + 4)
        );

        let mut buf = Vec::with_capacity(8 + meta_data_length as usize);
        buf.extend_from_slice(&META_MAGIC);
        buf.extend_from_slice(&META_VERSION.to_le_bytes());
        buf.extend_from_slice(&meta_data_length.to_le_bytes());

        // TLV entries
        // data_segment_size
        buf.push(META_DATA_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.data_segment_size.to_le_bytes());
        // index_segment_size
        buf.push(META_INDEX_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.index_segment_size.to_le_bytes());
        // compress_level
        buf.push(META_COMPRESS_LEVEL);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.compress_level);
        // compress_type
        buf.push(META_COMPRESS_TYPE);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.compress_type);
        // create_time
        buf.push(META_CREATE_TIME);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.create_time.to_le_bytes());
        // index_continuous
        buf.push(META_INDEX_CONTINUOUS);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(self.index_continuous);
        // initial_data_segment_size
        buf.push(META_INITIAL_DATA_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.initial_data_segment_size.to_le_bytes());
        // initial_index_segment_size
        buf.push(META_INITIAL_INDEX_SEGMENT_SIZE);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.initial_index_segment_size.to_le_bytes());
        // retention_window
        buf.push(META_RETENTION_WINDOW);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.retention_window.to_le_bytes());
        // enable_journal
        buf.push(META_ENABLE_JOURNAL);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.push(u8::from(self.enable_journal));
        // timestamp_units_per_seconds
        buf.push(META_TIMESTAMP_UNITS_PER_SECONDS);
        buf.extend_from_slice(&8u16.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_units_per_seconds.to_le_bytes());
        // index_data_size
        buf.push(META_INDEX_DATA_SIZE);
        buf.extend_from_slice(&4u16.to_le_bytes());
        buf.extend_from_slice(&self.index_data_size.to_le_bytes());

        buf
    }

    /// Deserialize: validate magic 鈫?read version 鈫?read meta_data_length 鈫?parse TLV
    /// Unknown type codes are skipped.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(TmslError::InvalidData("meta file too short".into()));
        }
        if buf[0..4] != META_MAGIC {
            return Err(TmslError::InvalidData("invalid meta magic".into()));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != META_VERSION {
            return Err(TmslError::InvalidData(format!(
                "unsupported meta version {version}, expected {META_VERSION}"
            )));
        }
        let meta_data_length = u16::from_le_bytes(buf[6..8].try_into().unwrap()) as usize;
        if buf.len() < 8 + meta_data_length {
            return Err(TmslError::InvalidData("meta file truncated".into()));
        }

        let mut data_segment_size = 0u64;
        let mut index_segment_size = 0u64;
        let mut compress_level = 0u8;
        let mut compress_type = None;
        let mut create_time = 0i64;
        let mut index_continuous = 0u8;
        let mut initial_data_segment_size = 0u64;
        let mut initial_index_segment_size = 0u64;
        let mut retention_window = 0u64;
        let mut enable_journal = true;
        let mut timestamp_units_per_seconds = 0u64;
        let mut index_data_size: Option<u32> = None;

        let mut off = 8;
        let end = 8 + meta_data_length;
        while off + 3 <= end {
            let t = buf[off];
            off += 1;
            let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
            off += 2;
            if off + len > end {
                return Err(TmslError::InvalidData("meta TLV entry truncated".into()));
            }
            match t {
                META_DATA_SEGMENT_SIZE if len == 8 => {
                    data_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INDEX_SEGMENT_SIZE if len == 8 => {
                    index_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_COMPRESS_LEVEL if len == 1 => {
                    compress_level = buf[off];
                }
                META_COMPRESS_TYPE if len == 1 => {
                    compress_type = Some(buf[off]);
                }
                META_CREATE_TIME if len == 8 => {
                    create_time = read_i64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INDEX_CONTINUOUS if len == 1 => {
                    index_continuous = buf[off];
                }
                META_INITIAL_DATA_SEGMENT_SIZE if len == 8 => {
                    initial_data_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_INITIAL_INDEX_SEGMENT_SIZE if len == 8 => {
                    initial_index_segment_size = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_RETENTION_WINDOW if len == 8 => {
                    retention_window = read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_TIMESTAMP_UNITS_PER_SECONDS if len == 8 => {
                    timestamp_units_per_seconds =
                        read_u64_le(buf[off..off + 8].try_into().unwrap());
                }
                META_ENABLE_JOURNAL if len == 1 => {
                    enable_journal = match buf[off] {
                        0 => false,
                        1 => true,
                        _ => {
                            return Err(TmslError::InvalidData(
                                "meta enable_journal must be 0 or 1".into(),
                            ));
                        }
                    };
                }
                META_INDEX_DATA_SIZE => {
                    if len != 4 {
                        return Err(TmslError::InvalidData(format!(
                            "meta index_data_size TLV must be 4 bytes, got {len}"
                        )));
                    }
                    if index_data_size.is_some() {
                        return Err(TmslError::InvalidData(
                            "meta has duplicate index_data_size TLV".into(),
                        ));
                    }
                    index_data_size =
                        Some(u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()));
                }
                _ => {} // Skip unknown
            }
            off += len;
        }
        if off != end {
            return Err(TmslError::InvalidData(
                "meta TLV data has trailing partial entry".into(),
            ));
        }

        if data_segment_size == 0 {
            return Err(TmslError::InvalidData(
                "meta missing data_segment_size".into(),
            ));
        }
        if index_segment_size == 0 {
            return Err(TmslError::InvalidData(
                "meta missing index_segment_size".into(),
            ));
        }
        if compress_level > 9 {
            return Err(TmslError::InvalidData(
                "meta compress_level must be <= 9".into(),
            ));
        }
        let compress_type = compress_type
            .ok_or_else(|| TmslError::InvalidData("meta missing compress_type".into()))?;
        validate_compress_type(compress_type)?;
        if index_continuous > 1 {
            return Err(TmslError::InvalidData(
                "meta index_continuous must be 0 or 1".into(),
            ));
        }
        if initial_data_segment_size == 0 {
            initial_data_segment_size = data_segment_size;
        }
        if initial_index_segment_size == 0 {
            initial_index_segment_size = index_segment_size;
        }
        if initial_data_segment_size > data_segment_size {
            return Err(TmslError::InvalidData(
                "meta initial_data_segment_size exceeds data_segment_size".into(),
            ));
        }
        if initial_index_segment_size > index_segment_size {
            return Err(TmslError::InvalidData(
                "meta initial_index_segment_size exceeds index_segment_size".into(),
            ));
        }
        validate_retention_window(retention_window)?;
        let index_data_size = index_data_size
            .ok_or_else(|| TmslError::InvalidData("meta missing index_data_size".into()))?;
        let stride = crate::config::index_entry_stride(index_data_size)?;
        let min_index_size = crate::header::INDEX_HEADER_SIZE
            .checked_add(stride as u64)
            .ok_or_else(|| TmslError::InvalidData("meta index capacity overflow".into()))?;
        if initial_index_segment_size < min_index_size {
            return Err(TmslError::InvalidData(format!(
                "meta initial_index_segment_size {initial_index_segment_size} cannot hold header {INDEX_HEADER_SIZE} plus one {stride}-byte entry (minimum {min_index_size})",
                INDEX_HEADER_SIZE = crate::header::INDEX_HEADER_SIZE
            )));
        }
        if index_segment_size < min_index_size {
            return Err(TmslError::InvalidData(format!(
                "meta index_segment_size {index_segment_size} cannot hold header {INDEX_HEADER_SIZE} plus one {stride}-byte entry (minimum {min_index_size})",
                INDEX_HEADER_SIZE = crate::header::INDEX_HEADER_SIZE
            )));
        }

        Ok(Self {
            data_segment_size,
            index_segment_size,
            compress_level,
            compress_type,
            create_time,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
            enable_journal,
            timestamp_units_per_seconds,
            index_data_size,
        })
    }

    /// Write to file (called once at dataset creation).
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        std::fs::write(path, self.to_bytes())
    }

    /// Read from file (called when opening existing dataset).
    pub fn read_from_file(path: &Path) -> Result<Self> {
        let buf = std::fs::read(path).map_err(|e| TmslError::Io(std::io::Error::other(e)))?;
        Self::from_bytes(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_roundtrip() {
        let meta = DataSetMeta::new(
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256 * 1024,
            4 * 1024,
            30 * 86400,
            true,
            0,
            0,
        );
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.data_segment_size, 64 * 1024 * 1024);
        assert_eq!(parsed.index_segment_size, 4 * 1024 * 1024);
        assert_eq!(parsed.compress_level, 6);
        assert_eq!(parsed.compress_type, crate::compress::COMPRESS_TYPE_ZSTD);
        assert_eq!(parsed.index_continuous, 0);
        assert_eq!(parsed.initial_data_segment_size, 256 * 1024);
        assert_eq!(parsed.initial_index_segment_size, 4 * 1024);
        assert_eq!(parsed.retention_window, 30 * 86400);
        assert_eq!(parsed.index_data_size, 0);
    }

    #[test]
    fn test_meta_timestamp_units_per_seconds_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            1_000_000, // timestamp_units_per_seconds
            0,
        );
        let parsed = DataSetMeta::from_bytes(&meta.to_bytes()).unwrap();
        assert_eq!(parsed.timestamp_units_per_seconds, 1_000_000);
        // Other fields must remain intact with the new TLV appended.
        assert_eq!(parsed.data_segment_size, 1024);
        assert_eq!(parsed.retention_window, 0);
        assert!(parsed.enable_journal);
        assert_eq!(parsed.index_data_size, 0);
    }

    fn valid_meta_bytes(index_data_size: u32) -> Vec<u8> {
        DataSetMeta::new(
            64 * 1024 * 1024,
            4 * 1024,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256 * 1024,
            4 * 1024,
            3600,
            true,
            0,
            index_data_size,
        )
        .to_bytes()
    }

    fn set_meta_values_len(bytes: &mut [u8]) {
        let values_len = (bytes.len() - 8) as u16;
        bytes[6..8].copy_from_slice(&values_len.to_le_bytes());
    }

    #[test]
    fn test_meta_index_data_size_roundtrip() {
        for size in [0u32, 3, 16] {
            let parsed = DataSetMeta::from_bytes(&valid_meta_bytes(size)).unwrap();
            assert_eq!(parsed.index_data_size, size);
        }
        assert_eq!(valid_meta_bytes(16).len(), 8 + META_VALUES_LEN_V3);
    }

    #[test]
    fn test_meta_requires_index_data_size_tlv() {
        // The index_data_size TLV is the final 7-byte entry.
        let mut bytes = valid_meta_bytes(0);
        bytes.truncate(bytes.len() - 7);
        set_meta_values_len(&mut bytes);

        let err = DataSetMeta::from_bytes(&bytes).unwrap_err();
        assert!(format!("{err}").contains("index_data_size"), "{err}");
    }

    #[test]
    fn test_meta_rejects_duplicate_index_data_size_tlv() {
        let mut bytes = valid_meta_bytes(16);
        let tlv = bytes[bytes.len() - 7..].to_vec();
        bytes.extend_from_slice(&tlv);
        set_meta_values_len(&mut bytes);

        let err = DataSetMeta::from_bytes(&bytes).unwrap_err();
        assert!(
            format!("{err}").contains("duplicate index_data_size"),
            "{err}"
        );
    }

    #[test]
    fn test_meta_rejects_wrong_length_index_data_size_tlv() {
        let mut bytes = valid_meta_bytes(16);
        let len_at = bytes.len() - 6;
        bytes[len_at..len_at + 2].copy_from_slice(&8u16.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        set_meta_values_len(&mut bytes);

        let err = DataSetMeta::from_bytes(&bytes).unwrap_err();
        assert!(format!("{err}").contains("index_data_size TLV"), "{err}");
    }

    #[test]
    fn test_meta_rejects_index_capacity_below_one_entry() {
        fn meta_with_sizes(
            index_segment_size: u64,
            initial_index_segment_size: u64,
            index_data_size: u32,
        ) -> Vec<u8> {
            DataSetMeta::new(
                1024,
                index_segment_size,
                6,
                crate::compress::COMPRESS_TYPE_ZSTD,
                0,
                256,
                initial_index_segment_size,
                0,
                true,
                0,
                index_data_size,
            )
            .to_bytes()
        }

        // N=0 → stride 16, minimum index file is 128 + 16 = 144 bytes.
        for size in [143u64, 144] {
            let bytes = meta_with_sizes(size, size, 0);
            let result = DataSetMeta::from_bytes(&bytes);
            assert_eq!(result.is_ok(), size == 144, "N=0 size {size}");
        }
        // Max fits but initial does not.
        let err = DataSetMeta::from_bytes(&meta_with_sizes(200, 143, 0)).unwrap_err();
        assert!(
            format!("{err}").contains("initial_index_segment_size"),
            "{err}"
        );
        // N=16 → stride 32, minimum index file is 160 bytes.
        for size in [159u64, 160] {
            let bytes = meta_with_sizes(size, size, 16);
            let result = DataSetMeta::from_bytes(&bytes);
            assert_eq!(result.is_ok(), size == 160, "N=16 size {size}");
        }
    }

    #[test]
    fn test_meta_rejects_stale_and_future_versions() {
        for version in [1u16, 3] {
            let mut bytes = valid_meta_bytes(0);
            bytes[4..6].copy_from_slice(&version.to_le_bytes());
            let err = DataSetMeta::from_bytes(&bytes).unwrap_err();
            assert!(format!("{err}").contains("version"), "{err}");
        }
    }

    #[test]
    fn test_meta_rejects_invalid_compress_type() {
        let invalid = DataSetMeta::new(1024, 512, 6, 99, 0, 256, 144, 0, true, 0, 0);

        assert!(matches!(
            DataSetMeta::from_bytes(&invalid.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_meta_rejects_missing_compress_type() {
        let meta_with = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let mut bytes = meta_with.to_bytes();
        let mut off = 8;
        while off + 3 <= bytes.len() {
            let t = bytes[off];
            let len = u16::from_le_bytes([bytes[off + 1], bytes[off + 2]]) as usize;
            let entry_len = 3 + len;
            if t == META_COMPRESS_TYPE {
                bytes.drain(off..off + entry_len);
                let meta_len = (bytes.len() - 8) as u16;
                bytes[6..8].copy_from_slice(&meta_len.to_le_bytes());
                break;
            }
            off += entry_len;
        }

        let result = DataSetMeta::from_bytes(&bytes);

        assert!(matches!(result, Err(TmslError::InvalidData(_))));
    }

    #[test]
    fn test_meta_rejects_missing_required_segment_size() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let mut bytes = meta.to_bytes();
        let mut off = 8;
        while off + 3 <= bytes.len() {
            let t = bytes[off];
            let len = u16::from_le_bytes([bytes[off + 1], bytes[off + 2]]) as usize;
            let entry_len = 3 + len;
            if t == META_DATA_SEGMENT_SIZE {
                bytes.drain(off..off + entry_len);
                let meta_len = (bytes.len() - 8) as u16;
                bytes[6..8].copy_from_slice(&meta_len.to_le_bytes());
                break;
            }
            off += entry_len;
        }

        let result = DataSetMeta::from_bytes(&bytes);

        assert!(matches!(result, Err(TmslError::InvalidData(_))));
    }

    #[test]
    fn test_meta_rejects_invalid_create_parameters() {
        let invalid_compress = DataSetMeta::new(
            1024,
            512,
            10,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_compress.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));

        let invalid_continuous = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            2,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_continuous.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));

        let invalid_initial = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            2048,
            144,
            0,
            true,
            0,
            0,
        );
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid_initial.to_bytes()),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_meta_index_continuous_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            1,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let bytes = meta.to_bytes();

        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.index_continuous, 1);

        // Also verify 0 works
        let meta_zero = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let parsed_zero = DataSetMeta::from_bytes(&meta_zero.to_bytes()).unwrap();
        assert_eq!(parsed_zero.index_continuous, 0);
    }

    #[test]
    fn test_meta_retention_window_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            7 * 86400,
            true,
            0,
            0,
        );
        let parsed = DataSetMeta::from_bytes(&meta.to_bytes()).unwrap();
        assert_eq!(parsed.retention_window, 7 * 86400);

        // Zero = no limit
        let meta_zero = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let parsed_zero = DataSetMeta::from_bytes(&meta_zero.to_bytes()).unwrap();
        assert_eq!(parsed_zero.retention_window, 0);
    }

    #[test]
    fn test_meta_enable_journal_roundtrip_default_and_invalid_value() {
        let disabled = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            false,
            0,
            0,
        );
        let parsed = DataSetMeta::from_bytes(&disabled.to_bytes()).unwrap();
        assert!(!parsed.enable_journal);

        let enabled = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let mut bytes = enabled.to_bytes();
        let mut off = 8;
        while off + 3 <= bytes.len() {
            let t = bytes[off];
            let len = u16::from_le_bytes([bytes[off + 1], bytes[off + 2]]) as usize;
            let entry_len = 3 + len;
            if t == META_ENABLE_JOURNAL {
                bytes.drain(off..off + entry_len);
                let meta_len = (bytes.len() - 8) as u16;
                bytes[6..8].copy_from_slice(&meta_len.to_le_bytes());
                break;
            }
            off += entry_len;
        }
        let parsed_missing = DataSetMeta::from_bytes(&bytes).unwrap();
        assert!(parsed_missing.enable_journal);

        let mut invalid = enabled.to_bytes();
        let mut off = 8;
        while off + 3 <= invalid.len() {
            let t = invalid[off];
            let len = u16::from_le_bytes([invalid[off + 1], invalid[off + 2]]) as usize;
            if t == META_ENABLE_JOURNAL {
                invalid[off + 3] = 2;
                break;
            }
            off += 3 + len;
        }
        assert!(matches!(
            DataSetMeta::from_bytes(&invalid),
            Err(TmslError::InvalidData(_))
        ));
    }

    #[test]
    fn test_meta_rejects_retention_window_above_i64_max() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            i64::MAX as u64 + 1,
            true,
            0,
            0,
        );

        let err = DataSetMeta::from_bytes(&meta.to_bytes()).unwrap_err();
        assert!(format!("{err}").contains("retention_window"));
    }

    #[test]
    fn test_meta_magic() {
        assert_eq!(&META_MAGIC, b"TMSM");
    }

    #[test]
    fn test_meta_invalid_magic() {
        let mut bytes = DataSetMeta::new(
            100,
            200,
            3,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            100,
            200,
            0,
            true,
            0,
            0,
        )
        .to_bytes();
        bytes[0..4].copy_from_slice(b"XXXX");
        let result = DataSetMeta::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_meta_file_roundtrip() {
        use std::fs;
        let dir = std::env::temp_dir().join("timslite_meta_test");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_meta");

        let meta = DataSetMeta::new(
            1024,
            512,
            9,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            86400,
            true,
            0,
            0,
        );
        meta.write_to_file(&path).unwrap();
        let loaded = DataSetMeta::read_from_file(&path).unwrap();
        assert_eq!(loaded.data_segment_size, 1024);
        assert_eq!(loaded.index_segment_size, 512);
        assert_eq!(loaded.compress_level, 9);
        assert_eq!(loaded.index_continuous, 0);
        assert_eq!(loaded.initial_data_segment_size, 256);
        assert_eq!(loaded.initial_index_segment_size, 144);
        assert_eq!(loaded.retention_window, 86400);
    }

    #[test]
    fn test_meta_roundtrip_zero_values() {
        // from_bytes backward compat: initial_data_segment_size=0 defaults to data_segment_size.
        // So passing 0 for init_data and data_seg=1 → parsed.initial_data_segment_size = 1.
        let meta = DataSetMeta::new(
            1,
            144,
            0,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            1,
            0,
            0,
            true,
            0,
            0,
        );
        let bytes = meta.to_bytes();
        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.initial_data_segment_size, 1);
        assert_eq!(parsed.initial_index_segment_size, 144);
    }

    #[test]
    fn test_meta_new_sets_create_time() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(meta.create_time >= before && meta.create_time <= after);
    }

    #[test]
    fn test_meta_from_bytes_empty() {
        let result = DataSetMeta::from_bytes(&[]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TmslError::InvalidData(_)));
    }

    #[test]
    fn test_meta_from_bytes_truncated_header() {
        let result = DataSetMeta::from_bytes(b"TMSM\x01");
        assert!(result.is_err());
    }

    #[test]
    fn test_meta_from_bytes_truncated_tlv() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let bytes = meta.to_bytes();
        let truncated = &bytes[..bytes.len() - 5];
        let result = DataSetMeta::from_bytes(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_meta_deflate_compress_type_roundtrip() {
        let meta = DataSetMeta::new(
            2048,
            1024,
            3,
            crate::compress::COMPRESS_TYPE_DEFLATE,
            0,
            512,
            256,
            86400,
            false,
            0,
            0,
        );
        let bytes = meta.to_bytes();
        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.compress_type, crate::compress::COMPRESS_TYPE_DEFLATE);
        assert_eq!(parsed.compress_level, 3);
        assert_eq!(parsed.data_segment_size, 2048);
        assert_eq!(parsed.index_segment_size, 1024);
        assert_eq!(parsed.initial_data_segment_size, 512);
        assert_eq!(parsed.initial_index_segment_size, 256);
        assert_eq!(parsed.retention_window, 86400);
        assert!(!parsed.enable_journal);
    }

    #[test]
    fn test_meta_to_bytes_length() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            true,
            0,
            0,
        );
        let bytes = meta.to_bytes();
        assert_eq!(bytes.len(), 8 + META_VALUES_LEN_V3);
    }

    #[test]
    fn test_meta_read_from_file_not_found() {
        let result = DataSetMeta::read_from_file(std::path::Path::new("/nonexistent/path/meta"));
        assert!(result.is_err());
    }

    #[test]
    fn test_meta_enable_journal_false_roundtrip() {
        let meta = DataSetMeta::new(
            1024,
            512,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
            0,
            256,
            144,
            0,
            false,
            0,
            0,
        );
        let bytes = meta.to_bytes();
        let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
        assert!(!parsed.enable_journal);
    }

    proptest::proptest! {
        #[test]
        fn proptest_meta_roundtrip(
            data_seg in proptest::num::u64::ANY,
            idx_seg in proptest::num::u64::ANY,
            compress in proptest::num::u8::ANY,
            continuous in proptest::num::u8::ANY,
            init_data in proptest::num::u64::ANY,
            init_idx in proptest::num::u64::ANY,
            retention in 0u64..=i64::MAX as u64,
            index_data_size in proptest::num::u32::ANY,
        ) {
            let data_seg = data_seg.max(1);
            let compress = compress % 10; // compress_level must be <= 9
            let continuous = continuous % 2;
            // init must be <= segment_size (from_bytes validation)
            let init_data = init_data % data_seg + 1;
            // Index files must hold the header plus at least one entry.
            let min_idx = crate::header::INDEX_HEADER_SIZE
                + crate::config::index_entry_stride(index_data_size).unwrap() as u64;
            let idx_seg = idx_seg.max(min_idx);
            let init_idx = min_idx + (init_idx % (idx_seg - min_idx + 1));
            let meta = DataSetMeta::new(
                data_seg, idx_seg, compress, crate::compress::COMPRESS_TYPE_ZSTD, continuous,
                init_data, init_idx, retention, true, 0, index_data_size);
            let bytes = meta.to_bytes();
            let parsed = DataSetMeta::from_bytes(&bytes).unwrap();
            assert_eq!(parsed.data_segment_size, data_seg);
            assert_eq!(parsed.index_segment_size, idx_seg);
            assert_eq!(parsed.compress_level, compress);
            assert_eq!(parsed.compress_type, crate::compress::COMPRESS_TYPE_ZSTD);
            assert_eq!(parsed.index_continuous, continuous);
            assert_eq!(parsed.initial_data_segment_size, init_data);
            assert_eq!(parsed.initial_index_segment_size, init_idx);
            assert_eq!(parsed.retention_window, retention);
            assert!(parsed.enable_journal);
            assert_eq!(parsed.index_data_size, index_data_size);
        }
    }
}
