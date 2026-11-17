//! HotBlockCache re-export from `cache.rs`.

pub use crate::cache::HotBlockCache;

use crate::block::RECORD_OFFSET_UNIT_BYTES;
use crate::error::{Result, TmslError};

/// `data_len: u32` + `timestamp: i64`, matching the on-disk record header.
const RECORD_HEADER_SIZE: usize = 12;

impl HotBlockCache {
    /// Read the record header from a cached block payload without copying the
    /// record body. Returns `(timestamp, data_len)`.
    ///
    /// `in_block_offset` is in 4-byte units (index-entry semantics); the byte
    /// position within the block payload is `in_block_offset * 4`.
    pub(crate) fn read_record_header_from(
        block_data: &[u8],
        in_block_offset: u16,
    ) -> Result<(i64, u32)> {
        let pos = in_block_offset as usize * RECORD_OFFSET_UNIT_BYTES;
        let header_end = pos
            .checked_add(RECORD_HEADER_SIZE)
            .ok_or_else(|| TmslError::InvalidData("hot block: record header overflow".into()))?;
        if header_end > block_data.len() {
            return Err(TmslError::InvalidData(
                "hot block: record index out of bounds".into(),
            ));
        }

        let data_len = u32::from_le_bytes(
            block_data[pos..pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("hot block: cannot read data_len".into()))?,
        );
        let timestamp = i64::from_le_bytes(
            block_data[pos + 4..pos + 12]
                .try_into()
                .map_err(|_| TmslError::InvalidData("hot block: cannot read timestamp".into()))?,
        );

        let data_end = header_end
            .checked_add(data_len as usize)
            .ok_or_else(|| TmslError::InvalidData("hot block: record data end overflow".into()))?;
        if data_end > block_data.len() {
            return Err(TmslError::InvalidData(
                "hot block: record data out of bounds".into(),
            ));
        }

        Ok((timestamp, data_len))
    }
}
