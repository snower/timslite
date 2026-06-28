use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use memmap2::{Mmap, MmapMut};

use crate::block::{
    BlockHeader, BLOCK_FLAG_COMPRESSED, BLOCK_FLAG_SEALED, BLOCK_FLAG_SINGLE_RECORD,
    BLOCK_HEADER_SIZE, BLOCK_MAX_SIZE,
};
use crate::compress::{compress, decompress, validate_compress_type};
use crate::error::{Result, TmslError};
use crate::header::{MAGIC, PENDING_NONE, VERSION};
use crate::segment::data::SegmentLifecycle;
use crate::util::{read_i64_from_mmap, read_u16_from_mmap, read_u64_from_mmap};

const JOURNAL_FILE_TYPE: u8 = 3;
const META_TYPE_CREATED_AT: u8 = 0x01;
const META_TYPE_BASE_SEQUENCE: u8 = 0x02;
const META_TYPE_FILE_SIZE: u8 = 0x03;
const META_TYPE_COMPRESS_LEVEL: u8 = 0x04;
const META_TYPE_COMPRESS_TYPE: u8 = 0x05;
const META_LENGTH: u16 = 41;
const STATE_LENGTH: u16 = 56;
const HEADER_SIZE: u64 = 9 + META_LENGTH as u64 + 2 + STATE_LENGTH as u64;
const RECORD_HEADER_SIZE: usize = 12;
const RECORD_OVERHEAD: u64 = RECORD_HEADER_SIZE as u64;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_FILE_TYPE: usize = 6;
const OFF_META_LENGTH: usize = 7;
const META_START: usize = 9;
const STATE_LENGTH_SIZE: usize = 2;

const JS_FILE_SIZE: usize = 0;
const JS_WROTE_POSITION: usize = 8;
const JS_RECORD_COUNT: usize = 16;
const JS_TOTAL_UNCOMPRESSED_SIZE: usize = 24;
const JS_PENDING_BLOCK_OFFSET: usize = 32;
const JS_PENDING_WROTE_POSITION: usize = 40;
const JS_PENDING_RECORD_COUNT: usize = 48;

#[derive(Debug, Clone)]
struct JournalSegmentHeader {
    base_sequence: i64,
    max_file_size: u64,
    compress_level: u8,
    compress_type: u8,
    file_size: u64,
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    pending_block_offset: u64,
    pending_wrote_position: u64,
    pending_record_count: u64,
}

impl JournalSegmentHeader {
    fn create_default(
        base_sequence: i64,
        max_file_size: u64,
        file_size: u64,
        compress_level: u8,
        compress_type: u8,
    ) -> Self {
        Self {
            base_sequence,
            max_file_size,
            compress_level,
            compress_type,
            file_size,
            wrote_position: HEADER_SIZE,
            record_count: 0,
            total_uncompressed_size: 0,
            pending_block_offset: PENDING_NONE,
            pending_wrote_position: 0,
            pending_record_count: 0,
        }
    }

    fn write_to(&self, mmap: &mut [u8]) {
        mmap[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&MAGIC);
        mmap[OFF_VERSION..OFF_VERSION + 2].copy_from_slice(&VERSION.to_le_bytes());
        mmap[OFF_FILE_TYPE] = JOURNAL_FILE_TYPE;
        mmap[OFF_META_LENGTH..OFF_META_LENGTH + 2].copy_from_slice(&META_LENGTH.to_le_bytes());

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut off = META_START;
        write_tlv_i64(mmap, &mut off, META_TYPE_CREATED_AT, created_at);
        write_tlv_i64(mmap, &mut off, META_TYPE_BASE_SEQUENCE, self.base_sequence);
        write_tlv_u64(mmap, &mut off, META_TYPE_FILE_SIZE, self.max_file_size);
        write_tlv_u8(
            mmap,
            &mut off,
            META_TYPE_COMPRESS_LEVEL,
            self.compress_level,
        );
        write_tlv_u8(mmap, &mut off, META_TYPE_COMPRESS_TYPE, self.compress_type);

        let state_len_off = state_length_offset();
        mmap[state_len_off..state_len_off + STATE_LENGTH_SIZE]
            .copy_from_slice(&STATE_LENGTH.to_le_bytes());
        self.write_state_to(mmap);
    }

    fn read_from(mmap: &[u8]) -> Result<Self> {
        if mmap.len() < HEADER_SIZE as usize {
            return Err(TmslError::InvalidData(
                "journal segment header truncated".into(),
            ));
        }
        if mmap[OFF_MAGIC..OFF_MAGIC + 4] != MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u16_from_mmap(mmap, OFF_VERSION);
        if version > VERSION {
            log::warn!(
                "Journal segment has version {} but current version is {}",
                version,
                VERSION
            );
        }
        if mmap[OFF_FILE_TYPE] != JOURNAL_FILE_TYPE {
            return Err(TmslError::InvalidData(format!(
                "unexpected journal file_type {}",
                mmap[OFF_FILE_TYPE]
            )));
        }
        let meta_len = read_u16_from_mmap(mmap, OFF_META_LENGTH);
        let state_len_off = META_START + meta_len as usize;
        if mmap.len() < state_len_off + STATE_LENGTH_SIZE {
            return Err(TmslError::InvalidData(
                "journal segment state length truncated".into(),
            ));
        }
        let state_len = read_u16_from_mmap(mmap, state_len_off);
        if state_len < STATE_LENGTH {
            return Err(TmslError::InvalidData(format!(
                "journal state_length {} is smaller than {}",
                state_len, STATE_LENGTH
            )));
        }
        let header_size = state_len_off + STATE_LENGTH_SIZE + state_len as usize;
        if mmap.len() < header_size {
            return Err(TmslError::InvalidData("journal state truncated".into()));
        }

        let (base_sequence, max_file_size, compress_level, compress_type) =
            parse_meta(mmap, META_START, meta_len as usize)?;
        validate_compress_type(compress_type)?;
        let state = state_start(meta_len);
        Ok(Self {
            base_sequence,
            max_file_size,
            compress_level,
            compress_type,
            file_size: read_u64_from_mmap(mmap, state + JS_FILE_SIZE),
            wrote_position: read_u64_from_mmap(mmap, state + JS_WROTE_POSITION),
            record_count: read_u64_from_mmap(mmap, state + JS_RECORD_COUNT),
            total_uncompressed_size: read_u64_from_mmap(mmap, state + JS_TOTAL_UNCOMPRESSED_SIZE),
            pending_block_offset: read_u64_from_mmap(mmap, state + JS_PENDING_BLOCK_OFFSET),
            pending_wrote_position: read_u64_from_mmap(mmap, state + JS_PENDING_WROTE_POSITION),
            pending_record_count: read_u64_from_mmap(mmap, state + JS_PENDING_RECORD_COUNT),
        })
    }

    fn write_state_to(&self, mmap: &mut [u8]) {
        let state = state_start(META_LENGTH);
        mmap[state + JS_FILE_SIZE..state + JS_FILE_SIZE + 8]
            .copy_from_slice(&self.file_size.to_le_bytes());
        mmap[state + JS_WROTE_POSITION..state + JS_WROTE_POSITION + 8]
            .copy_from_slice(&self.wrote_position.to_le_bytes());
        mmap[state + JS_RECORD_COUNT..state + JS_RECORD_COUNT + 8]
            .copy_from_slice(&self.record_count.to_le_bytes());
        mmap[state + JS_TOTAL_UNCOMPRESSED_SIZE..state + JS_TOTAL_UNCOMPRESSED_SIZE + 8]
            .copy_from_slice(&self.total_uncompressed_size.to_le_bytes());
        mmap[state + JS_PENDING_BLOCK_OFFSET..state + JS_PENDING_BLOCK_OFFSET + 8]
            .copy_from_slice(&self.pending_block_offset.to_le_bytes());
        mmap[state + JS_PENDING_WROTE_POSITION..state + JS_PENDING_WROTE_POSITION + 8]
            .copy_from_slice(&self.pending_wrote_position.to_le_bytes());
        mmap[state + JS_PENDING_RECORD_COUNT..state + JS_PENDING_RECORD_COUNT + 8]
            .copy_from_slice(&self.pending_record_count.to_le_bytes());
    }
}

fn state_length_offset() -> usize {
    META_START + META_LENGTH as usize
}

fn state_start(meta_len: u16) -> usize {
    META_START + meta_len as usize + STATE_LENGTH_SIZE
}

fn write_tlv_i64(mmap: &mut [u8], off: &mut usize, t: u8, value: i64) {
    mmap[*off] = t;
    *off += 1;
    mmap[*off..*off + 2].copy_from_slice(&8u16.to_le_bytes());
    *off += 2;
    mmap[*off..*off + 8].copy_from_slice(&value.to_le_bytes());
    *off += 8;
}

fn write_tlv_u64(mmap: &mut [u8], off: &mut usize, t: u8, value: u64) {
    mmap[*off] = t;
    *off += 1;
    mmap[*off..*off + 2].copy_from_slice(&8u16.to_le_bytes());
    *off += 2;
    mmap[*off..*off + 8].copy_from_slice(&value.to_le_bytes());
    *off += 8;
}

fn write_tlv_u8(mmap: &mut [u8], off: &mut usize, t: u8, value: u8) {
    mmap[*off] = t;
    *off += 1;
    mmap[*off..*off + 2].copy_from_slice(&1u16.to_le_bytes());
    *off += 2;
    mmap[*off] = value;
    *off += 1;
}

fn parse_meta(mmap: &[u8], start: usize, len: usize) -> Result<(i64, u64, u8, u8)> {
    let mut base_sequence = None;
    let mut file_size = None;
    let mut compress_level = None;
    let mut compress_type = None;
    let mut off = start;
    let end = start
        .checked_add(len)
        .ok_or_else(|| TmslError::InvalidData("journal meta length overflow".into()))?;
    if mmap.len() < end {
        return Err(TmslError::InvalidData("journal meta truncated".into()));
    }
    while off + 3 <= end {
        let t = mmap[off];
        off += 1;
        let tlv_len = read_u16_from_mmap(mmap, off) as usize;
        off += 2;
        if off + tlv_len > end {
            return Err(TmslError::InvalidData("journal meta TLV truncated".into()));
        }
        match t {
            META_TYPE_BASE_SEQUENCE if tlv_len == 8 => {
                base_sequence = Some(read_i64_from_mmap(mmap, off));
            }
            META_TYPE_FILE_SIZE if tlv_len == 8 => {
                file_size = Some(read_u64_from_mmap(mmap, off));
            }
            META_TYPE_COMPRESS_LEVEL if tlv_len == 1 => {
                compress_level = Some(mmap[off]);
            }
            META_TYPE_COMPRESS_TYPE if tlv_len == 1 => {
                compress_type = Some(mmap[off]);
            }
            _ => {}
        }
        off += tlv_len;
    }
    Ok((
        base_sequence
            .ok_or_else(|| TmslError::InvalidData("journal base_sequence meta missing".into()))?,
        file_size.ok_or_else(|| TmslError::InvalidData("journal file_size meta missing".into()))?,
        compress_level
            .ok_or_else(|| TmslError::InvalidData("journal compress_level meta missing".into()))?,
        compress_type
            .ok_or_else(|| TmslError::InvalidData("journal compress_type meta missing".into()))?,
    ))
}

fn checked_record_size(payload_len: usize) -> Result<usize> {
    RECORD_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or_else(|| TmslError::InvalidData("journal record size overflow".into()))
}

/// A single journal data segment backed by a memory-mapped file.
pub(crate) struct JournalSegment {
    pub(crate) base_sequence: i64,
    path: PathBuf,
    file_size: u64,
    max_file_size: u64,
    header_size: u64,
    wrote_position: u64,
    record_count: u64,
    total_uncompressed_size: u64,
    compress_level: u8,
    compress_type: u8,
    pending_block_offset: Option<u64>,
    pending_wrote_position: u64,
    pending_record_count: u64,
    pub(crate) is_flushed: bool,
    lifecycle: SegmentLifecycle,
    mmap: Option<JournalSegmentMmap>,
    read_only: bool,
    last_accessed_at: Instant,
}

enum JournalSegmentMmap {
    ReadOnly(Mmap),
    ReadWrite(MmapMut),
}

impl JournalSegmentMmap {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::ReadOnly(mmap) => mmap,
            Self::ReadWrite(mmap) => mmap,
        }
    }

    fn as_mut_slice(&mut self) -> Result<&mut [u8]> {
        match self {
            Self::ReadOnly(_) => Err(TmslError::InvalidData(
                "read-only journal segment cannot be modified".into(),
            )),
            Self::ReadWrite(mmap) => Ok(mmap),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Self::ReadOnly(_) => Ok(()),
            Self::ReadWrite(mmap) => {
                mmap.flush()?;
                Ok(())
            }
        }
    }
}

impl JournalSegment {
    pub(crate) fn create(
        path: &Path,
        base_sequence: i64,
        initial_size: u64,
        max_file_size: u64,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        if base_sequence <= 0 {
            return Err(TmslError::InvalidData(
                "journal base_sequence must be positive".into(),
            ));
        }
        validate_compress_type(compress_type)?;
        if initial_size < HEADER_SIZE {
            return Err(TmslError::InvalidData(format!(
                "initial journal segment size {initial_size} is smaller than header {HEADER_SIZE}"
            )));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(initial_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let header = JournalSegmentHeader::create_default(
            base_sequence,
            max_file_size,
            initial_size,
            compress_level,
            compress_type,
        );
        header.write_to(&mut mmap);
        mmap.flush()?;
        Ok(Self {
            base_sequence,
            path: path.to_path_buf(),
            file_size: initial_size,
            max_file_size,
            header_size: HEADER_SIZE,
            wrote_position: HEADER_SIZE,
            record_count: 0,
            total_uncompressed_size: 0,
            compress_level,
            compress_type,
            pending_block_offset: None,
            pending_wrote_position: 0,
            pending_record_count: 0,
            is_flushed: true,
            lifecycle: SegmentLifecycle::OpenReady,
            mmap: Some(JournalSegmentMmap::ReadWrite(mmap)),
            read_only: false,
            last_accessed_at: Instant::now(),
        })
    }

    pub(crate) fn open(path: &Path, base_sequence: i64, max_file_size: u64) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let actual_size = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Self::open_mapped(
            path,
            base_sequence,
            max_file_size,
            actual_size,
            JournalSegmentMmap::ReadWrite(mmap),
            false,
        )
    }

    pub(crate) fn open_read_only(
        path: &Path,
        base_sequence: i64,
        max_file_size: u64,
    ) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let actual_size = file.metadata()?.len();
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Self::open_mapped(
            path,
            base_sequence,
            max_file_size,
            actual_size,
            JournalSegmentMmap::ReadOnly(mmap),
            true,
        )
    }

    fn open_mapped(
        path: &Path,
        base_sequence: i64,
        max_file_size: u64,
        actual_size: u64,
        mmap: JournalSegmentMmap,
        read_only: bool,
    ) -> Result<Self> {
        let header = JournalSegmentHeader::read_from(mmap.as_slice())?;
        if header.base_sequence != base_sequence {
            return Err(TmslError::InvalidData(format!(
                "journal segment base_sequence mismatch: expected {base_sequence}, got {}",
                header.base_sequence
            )));
        }
        let mut segment = Self {
            base_sequence,
            path: path.to_path_buf(),
            file_size: actual_size,
            max_file_size: header.max_file_size.max(max_file_size),
            header_size: HEADER_SIZE,
            wrote_position: header.wrote_position.min(actual_size),
            record_count: header.record_count,
            total_uncompressed_size: header.total_uncompressed_size,
            compress_level: header.compress_level,
            compress_type: header.compress_type,
            pending_block_offset: (header.pending_block_offset != PENDING_NONE)
                .then_some(header.pending_block_offset),
            pending_wrote_position: header.pending_wrote_position,
            pending_record_count: header.pending_record_count,
            is_flushed: true,
            lifecycle: SegmentLifecycle::OpenReady,
            mmap: Some(mmap),
            read_only,
            last_accessed_at: Instant::now(),
        };
        segment.recover_visible_state()?;
        Ok(segment)
    }

    fn mmap(&self) -> Result<&[u8]> {
        self.mmap
            .as_ref()
            .map(JournalSegmentMmap::as_slice)
            .ok_or_else(|| TmslError::MmapError("journal segment is closed".into()))
    }

    fn mmap_mut(&mut self) -> Result<&mut [u8]> {
        self.mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("journal segment is closed".into()))?
            .as_mut_slice()
    }

    pub(crate) fn append_record(&mut self, sequence: i64, payload: &[u8]) -> Result<()> {
        if sequence != self.base_sequence + self.record_count as i64 {
            return Err(TmslError::InvalidData(format!(
                "journal sequence {sequence} is not next for segment {} record_count {}",
                self.base_sequence, self.record_count
            )));
        }
        let record_size = checked_record_size(payload.len())?;
        if record_size > BLOCK_MAX_SIZE as usize {
            if self.pending_block_offset.is_some() {
                self.seal_pending_block()?;
                self.clear_pending()?;
            }
            return self.create_single_record_block(sequence, payload);
        }
        if self.pending_block_offset.is_some() {
            let new_total = self.pending_wrote_position + record_size as u64;
            if new_total > BLOCK_MAX_SIZE as u64 {
                self.seal_pending_block()?;
                self.clear_pending()?;
            } else {
                self.ensure_capacity(self.wrote_position + record_size as u64)?;
                self.write_raw_record_to_pending(sequence, payload)?;
                return Ok(());
            }
        }
        self.ensure_capacity(
            self.wrote_position
                .checked_add(BLOCK_HEADER_SIZE)
                .and_then(|v| v.checked_add(record_size as u64))
                .ok_or_else(|| TmslError::InvalidData("journal append position overflow".into()))?,
        )?;
        self.create_pending_and_append(sequence, payload)
    }

    pub(crate) fn read(&mut self, sequence: i64) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;
        if sequence < self.base_sequence {
            return Ok(None);
        }
        let end_sequence = self.base_sequence + self.record_count as i64;
        if sequence >= end_sequence {
            return Ok(None);
        }
        let mmap = self.mmap()?;
        let mut pos = self.header_size;
        let mut current_sequence = self.base_sequence;
        while pos + BLOCK_HEADER_SIZE <= self.wrote_position {
            let header = BlockHeader::read_from(mmap, pos as usize);
            if header.record_count == 0 {
                break;
            }
            let block_end = pos
                .checked_add(BLOCK_HEADER_SIZE)
                .and_then(|v| v.checked_add(header.payload_size as u64))
                .ok_or_else(|| TmslError::InvalidData("journal block end overflow".into()))?;
            if block_end > self.wrote_position {
                return Ok(None);
            }
            let block_last = current_sequence + header.record_count as i64;
            if sequence >= block_last {
                current_sequence = block_last;
                pos = block_end;
                continue;
            }
            let block_data = self.block_payload(mmap, pos, header)?;
            let mut off = 0usize;
            for _ in 0..header.record_count {
                if off + RECORD_HEADER_SIZE > block_data.len() {
                    return Err(TmslError::InvalidData(
                        "journal record header out of block bounds".into(),
                    ));
                }
                let len = u32::from_le_bytes(block_data[off..off + 4].try_into().unwrap()) as usize;
                let record_sequence =
                    i64::from_le_bytes(block_data[off + 4..off + 12].try_into().unwrap());
                let data_start = off + RECORD_HEADER_SIZE;
                let data_end = data_start.checked_add(len).ok_or_else(|| {
                    TmslError::InvalidData("journal record length overflow".into())
                })?;
                if data_end > block_data.len() {
                    return Err(TmslError::InvalidData(
                        "journal record payload out of block bounds".into(),
                    ));
                }
                if record_sequence == sequence {
                    return Ok(Some(block_data[data_start..data_end].to_vec()));
                }
                off = data_end;
            }
            return Err(TmslError::InvalidData(
                "journal sequence not found in selected block".into(),
            ));
        }
        Ok(None)
    }

    pub(crate) fn recover_visible_state(&mut self) -> Result<()> {
        self.ensure_open()?;
        let mmap = self.mmap()?;
        let mut pos = self.header_size;
        let mut total_records = 0u64;
        let mut total_uncompressed = 0u64;
        let mut pending = None;
        let mut pending_wrote = 0u64;
        let mut pending_records = 0u64;

        while pos + BLOCK_HEADER_SIZE <= self.file_size {
            let header = BlockHeader::read_from(mmap, pos as usize);
            if header.record_count == 0 || header.payload_size == 0 {
                break;
            }
            let block_end = match pos
                .checked_add(BLOCK_HEADER_SIZE)
                .and_then(|v| v.checked_add(header.payload_size as u64))
            {
                Some(end) if end <= self.file_size => end,
                _ if !header.is_compressed() && !header.is_sealed() => {
                    let payload_start = pos + BLOCK_HEADER_SIZE;
                    if payload_start >= self.file_size {
                        break;
                    }
                    let available = &mmap[payload_start as usize..self.file_size as usize];
                    let (prefix_records, prefix_bytes) = scan_complete_record_prefix(
                        available,
                        self.base_sequence + total_records as i64,
                    );
                    if prefix_records == 0 {
                        break;
                    }
                    total_records += prefix_records as u64;
                    total_uncompressed += prefix_bytes as u64;
                    pending = Some(pos - self.header_size);
                    pending_wrote = prefix_bytes as u64;
                    pending_records = prefix_records as u64;
                    pos = payload_start + prefix_bytes as u64;
                    break;
                }
                _ => break,
            };
            let payload = &mmap[(pos + BLOCK_HEADER_SIZE) as usize..block_end as usize];
            let block_data = if header.is_compressed() {
                match decompress(payload, self.compress_type) {
                    Ok(data) => data,
                    Err(_) => break,
                }
            } else {
                payload.to_vec()
            };
            if header.uncompressed_size as usize != block_data.len() {
                break;
            }
            if !records_are_complete(
                &block_data,
                self.base_sequence + total_records as i64,
                header.record_count,
            ) {
                break;
            }
            total_records += header.record_count as u64;
            total_uncompressed += header.uncompressed_size as u64;
            if !header.is_sealed() {
                pending = Some(pos - self.header_size);
                pending_wrote = header.payload_size as u64;
                pending_records = header.record_count as u64;
            } else {
                pending = None;
                pending_wrote = 0;
                pending_records = 0;
            }
            pos = block_end;
        }

        self.wrote_position = pos;
        self.record_count = total_records;
        self.total_uncompressed_size = total_uncompressed;
        self.pending_block_offset = pending;
        self.pending_wrote_position = pending_wrote;
        self.pending_record_count = pending_records;
        if let Some(block_offset) = self.pending_block_offset {
            let header = BlockHeader::new(
                u32::try_from(self.pending_wrote_position).map_err(|_| {
                    TmslError::InvalidData("journal recovered pending block too large".into())
                })?,
                0,
                u16::try_from(self.pending_record_count).map_err(|_| {
                    TmslError::InvalidData("journal recovered record_count exceeds u16".into())
                })?,
                u32::try_from(self.pending_wrote_position).map_err(|_| {
                    TmslError::InvalidData("journal recovered uncompressed size exceeds u32".into())
                })?,
            );
            if !self.read_only {
                let header_pos = (self.header_size + block_offset) as usize;
                header.write_to(self.mmap_mut()?, header_pos);
            }
        }
        if !self.read_only {
            self.write_state()?;
        }
        Ok(())
    }

    pub(crate) fn record_count(&self) -> u64 {
        self.record_count
    }

    pub(crate) fn wrote_position(&self) -> u64 {
        self.wrote_position
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        if let Some(mmap) = self.mmap.as_mut() {
            mmap.flush()?;
        }
        self.is_flushed = true;
        Ok(())
    }

    fn ensure_open(&mut self) -> Result<()> {
        if self.mmap.is_some() {
            return Ok(());
        }
        let file = if self.read_only {
            OpenOptions::new().read(true).open(&self.path)?
        } else {
            OpenOptions::new().read(true).write(true).open(&self.path)?
        };
        self.file_size = file.metadata()?.len();
        self.mmap = Some(if self.read_only {
            JournalSegmentMmap::ReadOnly(unsafe { memmap2::MmapOptions::new().map(&file)? })
        } else {
            JournalSegmentMmap::ReadWrite(unsafe { MmapMut::map_mut(&file)? })
        });
        self.lifecycle = SegmentLifecycle::OpenReady;
        self.last_accessed_at = Instant::now();
        Ok(())
    }

    fn expand(&mut self) -> Result<()> {
        if self.read_only {
            return Err(TmslError::InvalidData(
                "read-only journal segment cannot expand".into(),
            ));
        }
        let target = self.file_size.saturating_mul(2).min(self.max_file_size);
        if target == self.file_size {
            return Err(TmslError::SegmentFull);
        }
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.mmap = None;
        file.set_len(target)?;
        self.mmap = Some(JournalSegmentMmap::ReadWrite(unsafe {
            MmapMut::map_mut(&file)?
        }));
        self.file_size = target;
        self.mark_dirty();
        Ok(())
    }

    fn ensure_capacity(&mut self, required_physical_end: u64) -> Result<()> {
        while self.file_size < required_physical_end {
            self.expand()?;
        }
        Ok(())
    }

    fn create_pending_and_append(&mut self, sequence: i64, payload: &[u8]) -> Result<()> {
        let block_offset = self.wrote_position - self.header_size;
        let hdr_pos = self.wrote_position as usize;
        let mmap = self.mmap_mut()?;
        BlockHeader::new(0, 0, 0, 0).write_to(mmap, hdr_pos);
        self.wrote_position += BLOCK_HEADER_SIZE;
        self.pending_block_offset = Some(block_offset);
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
        self.write_raw_record_to_pending(sequence, payload)
    }

    fn write_raw_record_to_pending(&mut self, sequence: i64, payload: &[u8]) -> Result<()> {
        let data_len = u32::try_from(payload.len())
            .map_err(|_| TmslError::InvalidData("journal payload exceeds u32".into()))?;
        let block_offset = self
            .pending_block_offset
            .ok_or_else(|| TmslError::InvalidData("journal pending block missing".into()))?;
        let base =
            self.header_size + block_offset + BLOCK_HEADER_SIZE + self.pending_wrote_position;
        let record_size = RECORD_OVERHEAD + payload.len() as u64;
        let base = base as usize;
        {
            let mmap = self.mmap_mut()?;
            mmap[base..base + 4].copy_from_slice(&data_len.to_le_bytes());
            mmap[base + 4..base + 12].copy_from_slice(&sequence.to_le_bytes());
            mmap[base + 12..base + 12 + payload.len()].copy_from_slice(payload);
        }

        self.pending_wrote_position += record_size;
        self.pending_record_count += 1;
        self.wrote_position += record_size;
        self.record_count += 1;
        self.total_uncompressed_size += record_size;

        let hdr_pos = (self.header_size + block_offset) as usize;
        let header = BlockHeader::new(
            u32::try_from(self.pending_wrote_position)
                .map_err(|_| TmslError::InvalidData("journal pending block too large".into()))?,
            0,
            u16::try_from(self.pending_record_count).map_err(|_| {
                TmslError::InvalidData("journal pending record_count exceeds u16".into())
            })?,
            u32::try_from(self.pending_wrote_position).map_err(|_| {
                TmslError::InvalidData("journal pending uncompressed size exceeds u32".into())
            })?,
        );
        header.write_to(self.mmap_mut()?, hdr_pos);
        self.write_state()?;
        self.mark_dirty();
        Ok(())
    }

    fn seal_pending_block(&mut self) -> Result<()> {
        let block_offset = self
            .pending_block_offset
            .ok_or_else(|| TmslError::InvalidData("journal pending block missing".into()))?;
        let hdr_pos = (self.header_size + block_offset) as usize;
        let payload_start = hdr_pos + BLOCK_HEADER_SIZE as usize;
        let payload_end = payload_start + self.pending_wrote_position as usize;
        let raw = {
            let mmap = self.mmap_mut()?;
            mmap[payload_start..payload_end].to_vec()
        };
        let compressed = compress(&raw, self.compress_level, self.compress_type)?;
        let compressed_len = u32::try_from(compressed.len())
            .map_err(|_| TmslError::InvalidData("journal compressed block too large".into()))?;
        let header = BlockHeader::new(
            compressed_len,
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED,
            u16::try_from(self.pending_record_count).map_err(|_| {
                TmslError::InvalidData("journal pending record_count exceeds u16".into())
            })?,
            u32::try_from(raw.len())
                .map_err(|_| TmslError::InvalidData("journal raw block too large".into()))?,
        );
        let mmap = self.mmap_mut()?;
        mmap[payload_start..payload_start + compressed.len()].copy_from_slice(&compressed);
        header.write_to(mmap, hdr_pos);
        self.wrote_position =
            self.header_size + block_offset + BLOCK_HEADER_SIZE + compressed.len() as u64;
        self.write_state()?;
        self.mark_dirty();
        Ok(())
    }

    fn create_single_record_block(&mut self, sequence: i64, payload: &[u8]) -> Result<()> {
        let data_len = u32::try_from(payload.len())
            .map_err(|_| TmslError::InvalidData("journal payload exceeds u32".into()))?;
        let mut raw = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
        raw.extend_from_slice(&data_len.to_le_bytes());
        raw.extend_from_slice(&sequence.to_le_bytes());
        raw.extend_from_slice(payload);
        let compressed = compress(&raw, self.compress_level, self.compress_type)?;
        let required = self
            .wrote_position
            .checked_add(BLOCK_HEADER_SIZE)
            .and_then(|v| v.checked_add(compressed.len() as u64))
            .ok_or_else(|| TmslError::InvalidData("journal single block overflow".into()))?;
        self.ensure_capacity(required)?;
        let hdr_pos = self.wrote_position as usize;
        let mmap = self.mmap_mut()?;
        let header = BlockHeader::new(
            u32::try_from(compressed.len()).map_err(|_| {
                TmslError::InvalidData("journal single block compressed size exceeds u32".into())
            })?,
            BLOCK_FLAG_SEALED | BLOCK_FLAG_COMPRESSED | BLOCK_FLAG_SINGLE_RECORD,
            1,
            u32::try_from(raw.len()).map_err(|_| {
                TmslError::InvalidData("journal single block raw size exceeds u32".into())
            })?,
        );
        header.write_to(mmap, hdr_pos);
        let payload_start = hdr_pos + BLOCK_HEADER_SIZE as usize;
        mmap[payload_start..payload_start + compressed.len()].copy_from_slice(&compressed);
        self.wrote_position = required;
        self.record_count += 1;
        self.total_uncompressed_size += raw.len() as u64;
        self.write_state()?;
        self.mark_dirty();
        Ok(())
    }

    fn clear_pending(&mut self) -> Result<()> {
        self.pending_block_offset = None;
        self.pending_wrote_position = 0;
        self.pending_record_count = 0;
        self.write_state()
    }

    fn block_payload(&self, mmap: &[u8], pos: u64, header: BlockHeader) -> Result<Vec<u8>> {
        let payload_start = (pos + BLOCK_HEADER_SIZE) as usize;
        let payload_end = payload_start + header.payload_size as usize;
        if payload_end > mmap.len() {
            return Err(TmslError::InvalidData(
                "journal block payload out of bounds".into(),
            ));
        }
        let payload = &mmap[payload_start..payload_end];
        if header.is_compressed() {
            decompress(payload, self.compress_type)
        } else {
            Ok(payload.to_vec())
        }
    }

    fn write_state(&mut self) -> Result<()> {
        let header = JournalSegmentHeader {
            base_sequence: self.base_sequence,
            max_file_size: self.max_file_size,
            compress_level: self.compress_level,
            compress_type: self.compress_type,
            file_size: self.file_size,
            wrote_position: self.wrote_position,
            record_count: self.record_count,
            total_uncompressed_size: self.total_uncompressed_size,
            pending_block_offset: self.pending_block_offset.unwrap_or(PENDING_NONE),
            pending_wrote_position: self.pending_wrote_position,
            pending_record_count: self.pending_record_count,
        };
        header.write_state_to(self.mmap_mut()?);
        Ok(())
    }

    fn mark_dirty(&mut self) {
        self.is_flushed = false;
        self.last_accessed_at = Instant::now();
    }

    #[cfg(test)]
    fn mmap_for_test(&self) -> &[u8] {
        self.mmap.as_ref().unwrap().as_slice()
    }

    #[cfg(test)]
    fn header_size(&self) -> u64 {
        self.header_size
    }
}

fn records_are_complete(block_data: &[u8], first_sequence: i64, record_count: u16) -> bool {
    let mut off = 0usize;
    for i in 0..record_count {
        if off + RECORD_HEADER_SIZE > block_data.len() {
            return false;
        }
        let len = u32::from_le_bytes(block_data[off..off + 4].try_into().unwrap()) as usize;
        let sequence = i64::from_le_bytes(block_data[off + 4..off + 12].try_into().unwrap());
        if sequence != first_sequence + i as i64 {
            return false;
        }
        let data_end = match off
            .checked_add(RECORD_HEADER_SIZE)
            .and_then(|v| v.checked_add(len))
        {
            Some(end) => end,
            None => return false,
        };
        if data_end > block_data.len() {
            return false;
        }
        off = data_end;
    }
    off == block_data.len()
}

fn scan_complete_record_prefix(block_data: &[u8], first_sequence: i64) -> (u16, usize) {
    let mut off = 0usize;
    let mut count = 0u16;
    loop {
        if off + RECORD_HEADER_SIZE > block_data.len() {
            return (count, off);
        }
        let len = u32::from_le_bytes(block_data[off..off + 4].try_into().unwrap()) as usize;
        let sequence = i64::from_le_bytes(block_data[off + 4..off + 12].try_into().unwrap());
        if sequence != first_sequence + count as i64 {
            return (count, off);
        }
        let data_end = match off
            .checked_add(RECORD_HEADER_SIZE)
            .and_then(|v| v.checked_add(len))
        {
            Some(end) => end,
            None => return (count, off),
        };
        if data_end > block_data.len() {
            return (count, off);
        }
        off = data_end;
        count = match count.checked_add(1) {
            Some(next) => next,
            None => return (count, off),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::{BlockHeader, BLOCK_FLAG_SINGLE_RECORD};
    use crate::compress::COMPRESS_TYPE_ZSTD;
    use std::fs;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_journal_segment_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_segment(name: &str) -> (JournalSegment, std::path::PathBuf) {
        let dir = temp_dir(name);
        let path = dir.join("00000000000000000001");
        let segment =
            JournalSegment::create(&path, 1, 512, 128 * 1024, 3, COMPRESS_TYPE_ZSTD).unwrap();
        (segment, path)
    }

    #[test]
    fn journal_segment_appends_sequence_records_and_reads_by_sequence() {
        let (mut segment, _path) = make_segment("roundtrip");

        segment.append_record(1, b"first").unwrap();
        segment.append_record(2, b"second").unwrap();

        assert_eq!(segment.record_count(), 2);
        assert_eq!(segment.read(1).unwrap().unwrap(), b"first");
        assert_eq!(segment.read(2).unwrap().unwrap(), b"second");
        assert!(segment.read(3).unwrap().is_none());
    }

    #[test]
    fn journal_segment_uses_single_record_block_for_max_tlv_payload() {
        let (mut segment, _path) = make_segment("single_record");
        let payload = vec![0xAB; 65_538];

        segment.append_record(1, &payload).unwrap();

        let mmap = segment.mmap_for_test();
        let header_pos = segment.header_size() as usize;
        let header = BlockHeader::read_from(mmap, header_pos);
        assert!(header.flags & BLOCK_FLAG_SINGLE_RECORD != 0);
        assert_eq!(segment.read(1).unwrap().unwrap(), payload);
    }

    #[test]
    fn journal_segment_recovery_truncates_half_written_tail() {
        let (mut segment, path) = make_segment("recovery");
        segment.append_record(1, b"stable").unwrap();
        let visible_end = segment.wrote_position();
        segment.append_record(2, b"tail").unwrap();
        drop(segment);

        {
            let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(visible_end + 6).unwrap();
        }

        let mut reopened = JournalSegment::open(&path, 1, 128 * 1024).unwrap();

        assert_eq!(reopened.record_count(), 1);
        assert_eq!(reopened.read(1).unwrap().unwrap(), b"stable");
        assert!(reopened.read(2).unwrap().is_none());
    }

    #[test]
    fn open_read_only_allows_reads_and_rejects_writes() {
        let (mut segment, path) = make_segment("open_read_only");
        segment.append_record(1, b"first").unwrap();
        segment.append_record(2, b"second").unwrap();
        drop(segment);

        let mut ro = JournalSegment::open_read_only(&path, 1, 128 * 1024).unwrap();
        assert_eq!(ro.read(1).unwrap().unwrap(), b"first");
        assert_eq!(ro.read(2).unwrap().unwrap(), b"second");
        assert!(ro.read(3).unwrap().is_none());

        assert!(ro.append_record(3, b"third").is_err());
    }

    #[test]
    fn expand_triggers_when_records_exceed_initial_file_size() {
        let (mut segment, _path) = make_segment("expand");
        let payload = vec![0xAB; 200];

        // initial_size=512, HEADER_SIZE=108, first block header=16.
        // After 2 records of 212 bytes each, wrote_position = 108+16+2*212=548 > 512.
        segment.append_record(1, &payload).unwrap();
        segment.append_record(2, &payload).unwrap();

        assert_eq!(segment.record_count(), 2);
        assert_eq!(segment.read(1).unwrap().unwrap(), payload);
        assert_eq!(segment.read(2).unwrap().unwrap(), payload);
    }

    #[test]
    fn seal_pending_block_when_block_overflow_triggers_compression() {
        let (mut segment, _path) = make_segment("seal_pending");
        let payload = vec![0xAB; 2000]; // 2012 bytes per record (12 header + 2000 payload)

        // BLOCK_MAX_SIZE = 65536. 32 records * 2012 = 64384 ≤ 65536.
        // 33rd record = 66396 > 65536 → seal_pending_block triggers.
        let total: i64 = 33;
        for i in 1i64..=total {
            segment.append_record(i, &payload).unwrap();
        }

        assert_eq!(segment.record_count(), total as u64);
        for i in 1i64..=total {
            assert_eq!(
                segment.read(i).unwrap().unwrap(),
                payload,
                "record {i} mismatch after seal"
            );
        }
    }

    #[test]
    fn mmap_for_test_returns_valid_slice_after_write() {
        let (mut segment, _path) = make_segment("mmap_access");
        segment.append_record(1, b"hello").unwrap();

        let mmap = segment.mmap_for_test();
        assert!(
            mmap.len() >= segment.wrote_position() as usize,
            "mmap should be large enough to contain all written data"
        );
        assert!(
            mmap.len() >= segment.header_size() as usize,
            "mmap should include the header"
        );
    }

    #[test]
    fn ensure_capacity_allows_many_records_beyond_initial_size() {
        let (mut segment, _path) = make_segment("capacity");
        let payload = vec![0u8; 100];

        // write 10 records; initial_size=512 but records total > 512 → ensure_capacity expands
        for i in 1i64..=10 {
            segment.append_record(i, &payload).unwrap();
        }

        assert_eq!(segment.record_count(), 10);
        for i in 1i64..=10 {
            assert_eq!(segment.read(i).unwrap().unwrap(), payload);
        }
    }

    #[test]
    fn write_raw_record_to_pending_exercised_via_append() {
        let (mut segment, _path) = make_segment("raw_record");
        // first append exercises create_pending_and_append → write_raw_record_to_pending
        segment.append_record(1, b"alpha").unwrap();
        assert_eq!(segment.record_count(), 1);
        assert_eq!(segment.read(1).unwrap().unwrap(), b"alpha");

        // second append to same pending block exercises write_raw_record_to_pending directly
        segment.append_record(2, b"beta").unwrap();
        assert_eq!(segment.record_count(), 2);
        assert_eq!(segment.read(2).unwrap().unwrap(), b"beta");
    }
}
