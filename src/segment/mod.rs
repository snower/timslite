//! DataSegmentSet: manages multiple DataSegment files for a single dataset.
//!
//! Handles lazy open, idle close, append, and cross-segment reads.

pub mod data;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use crate::error::Result;
use crate::error::TmslError;
use crate::header::{DataFileMetadata, TIMESTAMP_MAX_SENTINEL, TIMESTAMP_MIN_SENTINEL};

pub use self::data::{DataSegment, ReadIndexEntry};
use self::data::{DataSegment as DS, SegmentLifecycle as SL};

use crate::cache::{BlockCache, CacheKey, HotBlockCache};

/// Metadata for a closed data segment.
pub(crate) struct DataSegmentMeta {
    pub path: std::path::PathBuf,
    pub file_offset: u64,
    pub file_size: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

pub(crate) enum DataSegmentEntry {
    Open(DataSegment),
    Closed(DataSegmentMeta),
}

impl DataSegmentMeta {
    /// Returns true if this segment's time range overlaps with [start_ts, end_ts].
    pub fn overlaps_time_range(&self, start_ts: i64, end_ts: i64) -> bool {
        // Sentinel values (empty segment) never overlap
        if self.min_timestamp == TIMESTAMP_MIN_SENTINEL
            || self.max_timestamp == TIMESTAMP_MAX_SENTINEL
        {
            return false;
        }
        self.max_timestamp >= start_ts && self.min_timestamp <= end_ts
    }
}

// ─── DataSegmentSet ─────────────────────────────────────────────────────────

pub struct DataSegmentSet {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub initial_segment_size: u64,
    pub compress_level: u8,
    pub compress_type: u8,
    pub(crate) segments: BTreeMap<u64, DataSegmentEntry>,
    pub next_offset: u64,
    pub last_used_at: Instant,
}

impl DataSegmentSet {
    fn segment_offset_for(&self, absolute_offset: u64) -> u64 {
        (absolute_offset / self.segment_size) * self.segment_size
    }

    pub(crate) fn open_segments(&self) -> impl Iterator<Item = &DataSegment> {
        self.segments.values().filter_map(|entry| match entry {
            DataSegmentEntry::Open(seg) => Some(seg),
            DataSegmentEntry::Closed(_) => None,
        })
    }

    pub(crate) fn open_segments_mut(&mut self) -> impl Iterator<Item = &mut DataSegment> {
        self.segments.values_mut().filter_map(|entry| match entry {
            DataSegmentEntry::Open(seg) => Some(seg),
            DataSegmentEntry::Closed(_) => None,
        })
    }

    pub(crate) fn closed_segments(&self) -> impl Iterator<Item = &DataSegmentMeta> {
        self.segments.values().filter_map(|entry| match entry {
            DataSegmentEntry::Open(_) => None,
            DataSegmentEntry::Closed(meta) => Some(meta),
        })
    }

    pub(crate) fn open_len(&self) -> usize {
        self.open_segments().count()
    }

    pub(crate) fn closed_len(&self) -> usize {
        self.closed_segments().count()
    }

    /// Create a new (empty) DataSegmentSet for a freshly created dataset.
    pub fn new(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        compress_level: u8,
    ) -> Result<Self> {
        Self::new_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            compress_level,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
    }

    pub fn new_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        let data_dir = base_dir.join("data");
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self {
            base_dir: data_dir,
            segment_size,
            initial_segment_size,
            compress_level,
            compress_type,
            segments: BTreeMap::new(),
            next_offset: 0,
            last_used_at: Instant::now(),
        })
    }

    /// Sync all open data segments.
    pub fn sync_all(&mut self) -> Result<()> {
        for seg in self.open_segments_mut() {
            seg.sync()?;
        }
        Ok(())
    }

    pub(crate) fn sync_segment(&mut self, file_offset: u64) -> Result<()> {
        if let Some(DataSegmentEntry::Open(seg)) = self.segments.get_mut(&file_offset) {
            if !seg.is_flushed {
                seg.sync()?;
            }
        }
        Ok(())
    }

    /// Idle-close all open data segments.
    pub fn idle_close_all(&mut self) -> Result<()> {
        let keys: Vec<u64> = self
            .segments
            .iter()
            .filter_map(|(key, entry)| match entry {
                DataSegmentEntry::Open(_) => Some(*key),
                DataSegmentEntry::Closed(_) => None,
            })
            .collect();
        for key in keys {
            let Some(DataSegmentEntry::Open(mut seg)) = self.segments.remove(&key) else {
                continue;
            };
            let meta = DataSegmentMeta {
                path: seg.path.clone(),
                file_offset: seg.file_offset,
                file_size: seg.file_size,
                min_timestamp: seg.min_timestamp,
                max_timestamp: seg.max_timestamp,
            };
            seg.idle_close(6)?;
            self.segments.insert(key, DataSegmentEntry::Closed(meta));
        }
        Ok(())
    }

    /// Lazy open a segment by its file_offset.
    pub fn lazy_open(&mut self, file_offset: u64) -> Result<&mut DS> {
        if !self.segments.contains_key(&file_offset) {
            return Err(crate::error::TmslError::NotFound(format!(
                "no segment at offset {}",
                file_offset
            )));
        }
        let needs_open = matches!(
            self.segments.get(&file_offset),
            Some(DataSegmentEntry::Closed(_))
        );
        if needs_open {
            let Some(DataSegmentEntry::Closed(meta)) = self.segments.remove(&file_offset) else {
                unreachable!();
            };
            let seg = DS::open(&meta.path, meta.file_offset, self.segment_size)?;
            self.segments
                .insert(file_offset, DataSegmentEntry::Open(seg));
        }
        match self.segments.get_mut(&file_offset) {
            Some(DataSegmentEntry::Open(seg)) => Ok(seg),
            _ => Err(crate::error::TmslError::NotFound(format!(
                "no segment at offset {}",
                file_offset
            ))),
        }
    }

    /// Load existing data segments from disk (all start closed).
    /// Scans the `data/` subdirectory for segment files.
    pub fn load_existing(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        compress_level: u8,
    ) -> Result<Self> {
        Self::load_existing_with_compression(
            base_dir,
            segment_size,
            initial_segment_size,
            compress_level,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
    }

    pub fn load_existing_with_compression(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        compress_level: u8,
        compress_type: u8,
    ) -> Result<Self> {
        let mut metas: Vec<DataSegmentMeta> = Vec::new();
        // Data files are in `base_dir/data/`
        let data_dir = base_dir.join("data");
        if data_dir.exists() {
            for entry in std::fs::read_dir(data_dir)? {
                let p = entry?.path();
                if p.is_dir() {
                    continue;
                }
                if let Some(stem) = p.file_stem().and_then(|n| n.to_str()) {
                    if let Ok(offset) = stem.parse::<u64>() {
                        let file_size = std::fs::metadata(&p)?.len();
                        // Read min/max timestamps from file header
                        let (min_ts, max_ts) = read_segment_timestamps(&p);
                        metas.push(DataSegmentMeta {
                            path: p,
                            file_offset: offset,
                            file_size,
                            min_timestamp: min_ts,
                            max_timestamp: max_ts,
                        });
                    }
                }
            }
        }
        metas.sort_by_key(|m| m.file_offset);

        let next_offset = metas
            .last()
            .map(|m| m.file_offset + segment_size)
            .unwrap_or(0);

        Ok(Self {
            base_dir: base_dir.to_path_buf().join("data"),
            segment_size,
            initial_segment_size,
            compress_level,
            compress_type,
            segments: metas
                .into_iter()
                .map(|meta| (meta.file_offset, DataSegmentEntry::Closed(meta)))
                .collect(),
            next_offset,
            last_used_at: Instant::now(),
        })
    }

    // ─── Write operations ────────────────────────────────────────────────

    /// Append a record. Returns (segment_offset, block_relative_offset, in_block_offset).
    pub fn append(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u64, u16)> {
        // Get current segment for writing
        let segment_size = self.segment_size;
        let current_offset = if self.segments.is_empty() {
            self.next_offset
        } else {
            let latest_offset = *self.segments.last_key_value().unwrap().0;
            let last = self.lazy_open(latest_offset)?;
            if last.lifecycle == SL::Closed
                || last.data_wrote_position
                    + crate::block::BLOCK_HEADER_SIZE
                    + data::RECORD_OVERHEAD
                    > segment_size
            {
                self.next_offset
            } else {
                last.file_offset
            }
        };

        // Extract config values
        let compress_level = self.compress_level;
        let compress_type = self.compress_type;

        // Try to open existing segment, or create a new one
        let seg = match self.lazy_open(current_offset) {
            Ok(s) => s,
            Err(_) => {
                if let Some(DataSegmentEntry::Open(last)) =
                    self.segments.last_entry().map(|entry| entry.into_mut())
                {
                    last.sync()?;
                }
                // Create new segment with initial_size
                let file_name = format!("{:020}", current_offset);
                let path = self.base_dir.join(&file_name);
                let new_seg = DataSegment::create_with_compression(
                    &path,
                    current_offset,
                    self.initial_segment_size,
                    self.segment_size,
                    compress_level,
                    compress_type,
                )?;
                self.segments
                    .insert(current_offset, DataSegmentEntry::Open(new_seg));
                self.next_offset += self.segment_size;
                match self.segments.get_mut(&current_offset) {
                    Some(DataSegmentEntry::Open(seg)) => seg,
                    _ => unreachable!(),
                }
            }
        };
        if seg.lifecycle == SL::Closed {
            seg.ensure_open(compress_level)?;
        }

        // Try to append; if SegmentFull, expand and retry, or seal + create new.
        let mut written_segment_offset = current_offset;
        let (block_rel_off, in_block_off) = match seg.append_record(timestamp, data, compress_level)
        {
            Ok(result) => result,
            Err(crate::error::TmslError::SegmentFull) => {
                // Try to expand the current segment
                if seg.expand().is_ok() {
                    // Expansion succeeded, retry append
                    seg.append_record(timestamp, data, compress_level)?
                } else {
                    // Already at max_file_size → seal current, create new segment
                    // Mark this segment as needing seal
                    let seg_offset_to_seal = seg.file_offset;
                    seg.sync()?;

                    let new_offset = self.next_offset;
                    let file_name = format!("{:020}", new_offset);
                    let path = self.base_dir.join(&file_name);
                    let new_seg = DataSegment::create_with_compression(
                        &path,
                        new_offset,
                        self.initial_segment_size,
                        self.segment_size,
                        compress_level,
                        compress_type,
                    )?;
                    self.segments
                        .insert(new_offset, DataSegmentEntry::Open(new_seg));
                    self.next_offset = new_offset + self.segment_size;
                    written_segment_offset = new_offset;

                    // Seal the old segment (lazy approach: set lifecycle to Closed)
                    // It will be properly sealed on idle-close
                    {
                        if let Some(DataSegmentEntry::Open(seg)) =
                            self.segments.get_mut(&seg_offset_to_seal)
                        {
                            seg.lifecycle = SL::Closed;
                        }
                    }

                    let new_seg = match self.segments.get_mut(&new_offset) {
                        Some(DataSegmentEntry::Open(seg)) => seg,
                        _ => unreachable!(),
                    };
                    new_seg.append_record(timestamp, data, compress_level)?
                }
            }
            Err(e) => return Err(e),
        };

        self.last_used_at = Instant::now();
        Ok((written_segment_offset, block_rel_off, in_block_off))
    }

    /// Get the segment size configuration.
    pub fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Build the global cache key for an index entry block offset.
    pub fn cache_key_for_absolute_offset(&self, absolute_offset: u64) -> CacheKey {
        let segment_file_offset = (absolute_offset / self.segment_size) * self.segment_size;
        CacheKey::new(segment_file_offset, absolute_offset - segment_file_offset)
    }

    /// Correction write: route to the latest data segment and overwrite the
    /// data bytes of the target record in its last pending raw block.
    ///
    /// The record must be the last record in the last block of the latest segment.
    /// Returns `Err` if the block_offset does not lie within the open last segment
    /// or if the segment-level overwrite checks fail.
    pub fn overwrite_in_last_block(
        &mut self,
        block_offset: u64, // logical data-stream offset across segments
        in_block_offset: u16,
        _timestamp: i64,
        new_data: &[u8],
    ) -> Result<()> {
        if self.next_offset == 0 {
            return Err(TmslError::InvalidData(
                "no segment available for correction write".into(),
            ));
        }

        let target_segment_offset = (block_offset / self.segment_size) * self.segment_size;
        let latest_segment_offset = self.next_offset.saturating_sub(self.segment_size);
        if target_segment_offset != latest_segment_offset {
            return Err(TmslError::InvalidData(format!(
                "correction write: block_offset {} is not in the latest segment {}",
                block_offset, latest_segment_offset
            )));
        }

        let seg = self.lazy_open(target_segment_offset)?;

        // The block_offset is relative to each segment's data area start.
        // Each segment starts at seg.file_offset in the logical data stream.
        let seg_start = seg.file_offset;
        let seg_end_data = seg_start + seg.data_wrote_position;
        if block_offset < seg_start || block_offset >= seg_end_data {
            return Err(TmslError::InvalidData(format!(
                "correction write: block_offset {} is not in the latest segment [{}, {})",
                block_offset, seg_start, seg_end_data
            )));
        }

        let block_rel_offset = block_offset - seg_start;
        seg.overwrite_in_last_block(block_rel_offset, in_block_offset, new_data)
    }

    pub fn append_to_last_record(
        &mut self,
        block_offset: u64,
        in_block_offset: u16,
        append_data: &[u8],
    ) -> Result<u32> {
        let target_segment_offset = (block_offset / self.segment_size) * self.segment_size;
        let latest_segment_offset = self.next_offset.saturating_sub(self.segment_size);
        if target_segment_offset != latest_segment_offset {
            return Err(TmslError::InvalidData(
                "append: target block is not in the latest segment".into(),
            ));
        }
        let seg = self.lazy_open(target_segment_offset)?;
        if block_offset < seg.file_offset
            || block_offset >= seg.file_offset + seg.data_wrote_position
        {
            return Err(TmslError::InvalidData(
                "append: block_offset is outside latest segment data".into(),
            ));
        }
        seg.append_to_last_record(block_offset - seg.file_offset, in_block_offset, append_data)
    }

    pub fn append_single_record(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u64, u16)> {
        let compress_level = self.compress_level;
        let compress_type = self.compress_type;
        let latest_offset = self
            .segments
            .last_key_value()
            .map(|(offset, _)| *offset)
            .ok_or_else(|| TmslError::InvalidData("no data segment available".into()))?;
        let seg = self.lazy_open(latest_offset)?;
        if seg.pending_block_offset.is_some() {
            seg.seal_pending_block(compress_level)?;
            seg.clear_pending();
        }
        match seg.create_single_record_block(timestamp, data, compress_level) {
            Ok((block_rel, in_block)) => Ok((seg.file_offset, block_rel, in_block)),
            Err(TmslError::SegmentFull) => {
                seg.sync()?;
                let new_offset = self.next_offset;
                let file_name = format!("{:020}", new_offset);
                let path = self.base_dir.join(&file_name);
                let new_seg = DataSegment::create_with_compression(
                    &path,
                    new_offset,
                    self.initial_segment_size,
                    self.segment_size,
                    compress_level,
                    compress_type,
                )?;
                self.segments
                    .insert(new_offset, DataSegmentEntry::Open(new_seg));
                self.next_offset += self.segment_size;
                let seg = match self.segments.get_mut(&new_offset) {
                    Some(DataSegmentEntry::Open(seg)) => seg,
                    _ => unreachable!(),
                };
                let (block_rel, in_block) =
                    seg.create_single_record_block(timestamp, data, compress_level)?;
                Ok((seg.file_offset, block_rel, in_block))
            }
            Err(e) => Err(e),
        }
    }

    /// Increment invalid_record_count on the data segment that contains the given offset.
    ///
    /// Routes by `absolute_offset` (same coordinate as index entries' block_offset,
    /// relative to data area starts across the data stream). Opens the segment lazily
    /// if it is currently closed, then closes it again after the increment.
    pub fn increment_invalid_record_count(&mut self, absolute_offset: u64) -> Result<()> {
        let seg_start = self.segment_offset_for(absolute_offset);
        if let Some(DataSegmentEntry::Open(seg)) = self.segments.get_mut(&seg_start) {
            seg.increment_invalid_record_count()?;
            return Ok(());
        }
        // Closed segments — open briefly, increment, then idle_close back.
        if let Some(DataSegmentEntry::Closed(meta)) = self.segments.get(&seg_start) {
            let mut seg = DS::open(&meta.path, meta.file_offset, self.segment_size)?;
            seg.increment_invalid_record_count()?;
            seg.idle_close(self.compress_level)?;
            return Ok(());
        }
        Err(TmslError::NotFound(format!(
            "no segment contains offset {}",
            absolute_offset
        )))
    }

    // ─── Read operations ─────────────────────────────────────────────────

    /// Find the segment containing the given block_absolute_offset and read the record.
    pub fn read_at_index(
        &mut self,
        entry: &crate::segment::data::ReadIndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        let seg_offset = entry.block_offset;
        let seg = self.find_or_open_segment(seg_offset)?;
        let seg_file_offset = seg.file_offset;
        let rel_entry = crate::segment::data::ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset - seg_file_offset,
            in_block_offset: entry.in_block_offset,
        };
        seg.read_at_index(&rel_entry, cache)
    }

    /// Find the segment and read the record with HotBlockCache support.
    pub fn read_at_index_with_hot_cache(
        &mut self,
        entry: &crate::segment::data::ReadIndexEntry,
        cache: Option<&BlockCache>,
        hot_block: &mut HotBlockCache,
    ) -> Result<(i64, Vec<u8>)> {
        let seg = self.find_or_open_segment(entry.block_offset)?;
        let seg_file_offset = seg.file_offset;
        let rel_entry = crate::segment::data::ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset - seg_file_offset,
            in_block_offset: entry.in_block_offset,
        };
        seg.read_at_index_with_hot_cache(&rel_entry, cache, hot_block)
    }

    /// Find the segment and read only the record data_len (lightweight).
    pub fn read_record_data_len(
        &mut self,
        entry: &crate::segment::data::ReadIndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<u32> {
        let seg = self.find_or_open_segment(entry.block_offset)?;
        let seg_file_offset = seg.file_offset;
        let rel_entry = crate::segment::data::ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset - seg_file_offset,
            in_block_offset: entry.in_block_offset,
        };
        seg.read_record_data_len(&rel_entry, cache)
    }

    /// Find the segment and read record data_len with HotBlockCache support.
    pub fn read_record_data_len_with_hot_cache(
        &mut self,
        entry: &crate::segment::data::ReadIndexEntry,
        cache: Option<&BlockCache>,
        hot_block: &mut HotBlockCache,
    ) -> Result<u32> {
        let seg = self.find_or_open_segment(entry.block_offset)?;
        let seg_file_offset = seg.file_offset;
        let rel_entry = crate::segment::data::ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset - seg_file_offset,
            in_block_offset: entry.in_block_offset,
        };
        seg.read_record_data_len_with_hot_cache(&rel_entry, cache, hot_block)
    }

    fn find_or_open_segment(&mut self, absolute_offset: u64) -> Result<&mut DS> {
        let seg_start = self.segment_offset_for(absolute_offset);
        if matches!(
            self.segments.get(&seg_start),
            Some(DataSegmentEntry::Open(_))
        ) {
            return match self.segments.get_mut(&seg_start) {
                Some(DataSegmentEntry::Open(seg)) => Ok(seg),
                _ => unreachable!(),
            };
        }
        self.lazy_open(seg_start).map_err(|_| {
            crate::error::TmslError::NotFound(format!(
                "no segment contains offset {}",
                absolute_offset
            ))
        })
    }

    /// Flush all segments.
    pub fn flush_all(&mut self) -> Result<()> {
        self.sync_all()
    }

    /// Delete data segments whose `max_timestamp` is strictly less than `threshold`.
    /// Must be called only when all data segments are closed (via idle_close_all).
    /// Returns the number of files removed.
    pub fn reclaim_expired_segments(&mut self, threshold: i64) -> Result<usize> {
        let before = self.segments.len();
        self.segments.retain(|_, entry| {
            if let DataSegmentEntry::Closed(meta) = entry {
                if meta.max_timestamp < threshold && meta.max_timestamp != TIMESTAMP_MAX_SENTINEL {
                    let _ = std::fs::remove_file(&meta.path);
                    log::info!("[retention] deleted data segment: {:?}", meta.path);
                    return false;
                }
            }
            true
        });
        Ok(before - self.segments.len())
    }
}

// ─── Helper ──────────────────────────────────────────────────────────────────

use crate::header::FIXED_PREFIX_SIZE;

/// Read min_timestamp and max_timestamp from a data segment file header.
/// Opens the file, maps it briefly, reads the header, and unmaps.
/// Returns sentinel values on any error.
fn read_segment_timestamps(path: &Path) -> (i64, i64) {
    read_segment_timestamps_inner(path).unwrap_or((TIMESTAMP_MIN_SENTINEL, TIMESTAMP_MAX_SENTINEL))
}

fn read_segment_timestamps_inner(path: &Path) -> Result<(i64, i64)> {
    use std::fs::OpenOptions;
    let file = OpenOptions::new().read(true).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < FIXED_PREFIX_SIZE as u64 {
        return Ok((TIMESTAMP_MIN_SENTINEL, TIMESTAMP_MAX_SENTINEL));
    }
    // Use read-only mmap to avoid write-lock on Windows
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
    let meta = DataFileMetadata::read_from(&mmap)?;
    drop(mmap);
    drop(file);
    Ok((meta.min_timestamp, meta.max_timestamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("timslite_segment_set_{}", name));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn create_closed_segment(set: &mut DataSegmentSet, file_offset: u64) -> std::path::PathBuf {
        let path = set.base_dir.join(format!("{:020}", file_offset));
        let mut seg = DataSegment::create_with_compression(
            &path,
            file_offset,
            set.initial_segment_size,
            set.segment_size,
            set.compress_level,
            set.compress_type,
        )
        .unwrap();
        seg.idle_close(set.compress_level).unwrap();
        set.segments.insert(
            file_offset,
            DataSegmentEntry::Closed(DataSegmentMeta {
                path: path.clone(),
                file_offset,
                file_size: set.initial_segment_size,
                min_timestamp: TIMESTAMP_MIN_SENTINEL,
                max_timestamp: TIMESTAMP_MAX_SENTINEL,
            }),
        );
        path
    }

    #[test]
    fn test_lazy_open_keeps_data_segment_registries_sorted() {
        let dir = temp_dir("ordered_lazy_open");
        let mut set = DataSegmentSet::new_with_compression(
            &dir,
            256,
            256,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
        .unwrap();

        create_closed_segment(&mut set, 512);
        create_closed_segment(&mut set, 0);
        create_closed_segment(&mut set, 256);
        assert_eq!(
            set.closed_segments()
                .map(|meta| meta.file_offset)
                .collect::<Vec<_>>(),
            vec![0, 256, 512]
        );

        set.lazy_open(512).unwrap();
        set.lazy_open(0).unwrap();

        assert_eq!(
            set.segments
                .iter()
                .filter_map(|(_, entry)| match entry {
                    DataSegmentEntry::Open(seg) => Some(seg.file_offset),
                    DataSegmentEntry::Closed(_) => None,
                })
                .collect::<Vec<_>>(),
            vec![0, 512]
        );
        assert_eq!(
            set.closed_segments()
                .map(|meta| meta.file_offset)
                .collect::<Vec<_>>(),
            vec![256]
        );
    }

    #[test]
    fn test_increment_invalid_record_count_uses_computed_closed_offset() {
        let dir = temp_dir("invalid_count_closed_lookup");
        let mut set = DataSegmentSet::new_with_compression(
            &dir,
            256,
            256,
            6,
            crate::compress::COMPRESS_TYPE_ZSTD,
        )
        .unwrap();
        let path = create_closed_segment(&mut set, 256);

        set.increment_invalid_record_count(300).unwrap();

        let reopened = DataSegment::open(&path, 256, 256).unwrap();
        assert_eq!(reopened.invalid_record_count, 1);
        assert_eq!(set.open_len(), 0);
        assert_eq!(set.closed_len(), 1);
    }
}
