//! DataSegmentSet: manages multiple DataSegment files for a single dataset.
//!
//! Handles lazy open, idle close, append, and cross-segment reads.

pub mod data;

use std::path::Path;
use std::time::Instant;

use crate::error::Result;

pub use self::data::{DataSegment, ReadIndexEntry};
use self::data::{DataSegment as DS, SegmentLifecycle as SL};

use crate::cache::BlockCache;
use crate::cache::HotBlockCache;

/// Metadata for a closed data segment.
pub(crate) struct DataSegmentMeta {
    pub path: std::path::PathBuf,
    pub file_offset: u64,
    pub file_size: u64,
}

// ─── DataSegmentSet ─────────────────────────────────────────────────────────

pub struct DataSegmentSet {
    pub base_dir: std::path::PathBuf,
    pub segment_size: u64,
    pub initial_segment_size: u64,
    pub block_max_size: u32,
    pub compress_level: u8,
    pub segments: Vec<DataSegment>,
    #[allow(private_interfaces)]
    pub closed_segments: Vec<DataSegmentMeta>,
    pub next_offset: u64,
    pub last_used_at: Instant,
}

impl DataSegmentSet {
    /// Create a new (empty) DataSegmentSet for a freshly created dataset.
    pub fn new(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        block_max_size: u32,
        compress_level: u8,
    ) -> Result<Self> {
        let data_dir = base_dir.join("data");
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self {
            base_dir: data_dir,
            segment_size,
            initial_segment_size,
            block_max_size,
            compress_level,
            segments: Vec::new(),
            closed_segments: Vec::new(),
            next_offset: 0,
            last_used_at: Instant::now(),
        })
    }

    /// Sync all open data segments.
    pub fn sync_all(&mut self) -> Result<()> {
        for seg in &mut self.segments {
            seg.sync()?;
        }
        Ok(())
    }

    /// Idle-close all open data segments.
    pub fn idle_close_all(&mut self) -> Result<()> {
        let mut closed: Vec<DataSegmentMeta> = Vec::new();
        for mut seg in self.segments.drain(..) {
            closed.push(DataSegmentMeta {
                path: seg.path.clone(),
                file_offset: seg.file_offset,
                file_size: seg.file_size,
            });
            seg.idle_close(6)?;
        }
        self.closed_segments.extend(closed);
        Ok(())
    }

    /// Lazy open a segment by its file_offset.
    pub fn lazy_open(&mut self, file_offset: u64) -> Result<&mut DS> {
        // Check open segments
        if let Some(idx) = self
            .segments
            .iter()
            .position(|s| s.file_offset == file_offset)
        {
            return Ok(&mut self.segments[idx]);
        }
        // Check closed segments
        let meta_pos = self
            .closed_segments
            .iter()
            .position(|m| m.file_offset == file_offset)
            .ok_or_else(|| {
                crate::error::TmslError::NotFound(format!("no segment at offset {}", file_offset))
            })?;
        let meta = self.closed_segments.remove(meta_pos);
        let seg = DS::open(&meta.path, meta.file_offset, self.segment_size)?;
        self.segments.push(seg);
        Ok(self.segments.last_mut().unwrap())
    }

    /// Load existing data segments from disk (all start closed).
    /// Scans the `data/` subdirectory for segment files.
    pub fn load_existing(
        base_dir: &Path,
        segment_size: u64,
        initial_segment_size: u64,
        block_max_size: u32,
        compress_level: u8,
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
                        metas.push(DataSegmentMeta {
                            path: p,
                            file_offset: offset,
                            file_size,
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
            block_max_size,
            compress_level,
            segments: Vec::new(),
            closed_segments: metas,
            next_offset,
            last_used_at: Instant::now(),
        })
    }

    // ─── Write operations ────────────────────────────────────────────────

    /// Append a record. Returns (segment_offset, block_relative_offset, in_block_offset).
    pub fn append(&mut self, timestamp: i64, data: &[u8]) -> Result<(u64, u64, u16)> {
        // Get current segment for writing
        let current_offset = if self.segments.is_empty() {
            self.next_offset
        } else {
            let last = self.segments.last().unwrap();
            if last.lifecycle == SL::Closed
                || last.wrote_position + crate::block::BLOCK_HEADER_SIZE + 10 > self.segment_size
            {
                self.next_offset
            } else {
                last.file_offset
            }
        };

        // Extract config values
        let block_max_size = self.block_max_size;
        let compress_level = self.compress_level;

        // Try to open existing segment, or create a new one
        let seg = match self.lazy_open(current_offset) {
            Ok(s) => s,
            Err(_) => {
                // Create new segment with initial_size
                let file_name = format!("{:020}", current_offset);
                let path = self.base_dir.join(&file_name);
                let new_seg = DataSegment::create(
                    &path,
                    current_offset,
                    self.initial_segment_size,
                    self.segment_size,
                )?;
                self.segments.push(new_seg);
                self.next_offset += self.segment_size;
                self.segments.last_mut().unwrap()
            }
        };
        if seg.lifecycle == SL::Closed {
            seg.ensure_open(compress_level)?;
        }

        // Try to append; if SegmentFull, expand and retry, or seal + create new
        let (block_rel_off, in_block_off) =
            match seg.append_record(timestamp, data, block_max_size, compress_level) {
                Ok(result) => result,
                Err(crate::error::TmslError::SegmentFull) => {
                    // Try to expand the current segment
                    if seg.expand().is_ok() {
                        // Expansion succeeded, retry append
                        seg.append_record(timestamp, data, block_max_size, compress_level)?
                    } else {
                        // Already at max_file_size → seal current, create new segment
                        // Mark this segment as needing seal
                        let seg_offset_to_seal = seg.file_offset;
                        self.next_offset += self.segment_size;

                        let file_name = format!("{:020}", self.next_offset);
                        let path = self.base_dir.join(&file_name);
                        let new_seg = DataSegment::create(
                            &path,
                            self.next_offset,
                            self.initial_segment_size,
                            self.segment_size,
                        )?;
                        self.segments.push(new_seg);

                        // Seal the old segment (lazy approach: set lifecycle to Closed)
                        // It will be properly sealed on idle-close
                        {
                            let idx = self
                                .segments
                                .iter()
                                .position(|s| s.file_offset == seg_offset_to_seal)
                                .unwrap();
                            self.segments[idx].lifecycle = SL::Closed;
                        }

                        let new_seg = self.segments.last_mut().unwrap();
                        new_seg.append_record(timestamp, data, block_max_size, compress_level)?
                    }
                }
                Err(e) => return Err(e),
            };

        let seg_wrote_pos = self.segments.last().unwrap().wrote_position;
        let seg_size = seg_wrote_pos + crate::header::HEADER_SIZE;
        if seg_size >= self.segment_size {
            self.next_offset += self.segment_size;
        }

        self.last_used_at = Instant::now();
        Ok((current_offset, block_rel_off, in_block_off))
    }

    /// Get the segment size configuration.
    pub fn segment_size(&self) -> u64 {
        self.segment_size
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
        seg.read_at_index(entry, cache)
    }

    /// Find the segment and read the record with HotBlockCache support.
    pub fn read_at_index_with_hot_cache(
        &mut self,
        entry: &crate::segment::data::ReadIndexEntry,
        cache: Option<&BlockCache>,
        hot_block: &mut HotBlockCache,
    ) -> Result<(i64, Vec<u8>)> {
        let seg = self.find_or_open_segment(entry.block_offset)?;
        seg.read_at_index_with_hot_cache(entry, cache, hot_block)
    }

    fn find_or_open_segment(&mut self, absolute_offset: u64) -> Result<&mut DS> {
        // Find which segment this offset belongs to
        for seg in &self.segments {
            let seg_start = seg.file_offset;
            let seg_end = seg_start + self.segment_size;
            if absolute_offset >= seg_start && absolute_offset < seg_end {
                let idx = self
                    .segments
                    .iter()
                    .position(|s| s.file_offset == seg_start)
                    .unwrap();
                return Ok(&mut self.segments[idx]);
            }
        }
        // Not in open segments - find in closed
        for meta in &self.closed_segments {
            let seg_start = meta.file_offset;
            let seg_end = seg_start + self.segment_size;
            if absolute_offset >= seg_start && absolute_offset < seg_end {
                let meta_pos = self
                    .closed_segments
                    .iter()
                    .position(|m| m.file_offset == seg_start)
                    .unwrap();
                let m = self.closed_segments.remove(meta_pos);
                let seg = DS::open(&m.path, m.file_offset, self.segment_size)?;
                self.segments.push(seg);
                return Ok(self.segments.last_mut().unwrap());
            }
        }
        Err(crate::error::TmslError::NotFound(format!(
            "no segment contains offset {}",
            absolute_offset
        )))
    }

    /// Flush all segments.
    pub fn flush_all(&mut self) -> Result<()> {
        self.sync_all()
    }
}
