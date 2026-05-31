//! QueryIterator: lazy query iteration with HotBlockCache

use crate::cache::BlockCache;
use crate::error::Result;
use std::path::PathBuf;

use crate::index::segment::{IndexEntry, IndexSegment, BLOCK_OFFSET_FILLER};
use crate::segment::data::ReadIndexEntry;
use crate::segment::DataSegmentSet;

use super::hot_block::HotBlockCache;

/// Query data source
pub enum QuerySource {
    InMemory {
        entries: Vec<IndexEntry>,
        position: usize,
    },
    SegmentFile {
        path: PathBuf,
        start_timestamp: i64,
        segment_size: u64,
        index_continuous: bool,
        start_idx: usize,
        end_idx: usize,
        position: usize,
        first_timestamp: i64,
        segment: Option<IndexSegment>,
    },
}

impl QuerySource {
    pub fn segment_file(
        path: PathBuf,
        start_timestamp: i64,
        segment_size: u64,
        index_continuous: bool,
        start_idx: usize,
        end_idx: usize,
        first_timestamp: i64,
    ) -> Self {
        Self::SegmentFile {
            path,
            start_timestamp,
            segment_size,
            index_continuous,
            start_idx,
            end_idx,
            position: start_idx,
            first_timestamp,
            segment: None,
        }
    }

    pub fn first_timestamp(&self) -> Option<i64> {
        match self {
            QuerySource::InMemory { entries, .. } => entries.first().map(|e| e.timestamp),
            QuerySource::SegmentFile {
                first_timestamp, ..
            } => Some(*first_timestamp),
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<IndexEntry>> {
        match self {
            QuerySource::InMemory { entries, position } => {
                if *position < entries.len() {
                    let entry = entries[*position];
                    *position += 1;
                    Ok(Some(entry))
                } else {
                    Ok(None)
                }
            }
            QuerySource::SegmentFile {
                path,
                start_timestamp,
                segment_size,
                position,
                end_idx,
                segment,
                ..
            } => {
                if *position >= *end_idx {
                    if let Some(mut seg) = segment.take() {
                        let _ = seg.idle_close();
                    }
                    return Ok(None);
                }
                if segment.is_none() {
                    *segment = Some(IndexSegment::open(path, *start_timestamp, *segment_size)?);
                }
                let seg = segment
                    .as_mut()
                    .expect("segment is opened before reading query source");
                let entry = seg.read_entry_at_index(*position)?;
                *position += 1;
                Ok(Some(entry))
            }
        }
    }

    pub fn entries_remaining(&self) -> usize {
        match self {
            QuerySource::InMemory { entries, position } => entries.len().saturating_sub(*position),
            QuerySource::SegmentFile {
                position, end_idx, ..
            } => end_idx.saturating_sub(*position),
        }
    }
}

/// Iterator position state
pub struct SourceIndex {
    pub source_idx: usize,
    pub entries_remaining: usize,
}

/// Lazy query iterator
pub struct QueryIterator<'a, 'b> {
    sources: Vec<QuerySource>,
    current_source: usize,
    segments: &'a mut DataSegmentSet,
    cache: Option<&'b BlockCache>,
    hot_block: HotBlockCache,
}

impl<'a, 'b> QueryIterator<'a, 'b> {
    pub fn new(
        entries: Vec<IndexEntry>,
        segments: &'a mut DataSegmentSet,
        cache: Option<&'b BlockCache>,
    ) -> Self {
        Self::new_with_sources(
            vec![QuerySource::InMemory {
                entries,
                position: 0,
            }],
            segments,
            cache,
        )
    }

    pub fn new_with_sources(
        sources: Vec<QuerySource>,
        segments: &'a mut DataSegmentSet,
        cache: Option<&'b BlockCache>,
    ) -> Self {
        Self {
            sources,
            current_source: 0,
            segments,
            cache,
            hot_block: HotBlockCache::new(),
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<(i64, Vec<u8>)>> {
        while self.current_source < self.sources.len() {
            match self.next_entry_from_current_source() {
                Ok(Some(entry)) => {
                    if entry.block_offset == BLOCK_OFFSET_FILLER {
                        continue;
                    }
                    return self.read_record(&entry).map(Some);
                }
                Ok(None) => {
                    self.current_source += 1;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    fn next_entry_from_current_source(&mut self) -> Result<Option<IndexEntry>> {
        match self.sources.get_mut(self.current_source) {
            Some(source) => source.next_entry(),
            None => Ok(None),
        }
    }

    fn read_record(&mut self, entry: &IndexEntry) -> Result<(i64, Vec<u8>)> {
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        self.segments
            .read_at_index_with_hot_cache(&re, self.cache, &mut self.hot_block)
    }

    /// Collect all remaining records as Vec (backward compatible).
    pub fn collect_all(mut self) -> Result<Vec<(i64, Vec<u8>)>> {
        let estimate = self.entries_remaining();
        let mut records = Vec::with_capacity(estimate);
        while let Some(record) = self.next_entry()? {
            records.push(record);
        }
        Ok(records)
    }

    pub fn entries_remaining(&self) -> usize {
        self.sources
            .get(self.current_source)
            .map(QuerySource::entries_remaining)
            .unwrap_or(0)
    }

    pub fn current_index(&self) -> SourceIndex {
        SourceIndex {
            source_idx: self.current_source,
            entries_remaining: self.entries_remaining(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_entries() {
        let dir = std::env::temp_dir().join("timslite_query_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments =
            DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 65536, 6).unwrap();

        let iter = QueryIterator::new(vec![], &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_skip_fillers_and_read_real() {
        let dir = std::env::temp_dir().join("ts_qi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments =
            DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 65536, 6).unwrap();

        let (_seg_off, _blk_rel, in_block_0) = segments.append(100, b"hello").unwrap();
        let _ = segments.append(200, b"world2").unwrap();

        let entries = vec![
            IndexEntry::new(50, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(100, 0, in_block_0),
            IndexEntry::new(150, BLOCK_OFFSET_FILLER, 0xFFFF),
        ];

        let cache = BlockCache::new(1024);
        let iter = QueryIterator::new(entries, &mut segments, Some(&cache));
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 100);
        assert_eq!(results[0].1, b"hello");
    }
}
