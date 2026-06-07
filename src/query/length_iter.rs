//! QueryLengthIterator: lazy iteration for data lengths with HotBlockCache

use crate::cache::BlockCache;
use crate::error::Result;
use crate::index::segment::{IndexEntry, BLOCK_OFFSET_FILLER};
use crate::segment::data::ReadIndexEntry;
use crate::segment::DataSegmentSet;
use std::sync::Arc;

use super::hot_block::HotBlockCache;
use super::iter::QuerySource;

/// Lazy iterator for data lengths in [start_ts, end_ts].
/// Each next() returns (timestamp, data_len) for valid records.
/// Skips filler entries. Uses HotBlockCache for efficiency.
pub struct QueryLengthIterator<'a> {
    sources: Vec<QuerySource>,
    current_source: usize,
    segments: &'a mut DataSegmentSet,
    cache: Option<Arc<BlockCache>>,
    hot_block: HotBlockCache,
}

impl<'a> QueryLengthIterator<'a> {
    pub fn new(
        entries: Vec<IndexEntry>,
        segments: &'a mut DataSegmentSet,
        cache: Option<Arc<BlockCache>>,
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
        cache: Option<Arc<BlockCache>>,
    ) -> Self {
        Self {
            sources,
            current_source: 0,
            segments,
            cache,
            hot_block: HotBlockCache::new(),
        }
    }

    /// Get the next (timestamp, data_len) pair.
    /// Returns None when iteration is complete.
    pub fn next_entry(&mut self) -> Result<Option<(i64, u32)>> {
        while self.current_source < self.sources.len() {
            match self.next_entry_from_current_source() {
                Ok(Some(entry)) => {
                    if entry.block_offset == BLOCK_OFFSET_FILLER {
                        continue;
                    }
                    return self.read_data_len(&entry).map(Some);
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

    fn read_data_len(&mut self, entry: &IndexEntry) -> Result<(i64, u32)> {
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        let data_len = self.segments.read_record_data_len_with_hot_cache(
            &re,
            self.cache.as_deref(),
            &mut self.hot_block,
        )?;
        Ok((entry.timestamp, data_len))
    }

    /// Collect all remaining (timestamp, data_len) pairs as Vec.
    pub fn collect_all(mut self) -> Result<Vec<(i64, u32)>> {
        let mut result = Vec::new();
        while let Some(pair) = self.next_entry()? {
            result.push(pair);
        }
        Ok(result)
    }
}

impl<'a> Iterator for QueryLengthIterator<'a> {
    type Item = Result<(i64, u32)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry().transpose()
    }
}
