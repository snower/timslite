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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BlockCache;
    use crate::index::segment::{IndexEntry, BLOCK_OFFSET_FILLER};
    use crate::segment::DataSegmentSet;
    use std::sync::Arc;

    #[test]
    fn test_length_iter_empty_entries() {
        let dir = std::env::temp_dir().join("timslite_li_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let iter = QueryLengthIterator::new(vec![], &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_length_iter_single_entry() {
        let dir = std::env::temp_dir().join("timslite_li_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off, _blk_rel, in_block_0) = segments.append(100, b"hello").unwrap();

        let entries = vec![IndexEntry::new(100, 0, in_block_0)];
        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 100);
        assert_eq!(results[0].1, 5); // "hello" is 5 bytes
    }

    #[test]
    fn test_length_iter_skips_fillers() {
        let dir = std::env::temp_dir().join("timslite_li_filler");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off, _blk_rel, in_block_0) = segments.append(100, b"hello").unwrap();
        let _ = segments.append(200, b"world2").unwrap();

        let entries = vec![
            IndexEntry::new(50, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(100, 0, in_block_0),
            IndexEntry::new(150, BLOCK_OFFSET_FILLER, 0xFFFF),
        ];

        let cache = Arc::new(BlockCache::new(1024));
        let iter = QueryLengthIterator::new(entries, &mut segments, Some(cache));
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 100);
        assert_eq!(results[0].1, 5); // "hello" is 5 bytes
    }

    #[test]
    fn test_length_iter_multiple_entries() {
        let dir = std::env::temp_dir().join("timslite_li_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off_1, _blk_rel_1, in_block_1) = segments.append(100, b"hello").unwrap();
        let (_seg_off_2, _blk_rel_2, in_block_2) =
            segments.append(200, b"world2024").unwrap();
        let (_seg_off_3, _blk_rel_3, in_block_3) = segments.append(300, b"rust").unwrap();

        let entries = vec![
            IndexEntry::new(100, 0, in_block_1),
            IndexEntry::new(200, 0, in_block_2),
            IndexEntry::new(300, 0, in_block_3),
        ];

        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (100, 5)); // "hello"
        assert_eq!(results[1], (200, 9)); // "world2024"
        assert_eq!(results[2], (300, 4)); // "rust"
    }

    #[test]
    fn test_length_iter_with_cache() {
        let dir = std::env::temp_dir().join("timslite_li_cache");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off, _blk_rel, in_block_0) = segments.append(100, b"cached").unwrap();

        let entries = vec![IndexEntry::new(100, 0, in_block_0)];
        let cache = Arc::new(BlockCache::new(1024));
        let iter = QueryLengthIterator::new(entries, &mut segments, Some(cache));
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 100);
        assert_eq!(results[0].1, 6); // "cached" is 6 bytes
    }

    #[test]
    fn test_length_iter_iterator_trait() {
        let dir = std::env::temp_dir().join("timslite_li_iter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off_1, _blk_rel_1, in_block_1) = segments.append(100, b"hello").unwrap();
        let (_seg_off_2, _blk_rel_2, in_block_2) = segments.append(200, b"world").unwrap();

        let entries = vec![
            IndexEntry::new(100, 0, in_block_1),
            IndexEntry::new(150, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(200, 0, in_block_2),
        ];

        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (100, 5));
        assert_eq!(results[1], (200, 5));
    }

    #[test]
    fn test_length_iter_new_with_sources() {
        let dir = std::env::temp_dir().join("timslite_li_sources");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off_1, _blk_rel_1, in_block_1) = segments.append(100, b"hello").unwrap();
        let (_seg_off_2, _blk_rel_2, in_block_2) = segments.append(200, b"world").unwrap();

        let source1 = QuerySource::InMemory {
            entries: vec![IndexEntry::new(100, 0, in_block_1)],
            position: 0,
        };
        let source2 = QuerySource::InMemory {
            entries: vec![IndexEntry::new(200, 0, in_block_2)],
            position: 0,
        };

        let iter = QueryLengthIterator::new_with_sources(
            vec![source1, source2],
            &mut segments,
            None,
        );
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (100, 5));
        assert_eq!(results[1], (200, 5));
    }

    #[test]
    fn test_length_iter_error_propagation() {
        let dir = std::env::temp_dir().join("timslite_li_err");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        // Use an invalid block_offset that does not exist in any segment
        let entries = vec![IndexEntry::new(100, 99999999, 0)];

        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let result = iter.collect_all();
        assert!(result.is_err());
    }

    #[test]
    fn test_length_iter_all_fillers() {
        let dir = std::env::temp_dir().join("timslite_li_allf");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let entries = vec![
            IndexEntry::new(50, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(150, BLOCK_OFFSET_FILLER, 0xFFFF),
        ];

        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_length_iter_no_cache() {
        let dir = std::env::temp_dir().join("timslite_li_noc");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off, _blk_rel, in_block_0) = segments.append(100, b"nocache").unwrap();

        let entries = vec![IndexEntry::new(100, 0, in_block_0)];
        // Pass None for cache to test cache-less path
        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (100, 7)); // "nocache" is 7 bytes
    }

    #[test]
    fn test_length_iter_mixed_real_and_fillers() {
        let dir = std::env::temp_dir().join("timslite_li_mix");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut segments = DataSegmentSet::new(&dir, 64 * 1024 * 1024, 256 * 1024, 6).unwrap();

        let (_seg_off_1, _blk_rel_1, in_block_1) = segments.append(100, b"a").unwrap();
        let (_seg_off_2, _blk_rel_2, in_block_2) = segments.append(300, b"c").unwrap();

        let entries = vec![
            IndexEntry::new(50, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(100, 0, in_block_1),
            IndexEntry::new(200, BLOCK_OFFSET_FILLER, 0xFFFF),
            IndexEntry::new(300, 0, in_block_2),
            IndexEntry::new(400, BLOCK_OFFSET_FILLER, 0xFFFF),
        ];

        let iter = QueryLengthIterator::new(entries, &mut segments, None);
        let results = iter.collect_all().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (100, 1)); // "a"
        assert_eq!(results[1], (300, 1)); // "c"
    }
}
