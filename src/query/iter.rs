//! QueryIterator: lazy query iteration with HotBlockCache.

use crate::dataset::DataSetInner;
use crate::error::Result;
use std::sync::{Arc, Mutex};

use super::hot_block::HotBlockCache;
use super::index_iter::IndexQueryIterator;

/// Public lazy query iterator backed by a Store-managed dataset.
pub struct QueryIterator {
    index_iter: IndexQueryIterator,
    hot_block: HotBlockCache,
}

impl QueryIterator {
    pub(crate) fn new(dataset: Arc<Mutex<DataSetInner>>, start_ts: i64, end_ts: i64) -> Self {
        Self {
            index_iter: IndexQueryIterator::new(dataset, start_ts, end_ts),
            hot_block: HotBlockCache::new(),
        }
    }

    pub fn reverse(mut self) -> Self {
        self.index_iter = self.index_iter.reverse();
        self.hot_block = HotBlockCache::new();
        self
    }

    pub fn skip(mut self, count: usize) -> Self {
        self.index_iter = self.index_iter.skip(count);
        self.hot_block = HotBlockCache::new();
        self
    }

    pub fn next_entry(&mut self) -> Result<Option<(i64, Vec<u8>)>> {
        // Seek and record read run under the single lock held by `next_with`.
        let Self {
            index_iter,
            hot_block,
            ..
        } = self;
        index_iter.next_with(|inner, entry| {
            if let Some(record) = inner.try_read_entry_from_hot_cache(entry, hot_block)? {
                return Ok(Some(record));
            }
            inner.read_entry_with_hot_cache(entry, hot_block)
        })
    }

    pub fn collect_all(mut self) -> Result<Vec<(i64, Vec<u8>)>> {
        let mut records = Vec::new();
        while let Some(record) = self.next_entry()? {
            records.push(record);
        }
        Ok(records)
    }

    pub fn collect_take(mut self, count: usize) -> Result<Vec<(i64, Vec<u8>)>> {
        let mut records = Vec::new();
        for _ in 0..count {
            let Some(record) = self.next_entry()? else {
                break;
            };
            records.push(record);
        }
        Ok(records)
    }
}

impl Iterator for QueryIterator {
    type Item = Result<(i64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry().transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::segment::test_hooks;
    use crate::{Store, StoreConfig};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let root = std::env::temp_dir().join("timslite_query_iter_unit");
        fs::create_dir_all(&root).unwrap();
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        root.join(format!(
            "test_{:?}_{id}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn next_entry_uses_hot_cache_before_data_segment_lookup() {
        let dir = temp_dir();
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store
            .create_dataset(
                "hot_iter",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .unwrap();
        let ds = store.open_dataset("hot_iter", "data").unwrap();
        // Records must overflow the 256 KiB block cap so entries 1..2 sit in a
        // sealed+compressed block (only compressed blocks use the hot cache).
        let data = vec![0xABu8; 20_000];
        for ts in 1..=20 {
            ds.write(ts, &data).unwrap();
        }

        let mut iter = ds.query_iter(1, 2).unwrap();
        let first = iter.next_entry().unwrap().unwrap();
        assert_eq!(first.0, 1);
        assert_eq!(first.1, data);

        test_hooks::reset_find_or_open_segment_calls();

        let second = iter.next_entry().unwrap().unwrap();
        assert_eq!(second.0, 2);
        assert_eq!(second.1, vec![0xABu8; 20_000]);
        assert_eq!(test_hooks::find_or_open_segment_calls(), 0);
    }

    // ── Lane D regression (unique tempdir; safe to run standalone) ──

    #[test]
    fn iterator_skips_fillers_deletes_and_honors_reverse_and_skip() {
        let dir = tempfile::Builder::new()
            .prefix("timslite_lane_d_iter_")
            .tempdir()
            .unwrap();
        let config = StoreConfig::builder()
            .enable_journal(false)
            .enable_background_thread(false)
            .build();
        let mut store = Store::open(dir.path(), config).unwrap();
        let ds = store
            .create_dataset("iter_stream", "data", 64 * 1024 * 1024, 256, 6, 1, 0)
            .unwrap();
        ds.write(10, b"a").unwrap();
        ds.write(20, b"bb").unwrap();
        ds.write(30, b"ccc").unwrap();
        ds.delete(20).unwrap();

        let forward = ds.query_iter(10, 30).unwrap().collect_all().unwrap();
        assert_eq!(
            forward.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![10, 30]
        );

        let reversed = ds
            .query_iter(10, 30)
            .unwrap()
            .reverse()
            .collect_all()
            .unwrap();
        assert_eq!(
            reversed.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![30, 10]
        );

        let skipped = ds
            .query_iter(10, 30)
            .unwrap()
            .skip(1)
            .collect_all()
            .unwrap();
        assert_eq!(
            skipped.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![30]
        );

        // Length iterator shares the merged seek+read lock path.
        let lengths = ds.query_length_iter(10, 30).unwrap().collect_all().unwrap();
        assert_eq!(lengths, vec![(10, 1u32), (30, 3u32)]);

        // Retention clamps the iterator range at creation time.
        let retained = store
            .create_dataset("iter_stream", "retained", 64 * 1024 * 1024, 256, 6, 1, 50)
            .unwrap();
        retained.write(100, b"a").unwrap();
        retained.write(200, b"b").unwrap();
        let visible = retained.query_iter(0, 200).unwrap().collect_all().unwrap();
        assert_eq!(
            visible.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![200]
        );
        assert!(retained
            .query_iter(0, 199)
            .unwrap()
            .collect_all()
            .unwrap()
            .is_empty());

        // Negative and i64-boundary timestamps survive the cursor advance.
        let bounds = store
            .create_dataset("iter_stream", "bounds", 64 * 1024 * 1024, 256, 6, 0, 0)
            .unwrap();
        bounds.write(-5, b"neg").unwrap();
        bounds.write(0, b"zero").unwrap();
        bounds.write(i64::MAX - 1, b"max").unwrap();
        let negatives = bounds.query_iter(-5, 0).unwrap().collect_all().unwrap();
        assert_eq!(
            negatives.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![-5, 0]
        );
        let over_max = bounds
            .query_iter(i64::MAX - 2, i64::MAX)
            .unwrap()
            .collect_all()
            .unwrap();
        assert_eq!(
            over_max.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![i64::MAX - 1]
        );
    }
}
