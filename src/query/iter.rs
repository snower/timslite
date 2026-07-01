//! QueryIterator: lazy query iteration with HotBlockCache.

use crate::dataset::DataSetInner;
use crate::error::Result;
use std::sync::{Arc, Mutex};

use super::hot_block::HotBlockCache;
use super::index_iter::IndexQueryIterator;

/// Public lazy query iterator backed by a Store-managed dataset.
pub struct QueryIterator {
    dataset: Arc<Mutex<DataSetInner>>,
    index_iter: IndexQueryIterator,
    hot_block: HotBlockCache,
}

impl QueryIterator {
    pub(crate) fn new(dataset: Arc<Mutex<DataSetInner>>, start_ts: i64, end_ts: i64) -> Self {
        Self {
            dataset: Arc::clone(&dataset),
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
        loop {
            let Some(entry) = self.index_iter.next_entry()? else {
                return Ok(None);
            };

            let mut inner = self.dataset.lock().map_err(|_| {
                crate::error::TmslError::InvalidData("dataset mutex poisoned".into())
            })?;
            if let Some(record) = inner.try_read_entry_from_hot_cache(&entry, &self.hot_block)? {
                return Ok(Some(record));
            }
            if let Some(record) = inner.read_entry_with_hot_cache(&entry, &mut self.hot_block)? {
                return Ok(Some(record));
            }
        }
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
        let data = vec![0xABu8; 4096];
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
        assert_eq!(second.1, vec![0xABu8; 4096]);
        assert_eq!(test_hooks::find_or_open_segment_calls(), 0);
    }
}
