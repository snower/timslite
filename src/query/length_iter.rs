//! QueryLengthIterator: lazy iteration for data lengths with HotBlockCache.

use crate::dataset::DataSetInner;
use crate::error::Result;
use std::sync::{Arc, Mutex};

use super::hot_block::HotBlockCache;
use super::index_iter::IndexQueryIterator;

/// Public lazy iterator for data lengths returned by `DataSet::query_length_iter`.
pub struct QueryLengthIterator {
    dataset: Arc<Mutex<DataSetInner>>,
    index_iter: IndexQueryIterator,
    hot_block: HotBlockCache,
}

impl QueryLengthIterator {
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

    pub fn next_entry(&mut self) -> Result<Option<(i64, u32)>> {
        loop {
            let Some(entry) = self.index_iter.next_entry()? else {
                return Ok(None);
            };

            let mut inner = self.dataset.lock().map_err(|_| {
                crate::error::TmslError::InvalidData("dataset mutex poisoned".into())
            })?;
            if let Some(data_len) =
                inner.try_read_entry_data_len_from_hot_cache(&entry, &self.hot_block)?
            {
                return Ok(Some((entry.timestamp, data_len)));
            }
            if let Some(data_len) =
                inner.read_entry_data_len_with_hot_cache(&entry, &mut self.hot_block)?
            {
                return Ok(Some((entry.timestamp, data_len)));
            }
        }
    }

    pub fn collect_all(mut self) -> Result<Vec<(i64, u32)>> {
        let mut result = Vec::new();
        while let Some(pair) = self.next_entry()? {
            result.push(pair);
        }
        Ok(result)
    }

    pub fn collect_take(mut self, count: usize) -> Result<Vec<(i64, u32)>> {
        let mut result = Vec::new();
        for _ in 0..count {
            let Some(pair) = self.next_entry()? else {
                break;
            };
            result.push(pair);
        }
        Ok(result)
    }
}

impl Iterator for QueryLengthIterator {
    type Item = Result<(i64, u32)>;

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
        let root = std::env::temp_dir().join("timslite_query_length_iter_unit");
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
                "hot_length_iter",
                "data",
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                0,
            )
            .unwrap();
        let ds = store.open_dataset("hot_length_iter", "data").unwrap();
        let data = vec![0xCDu8; 4096];
        for ts in 1..=20 {
            ds.write(ts, &data).unwrap();
        }

        let mut iter = ds.query_length_iter(1, 2).unwrap();
        assert_eq!(iter.next_entry().unwrap(), Some((1, 4096)));

        test_hooks::reset_find_or_open_segment_calls();

        assert_eq!(iter.next_entry().unwrap(), Some((2, 4096)));
        assert_eq!(test_hooks::find_or_open_segment_calls(), 0);
    }
}
