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

    pub fn next_entry(&mut self) -> Result<Option<(i64, Vec<u8>)>> {
        loop {
            let Some(entry) = self.index_iter.next_entry()? else {
                return Ok(None);
            };

            let mut inner = self.dataset.lock().map_err(|_| {
                crate::error::TmslError::InvalidData("dataset mutex poisoned".into())
            })?;
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
}

impl Iterator for QueryIterator {
    type Item = Result<(i64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry().transpose()
    }
}
