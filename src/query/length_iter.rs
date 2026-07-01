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
