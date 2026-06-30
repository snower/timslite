//! QueryIterator: lazy query iteration with HotBlockCache.

use crate::dataset::DataSetInner;
use crate::error::Result;
use std::sync::{Arc, Mutex};

use super::hot_block::HotBlockCache;

/// Public lazy query iterator backed by a Store-managed dataset.
pub struct QueryIterator {
    dataset: Arc<Mutex<DataSetInner>>,
    next_ts: Option<i64>,
    end_ts: i64,
    hot_block: HotBlockCache,
}

impl QueryIterator {
    pub(crate) fn new(dataset: Arc<Mutex<DataSetInner>>, start_ts: i64, end_ts: i64) -> Self {
        Self {
            dataset,
            next_ts: (start_ts <= end_ts).then_some(start_ts),
            end_ts,
            hot_block: HotBlockCache::new(),
        }
    }

    pub fn next_entry(&mut self) -> Result<Option<(i64, Vec<u8>)>> {
        loop {
            let Some(next_ts) = self.next_ts else {
                return Ok(None);
            };

            let entry = {
                let mut inner = self.dataset.lock().map_err(|_| {
                    crate::error::TmslError::InvalidData("dataset mutex poisoned".into())
                })?;
                inner.next_query_index_entry(next_ts, self.end_ts)?
            };

            let Some(entry) = entry else {
                self.next_ts = None;
                return Ok(None);
            };
            self.next_ts = entry.timestamp.checked_add(1);

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
