use crate::dataset::DataSetInner;
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;
use crate::index::IndexEntryPosition;
use std::sync::{Arc, Mutex};

pub(crate) struct IndexQueryIterator {
    dataset: Arc<Mutex<DataSetInner>>,
    next_ts: Option<i64>,
    end_ts: i64,
    cursor: Option<IndexEntryPosition>,
}

impl IndexQueryIterator {
    pub(crate) fn new(dataset: Arc<Mutex<DataSetInner>>, start_ts: i64, end_ts: i64) -> Self {
        Self {
            dataset,
            next_ts: (start_ts <= end_ts).then_some(start_ts),
            end_ts,
            cursor: None,
        }
    }

    pub(crate) fn next_entry(&mut self) -> Result<Option<IndexEntry>> {
        let Some(next_ts) = self.next_ts else {
            return Ok(None);
        };

        let next = {
            let mut inner = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            inner.next_query_index_entry_from_position(next_ts, self.end_ts, self.cursor)?
        };

        let Some((cursor, entry)) = next else {
            self.next_ts = None;
            return Ok(None);
        };

        self.cursor = Some(cursor);
        self.next_ts = entry.timestamp.checked_add(1);
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::IndexQueryIterator;
    use crate::{Store, StoreConfig};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("timslite_index_query_iter_{nanos}"))
    }

    #[test]
    fn index_query_iterator_advances_across_segments_and_fillers() {
        let dir = temp_dir();
        let config = StoreConfig::builder().enable_journal(false).build();
        let mut store = Store::open(&dir, config).unwrap();
        store
            .create_dataset("ds", "type", 64 * 1024 * 1024, 256, 6, 1, 0)
            .unwrap();
        let ds = store.open_dataset("ds", "type").unwrap();

        ds.write(10, b"ten").unwrap();
        ds.write(30, b"thirty").unwrap();

        let mut iter = IndexQueryIterator::new(ds.inner_arc(), 10, 30);
        let first = iter.next_entry().unwrap().unwrap();
        assert_eq!(first.timestamp, 10);
        assert!(!first.is_filler());

        let filler = iter.next_entry().unwrap().unwrap();
        assert_eq!(filler.timestamp, 11);
        assert!(filler.is_filler());

        let mut last = filler;
        while last.timestamp < 30 {
            last = iter.next_entry().unwrap().unwrap();
        }
        assert_eq!(last.timestamp, 30);
        assert!(!last.is_filler());
        assert!(iter.next_entry().unwrap().is_none());
    }
}
