use crate::dataset::DataSetInner;
use crate::error::{Result, TmslError};
use crate::index::segment::IndexEntry;
use crate::index::IndexEntryPosition;
use std::sync::{Arc, Mutex};

pub(crate) struct IndexQueryIterator {
    dataset: Arc<Mutex<DataSetInner>>,
    start_ts: i64,
    end_ts: i64,
    cursor: Option<IndexEntryPosition>,
    direction: QueryDirection,
    pending_skip_low: usize,
    pending_skip_high: usize,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryDirection {
    Forward,
    Reverse,
}

impl IndexQueryIterator {
    pub(crate) fn new(dataset: Arc<Mutex<DataSetInner>>, start_ts: i64, end_ts: i64) -> Self {
        Self {
            dataset,
            start_ts,
            end_ts,
            cursor: None,
            direction: QueryDirection::Forward,
            pending_skip_low: 0,
            pending_skip_high: 0,
            done: start_ts > end_ts,
        }
    }

    pub(crate) fn reverse(mut self) -> Self {
        self.direction = match self.direction {
            QueryDirection::Forward => QueryDirection::Reverse,
            QueryDirection::Reverse => QueryDirection::Forward,
        };
        self.cursor = None;
        self
    }

    pub(crate) fn skip(mut self, count: usize) -> Self {
        match self.direction {
            QueryDirection::Forward => {
                self.pending_skip_low = self.pending_skip_low.saturating_add(count);
            }
            QueryDirection::Reverse => {
                self.pending_skip_high = self.pending_skip_high.saturating_add(count);
            }
        }
        self.cursor = None;
        self
    }

    pub(crate) fn next_entry(&mut self) -> Result<Option<IndexEntry>> {
        self.apply_pending_skips()?;
        if self.done {
            return Ok(None);
        }

        let next = {
            let mut inner = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            match self.direction {
                QueryDirection::Forward => inner.next_query_index_entry_from_position(
                    self.start_ts,
                    self.end_ts,
                    self.cursor,
                )?,
                QueryDirection::Reverse => inner.previous_query_index_entry_from_position(
                    self.start_ts,
                    self.end_ts,
                    self.cursor,
                )?,
            }
        };

        let Some((cursor, entry)) = next else {
            self.done = true;
            return Ok(None);
        };

        self.cursor = Some(cursor);
        match self.direction {
            QueryDirection::Forward => {
                if let Some(next) = entry.timestamp.checked_add(1) {
                    self.start_ts = next;
                } else {
                    self.done = true;
                }
            }
            QueryDirection::Reverse => {
                if let Some(previous) = entry.timestamp.checked_sub(1) {
                    self.end_ts = previous;
                } else {
                    self.done = true;
                }
            }
        }
        Ok(Some(entry))
    }

    fn apply_pending_skips(&mut self) -> Result<()> {
        if self.done {
            self.pending_skip_low = 0;
            self.pending_skip_high = 0;
            return Ok(());
        }

        if self.pending_skip_low > 0 {
            let count = std::mem::take(&mut self.pending_skip_low);
            let next_start = {
                let mut inner = self
                    .dataset
                    .lock()
                    .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
                inner.skip_query_index_entries_forward(self.start_ts, self.end_ts, count)?
            };
            match next_start {
                Some(next_start) => {
                    self.start_ts = next_start;
                    self.cursor = None;
                }
                None => {
                    self.done = true;
                    self.pending_skip_high = 0;
                    return Ok(());
                }
            }
        }

        if self.pending_skip_high > 0 {
            let count = std::mem::take(&mut self.pending_skip_high);
            let next_end = {
                let mut inner = self
                    .dataset
                    .lock()
                    .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
                inner.skip_query_index_entries_reverse(self.start_ts, self.end_ts, count)?
            };
            match next_end {
                Some(next_end) => {
                    self.end_ts = next_end;
                    self.cursor = None;
                }
                None => {
                    self.done = true;
                }
            }
        }

        if self.start_ts > self.end_ts {
            self.done = true;
        }

        Ok(())
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
