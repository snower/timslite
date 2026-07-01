use std::sync::Mutex;

use crate::bridge::{LengthEntry, Record};
use crate::errors::TmslError;

pub struct QueryIteratorBridge {
    iter: Mutex<Option<timslite::QueryIterator>>,
}

impl QueryIteratorBridge {
    pub fn new(iter: timslite::QueryIterator) -> Self {
        Self {
            iter: Mutex::new(Some(iter)),
        }
    }

    pub fn next(&self) -> Result<Option<Record>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some((ts, data))) => Ok(Some(Record {
                timestamp: ts,
                data,
            })),
            Ok(None) => {
                *guard = None;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn reverse(&self) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(());
        };
        *guard = Some(iter.reverse());
        Ok(())
    }

    pub fn skip(&self, count: u32) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(());
        };
        *guard = Some(iter.skip(count as usize));
        Ok(())
    }

    pub fn collect_all(&self) -> Result<Vec<Record>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records
                .into_iter()
                .map(|(ts, data)| Record {
                    timestamp: ts,
                    data,
                })
                .collect()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn collect_take(&self, count: u32) -> Result<Vec<Record>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count as usize) {
            Ok(records) => Ok(records
                .into_iter()
                .map(|(ts, data)| Record {
                    timestamp: ts,
                    data,
                })
                .collect()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        *guard = None;
        Ok(())
    }
}

pub struct QueryLengthIteratorBridge {
    iter: Mutex<Option<timslite::QueryLengthIterator>>,
}

impl QueryLengthIteratorBridge {
    pub fn new(iter: timslite::QueryLengthIterator) -> Self {
        Self {
            iter: Mutex::new(Some(iter)),
        }
    }

    pub fn next(&self) -> Result<Option<LengthEntry>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some((ts, len))) => Ok(Some(LengthEntry {
                timestamp: ts,
                length: len,
            })),
            Ok(None) => {
                *guard = None;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn reverse(&self) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(());
        };
        *guard = Some(iter.reverse());
        Ok(())
    }

    pub fn skip(&self, count: u32) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(());
        };
        *guard = Some(iter.skip(count as usize));
        Ok(())
    }

    pub fn collect_all(&self) -> Result<Vec<LengthEntry>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(entries) => Ok(entries
                .into_iter()
                .map(|(ts, len)| LengthEntry {
                    timestamp: ts,
                    length: len,
                })
                .collect()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn collect_take(&self, count: u32) -> Result<Vec<LengthEntry>, TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let Some(iter) = guard.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count as usize) {
            Ok(entries) => Ok(entries
                .into_iter()
                .map(|(ts, len)| LengthEntry {
                    timestamp: ts,
                    length: len,
                })
                .collect()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .iter
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        *guard = None;
        Ok(())
    }
}
