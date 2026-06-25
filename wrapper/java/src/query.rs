use std::sync::Mutex;

use crate::bridge::{LengthEntry, Record};
use crate::errors::TmslError;

pub struct QueryIteratorBridge {
    inner: Mutex<Vec<Record>>,
}

impl QueryIteratorBridge {
    pub fn new(records: Vec<Record>) -> Self {
        Self {
            inner: Mutex::new(records),
        }
    }

    pub fn next(&self) -> Result<Option<Record>, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        Ok(guard.pop())
    }

    pub fn close(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.clear();
        Ok(())
    }
}

pub struct QueryLengthIteratorBridge {
    inner: Mutex<Vec<LengthEntry>>,
}

impl QueryLengthIteratorBridge {
    pub fn new(entries: Vec<LengthEntry>) -> Self {
        Self {
            inner: Mutex::new(entries),
        }
    }

    pub fn next(&self) -> Result<Option<LengthEntry>, TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        Ok(guard.pop())
    }

    pub fn close(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.clear();
        Ok(())
    }
}
