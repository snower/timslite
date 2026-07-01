use std::sync::Mutex;

use crate::bridge::{LengthEntry, Record};
use crate::errors::TmslError;

pub struct QueryIteratorBridge {
    inner: Mutex<Vec<Record>>,
    position: Mutex<usize>,
}

impl QueryIteratorBridge {
    pub fn new(records: Vec<Record>) -> Self {
        Self {
            inner: Mutex::new(records),
            position: Mutex::new(0),
        }
    }

    pub fn next(&self) -> Result<Option<Record>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        if *pos >= guard.len() {
            return Ok(None);
        }
        let record = guard[*pos].clone();
        *pos += 1;
        Ok(Some(record))
    }

    pub fn reverse(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.reverse();
        *pos = 0;
        Ok(())
    }

    pub fn skip(&self, count: u32) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        *pos = (*pos + count as usize).min(guard.len());
        Ok(())
    }

    pub fn collect_all(&self) -> Result<Vec<Record>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let result = guard[*pos..].to_vec();
        *pos = guard.len();
        Ok(result)
    }

    pub fn collect_take(&self, count: u32) -> Result<Vec<Record>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let end = (*pos + count as usize).min(guard.len());
        let result = guard[*pos..end].to_vec();
        *pos = end;
        Ok(result)
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.clear();
        *pos = 0;
        Ok(())
    }
}

pub struct QueryLengthIteratorBridge {
    inner: Mutex<Vec<LengthEntry>>,
    position: Mutex<usize>,
}

impl QueryLengthIteratorBridge {
    pub fn new(entries: Vec<LengthEntry>) -> Self {
        Self {
            inner: Mutex::new(entries),
            position: Mutex::new(0),
        }
    }

    pub fn next(&self) -> Result<Option<LengthEntry>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        if *pos >= guard.len() {
            return Ok(None);
        }
        let entry = guard[*pos].clone();
        *pos += 1;
        Ok(Some(entry))
    }

    pub fn reverse(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.reverse();
        *pos = 0;
        Ok(())
    }

    pub fn skip(&self, count: u32) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        *pos = (*pos + count as usize).min(guard.len());
        Ok(())
    }

    pub fn collect_all(&self) -> Result<Vec<LengthEntry>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let result = guard[*pos..].to_vec();
        *pos = guard.len();
        Ok(result)
    }

    pub fn collect_take(&self, count: u32) -> Result<Vec<LengthEntry>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let end = (*pos + count as usize).min(guard.len());
        let result = guard[*pos..end].to_vec();
        *pos = end;
        Ok(result)
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        let mut pos = self
            .position
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.clear();
        *pos = 0;
        Ok(())
    }
}
