use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types;

#[napi(iterator)]
pub struct QueryIterator {
    iter: Option<timslite::QueryIterator>,
}

impl QueryIterator {
    pub fn new(iter: timslite::QueryIterator) -> Self {
        Self { iter: Some(iter) }
    }
}

impl Generator for QueryIterator {
    type Yield = (i64, Buffer);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        let iter = self.iter.as_mut()?;
        match iter.next_entry() {
            Ok(Some((ts, data))) => Some((ts, types::vec_to_buffer(data))),
            Ok(None) => {
                self.iter = None;
                None
            }
            Err(_) => {
                self.iter = None;
                None
            }
        }
    }
}

#[napi]
impl QueryIterator {
    #[napi]
    pub fn reverse(&mut self) -> Result<&mut Self> {
        let Some(iter) = self.iter.take() else {
            return Ok(self);
        };
        self.iter = Some(iter.reverse());
        Ok(self)
    }

    #[napi]
    pub fn skip(&mut self, count: u32) -> Result<&mut Self> {
        let Some(iter) = self.iter.take() else {
            return Ok(self);
        };
        self.iter = Some(iter.skip(count as usize));
        Ok(self)
    }

    #[napi]
    pub fn collect_all(&mut self) -> Result<Vec<(i64, Buffer)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records
                .into_iter()
                .map(|(ts, data)| (ts, types::vec_to_buffer(data)))
                .collect()),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }

    #[napi]
    pub fn collect_take(&mut self, count: u32) -> Result<Vec<(i64, Buffer)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count as usize) {
            Ok(records) => Ok(records
                .into_iter()
                .map(|(ts, data)| (ts, types::vec_to_buffer(data)))
                .collect()),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }
}

#[napi(iterator)]
pub struct QueryLengthIterator {
    iter: Option<timslite::QueryLengthIterator>,
}

impl QueryLengthIterator {
    pub fn new(iter: timslite::QueryLengthIterator) -> Self {
        Self { iter: Some(iter) }
    }
}

impl Generator for QueryLengthIterator {
    type Yield = (i64, u32);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        let iter = self.iter.as_mut()?;
        match iter.next_entry() {
            Ok(Some(entry)) => Some(entry),
            Ok(None) => {
                self.iter = None;
                None
            }
            Err(_) => {
                self.iter = None;
                None
            }
        }
    }
}

#[napi]
impl QueryLengthIterator {
    #[napi]
    pub fn reverse(&mut self) -> Result<&mut Self> {
        let Some(iter) = self.iter.take() else {
            return Ok(self);
        };
        self.iter = Some(iter.reverse());
        Ok(self)
    }

    #[napi]
    pub fn skip(&mut self, count: u32) -> Result<&mut Self> {
        let Some(iter) = self.iter.take() else {
            return Ok(self);
        };
        self.iter = Some(iter.skip(count as usize));
        Ok(self)
    }

    #[napi]
    pub fn collect_all(&mut self) -> Result<Vec<(i64, u32)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }

    #[napi]
    pub fn collect_take(&mut self, count: u32) -> Result<Vec<(i64, u32)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count as usize) {
            Ok(records) => Ok(records),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }
}
