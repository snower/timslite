use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types;

#[napi(iterator)]
pub struct QueryIterator {
    rows: Vec<(i64, Vec<u8>)>,
    position: usize,
}

impl QueryIterator {
    pub fn new(rows: Vec<(i64, Vec<u8>)>) -> Self {
        Self { rows, position: 0 }
    }
}

impl Generator for QueryIterator {
    type Yield = (i64, Buffer);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        if self.position >= self.rows.len() {
            return None;
        }
        let (ts, data) = self.rows[self.position].clone();
        self.position += 1;
        Some((ts, types::vec_to_buffer(data)))
    }
}

#[napi]
impl QueryIterator {
    #[napi]
    pub fn reverse(&mut self) {
        self.rows.reverse();
        self.position = 0;
    }

    #[napi]
    pub fn skip(&mut self, count: u32) {
        self.position = (self.position + count as usize).min(self.rows.len());
    }

    #[napi]
    pub fn collect_all(&mut self) -> Vec<(i64, Buffer)> {
        let result: Vec<_> = self.rows[self.position..]
            .iter()
            .map(|(ts, data)| (*ts, types::vec_to_buffer(data.clone())))
            .collect();
        self.position = self.rows.len();
        result
    }

    #[napi]
    pub fn collect_take(&mut self, count: u32) -> Vec<(i64, Buffer)> {
        let end = (self.position + count as usize).min(self.rows.len());
        let result: Vec<_> = self.rows[self.position..end]
            .iter()
            .map(|(ts, data)| (*ts, types::vec_to_buffer(data.clone())))
            .collect();
        self.position = end;
        result
    }
}

#[napi(iterator)]
pub struct QueryLengthIterator {
    rows: Vec<(i64, u32)>,
    position: usize,
}

impl QueryLengthIterator {
    pub fn new(rows: Vec<(i64, u32)>) -> Self {
        Self { rows, position: 0 }
    }
}

impl Generator for QueryLengthIterator {
    type Yield = (i64, u32);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        if self.position >= self.rows.len() {
            return None;
        }
        let entry = self.rows[self.position];
        self.position += 1;
        Some(entry)
    }
}

#[napi]
impl QueryLengthIterator {
    #[napi]
    pub fn reverse(&mut self) {
        self.rows.reverse();
        self.position = 0;
    }

    #[napi]
    pub fn skip(&mut self, count: u32) {
        self.position = (self.position + count as usize).min(self.rows.len());
    }

    #[napi]
    pub fn collect_all(&mut self) -> Vec<(i64, u32)> {
        let result = self.rows[self.position..].to_vec();
        self.position = self.rows.len();
        result
    }

    #[napi]
    pub fn collect_take(&mut self, count: u32) -> Vec<(i64, u32)> {
        let end = (self.position + count as usize).min(self.rows.len());
        let result = self.rows[self.position..end].to_vec();
        self.position = end;
        result
    }
}
