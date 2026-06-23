use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types;

#[napi(iterator)]
pub struct QueryIterator {
    rows: std::vec::IntoIter<(i64, Vec<u8>)>,
}

impl QueryIterator {
    pub fn new(rows: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl Generator for QueryIterator {
    type Yield = (i64, Buffer);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        self.rows
            .next()
            .map(|(ts, data)| (ts, types::vec_to_buffer(data)))
    }
}

#[napi(iterator)]
pub struct QueryLengthIterator {
    rows: std::vec::IntoIter<(i64, u32)>,
}

impl QueryLengthIterator {
    pub fn new(rows: Vec<(i64, u32)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl Generator for QueryLengthIterator {
    type Yield = (i64, u32);
    type Next = ();
    type Return = ();

    fn next(&mut self, _value: Option<Self::Next>) -> Option<Self::Yield> {
        self.rows.next()
    }
}
