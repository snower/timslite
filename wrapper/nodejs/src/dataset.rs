use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

use crate::errors;
use crate::types;
use crate::store::InspectResult;

#[napi]
pub struct Dataset {
    pub(crate) inner: Arc<timslite::DataSet>,
    identifier: u64,
    read_only: bool,
}

#[napi]
impl Dataset {
    pub(crate) fn new(
        inner: Arc<timslite::DataSet>,
        identifier: u64,
        read_only: bool,
    ) -> Self {
        Self {
            inner,
            identifier,
            read_only,
        }
    }

    #[napi]
    pub fn write(&self, timestamp: BigInt, data: Buffer) -> napi::Result<()> {
        if self.read_only {
            return Err(errors::invalid_data("dataset is read-only"));
        }
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(self.inner.write(ts, &data))
    }

    #[napi]
    pub fn append(&self, timestamp: BigInt, data: Buffer) -> napi::Result<()> {
        if self.read_only {
            return Err(errors::invalid_data("dataset is read-only"));
        }
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(self.inner.append(ts, &data))
    }

    #[napi]
    pub fn delete(&self, timestamp: BigInt) -> napi::Result<()> {
        if self.read_only {
            return Err(errors::invalid_data("dataset is read-only"));
        }
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(self.inner.delete(ts))
    }

    #[napi]
    pub fn read(&self, timestamp: BigInt) -> napi::Result<Option<(BigInt, Buffer)>> {
        let ts = types::bigint_to_i64(&timestamp)?;
        let result = errors::wrap(self.inner.read(ts))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn read_latest(&self) -> napi::Result<Option<(BigInt, Buffer)>> {
        let result = errors::wrap(self.inner.read_latest())?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn read_exist(&self, timestamp: BigInt) -> napi::Result<bool> {
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(self.inner.read_exist(ts))
    }

    #[napi]
    pub fn read_length(&self, timestamp: BigInt) -> napi::Result<Option<u32>> {
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(self.inner.read_length(ts))
    }

    #[napi]
    pub fn query(&self, start_ts: BigInt, end_ts: BigInt) -> napi::Result<crate::query::QueryIterator> {
        let s = types::bigint_to_i64(&start_ts)?;
        let e = types::bigint_to_i64(&end_ts)?;
        let rows = errors::wrap(self.inner.query(s, e))?;
        Ok(crate::query::QueryIterator::new(rows))
    }

    #[napi]
    pub fn query_all(&self, start_ts: BigInt, end_ts: BigInt) -> napi::Result<Vec<(BigInt, Buffer)>> {
        let s = types::bigint_to_i64(&start_ts)?;
        let e = types::bigint_to_i64(&end_ts)?;
        let rows = errors::wrap(self.inner.query(s, e))?;
        Ok(rows
            .into_iter()
            .map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data)))
            .collect())
    }

    #[napi]
    pub fn query_exist(&self, start_ts: BigInt, end_ts: BigInt) -> napi::Result<Buffer> {
        let s = types::bigint_to_i64(&start_ts)?;
        let e = types::bigint_to_i64(&end_ts)?;
        let bitmap = errors::wrap(self.inner.query_exist(s, e))?;
        Ok(types::vec_to_buffer(bitmap))
    }

    #[napi]
    pub fn query_length(
        &self,
        start_ts: BigInt,
        end_ts: BigInt,
    ) -> napi::Result<crate::query::QueryLengthIterator> {
        let s = types::bigint_to_i64(&start_ts)?;
        let e = types::bigint_to_i64(&end_ts)?;
        let rows = errors::wrap(self.inner.query_length(s, e))?;
        Ok(crate::query::QueryLengthIterator::new(rows))
    }

    #[napi]
    pub fn query_length_all(
        &self,
        start_ts: BigInt,
        end_ts: BigInt,
    ) -> napi::Result<Vec<(BigInt, u32)>> {
        let s = types::bigint_to_i64(&start_ts)?;
        let e = types::bigint_to_i64(&end_ts)?;
        let rows = errors::wrap(self.inner.query_length(s, e))?;
        Ok(rows
            .into_iter()
            .map(|(ts, len)| (types::i64_to_bigint(ts), len))
            .collect())
    }

    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        errors::wrap(self.inner.flush())
    }

    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        errors::wrap(self.inner.close())
    }

    #[napi]
    pub fn inspect(&self) -> napi::Result<InspectResult> {
        let result = errors::wrap(self.inner.inspect())?;
        Ok(InspectResult {
            info: crate::store::info_to_result(&result.info),
            state: crate::store::state_to_result(&result.state),
        })
    }

    #[napi(getter)]
    pub fn id(&self) -> BigInt {
        types::u64_to_bigint(self.identifier)
    }

    #[napi(getter)]
    pub fn identifier(&self) -> BigInt {
        types::u64_to_bigint(self.identifier)
    }

    #[napi(getter)]
    pub fn data_dir(&self) -> String {
        self.inner.base_dir().to_string_lossy().into_owned()
    }

    #[napi(getter)]
    pub fn latest_timestamp(&self) -> Option<BigInt> {
        self.inner.latest_written_timestamp().map(types::i64_to_bigint)
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        false
    }
}
