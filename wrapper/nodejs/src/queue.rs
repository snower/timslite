use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use std::sync::Arc;
use std::time::Duration;

use crate::errors;
use crate::types;

#[napi(object)]
pub struct QueueConsumerOptions {
    pub running_expired_seconds: Option<u32>,
    pub max_retry_count: Option<u16>,
}

#[napi(object)]
pub struct QueueConsumerInfo {
    pub group_name: String,
    pub running_expired_seconds: u32,
    pub max_retry_count: u8,
}

#[napi(object)]
pub struct QueueConsumerPendingEntry {
    pub timestamp: BigInt,
    pub start_time: BigInt,
    pub status: u8,
    pub retry_count: u8,
}

#[napi(object)]
pub struct QueueConsumerState {
    pub processed_ts: BigInt,
    pub pending_entries: Vec<QueueConsumerPendingEntry>,
}

#[napi(object)]
pub struct QueueConsumerInspectResult {
    pub info: QueueConsumerInfo,
    pub state: QueueConsumerState,
}

fn build_consumer_config(opts: &Option<QueueConsumerOptions>) -> napi::Result<timslite::QueueConsumerConfig> {
    let mut builder = timslite::QueueConsumerConfig::builder();
    if let Some(ref o) = opts {
        if let Some(v) = o.running_expired_seconds {
            builder = builder.running_expired_seconds(v as u64);
        }
        if let Some(v) = o.max_retry_count {
            builder = builder.max_retry_count(v);
        }
    }
    errors::wrap(builder.build())
}

#[napi]
pub struct Queue {
    inner: Option<timslite::DatasetQueue>,
}

impl Queue {
    pub fn new(inner: timslite::DatasetQueue) -> Self {
        Self { inner: Some(inner) }
    }
}

#[napi]
impl Queue {
    #[napi]
    pub fn push(&self, data: Buffer) -> napi::Result<BigInt> {
        let queue = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let ts = errors::wrap(queue.push(&data))?;
        Ok(types::i64_to_bigint(ts))
    }

    #[napi]
    pub fn open_consumer(
        &self,
        group_name: String,
        options: Option<QueueConsumerOptions>,
    ) -> napi::Result<QueueConsumer> {
        let queue = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let config = build_consumer_config(&options)?;
        let consumer = errors::wrap(queue.open_consumer_with_config(&group_name, config))?;
        Ok(QueueConsumer {
            inner: Some(consumer),
        })
    }

    #[napi]
    pub fn get_consumer_group_names(&self) -> napi::Result<Vec<String>> {
        let queue = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(queue.get_consumer_group_names())
    }

    #[napi]
    pub fn drop_consumer(&self, group_name: String) -> napi::Result<()> {
        let queue = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(queue.drop_consumer(&group_name))
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        let queue = self.inner.take().ok_or_else(errors::store_closed)?;
        errors::wrap(queue.close())
    }
}

#[napi]
pub struct QueueConsumer {
    inner: Option<timslite::DatasetQueueConsumer>,
}

#[napi]
impl QueueConsumer {
    #[napi]
    pub async fn poll(&self, timeout_ms: Option<u32>) -> napi::Result<Option<(BigInt, Buffer)>> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(0) as u64);
        let result = errors::wrap(consumer.poll(timeout))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn poll_sync(&self, timeout_ms: Option<u32>) -> napi::Result<Option<(BigInt, Buffer)>> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(0) as u64);
        let result = errors::wrap(consumer.poll(timeout))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn ack(&self, timestamp: BigInt) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let ts = types::bigint_to_i64(&timestamp)?;
        errors::wrap(consumer.ack(ts))
    }

    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(consumer.flush())
    }

    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        errors::wrap(consumer.close())
    }

    #[napi]
    pub fn inspect(&self) -> napi::Result<QueueConsumerInspectResult> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let result = errors::wrap(consumer.inspect())?;
        Ok(QueueConsumerInspectResult {
            info: QueueConsumerInfo {
                group_name: result.info.group_name,
                running_expired_seconds: result.info.running_expired_seconds as u32,
                max_retry_count: result.info.max_retry_count,
            },
            state: QueueConsumerState {
                processed_ts: types::i64_to_bigint(result.state.processed_ts),
                pending_entries: result
                    .state
                    .pending_entries
                    .into_iter()
                    .map(|entry| QueueConsumerPendingEntry {
                        timestamp: types::i64_to_bigint(entry.timestamp),
                        start_time: types::i64_to_bigint(entry.start_time),
                        status: entry.status,
                        retry_count: entry.retry_count,
                    })
                    .collect(),
            },
        })
    }

    #[napi]
    pub fn poll_callback(
        &self,
        callback: Option<ThreadsafeFunction<(), ()>>,
    ) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let cb: Option<timslite::QueuePollCallback> = callback.map(|tsfn| {
            Arc::new(move || {
                tsfn.call(Ok(()), ThreadsafeFunctionCallMode::NonBlocking);
            }) as timslite::QueuePollCallback
        });
        errors::wrap(consumer.poll_callback(cb))
    }
}

#[napi]
pub struct JournalQueue {
    inner: Option<timslite::JournalQueue>,
}

impl JournalQueue {
    pub fn new(inner: timslite::JournalQueue) -> Self {
        Self { inner: Some(inner) }
    }
}

#[napi]
impl JournalQueue {
    #[napi]
    pub fn open_consumer(
        &self,
        group_name: String,
        options: Option<QueueConsumerOptions>,
    ) -> napi::Result<JournalQueueConsumer> {
        let jq = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let config = build_consumer_config(&options)?;
        let consumer = errors::wrap(jq.open_consumer_with_config(&group_name, config))?;
        Ok(JournalQueueConsumer {
            inner: Some(consumer),
        })
    }

    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        let jq = self.inner.take().ok_or_else(errors::store_closed)?;
        errors::wrap(jq.close())
    }
}

#[napi]
pub struct JournalQueueConsumer {
    inner: Option<timslite::JournalQueueConsumer>,
}

#[napi]
impl JournalQueueConsumer {
    #[napi]
    pub async fn poll(&self, timeout_ms: Option<u32>) -> napi::Result<Option<(BigInt, Buffer)>> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(0) as u64);
        let result = errors::wrap(consumer.poll(timeout))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn poll_sync(&self, timeout_ms: Option<u32>) -> napi::Result<Option<(BigInt, Buffer)>> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(0) as u64);
        let result = errors::wrap(consumer.poll(timeout))?;
        Ok(result.map(|(ts, data)| (types::i64_to_bigint(ts), types::vec_to_buffer(data))))
    }

    #[napi]
    pub fn ack(&self, sequence: BigInt) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let seq = types::bigint_to_i64(&sequence)?;
        errors::wrap(consumer.ack(seq))
    }

    #[napi]
    pub fn poll_callback(
        &self,
        callback: Option<ThreadsafeFunction<(), ()>>,
    ) -> napi::Result<()> {
        let consumer = self.inner.as_ref().ok_or_else(errors::store_closed)?;
        let cb: Option<timslite::QueuePollCallback> = callback.map(|tsfn| {
            Arc::new(move || {
                tsfn.call(Ok(()), ThreadsafeFunctionCallMode::NonBlocking);
            }) as timslite::QueuePollCallback
        });
        errors::wrap(consumer.poll_callback(cb))
    }
}
