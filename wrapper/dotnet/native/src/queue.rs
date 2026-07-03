use std::sync::Mutex;

use crate::bridge::{
    QueueConsumerInfo, QueueConsumerInspectResult, QueueConsumerPendingEntry, QueueConsumerState,
    Record,
};
use crate::errors::TmslError;

pub struct QueueConsumerBridge {
    inner: Mutex<Option<timslite::DatasetQueueConsumer>>,
}

impl QueueConsumerBridge {
    pub fn new(consumer: timslite::DatasetQueueConsumer) -> Self {
        Self {
            inner: Mutex::new(Some(consumer)),
        }
    }

    pub fn poll(&self, timeout_ms: u64) -> Result<Option<Record>, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                let result = consumer.poll(std::time::Duration::from_millis(timeout_ms))?;
                Ok(result.map(|(ts, data)| Record {
                    timestamp: ts,
                    data,
                }))
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "consumer is closed".into(),
            }),
        }
    }

    pub fn ack(&self, timestamp: i64) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                consumer.ack(timestamp)?;
                Ok(())
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "consumer is closed".into(),
            }),
        }
    }

    pub fn flush(&self) -> Result<(), TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                consumer.flush()?;
                Ok(())
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "consumer is closed".into(),
            }),
        }
    }

    pub fn release(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        if let Some(consumer) = guard.take() {
            consumer.close()?;
        }
        Ok(())
    }

    pub fn inspect(&self) -> Result<QueueConsumerInspectResult, TmslError> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        match guard.as_ref() {
            Some(consumer) => {
                let result = consumer.inspect()?;
                Ok(QueueConsumerInspectResult {
                    info: QueueConsumerInfo {
                        group_name: result.info.group_name,
                        running_expired_seconds: result.info.running_expired_seconds as u64,
                        max_retry_count: result.info.max_retry_count as u16,
                    },
                    state: QueueConsumerState {
                        processed_ts: result.state.processed_ts,
                        pending_entries: result
                            .state
                            .pending_entries
                            .into_iter()
                            .map(|entry| QueueConsumerPendingEntry {
                                timestamp: entry.timestamp,
                                start_time: entry.start_time,
                                status: entry.status,
                                retry_count: entry.retry_count,
                            })
                            .collect(),
                    },
                })
            }
            None => Err(TmslError::QueueBridgeClosed {
                message: "consumer is closed".into(),
            }),
        }
    }
}
