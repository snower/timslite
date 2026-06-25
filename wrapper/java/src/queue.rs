use std::sync::Mutex;

use crate::bridge::Record;
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

    pub fn close(&self) -> Result<(), TmslError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| TmslError::Io { message: e.to_string() })?;
        guard.take();
        Ok(())
    }
}
