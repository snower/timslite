//! PyDatasetQueue and PyDatasetQueueConsumer 鈥?Python bindings for queue semantics.
//!
//! Wraps DatasetQueue (push + consumer group management) and
//! DatasetQueueConsumer (poll + ack) with Pythonic API.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use crate::exceptions::wrap;

/// Public configuration for a dataset queue consumer group.
#[pyclass(name = "DatasetQueueConsumerInfo", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyDatasetQueueConsumerInfo {
    #[pyo3(get)]
    pub group_name: String,
    #[pyo3(get)]
    pub running_expired_seconds: u16,
    #[pyo3(get)]
    pub max_retry_count: u8,
}

/// Pending queue record state returned by DatasetQueueConsumer.inspect().
#[pyclass(name = "DatasetQueueConsumerPendingEntry", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyDatasetQueueConsumerPendingEntry {
    #[pyo3(get)]
    pub timestamp: i64,
    #[pyo3(get)]
    pub start_time: i64,
    #[pyo3(get)]
    pub status: u8,
    #[pyo3(get)]
    pub retry_count: u8,
}

/// Durable state for a dataset queue consumer group.
#[pyclass(name = "DatasetQueueConsumerState", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyDatasetQueueConsumerState {
    #[pyo3(get)]
    pub processed_ts: i64,
    #[pyo3(get)]
    pub pending_entries: Vec<PyDatasetQueueConsumerPendingEntry>,
}

/// Result of DatasetQueueConsumer.inspect().
#[pyclass(name = "DatasetQueueConsumerInspectResult", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyDatasetQueueConsumerInspectResult {
    #[pyo3(get)]
    pub info: PyDatasetQueueConsumerInfo,
    #[pyo3(get)]
    pub state: PyDatasetQueueConsumerState,
}

impl From<timslite::DatasetQueueConsumerInfo> for PyDatasetQueueConsumerInfo {
    fn from(info: timslite::DatasetQueueConsumerInfo) -> Self {
        Self {
            group_name: info.group_name,
            running_expired_seconds: info.running_expired_seconds,
            max_retry_count: info.max_retry_count,
        }
    }
}

impl From<timslite::DatasetQueueConsumerPendingEntry> for PyDatasetQueueConsumerPendingEntry {
    fn from(entry: timslite::DatasetQueueConsumerPendingEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            start_time: entry.start_time,
            status: entry.status,
            retry_count: entry.retry_count,
        }
    }
}

impl From<timslite::DatasetQueueConsumerState> for PyDatasetQueueConsumerState {
    fn from(state: timslite::DatasetQueueConsumerState) -> Self {
        Self {
            processed_ts: state.processed_ts,
            pending_entries: state
                .pending_entries
                .into_iter()
                .map(PyDatasetQueueConsumerPendingEntry::from)
                .collect(),
        }
    }
}

impl From<timslite::DatasetQueueConsumerInspectResult> for PyDatasetQueueConsumerInspectResult {
    fn from(result: timslite::DatasetQueueConsumerInspectResult) -> Self {
        Self {
            info: PyDatasetQueueConsumerInfo::from(result.info),
            state: PyDatasetQueueConsumerState::from(result.state),
        }
    }
}

/// Python wrapper for DatasetQueue.
///
/// Maintains an open queue on a dataset, allowing push and consumer
/// group operations.
#[pyclass(name = "DatasetQueue")]
pub struct PyDatasetQueue {
    inner: timslite::DatasetQueue,
}

#[pymethods]
impl PyDatasetQueue {
    fn __repr__(&self) -> String {
        "DatasetQueue()".to_string()
    }

    /// Push data into the queue.
    ///
    /// Auto-increments the dataset timestamp and notifies all waiting
    /// consumers across all consumer groups.
    ///
    /// Returns the assigned timestamp.
    fn push(&self, data: Vec<u8>) -> PyResult<i64> {
        wrap(self.inner.push(&data))
    }

    /// Open or create a consumer group and return a consumer handle.
    ///
    /// Multiple consumers in the same group share progress via the
    /// shared 4KB mmap state file. The first call for a group creates
    /// the state file; subsequent calls open the existing file.
    #[pyo3(signature = (group_name, running_expired_seconds=900, max_retry_count=3))]
    fn open_consumer(
        &self,
        group_name: &str,
        running_expired_seconds: u64,
        max_retry_count: u16,
    ) -> PyResult<PyDatasetQueueConsumer> {
        let config = wrap(
            timslite::QueueConsumerConfig::builder()
                .running_expired_seconds(running_expired_seconds)
                .max_retry_count(max_retry_count)
                .build(),
        )?;
        let consumer = wrap(self.inner.open_consumer_with_config(group_name, config))?;
        Ok(PyDatasetQueueConsumer { inner: consumer })
    }

    /// Return current consumer group names.
    ///
    /// This lists existing state file directory entries without opening or
    /// validating the state files.
    fn get_consumer_group_names(&self) -> PyResult<Vec<String>> {
        wrap(self.inner.get_consumer_group_names())
    }

    /// Close the queue and all associated consumers.
    ///
    /// All pending records are synced, consumer state files are
    /// flushed, and waiting polls are unblocked with `QueueClosed`.
    fn close(&self) -> PyResult<()> {
        wrap(self.inner.close())
    }

    /// Drop (close and remove) a consumer group.
    ///
    /// The consumer group's state file is synced and deleted.
    /// Subsequent calls to open_consumer with the same group name
    /// will create a fresh group.
    fn drop_consumer(&self, group_name: &str) -> PyResult<()> {
        wrap(self.inner.drop_consumer(group_name))
    }
}

impl PyDatasetQueue {
    /// Create a new PyDatasetQueue from a DatasetQueue.
    pub fn new(inner: timslite::DatasetQueue) -> Self {
        Self { inner }
    }
}

/// Python wrapper for DatasetQueueConsumer.
///
/// Polls for new records from a consumer group with configurable timeout.
/// Multiple consumers for the same group share progress via mmap.
#[pyclass(name = "DatasetQueueConsumer")]
pub struct PyDatasetQueueConsumer {
    inner: timslite::DatasetQueueConsumer,
}

#[pymethods]
impl PyDatasetQueueConsumer {
    fn __repr__(&self) -> String {
        "DatasetQueueConsumer()".to_string()
    }

    /// Poll for the next record.
    ///
    /// Returns the next unacked record as `(timestamp, data)`, or `None`
    /// if the timeout expires with no data available.
    ///
    /// Args:
    ///     timeout_ms: Maximum wait time in milliseconds. Use 0 for
    ///         non-blocking poll (returns immediately if no data).
    ///
    /// Raises:
    ///     TmslQueueClosedError: Queue has been closed.
    fn poll(&self, timeout_ms: u64) -> PyResult<Option<(i64, Vec<u8>)>> {
        wrap(self.inner.poll(Duration::from_millis(timeout_ms)))
    }

    /// Acknowledge a previously polled record.
    ///
    /// Removes the pending entry and advances the consumer's processed
    /// timestamp. Only call after successfully processing a record
    /// returned by `poll()`.
    fn ack(&self, timestamp: i64) -> PyResult<()> {
        wrap(self.inner.ack(timestamp))
    }

    /// Flush this consumer group's state file.
    fn flush(&self) -> PyResult<()> {
        wrap(self.inner.flush())
    }

    /// Close this consumer group.
    ///
    /// All opened handles for the same group become invalid, and any
    /// unacknowledged pending entries are released for redelivery after reopen.
    fn close(&self) -> PyResult<()> {
        wrap(self.inner.close())
    }

    /// Inspect this consumer group's public config and durable state.
    fn inspect(&self) -> PyResult<PyDatasetQueueConsumerInspectResult> {
        let result = wrap(self.inner.inspect())?;
        Ok(PyDatasetQueueConsumerInspectResult::from(result))
    }

    /// Register or clear a lightweight wake callback.
    ///
    /// The callback is invoked synchronously after data waiters are notified.
    /// It must only wake external processing; call poll() separately to handle
    /// data. Pass None to clear the callback.
    fn poll_callback(&self, py: Python<'_>, callback: Option<Py<PyAny>>) -> PyResult<()> {
        let callback = match callback {
            Some(callback) => {
                if !callback.bind(py).is_callable() {
                    return Err(PyTypeError::new_err("poll_callback requires a callable or None"));
                }
                Some(Arc::new(move || {
                    Python::attach(|py| {
                        if let Err(err) = callback.call0(py) {
                            err.write_unraisable(py, None);
                        }
                    });
                }) as timslite::QueuePollCallback)
            }
            None => None,
        };
        wrap(self.inner.poll_callback(callback))
    }
}

impl PyDatasetQueueConsumer {
    /// Create a new PyDatasetQueueConsumer from a DatasetQueueConsumer.
    pub fn new(inner: timslite::DatasetQueueConsumer) -> Self {
        Self { inner }
    }
}

/// Python wrapper for the built-in journal queue.
#[pyclass(name = "JournalQueue")]
pub struct PyJournalQueue {
    inner: timslite::JournalQueue,
}

#[pymethods]
impl PyJournalQueue {
    fn __repr__(&self) -> String {
        "JournalQueue()".to_string()
    }

    /// Open or create a journal consumer group.
    #[pyo3(signature = (group_name, running_expired_seconds=900, max_retry_count=3))]
    fn open_consumer(
        &self,
        group_name: &str,
        running_expired_seconds: u64,
        max_retry_count: u16,
    ) -> PyResult<PyJournalQueueConsumer> {
        let config = wrap(
            timslite::QueueConsumerConfig::builder()
                .running_expired_seconds(running_expired_seconds)
                .max_retry_count(max_retry_count)
                .build(),
        )?;
        let consumer = wrap(self.inner.open_consumer_with_config(group_name, config))?;
        Ok(PyJournalQueueConsumer { inner: consumer })
    }

    /// Close the journal queue handle and unblock waiting polls.
    fn close(&self) -> PyResult<()> {
        wrap(self.inner.close())
    }
}

impl PyJournalQueue {
    pub fn new(inner: timslite::JournalQueue) -> Self {
        Self { inner }
    }
}

/// Python wrapper for a journal queue consumer.
#[pyclass(name = "JournalQueueConsumer")]
pub struct PyJournalQueueConsumer {
    inner: timslite::JournalQueueConsumer,
}

#[pymethods]
impl PyJournalQueueConsumer {
    fn __repr__(&self) -> String {
        "JournalQueueConsumer()".to_string()
    }

    /// Poll for the next encoded journal record.
    fn poll(&self, timeout_ms: u64) -> PyResult<Option<(i64, Vec<u8>)>> {
        wrap(self.inner.poll(Duration::from_millis(timeout_ms)))
    }

    /// Acknowledge a previously polled journal sequence.
    fn ack(&self, sequence: i64) -> PyResult<()> {
        wrap(self.inner.ack(sequence))
    }

    /// Register or clear a lightweight wake callback.
    fn poll_callback(&self, py: Python<'_>, callback: Option<Py<PyAny>>) -> PyResult<()> {
        let callback = match callback {
            Some(callback) => {
                if !callback.bind(py).is_callable() {
                    return Err(PyTypeError::new_err("poll_callback requires a callable or None"));
                }
                Some(Arc::new(move || {
                    Python::attach(|py| {
                        if let Err(err) = callback.call0(py) {
                            err.write_unraisable(py, None);
                        }
                    });
                }) as timslite::QueuePollCallback)
            }
            None => None,
        };
        wrap(self.inner.poll_callback(callback))
    }
}
