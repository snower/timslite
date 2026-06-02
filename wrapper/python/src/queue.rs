//! PyDatasetQueue and PyDatasetQueueConsumer — Python bindings for queue semantics.
//!
//! Wraps DatasetQueue (push + consumer group management) and
//! DatasetQueueConsumer (poll + ack) with Pythonic API.

use pyo3::prelude::*;
use std::time::Duration;

use crate::exceptions::wrap;

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
    fn open_consumer(&self, group_name: &str) -> PyResult<PyDatasetQueueConsumer> {
        let consumer = wrap(self.inner.open_consumer(group_name))?;
        Ok(PyDatasetQueueConsumer { inner: consumer })
    }

    /// Close the queue and all associated consumers.
    ///
    /// All pending records are synced, consumer state files are
    /// flushed, and waiting polls are unblocked with `QueueClosed`.
    fn close(&self) -> PyResult<()> {
        wrap(self.inner.close())
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
}

impl PyDatasetQueueConsumer {
    /// Create a new PyDatasetQueueConsumer from a DatasetQueueConsumer.
    pub fn new(inner: timslite::DatasetQueueConsumer) -> Self {
        Self { inner }
    }
}
