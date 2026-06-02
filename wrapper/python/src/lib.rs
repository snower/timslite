//! timslite — High-performance time-series data storage for Python.
//!
//! Rust bindings via PyO3. Thin wrapper, Pythonic API.

mod config;
mod dataset;
mod exceptions;
mod query;
mod queue;
mod store;

use pyo3::prelude::*;

#[pymodule]
fn timslite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register exception hierarchy
    exceptions::register(m)?;

    // Register Python classes
    m.add_class::<store::PyStore>()?;
    m.add_class::<config::PyStoreConfig>()?;
    m.add_class::<dataset::PyDataset>()?;
    m.add_class::<query::PyQueryIterator>()?;
    m.add_class::<queue::PyDatasetQueue>()?;
    m.add_class::<queue::PyDatasetQueueConsumer>()?;

    Ok(())
}
