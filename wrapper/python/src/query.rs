//! Python query iterators backed by lazy DataSet query iterators.

use pyo3::prelude::*;
use std::sync::RwLock;

#[pyclass(name = "QueryIterator")]
pub struct PyQueryIterator {
    iter: RwLock<Option<timslite::QueryIterator>>,
}

impl PyQueryIterator {
    pub fn new(iter: timslite::QueryIterator) -> Self {
        Self { iter: RwLock::new(Some(iter)) }
    }
}

#[pymethods]
impl PyQueryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> PyResult<Option<(i64, Vec<u8>)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => {
                *iter_ref = None;
                Ok(None)
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn reverse(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        {
            let mut iter_ref = slf.iter.write().unwrap();
            if let Some(iter) = iter_ref.take() {
                *iter_ref = Some(iter.reverse());
            }
        }
        Ok(slf)
    }

    fn skip(slf: PyRef<'_, Self>, count: usize) -> PyResult<PyRef<'_, Self>> {
        {
            let mut iter_ref = slf.iter.write().unwrap();
            if let Some(iter) = iter_ref.take() {
                *iter_ref = Some(iter.skip(count));
            }
        }
        Ok(slf)
    }

    fn collect_all(&self) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn collect_take(&self, count: usize) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count) {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn close(&self) {
        let mut iter_ref = self.iter.write().unwrap();
        *iter_ref = None;
    }
}

#[pyclass(name = "QueryLengthIterator")]
pub struct PyQueryLengthIterator {
    iter: RwLock<Option<timslite::QueryLengthIterator>>,
}

impl PyQueryLengthIterator {
    pub fn new(iter: timslite::QueryLengthIterator) -> Self {
        Self { iter: RwLock::new(Some(iter)) }
    }
}

#[pymethods]
impl PyQueryLengthIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> PyResult<Option<(i64, u32)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => {
                *iter_ref = None;
                Ok(None)
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn reverse(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        {
            let mut iter_ref = slf.iter.write().unwrap();
            if let Some(iter) = iter_ref.take() {
                *iter_ref = Some(iter.reverse());
            }
        }
        Ok(slf)
    }

    fn skip(slf: PyRef<'_, Self>, count: usize) -> PyResult<PyRef<'_, Self>> {
        {
            let mut iter_ref = slf.iter.write().unwrap();
            if let Some(iter) = iter_ref.take() {
                *iter_ref = Some(iter.skip(count));
            }
        }
        Ok(slf)
    }

    fn collect_all(&self) -> PyResult<Vec<(i64, u32)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn collect_take(&self, count: usize) -> PyResult<Vec<(i64, u32)>> {
        let mut iter_ref = self.iter.write().unwrap();
        let Some(iter) = iter_ref.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count) {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn close(&self) {
        let mut iter_ref = self.iter.write().unwrap();
        *iter_ref = None;
    }
}
