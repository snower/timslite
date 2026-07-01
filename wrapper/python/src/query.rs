//! Python query iterators backed by lazy DataSet query iterators.

use pyo3::prelude::*;

#[pyclass(name = "QueryIterator")]
pub struct PyQueryIterator {
    iter: Option<timslite::QueryIterator>,
}

impl PyQueryIterator {
    pub fn new(iter: timslite::QueryIterator) -> Self {
        Self { iter: Some(iter) }
    }
}

#[pymethods]
impl PyQueryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, Vec<u8>)>> {
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => {
                self.iter = None;
                Ok(None)
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn reverse(&mut self) -> PyResult<()> {
        let Some(iter) = self.iter.take() else {
            return Ok(());
        };
        self.iter = Some(iter.reverse());
        Ok(())
    }

    fn skip(&mut self, count: usize) -> PyResult<()> {
        let Some(iter) = self.iter.take() else {
            return Ok(());
        };
        self.iter = Some(iter.skip(count));
        Ok(())
    }

    fn collect_all(&mut self) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn collect_take(&mut self, count: usize) -> PyResult<Vec<(i64, Vec<u8>)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count) {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn close(&mut self) {
        self.iter = None;
    }
}

#[pyclass(name = "QueryLengthIterator")]
pub struct PyQueryLengthIterator {
    iter: Option<timslite::QueryLengthIterator>,
}

impl PyQueryLengthIterator {
    pub fn new(iter: timslite::QueryLengthIterator) -> Self {
        Self { iter: Some(iter) }
    }
}

#[pymethods]
impl PyQueryLengthIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, u32)>> {
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        match iter.next_entry() {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => {
                self.iter = None;
                Ok(None)
            }
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn reverse(&mut self) -> PyResult<()> {
        let Some(iter) = self.iter.take() else {
            return Ok(());
        };
        self.iter = Some(iter.reverse());
        Ok(())
    }

    fn skip(&mut self, count: usize) -> PyResult<()> {
        let Some(iter) = self.iter.take() else {
            return Ok(());
        };
        self.iter = Some(iter.skip(count));
        Ok(())
    }

    fn collect_all(&mut self) -> PyResult<Vec<(i64, u32)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_all() {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn collect_take(&mut self, count: usize) -> PyResult<Vec<(i64, u32)>> {
        let Some(iter) = self.iter.take() else {
            return Ok(Vec::new());
        };
        match iter.collect_take(count) {
            Ok(records) => Ok(records),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn close(&mut self) {
        self.iter = None;
    }
}
