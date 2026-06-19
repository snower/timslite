//! Python query iterators backed by safe DataSet query snapshots.

use pyo3::prelude::*;

#[pyclass(name = "QueryIterator")]
pub struct PyQueryIterator {
    rows: std::vec::IntoIter<(i64, Vec<u8>)>,
}

impl PyQueryIterator {
    pub fn new(rows: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

#[pymethods]
impl PyQueryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, Vec<u8>)>> {
        Ok(self.rows.next())
    }

    fn __len__(&self) -> usize {
        self.rows.as_slice().len()
    }

    fn close(&mut self) {
        self.rows = Vec::new().into_iter();
    }
}

#[pyclass(name = "QueryLengthIterator")]
pub struct PyQueryLengthIterator {
    rows: std::vec::IntoIter<(i64, u32)>,
}

impl PyQueryLengthIterator {
    pub fn new(rows: Vec<(i64, u32)>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

#[pymethods]
impl PyQueryLengthIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, u32)>> {
        Ok(self.rows.next())
    }

    fn __len__(&self) -> usize {
        self.rows.as_slice().len()
    }

    fn close(&mut self) {
        self.rows = Vec::new().into_iter();
    }
}
