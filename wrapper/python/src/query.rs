//! Python query iterators backed by safe DataSet query snapshots.

use pyo3::prelude::*;

#[pyclass(name = "QueryIterator")]
pub struct PyQueryIterator {
    rows: Vec<(i64, Vec<u8>)>,
    position: usize,
}

impl PyQueryIterator {
    pub fn new(rows: Vec<(i64, Vec<u8>)>) -> Self {
        Self { rows, position: 0 }
    }
}

#[pymethods]
impl PyQueryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, Vec<u8>)>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let entry = self.rows[self.position].clone();
        self.position += 1;
        Ok(Some(entry))
    }

    fn __len__(&self) -> usize {
        self.rows.len() - self.position
    }

    fn reverse(&mut self) {
        self.rows.reverse();
        self.position = 0;
    }

    fn skip(&mut self, count: usize) {
        self.position = (self.position + count).min(self.rows.len());
    }

    fn collect_all(&mut self) -> Vec<(i64, Vec<u8>)> {
        let result = self.rows[self.position..].to_vec();
        self.position = self.rows.len();
        result
    }

    fn collect_take(&mut self, count: usize) -> Vec<(i64, Vec<u8>)> {
        let end = (self.position + count).min(self.rows.len());
        let result = self.rows[self.position..end].to_vec();
        self.position = end;
        result
    }

    fn close(&mut self) {
        self.rows.clear();
        self.position = 0;
    }
}

#[pyclass(name = "QueryLengthIterator")]
pub struct PyQueryLengthIterator {
    rows: Vec<(i64, u32)>,
    position: usize,
}

impl PyQueryLengthIterator {
    pub fn new(rows: Vec<(i64, u32)>) -> Self {
        Self { rows, position: 0 }
    }
}

#[pymethods]
impl PyQueryLengthIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<(i64, u32)>> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let entry = self.rows[self.position];
        self.position += 1;
        Ok(Some(entry))
    }

    fn __len__(&self) -> usize {
        self.rows.len() - self.position
    }

    fn reverse(&mut self) {
        self.rows.reverse();
        self.position = 0;
    }

    fn skip(&mut self, count: usize) {
        self.position = (self.position + count).min(self.rows.len());
    }

    fn collect_all(&mut self) -> Vec<(i64, u32)> {
        let result = self.rows[self.position..].to_vec();
        self.position = self.rows.len();
        result
    }

    fn collect_take(&mut self, count: usize) -> Vec<(i64, u32)> {
        let end = (self.position + count).min(self.rows.len());
        let result = self.rows[self.position..end].to_vec();
        self.position = end;
        result
    }

    fn close(&mut self) {
        self.rows.clear();
        self.position = 0;
    }
}
