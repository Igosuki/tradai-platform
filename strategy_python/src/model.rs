use std::sync::Arc;

use pyo3::prelude::PyModule;
use pyo3::types::{PyList, PyTuple};
use pyo3::PyResult;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use stats::indicators::macd_apo::MACDApo;
use stats::{Close, Next};
use strategy::models::IndicatorModel;
use strategy::prelude::Model;

use crate::backtest::PyDb;
use crate::ta::{PyIndicator, TechnicalIndicator};

#[pyclass]
pub(crate) struct PyIndicatorModel {
    inner: IndicatorModel<TechnicalIndicator, f64>,
}

#[pymethods]
impl PyIndicatorModel {
    fn next(&mut self, value: f64) { self.inner.update(value); }

    fn values(&self) -> Option<Vec<f64>> { self.inner.value().map(|ta| ta.values()) }
}

impl From<IndicatorModel<TechnicalIndicator, f64>> for PyIndicatorModel {
    fn from(inner: IndicatorModel<TechnicalIndicator, f64>) -> Self { Self { inner } }
}

#[doc = "Persist a technical indicator"]
#[pyfunction]
pub(crate) fn persistent_ta(key: &str, db: PyDb, init: PyIndicator) -> PyIndicatorModel {
    let indicator: TechnicalIndicator = init.into();
    IndicatorModel::new(key, db.db(), indicator).into()
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(persistent_ta, m)?)?;
    Ok(())
}
