use pyo3::prelude::*;
use pyo3::{PyNumberProtocol, PyResult};
use serde::Serialize;
use serde_json::Value;

use ext::ResultExt;
use strategy::models::indicator_windowed_model::IndicatorWindowedModel;
use strategy::models::{IndicatorModel, WindowedModel};
use strategy::prelude::Model;

use crate::backtest::PyDb;
use crate::ta::{PyIndicator, TechnicalIndicator};
use crate::windowed_ta::{PyWindowedIndicator, WindowedTechnicalIndicator};
use crate::PyMarketEvent;

#[derive(Clone)]
#[pyclass]
pub(crate) struct PyJsonValue {
    inner: serde_json::Value,
}

impl From<serde_json::Value> for PyJsonValue {
    fn from(inner: Value) -> Self { Self { inner } }
}

impl From<PyJsonValue> for serde_json::Value {
    fn from(p: PyJsonValue) -> Self { p.inner }
}

#[pyproto]
impl PyNumberProtocol for PyJsonValue {
    fn __add__(mut lhs: PyJsonValue, mut rhs: PyJsonValue) -> PyResult<PyJsonValue> {
        match (lhs.inner.as_object_mut(), rhs.inner.as_object_mut()) {
            (None, None) => Ok(serde_json::Value::Null.into()),
            (None, Some(_)) => Ok(rhs),
            (Some(_), None) => Ok(lhs),
            (Some(v1), Some(v2)) => {
                v1.append(v2);
                Ok(serde_json::to_value(v1).unwrap().into())
            }
        }
    }
}

#[pyclass]
pub(crate) struct PyIndicatorModel {
    inner: IndicatorModel<TechnicalIndicator, f64>,
}

#[pymethods]
impl PyIndicatorModel {
    fn next(&mut self, value: f64) -> PyResult<()> {
        // TODO: shouldn't this be a 'ModelError' exception ?
        self.inner
            .update(value)
            .map_err(crate::error::Error::ExecutionError)
            .err_into()
    }

    fn next2(&mut self, event: PyMarketEvent) -> PyResult<()> { self.next(event.vwap()) }

    fn values(&self) -> Option<Vec<f64>> { self.inner.value().map(|ta| ta.values()) }

    fn try_load(&mut self) -> PyResult<()> {
        self.inner
            .try_load()
            .map_err(crate::error::Error::ExecutionError)
            .err_into()
    }

    fn export(&self) -> PyResult<PyJsonValue> { Ok(to_py_json(self.inner.value())) }
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

#[pyclass]
pub(crate) struct PyWindowedIndicatorModel {
    inner: IndicatorWindowedModel<f64, WindowedTechnicalIndicator>,
}

#[pymethods]
impl PyWindowedIndicatorModel {
    fn next(&mut self, value: f64) -> PyResult<()> {
        // TODO: shouldn't this be a 'ModelError' exception ?
        self.inner.push(value);
        Ok(())
    }

    fn next2(&mut self, event: PyMarketEvent) -> PyResult<()> { self.next(event.vwap()) }

    fn update(&mut self) -> PyResult<()> {
        self.inner
            .update()
            .map_err(crate::error::Error::ExecutionError)
            .err_into()
    }

    fn is_filled(&self) -> bool { self.inner.is_filled() }

    fn values(&self) -> Option<Vec<f64>> { self.inner.value().map(|ta| ta.values()) }

    fn try_load(&mut self) -> PyResult<()> {
        self.inner
            .try_load()
            .map_err(crate::error::Error::ExecutionError)
            .err_into()
    }

    fn export(&self) -> PyResult<PyJsonValue> { Ok(to_py_json(self.inner.value())) }
}

impl From<IndicatorWindowedModel<f64, WindowedTechnicalIndicator>> for PyWindowedIndicatorModel {
    fn from(inner: IndicatorWindowedModel<f64, WindowedTechnicalIndicator>) -> Self { Self { inner } }
}

fn to_py_json<T: Serialize>(t: Option<T>) -> PyJsonValue {
    t.map(|m| {
        let ser: serde_json::Value = serde_json::to_value(m).unwrap();
        ser.into()
    })
    .unwrap_or_else(|| serde_json::Value::Null.into())
}

#[doc = "Persist a windowed technical indicator"]
#[pyfunction(text_signature = "(key, db, window_size, init, max_size, /)")]
pub(crate) fn persistent_window_ta(
    key: &str,
    db: PyDb,
    window_size: usize,
    init: PyWindowedIndicator,
    max_size: Option<usize>,
) -> PyWindowedIndicatorModel {
    let indicator: WindowedTechnicalIndicator = init.into();
    IndicatorWindowedModel::new(key, db.db(), window_size, max_size, indicator).into()
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(persistent_ta, m)?)?;
    m.add_function(wrap_pyfunction!(persistent_window_ta, m)?)?;
    Ok(())
}
