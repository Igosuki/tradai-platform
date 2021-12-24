use pyo3::prelude::PyModule;
use pyo3::PyResult;

use ext::ResultExt;
use strategy::models::{IndicatorModel, PersistentWindowedModel};
use strategy::prelude::Model;

use crate::backtest::PyDb;
use crate::ta::{PyIndicator, TechnicalIndicator};

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

// #[doc = "Persist a windowed technical indicator"]
// #[pyfunction]
// pub(crate) fn persistent_window_ta(key: &str, db: PyDb, window_size: usize, init: PyIndicator) -> PyIndicatorModel {
//     let indicator: TechnicalIndicator = init.into();
//     window_size: usize,
//     max_size_o: Option<usize>,
//     window_fn: WindowFn<T, M>,
//     init: Option<M>,
//     PersistentWindowedModel::new(key, db.db(), indicator).into()
// }

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(persistent_ta, m)?)?;
    Ok(())
}
