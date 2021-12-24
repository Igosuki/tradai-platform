use std::sync::Arc;
use std::thread;

use chrono::{Date, TimeZone, Utc};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_chrono::NaiveDate;

use db::Storage;
use strategy::coinnect::prelude::Exchange;
use strategy_test_util::it_backtest::{generic_backtest, BacktestRange, BacktestStratProviderRef, GenericTestContext};
use trading::position::Position;

use crate::{PyPosition, PyStrategyWrapper};

/// Launch an integration backtest with the generic test driver
/// The profider_fn must return a [`PyStrategy`] (or strategy.Strategy in python)
#[pyfunction(name = "it_backtest", module = "backtest")]
#[pyo3(text_signature = "(test_name, provider_fn, from, to, /)")]
fn it_backtest_wrapper<'p>(
    py: Python<'p>,
    test_name: &'p PyAny,
    provider_fn: &'p PyAny,
    from: NaiveDate,
    to: NaiveDate,
) -> PyResult<&'p PyAny> {
    let name: String = test_name.extract()?;
    if !provider_fn.is_callable() {
        return Err(PyErr::new::<PyTypeError, _>("provider_fn must be a function"));
    }
    let provider_fn = Arc::new(provider_fn.to_object(py));
    let from: Date<Utc> = Utc.from_utc_date(&from.0);
    let to: Date<Utc> = Utc.from_utc_date(&to.0);

    pyo3_asyncio::tokio::future_into_py_with_locals(py, pyo3_asyncio::tokio::get_current_locals(py)?, async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Position>>(1);
        thread::spawn(move || {
            actix::System::new().block_on(async move {
                let provider: BacktestStratProviderRef = Arc::new(move |ctx: GenericTestContext| {
                    Python::with_gil(|py| {
                        let py_ctx: PyGenericTestContext = ctx.into();
                        let o: PyObject = provider_fn.call1(py, (py_ctx,)).unwrap().to_object(py);
                        Box::new(PyStrategyWrapper::new(o))
                    })
                });
                let positions = generic_backtest(
                    &name,
                    provider,
                    &[],
                    &BacktestRange::new(from, to),
                    &[Exchange::Binance],
                    100.0,
                    0.001,
                )
                .await;
                debug!("positions = {:?}", positions);
                tx.send(positions).await.unwrap();
            })
        });
        let positions = rx.recv().await.unwrap_or_default();
        let py_positions: Vec<PyPosition> = positions.into_iter().map(|v| v.into()).collect();
        Python::with_gil(|py| Ok(py_positions.into_py(py)))
    })
}

#[pyclass]
pub(crate) struct PyGenericTestContext {
    inner: GenericTestContext,
}

#[pymethods]
impl PyGenericTestContext {
    #[getter]
    fn db(&self) -> PyResult<PyDb> { Ok(self.inner.db.clone().into()) }
}

impl From<GenericTestContext> for PyGenericTestContext {
    fn from(inner: GenericTestContext) -> Self { Self { inner } }
}

#[derive(Clone)]
#[pyclass(name = "Storage")]
pub(crate) struct PyDb {
    inner: Arc<dyn Storage>,
}

impl PyDb {
    pub(crate) fn db(&self) -> Arc<dyn Storage> { self.inner.clone() }
}

impl From<Arc<dyn Storage>> for PyDb {
    fn from(inner: Arc<dyn Storage>) -> Self { Self { inner } }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(it_backtest_wrapper, m)?)?;
    Ok(())
}
