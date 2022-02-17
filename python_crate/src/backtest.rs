use actix::SystemRunner;
use std::sync::Arc;
use std::thread;

use backtest::backtest_single;
use backtest::report::BacktestReport;
use chrono::{Date, TimeZone, Utc};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_chrono::NaiveDate;
use pythonize::pythonize;

use brokers::prelude::Exchange;
use strategy::driver::{StratProviderRef, StrategyInitContext};
use strategy_test_util::draw::StrategyEntryFnRef;
use strategy_test_util::it_backtest::{generic_backtest, BacktestRange};
use strategy_test_util::log::StrategyLog;
use trading::position::Position;

use crate::db::PyDb;
use crate::{PyPosition, PyStrategyWrapper};

/// Wraps a python function that returns a list of tuples of (str, f64) for a dict corresponding to [`StrategyLog`]
pub(crate) fn wrap_draw_entry_fn(entry_fn: PyObject) -> StrategyEntryFnRef<StrategyLog, String> {
    Arc::new(move |log| {
        Python::with_gil(|py| {
            let py_val = pythonize(py, log).unwrap();
            let py1 = entry_fn.call1(py, (py_val,)).unwrap();
            let r: Vec<(String, f64)> = py1.extract(py).unwrap();
            r
        })
    })
}

/// Launch an integration backtest with the generic test driver
/// The profider_fn must return a [`PyStrategy`] (or strategy.Strategy in python)
/// draw_entries must be a tuple of name for a figure, and a fn(log) -> (line_name, float).
/// See [`wrap_draw_entry_fn`] for details
#[pyfunction(name = "it_backtest", module = "backtest")]
#[pyo3(text_signature = "(test_name, provider_fn, from, to, /)")]
fn it_backtest_wrapper<'p>(
    py: Python<'p>,
    test_name: &'p PyAny,
    provider_fn: &'p PyAny,
    from: NaiveDate,
    to: NaiveDate,
    draw_entries: Vec<(&'p str, &'p PyAny)>,
) -> PyResult<&'p PyAny> {
    let name: String = test_name.extract()?;
    if !provider_fn.is_callable() {
        return Err(PyErr::new::<PyTypeError, _>("provider_fn must be a function"));
    }
    let provider_fn = Arc::new(provider_fn.to_object(py));
    let from: Date<Utc> = Utc.from_utc_date(&from.0);
    let to: Date<Utc> = Utc.from_utc_date(&to.0);
    let draw_entries: Vec<(String, StrategyEntryFnRef<StrategyLog, String>)> = draw_entries
        .into_iter()
        .map(|entry| {
            let entry_fn = entry.1.to_object(py);
            (entry.0.to_string(), wrap_draw_entry_fn(entry_fn))
        })
        .collect();
    let draw_entries = Arc::new(draw_entries);
    pyo3_asyncio::tokio::future_into_py_with_locals(py, pyo3_asyncio::tokio::get_current_locals(py)?, async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Position>>(1);
        thread::spawn(move || {
            let arc = draw_entries.clone();
            actix_multi_rt().block_on(async move {
                let provider: StratProviderRef = Arc::new(move |ctx: StrategyInitContext| {
                    Python::with_gil(|py| {
                        let py_ctx: PyStrategyInitContext = ctx.into();
                        let o: PyObject = provider_fn.call1(py, (py_ctx,)).unwrap().to_object(py);
                        Box::new(PyStrategyWrapper::new(o))
                    })
                });
                let positions = generic_backtest(
                    &name,
                    provider,
                    arc.as_slice(),
                    &BacktestRange::new(from, to),
                    &[Exchange::Binance],
                    100.0,
                    0.001,
                )
                .await;
                debug!("positions = {:?}", positions);
                tx.send(positions).await.unwrap();
            });
        });
        let positions = rx.recv().await.unwrap_or_default();
        let py_positions: Vec<PyPosition> = positions.into_iter().map(Into::into).collect();
        Python::with_gil(|py| Ok(py_positions.into_py(py)))
    })
}

#[pyclass]
pub(crate) struct PyStrategyInitContext {
    inner: StrategyInitContext,
}

#[pymethods]
impl PyStrategyInitContext {
    #[getter]
    fn db(&self) -> PyResult<PyDb> { Ok(self.inner.db.clone().into()) }
}

impl From<StrategyInitContext> for PyStrategyInitContext {
    fn from(inner: StrategyInitContext) -> Self { Self { inner } }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(it_backtest_wrapper, m)?)?;
    m.add_function(wrap_pyfunction!(backtest_wrapper, m)?)?;
    m.add_class::<PyPosition>()?;
    m.add_class::<PyBacktestReport>()?;
    Ok(())
}

#[pyclass(name = "BacktestReport", module = "backtest", subclass)]
#[derive(Debug)]
pub(crate) struct PyBacktestReport {
    pub(crate) inner: BacktestReport,
}

#[pymethods]
impl PyBacktestReport {
    fn debug(&self) {
        info!("{:?}", self);
    }

    fn draw_tradeview(&self) -> String { self.inner.draw_tradeview() }

    fn draw_report(&self) -> String { self.inner.draw_report() }
}

impl From<PyBacktestReport> for BacktestReport {
    fn from(event: PyBacktestReport) -> BacktestReport { event.inner }
}

impl From<BacktestReport> for PyBacktestReport {
    fn from(e: BacktestReport) -> Self { Self { inner: e } }
}

/// Launch an integration backtest with the generic test driver
/// The profider_fn must return a [`PyStrategy`] (or strategy.Strategy in python)
/// draw_entries must be a tuple of name for a figure, and a fn(log) -> (line_name, float).
/// See [`wrap_draw_entry_fn`] for details
#[pyfunction(name = "single_backtest", module = "backtest")]
#[pyo3(text_signature = "(test_name, provider_fn, from, to, /)")]
fn backtest_wrapper<'p>(
    py: Python<'p>,
    test_name: &'p PyAny,
    provider_fn: &'p PyAny,
    from: NaiveDate,
    to: NaiveDate,
    draw_entries: Vec<(&'p str, &'p PyAny)>,
) -> PyResult<&'p PyAny> {
    let name: String = test_name.extract()?;
    if !provider_fn.is_callable() {
        return Err(PyErr::new::<PyTypeError, _>("provider_fn must be a function"));
    }
    let provider_fn = Arc::new(provider_fn.to_object(py));
    let from: Date<Utc> = Utc.from_utc_date(&from.0);
    let to: Date<Utc> = Utc.from_utc_date(&to.0);
    let draw_entries: Vec<(String, StrategyEntryFnRef<StrategyLog, String>)> = draw_entries
        .into_iter()
        .map(|entry| {
            let entry_fn = entry.1.to_object(py);
            (entry.0.to_string(), wrap_draw_entry_fn(entry_fn))
        })
        .collect();
    let draw_entries = Arc::new(draw_entries);
    pyo3_asyncio::tokio::future_into_py_with_locals(py, pyo3_asyncio::tokio::get_current_locals(py)?, async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BacktestReport>(1);
        thread::spawn(move || {
            actix_multi_rt().block_on(async move {
                let provider: StratProviderRef = Arc::new(move |ctx: StrategyInitContext| {
                    Python::with_gil(|py| {
                        let py_ctx: PyStrategyInitContext = ctx.into();
                        let o: PyObject = provider_fn.call1(py, (py_ctx,)).unwrap().to_object(py);
                        Box::new(PyStrategyWrapper::new(o))
                    })
                });
                let report = backtest_single(
                    &name,
                    provider,
                    &backtest::BacktestRange::new(from, to),
                    &[Exchange::Binance],
                    100.0,
                    0.001,
                )
                .await;
                tx.send(report.unwrap()).await.unwrap();
            });
        });
        let report = rx.recv().await.unwrap();

        let py_report: PyBacktestReport = report.into();
        Python::with_gil(|py| Ok(py_report.into_py(py)))
    })
}

fn actix_multi_rt() -> SystemRunner {
    actix::System::with_tokio_rt(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Default Tokio runtime could not be created.")
    })
}
