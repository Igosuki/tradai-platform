use std::sync::Arc;

use chrono::{Date, TimeZone, Utc};
use pyo3::prelude::*;
use pyo3_chrono::NaiveDate;

use db::Storage;
use strategy_test_util::it_backtest::GenericTestContext;

#[pyfunction(name = "it_backtest", module = "backtest")]
fn it_backtest_wrapper<'p>(
    py: Python<'p>,
    test_name: &'p PyAny,
    provider: &'p PyAny,
    from: NaiveDate,
    to: NaiveDate,
) -> PyResult<&'p PyAny> {
    let name: String = test_name.extract()?;
    //let provider: BacktestStratProvider = provider.extract()?;
    pyo3_asyncio::tokio::future_into_py_with_locals(py, pyo3_asyncio::tokio::get_current_locals(py)?, async move {
        // let provider: BacktestStratProvider = |ctx: GenericTestContext| {
        //     let exchange = Exchange::Binance;
        //     let conf = Options::new_test_default(PAIR, exchange);
        //     Box::new(MeanRevertingStrategy::new(
        //         ctx.db,
        //         "mean_reverting_test".to_string(),
        //         &conf,
        //         ctx.engine,
        //         None,
        //     ))
        // };
        let from: Date<Utc> = Utc.from_utc_date(&from.0);
        let to: Date<Utc> = Utc.from_utc_date(&to.0);
        // let positions = generic_backtest(
        //     &name,
        //     move |ctx: GenericTestContext| {
        //         let py_any: &PyAny = provider.call1((ctx,)).unwrap();
        //         py_any.extract().unwrap()
        //     },
        //     &[],
        //     &BacktestRange::new(from, to),
        //     &[Exchange::Binance],
        //     100.0,
        //     0.001,
        // )
        // .await;
        Python::with_gil(|py| Ok(py.None()))
    })
}

#[pyclass]
pub(crate) struct PyGenericTestContext {
    inner: GenericTestContext,
}

#[pyclass(name = "Storage")]
pub(crate) struct PyDb {
    inner: Arc<dyn Storage>,
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(it_backtest_wrapper, m)?)?;
    Ok(())
}
