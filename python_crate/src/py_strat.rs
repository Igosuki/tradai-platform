use pyo3::exceptions::PyNotImplementedError;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::asyncio::get_event_loop;
use brokers::prelude::MarketEventEnvelope;
use ext::ResultExt;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::models::io::SerializedModel;
use strategy::Channel;
use trading::position::OperationKind;
use trading::signal::TradeSignal;

use crate::channel::PyChannel;
use crate::model::PyJsonValue;
use crate::trading::PyTradeSignal;
use crate::PyMarketEvent;

#[pyclass(name = "DefaultStrategyContext")]
pub(crate) struct PyStrategyContext {}

#[pyclass(name = "Strategy", module = "strategy", subclass)]
#[derive(Clone)]
pub(crate) struct PyStrategy {}

#[pymethods]
impl PyStrategy {
    #[new]
    fn new(_conf: PyObject) -> Self { Self {} }

    fn whoami(&self) -> PyResult<&'static str> { Ok("PythonStrat") }

    fn init(&mut self) -> PyResult<()> { Err(PyNotImplementedError::new_err("init")) }

    /// This is an async function
    fn eval(&mut self, _e: PyObject) -> PyResult<Vec<PyTradeSignal>> { Err(PyNotImplementedError::new_err("eval")) }

    fn model(&self) -> PyResult<Vec<(&str, Option<PyObject>)>> { Err(PyNotImplementedError::new_err("model")) }

    fn channels(&self) -> PyResult<Vec<PyChannel>> { Err(PyNotImplementedError::new_err("channels")) }
}

#[pyclass(name = "StrategyWrapper", module = "strategy")]
pub(crate) struct PyStrategyWrapper {
    event_loop_hdl: PyObject,
    inner: PyObject,
}

impl PyStrategyWrapper {
    pub(crate) fn with_strat<F, T>(&self, f: F) -> T
    where
        F: Fn(&PyAny) -> T,
    {
        Python::with_gil(|py| {
            let py_strat = self.inner.as_ref(py);
            f(py_strat)
        })
    }
}

impl ToPyObject for PyStrategyWrapper {
    fn to_object(&self, py: Python) -> PyObject { self.inner.to_object(py) }
}

#[pymethods]
impl PyStrategyWrapper {
    #[new]
    pub(crate) fn new(inner: PyObject) -> Self {
        Self {
            inner,
            event_loop_hdl: get_event_loop(),
        }
    }
}

#[async_trait]
impl Strategy for PyStrategyWrapper {
    fn key(&self) -> String {
        self.with_strat(|inner| inner.call_method0("whoami").and_then(PyAny::extract))
            .expect("expected a string")
    }

    fn init(&mut self) -> strategy::error::Result<()> {
        self.with_strat(|inner| inner.call_method0("init").map(|_| ()))
            .err_into()
    }

    async fn eval(
        &mut self,
        e: &MarketEventEnvelope,
        ctx: &DefaultStrategyContext,
    ) -> strategy::error::Result<Option<TradeSignals>> {
        let e: PyMarketEvent = e.clone().into();
        let inner = Python::with_gil(|py| self.inner.to_object(py));
        let event_loop_hdl = self.event_loop_hdl.clone();
        let py_fut_r = pyo3_asyncio::tokio::get_runtime()
            .spawn_blocking(move || {
                Python::with_gil(|py| {
                    let coro = inner.call_method1(py, "eval", (e,))?;
                    pyo3_asyncio::tokio::run_until_complete(event_loop_hdl.as_ref(py), async move {
                        Python::with_gil(|py| pyo3_asyncio::tokio::into_future(coro.as_ref(py)))?.await
                    })
                })
            })
            .await
            .unwrap();
        Python::with_gil(|py| {
            py_fut_r.and_then(|signals| {
                if signals.is_none(py) {
                    Ok(None)
                } else {
                    let signals: Vec<PyTradeSignal> = signals.extract(py)?;
                    let tss = signals
                        .into_iter()
                        .filter_map(|s| {
                            let ts: TradeSignal = s.into();
                            match ctx.portfolio.open_position(ts.exchange, ts.pair.clone()) {
                                // Open
                                None if ts.op_kind == OperationKind::Open => Some(ts),
                                // Close
                                Some(pos) if ts.op_kind == OperationKind::Close && ts.pos_kind == pos.kind => Some(ts),
                                _ => None,
                            }
                        })
                        .collect();
                    Ok(Some(tss))
                }
            })
        })
        .err_into()
    }

    fn model(&self) -> SerializedModel {
        let exported: PyJsonValue = self.with_strat(|inner| {
            inner
                .call_method0("models")
                .and_then(PyAny::extract)
                .unwrap_or_else(|_| serde_json::Value::Null.into())
        });
        let inner: serde_json::Value = exported.into();
        inner.as_object().map_or_else(Vec::new, |v| {
            v.into_iter().map(|(k, v)| (k.clone(), Some(v.clone()))).collect()
        })
    }

    fn channels(&self) -> HashSet<Channel> {
        self.with_strat(|inner| {
            inner
                .call_method0("channels")
                .and_then(PyAny::extract)
                .unwrap_or_else(|_| Vec::<PyChannel>::new())
        })
        .into_iter()
        .map(Into::<Channel>::into)
        .collect()
    }
}

#[cfg(test)]
mod test {
    use inline_python::Context;
    use pyo3::{PyObject, Python};

    use brokers::exchange::Exchange;
    use strategy::driver::Strategy;
    use strategy::Channel;

    use crate::util::register_tradai_module;
    use crate::PyStrategyWrapper;

    #[test]
    fn test_strat_methods() {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_tradai_module(py).unwrap();

        context.run_with_gil(py, python! {
            from strategy import Strategy, Channel
            class MyStrat(Strategy):
                def __new__(cls, conf):
                    dis = super().__new__(cls, conf)
                    dis.conf = conf
                    return dis

                def channels(self):
                    return (Channel(), Channel(),)

            strat = MyStrat({})

            channels = strat.channels()
        });

        let strat: PyObject = context.get("strat");
        let wrapper = PyStrategyWrapper::new(strat);
        let channels = Strategy::channels(&wrapper);
        assert!(!channels.is_empty());
        assert_eq!(
            channels.iter().last(),
            Some(&Channel::Orderbooks {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into()
            })
        );
    }
}
