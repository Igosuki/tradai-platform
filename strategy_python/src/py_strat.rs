use pyo3::exceptions::PyNotImplementedError;
use std::collections::HashSet;

use pyo3::prelude::*;
use pyo3_asyncio::TaskLocals;
use tokio::runtime::Handle;

use ext::ResultExt;
use strategy::coinnect::prelude::MarketEventEnvelope;
use strategy::driver::{DefaultStrategyContext, Strategy};
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
    pub(crate) fn new(inner: PyObject) -> Self { Self { inner } }
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
    ) -> strategy::error::Result<Option<Vec<TradeSignal>>> {
        info!("eval py_strat e = {:?}", e);
        let e: PyMarketEvent = e.clone().into();
        let inner = Python::with_gil(|py| self.inner.to_object(py));
        let locals = Python::with_gil(|py| {
            let asyncio = py.import("asyncio").unwrap();
            let event_loop = asyncio.call_method0("get_event_loop").unwrap_or_else(|_| {
                let event_loop = asyncio.call_method0("new_event_loop").unwrap();
                asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
                event_loop
            });
            TaskLocals::new(PyObject::from(event_loop).as_ref(py))
            //pyo3_asyncio::tokio::get_current_locals(py)
        });
        let event_loop = Python::with_gil(|py| {
            let asyncio = py.import("asyncio").unwrap();
            asyncio
                .call_method0("get_event_loop")
                .unwrap_or_else(|_| {
                    let event_loop = asyncio.call_method0("new_event_loop").unwrap();
                    asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
                    event_loop
                })
                .into_py(py)
        });
        let py_fut_r = pyo3_asyncio::tokio::get_runtime().block_on(async {
            Python::with_gil(|py| {
                let asyncio = py.import("asyncio").unwrap();
                let event_loop = asyncio.call_method0("get_event_loop").unwrap_or_else(|_| {
                    let event_loop = asyncio.call_method0("new_event_loop").unwrap();
                    asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
                    event_loop
                });
                let coro = inner.call_method1(py, "eval", (e,))?;
                pyo3_asyncio::tokio::run_until_complete(event_loop, async move {
                    pyo3_asyncio::tokio::into_future(coro.as_ref(py))?.await
                })
            })
        });

        // let py_fut_r = tokio::task::spawn_blocking(move || {
        //     tokio::task::block_in_place(move || {
        //         Handle::current().block_on(async move {
        //             pyo3_asyncio::tokio::scope_local(locals.clone(), async move {
        //                 Python::with_gil(|py| {
        //                     let py1 = inner.call_method1(py, "eval", (e,))?;
        //                     let coro = py1.as_ref(py);
        //                     pyo3_asyncio::tokio::into_future(coro)
        //                 })?
        //                 .await
        //             })
        //             .await
        //         })
        //     })
        // })
        // .await
        // .unwrap();
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

    use strategy::coinnect::exchange::Exchange;
    use strategy::driver::Strategy;
    use strategy::Channel;

    use crate::util::register_strat_module;
    use crate::PyStrategyWrapper;

    #[test]
    fn test_strat_methods() {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_strat_module(py).unwrap();

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
