use std::collections::HashSet;

use pyo3::prelude::*;

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

    fn init(&mut self) -> PyResult<()> { unimplemented!() }

    fn eval(&mut self, _e: PyObject) -> PyResult<Vec<PyTradeSignal>> { unimplemented!() }

    fn model(&self) -> PyResult<Vec<(&str, Option<PyObject>)>> { unimplemented!() }

    fn channels(&self) -> PyResult<Vec<PyChannel>> { unimplemented!() }
}

#[pyclass(name = "StrategyWrapper", module = "strategy")]
pub(crate) struct PyStrategyWrapper {
    inner: PyObject,
}

impl ToPyObject for PyStrategyWrapper {
    fn to_object(&self, py: Python) -> PyObject { self.inner.to_object(py) }
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

#[pymethods]
impl PyStrategyWrapper {
    #[new]
    pub(crate) fn new(inner: PyObject) -> Self { Self { inner } }
}

#[async_trait]
impl Strategy for PyStrategyWrapper {
    fn key(&self) -> String {
        self.with_strat(|inner| inner.call_method0("whoami").and_then(|v| v.extract()))
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
        self.with_strat(|inner| {
            let e: PyMarketEvent = e.clone().into();
            inner.call_method1("eval", (e,)).and_then(|signals| {
                if signals.is_none() {
                    Ok(None)
                } else {
                    let signals: Vec<PyTradeSignal> = signals.extract()?;
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
                .and_then(|v| v.extract())
                .unwrap_or_else(|_| serde_json::Value::Null.into())
        });
        let inner: serde_json::Value = exported.into();
        inner
            .as_object()
            .map(|v| v.into_iter().map(|(k, v)| (k.clone(), Some(v.clone()))).collect())
            .unwrap_or_else(Vec::new)
    }

    fn channels(&self) -> HashSet<Channel> {
        self.with_strat(|inner| {
            inner
                .call_method0("channels")
                .and_then(|v| v.extract())
                .unwrap_or_else(|_| vec![])
        })
        .into_iter()
        .map(|sc: PyChannel| sc.into())
        .collect()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use inline_python::Context;
    use pyo3::{PyObject, Python};

    use strategy::coinnect::exchange::Exchange;
    use strategy::driver::Strategy;
    use strategy::Channel;

    use crate::json_cannonical::from_json;
    use crate::util::register_strat_module;
    use crate::PyStrategyWrapper;

    #[test]
    fn test_strat_methods() {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_strat_module(py).unwrap();
        let conf: HashMap<String, serde_json::Value> = Default::default();
        let py_conf = from_json(py, conf).unwrap();

        context.run_with_gil(py, python! {
            from strategy import Strategy, Channel
            class MyStrat(Strategy):
                def __new__(cls, conf):
                    dis = super().__new__(cls, conf)
                    dis.conf = conf
                    return dis

                def channels(self):
                    return (Channel(), Channel(),)

            strat = MyStrat('py_conf)

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
        )
    }
}
