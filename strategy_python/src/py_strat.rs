use std::collections::HashSet;

use pyo3::prelude::*;

use strategy::coinnect::prelude::MarketEventEnvelope;
use strategy::driver::{DefaultStrategyContext, Strategy};
use strategy::models::io::SerializedModel;
use strategy::Channel;
use trading::signal::TradeSignal;

use crate::channel::PyChannel;
use crate::trading::PyTradeSignal;

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
    inner: Py<PyAny>,
}

#[pymethods]
impl PyStrategyWrapper {
    #[new]
    fn new(inner: Py<PyAny>) -> Self { Self { inner } }
}

#[async_trait]
impl Strategy for PyStrategyWrapper {
    fn key(&self) -> String { todo!() }

    fn init(&mut self) -> strategy::error::Result<()> { todo!() }

    async fn eval(
        &mut self,
        _e: &MarketEventEnvelope,
        _ctx: &DefaultStrategyContext,
    ) -> strategy::error::Result<Option<Vec<TradeSignal>>> {
        todo!()
    }

    fn model(&self) -> SerializedModel { todo!() }

    fn channels(&self) -> HashSet<Channel> {
        Python::with_gil(|py| {
            let py_strat = self.inner.as_ref(py);
            py_strat
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
    use crate::util::register_strat;
    use crate::PyStrategyWrapper;

    #[test]
    fn test_strat_methods() {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_strat(py).unwrap();
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
