use std::collections::{HashMap, HashSet};

use inline_python::Context;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3::{wrap_pymodule, Python};
use serde_json::Value;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::prelude::{MarketEventEnvelope, Pair};
use ext::ResultExt;
use trading::python_impls::types::PyMarketEventEnvelope;
use trading::signal::TradeSignal;

use crate::driver::{DefaultStrategyContext, Strategy};
use crate::error::Result;
use crate::plugin::{provide_options, StrategyPluginContext};
use crate::settings::{StrategyOptions, StrategySettingsReplicator};
use crate::{Channel, StrategyKey, StrategyPlugin};

#[pyclass(subclass)]
struct PythonStrat {}

#[pyclass]
struct SubscriptionChannel {
    source: String,
    exchange: String,
    pair: String,
}

impl From<SubscriptionChannel> for Channel {
    fn from(sc: SubscriptionChannel) -> Self {
        match sc.source.as_str() {
            "orderbooks" => Channel::Orderbooks {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            "trades" => Channel::Trades {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            "orders" => Channel::Orders {
                xch: Exchange::from(sc.exchange),
                pair: sc.pair.into(),
            },
            _ => unimplemented!(),
        }
    }
}

#[pymethods]
impl PythonStrat {
    #[new]
    fn new(_conf: PyObject) -> Self { Self {} }

    fn whoami(&self) -> PyResult<&'static str> { Ok("PythonStrat") }

    fn init(&mut self) -> PyResult<()> { unimplemented!() }

    fn eval(&mut self, _e: PyObject) -> PyResult<Vec<TradeSignal>> { unimplemented!() }

    fn update_model(&mut self, _e: PyObject) -> PyResult<()> { unimplemented!() }

    fn models(&self) -> PyResult<Vec<(&str, Option<PyObject>)>> { unimplemented!() }

    fn channels(&self) -> PyResult<Vec<SubscriptionChannel>> { unimplemented!() }
}

struct PythonStratWrapper {
    context: Context,
}

create_exception!(strat, ModelError, pyo3::exceptions::PyException);
create_exception!(strat, EvalError, pyo3::exceptions::PyException);

#[pymodule]
#[pyo3(name = "strat")]
pub fn strat(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PythonStrat>()?;
    m.add_class::<TradeSignal>()?;
    m.add("ModelError", py.get_type::<ModelError>())?;
    m.add("EvalError", py.get_type::<EvalError>())?;
    Ok(())
}

impl PythonStratWrapper {
    fn new(_key: &str, _conf: HashMap<String, serde_json::Value>, python_script: String) -> Self {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        let m = wrap_pymodule!(strat)(py);
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("m", m)?;
            py.run(r#"import sys; sys.modules["strategy"] = m"#, None, Some(dict))?;
            PyModule::from_code(py, &python_script, "yourstrat.py", "yourstrat")?;
            py.run(r#"from yourstrat import Strat; strat = Strat({})"#, None, None)
        })
        .map_err(|e| {
            e.print_and_set_sys_last_vars(py);
            e
        })
        .unwrap();
        //let py_conf = python::from_json(py, conf).unwrap();
        // context.run_with_gil(py, python! {
        //     from yourstrat import Strat
        //     strat = Strat('py_conf)
        // });
        // let class = module.getattr("Strat").unwrap();
        // let instance = class.call1((py_conf,));
        Self { context }
    }

    fn strat(&self) -> PyObject {
        Python::with_gil(|py| py.eval("strat", None, None).unwrap().into_py(py))
        //self.context.get("strat") }
    }
}

#[async_trait]
impl Strategy for PythonStratWrapper {
    fn key(&self) -> String {
        let inner = self.strat();
        Python::with_gil(|py| inner.call_method0(py, "key"))
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "python".to_string())
    }

    fn init(&mut self) -> crate::error::Result<()> {
        let inner = self.strat();
        Python::with_gil(|py| inner.call_method0(py, "init"))
            .map(|_| ())
            .err_into()
    }

    async fn eval(
        &mut self,
        e: &MarketEventEnvelope,
        _ctx: &DefaultStrategyContext,
    ) -> crate::error::Result<Option<Vec<TradeSignal>>> {
        let inner = self.strat();
        let val = Python::with_gil(|py| {
            let py_event = PyMarketEventEnvelope::from(e);
            inner.call_method1(py, "eval", (py_event,))
        });
        eprintln!("val = {:?}", val);
        val.map(|_v| Some(vec![])).err_into()
    }

    async fn update_model(&mut self, e: &MarketEventEnvelope) -> crate::error::Result<()> {
        let inner = self.strat();
        let val = Python::with_gil(|py| {
            let py_event = PyMarketEventEnvelope::from(e);
            inner.call_method1(py, "update_model", (py_event,))
        });
        eprintln!("val = {:?}", val);
        val.map(|_| ()).err_into()
    }

    fn model(&self) -> Vec<(String, Option<Value>)> { vec![] }

    fn channels(&self) -> HashSet<Channel> { Default::default() }
}

#[derive(Serialize, Deserialize)]
pub struct PythonStratWrapperOptions {
    conf: HashMap<String, serde_json::Value>,
    script_path: String,
    key: String,
}

impl StrategySettingsReplicator for PythonStratWrapperOptions {
    fn replicate_for_pairs(&self, _pairs: HashSet<Pair>) -> Vec<Value> { vec![] }
}

impl StrategyOptions for PythonStratWrapperOptions {
    fn key(&self) -> StrategyKey { StrategyKey("python".to_string(), self.key.clone()) }
}

pub fn provide_python_strat(
    name: &str,
    _ctx: StrategyPluginContext,
    conf: serde_json::Value,
) -> Result<Box<dyn Strategy>> {
    let options: PythonStratWrapperOptions = serde_json::from_value(conf)?;
    Ok(Box::new(PythonStratWrapper::new(
        name,
        options.conf,
        options.script_path,
    )))
}

inventory::submit! {
    StrategyPlugin::new("python", provide_options::<PythonStratWrapperOptions>, provide_python_strat)
}

#[cfg(test)]
mod test {
    use pyo3::Python;

    use crate::driver::Strategy;
    use crate::python_wrapper::python_strat::PythonStratWrapper;
    use crate::test_util::fixtures::default_order_book_event;

    fn python_script(name: &str) -> std::io::Result<String> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/python_scripts/{}.py", manifest_dir, name);
        std::fs::read_to_string(path)
    }

    #[test]
    fn test_python_from_python() {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PythonStratWrapper::new("default_wrapper", Default::default(), python_script);
        let strat = strat_wrapper.strat();
        let whoami: String = Python::with_gil(|py| {
            let whoami = strat.call_method0(py, "whoami");
            whoami.unwrap().extract(py).unwrap()
        });
        assert_eq!(whoami, "PythonStrat");
        strat_wrapper.context.run(python! {
            'strat.init()
            'strat.update_model({})
            'strat.eval({})
            'strat.models()
            'strat.channels()
            import sys; sys.stdout.flush()
        })
    }

    #[tokio::test]
    async fn test_python_baseline_impl() -> crate::error::Result<()> {
        let python_script = python_script("calls").unwrap();
        let mut strat_wrapper = PythonStratWrapper::new("default_wrapper", Default::default(), python_script);
        strat_wrapper.init()?;
        let event = default_order_book_event();
        strat_wrapper.update_model(&event).await?;
        // TODO: context should be like ActorContext
        //strat_wrapper.eval(&event).await?;
        strat_wrapper.model();
        strat_wrapper.channels();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_fail_unimplemented() {
        let python_script = python_script("unimplemented").unwrap();
        let strat_wrapper = PythonStratWrapper::new("default_wrapper", Default::default(), python_script);
        let strat = strat_wrapper.strat();
        let r = Python::with_gil(|py| strat.call_method0(py, "init"));
        assert!(r.is_ok())
    }
}
