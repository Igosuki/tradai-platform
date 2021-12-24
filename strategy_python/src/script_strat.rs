use std::collections::{HashMap, HashSet};

use inline_python::Context;
use pyo3::types::PyModule;
use pyo3::{PyObject, Python};
use serde_json::Value;

use ext::ResultExt;
use strategy::coinnect::prelude::*;
use strategy::driver::{DefaultStrategyContext, Strategy};
use strategy::error::*;
use strategy::models::io::SerializedModel;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::{Channel, StrategyKey};
use trading::signal::TradeSignal;

use crate::json_cannonical::from_json;
use crate::util::register_strat;
use crate::PyMarketEvent;

struct PyScriptStrategy {
    wrapper_key: String,
    context: Context,
}

impl PyScriptStrategy {
    fn new(key: &str, conf: HashMap<String, serde_json::Value>, python_script: String) -> Self {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_strat(py).unwrap();
        Python::with_gil(|py| {
            PyModule::from_code(py, &python_script, "yourstrat.py", "yourstrat")?;
            py.run(r#"from yourstrat import Strat; strat = Strat({})"#, None, None)
        })
        .map_err(|e| {
            e.print_and_set_sys_last_vars(py);
            e
        })
        .unwrap();
        let py_conf = from_json(py, conf).unwrap();
        context.run_with_gil(py, python! {
            from yourstrat import Strat
            strat = Strat('py_conf)
        });
        Self {
            context,
            wrapper_key: key.to_string(),
        }
    }

    fn strat(&self) -> PyObject {
        //Python::with_gil(|py| py.eval("strat", None, None).unwrap().into_py(py))
        self.context.get("strat")
    }
}

#[async_trait]
impl Strategy for PyScriptStrategy {
    fn key(&self) -> String {
        let inner = self.strat();
        let inner_key = Python::with_gil(|py| inner.call_method0(py, "key"))
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "python".to_string());
        format!("{}_{}", self.wrapper_key, inner_key)
    }

    fn init(&mut self) -> strategy::error::Result<()> {
        let inner = self.strat();
        Python::with_gil(|py| inner.call_method0(py, "init"))
            .map(|_| ())
            .err_into()
    }

    async fn eval(
        &mut self,
        e: &MarketEventEnvelope,
        _ctx: &DefaultStrategyContext,
    ) -> strategy::error::Result<Option<Vec<TradeSignal>>> {
        let inner = self.strat();
        let val = Python::with_gil(|py| {
            let py_event: PyMarketEvent = e.clone().into();
            inner.call_method1(py, "eval", (py_event,))
        });
        eprintln!("val = {:?}", val);
        val.map(|_v| Some(vec![])).err_into()
    }

    fn model(&self) -> SerializedModel { vec![] }

    fn channels(&self) -> HashSet<Channel> { Default::default() }
}

#[derive(Serialize, Deserialize)]
pub struct PyScriptStrategyOptions {
    conf: HashMap<String, serde_json::Value>,
    script_path: String,
    key: String,
}

impl StrategySettingsReplicator for PyScriptStrategyOptions {
    fn replicate_for_pairs(&self, _pairs: HashSet<Pair>) -> Vec<Value> { vec![] }
}

impl StrategyOptions for PyScriptStrategyOptions {
    fn key(&self) -> StrategyKey { StrategyKey("python".to_string(), self.key.clone()) }
}

pub fn provide_python_script_strat(
    name: &str,
    _ctx: StrategyPluginContext,
    conf: serde_json::Value,
) -> Result<Box<dyn Strategy>> {
    let options: PyScriptStrategyOptions = serde_json::from_value(conf)?;
    Ok(Box::new(PyScriptStrategy::new(name, options.conf, options.script_path)))
}

inventory::submit! {
    StrategyPlugin::new("python_script", provide_options::<PyScriptStrategyOptions>, provide_python_script_strat)
}

#[cfg(test)]
mod test {
    use pyo3::Python;

    use strategy::driver::Strategy;

    use crate::script_strat::PyScriptStrategy;
    use crate::test_util::fixtures::default_order_book_event;

    fn python_script(name: &str) -> std::io::Result<String> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/python_scripts/{}.py", manifest_dir, name);
        std::fs::read_to_string(path)
    }

    #[test]
    fn test_python_from_python() {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategy::new("default_wrapper", Default::default(), python_script);
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
    async fn test_python_baseline_impl() -> strategy::error::Result<()> {
        let python_script = python_script("calls").unwrap();
        let mut strat_wrapper = PyScriptStrategy::new("default_wrapper", Default::default(), python_script);
        strat_wrapper.init()?;
        let _event = default_order_book_event();
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
        let strat_wrapper = PyScriptStrategy::new("default_wrapper", Default::default(), python_script);
        let strat = strat_wrapper.strat();
        let r = Python::with_gil(|py| strat.call_method0(py, "init"));
        assert!(r.is_ok())
    }
}
