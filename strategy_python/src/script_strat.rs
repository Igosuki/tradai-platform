use std::collections::{HashMap, HashSet};

use inline_python::Context;
use pyo3::types::{PyDict, PyModule};
use pyo3::{IntoPy, PyObject, PyResult, Python};
use serde_json::Value;

use strategy::coinnect::prelude::*;
use strategy::driver::Strategy;
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::StrategyKey;
use strategy_test_util::it_backtest::GenericTestContext;

use crate::backtest::PyGenericTestContext;
use crate::json_cannonical::from_json;
use crate::util::register_strat_module;
use crate::PyStrategyWrapper;

struct PyScriptStrategyProvider {
    context: Context,
}

impl PyScriptStrategyProvider {
    fn new(ctx: StrategyPluginContext, conf: HashMap<String, serde_json::Value>, python_script: String) -> Self {
        Self::try_new(ctx, conf, python_script).unwrap()
    }

    fn try_new(
        ctx: StrategyPluginContext,
        conf: HashMap<String, serde_json::Value>,
        python_script: String,
    ) -> Result<Self> {
        let guard = Python::acquire_gil();
        let py = guard.python();
        let context = Context::new_with_gil(py);
        register_strat_module(py).unwrap();
        PyModule::from_code(py, &python_script, "_dyn_strat_mod.py", "_dyn_strat_mod")?;
        py.run(r#"print(f"loaded strategy class {type(Strat).__name__}")"#, None, None)
            .map_err(|e| {
                e.print_and_set_sys_last_vars(py);
                e
            })
            .unwrap();
        let py_conf = from_json(py, conf).unwrap();
        let driver_ctx: PyGenericTestContext = GenericTestContext {
            engine: ctx.engine.clone(),
            db: ctx.db.clone(),
        }
        .into();
        let py_ctx = driver_ctx.into_py(py);
        context.run_with_gil(py, python! {
            from _dyn_strat_mod import Strat
            strat = Strat('py_conf, 'py_ctx)
        });
        Ok(Self { context })
    }

    fn wrapped(&self) -> PyStrategyWrapper { PyStrategyWrapper::new(self.context.get("strat")) }
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
    _name: &str,
    ctx: StrategyPluginContext,
    conf: serde_json::Value,
) -> Result<Box<dyn Strategy>> {
    let options: PyScriptStrategyOptions = serde_json::from_value(conf)?;
    let script_content = std::fs::read_to_string(options.script_path)?;
    let provider = PyScriptStrategyProvider::new(ctx, options.conf, script_content);
    Ok(Box::new(provider.wrapped()))
}

inventory::submit! {
    StrategyPlugin::new("python_script", provide_options::<PyScriptStrategyOptions>, provide_python_script_strat)
}

#[pyfunction]
pub(crate) fn mstrategy(py: Python, class: PyObject) -> PyResult<()> {
    let locals = PyDict::new(py);
    locals.setattr("strat_class", class)?;
    py.run("Strat = strat_class", None, Some(locals))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use strategy::coinnect::exchange::Exchange;

    use strategy::driver::Strategy;
    use strategy::plugin::StrategyPluginContext;
    use strategy_test_util::plugin::test_plugin_context;
    use util::test::test_dir;

    use crate::script_strat::PyScriptStrategyProvider;

    fn python_script(name: &str) -> std::io::Result<String> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/python_scripts/{}.py", manifest_dir, name);
        std::fs::read_to_string(path)
    }

    fn default_test_context() -> StrategyPluginContext {
        let test_dir = test_dir();
        test_plugin_context(test_dir, &[Exchange::Binance])
    }

    #[test]
    fn test_python_from_python() {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let strat = strat_wrapper.wrapped();
        let whoami: String = strat.with_strat(|py_strat| {
            let r = py_strat.call_method0("whoami");
            r.unwrap().extract().unwrap()
        });
        assert_eq!(whoami, "PythonStrat");
        strat_wrapper.context.run(python! {
            'strat.init()
            'strat.update_model({})
            'strat.eval({})
            'strat.models()
            'strat.channels()
            import sys; sys.stdout.flush()
        });
    }

    #[tokio::test]
    async fn test_python_baseline_impl() -> strategy::error::Result<()> {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let mut strat = strat_wrapper.wrapped();
        strat.init()?;
        strat.model();
        strat.channels();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_fail_unimplemented() {
        let python_script = python_script("unimplemented").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let strat = strat_wrapper.wrapped();
        strat.with_strat(|py_strat| {
            let r = py_strat.call_method0("init");
            assert!(r.is_ok());
        });
    }
}
