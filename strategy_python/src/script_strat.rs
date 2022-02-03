use std::collections::{HashMap, HashSet};

use inline_python::Context;
use pyo3::types::{PyDict, PyModule};
use pyo3::{IntoPy, PyObject, PyResult, Python};
use serde_json::Value;

use coinnect_rt::prelude::*;
use strategy::driver::Strategy;
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::StrategyKey;
use strategy_test_util::it_backtest::GenericTestContext;

use crate::backtest::PyGenericTestContext;
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
        PyModule::from_code(py, &python_script, "_dyn_strat_mod", "_dyn_strat_mod")?;
        context.run_with_gil(py, python! {
            import _dyn_strat_mod
            print("loaded strategy class %s" % _dyn_strat_mod.__strat_class__.__name__)
        });
        let py_conf = pythonize::pythonize(py, &conf).unwrap();
        let driver_ctx: PyGenericTestContext = GenericTestContext {
            engine: ctx.engine.clone(),
            db: ctx.db.clone(),
        }
        .into();
        let py_ctx = driver_ctx.into_py(py);
        context.run_with_gil(py, python! {
            from _dyn_strat_mod import __strat_class__ as Strat

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

/// Defines this class as the module strategy, only the last invocation matters
///
/// In detail, it sets the __strat_class__ attribute on the execution module,
/// allowing for the system to instantiate the strategy from the script without requiring a specific global
#[pyfunction]
pub(crate) fn mstrategy(py: Python, class: PyObject) -> PyResult<()> {
    let locals = PyDict::new(py);
    locals.set_item("theclass", class)?;
    // let sys = py.import("sys")?;
    // sys.setattr("__strat_class")
    // let name: String = py.eval("__name__", None, None)?.extract()?;
    // eprintln!("name = {:?}", name);
    py.run(
        "import sys; setattr(sys.modules['_dyn_strat_mod'], '__strat_class__', theclass); __strat_class__ = theclass",
        None,
        Some(locals),
    )
}

#[pyclass]
pub(crate) struct LoggingStdout;

#[pymethods]
impl LoggingStdout {
    /// This replaces python stdout with the process stdout
    /// useful when the python code is invoked in a sandbox
    #[new]
    pub(crate) fn new() -> Self { Self }
    pub(crate) fn write(&self, data: &str) {
        print!("{}", data);
    }
}

#[cfg(test)]
mod test {
    use coinnect_rt::exchange::Exchange;
    use pyo3::{IntoPy, PyObject, Python};
    use std::collections::HashMap;

    use crate::PyMarketEvent;
    use strategy::driver::Strategy;
    use strategy::plugin::StrategyPluginContext;
    use strategy_test_util::plugin::test_plugin_context;
    use util::test::test_dir;

    use crate::script_strat::PyScriptStrategyProvider;
    use crate::test_util::fixtures::default_order_book_event;

    fn python_script(name: &str) -> std::io::Result<String> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/python_scripts/{}.py", manifest_dir, name);
        std::fs::read_to_string(path)
    }

    fn default_test_context() -> StrategyPluginContext {
        let test_dir = test_dir();
        test_plugin_context(test_dir, &[Exchange::Binance])
    }

    #[actix::test]
    async fn test_python_from_python() {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let strat = strat_wrapper.wrapped();
        let whoami: String = strat.with_strat(|py_strat| {
            let r = py_strat.call_method0("whoami");
            r.unwrap().extract().unwrap()
        });
        assert_eq!(whoami, "CallStrat");
        let e = default_order_book_event();
        let py_e: PyMarketEvent = e.into();
        let py_e_o: PyObject = Python::with_gil(|py| py_e.into_py(py));
        strat_wrapper.context.run(python! {
            'strat.init()
            'strat.eval('py_e_o)
            'strat.models()
            'strat.channels()
            import sys; sys.stdout.flush()
        });
    }

    #[actix::test]
    async fn test_python_baseline_impl() -> strategy::error::Result<()> {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let mut strat = strat_wrapper.wrapped();
        strat.init()?;
        strat.model();
        strat.channels();
        Ok(())
    }

    #[actix::test]
    async fn test_fail_unimplemented() {
        let python_script = python_script("unimplemented").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let strat = strat_wrapper.wrapped();
        strat.with_strat(|py_strat| {
            let r = py_strat.call_method0("init");
            assert_eq!(format!("{:?}", r), "Err(PyErr { type: <class 'NotImplementedError'>, value: NotImplementedError('init'), traceback: None })");
        });
    }

    #[actix::test]
    async fn test_multi_strat() {
        let whoami_calls: String = {
            let python_script = python_script("calls").unwrap();
            let strat_wrapper =
                PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
            let strat = strat_wrapper.wrapped();
            strat.with_strat(|py_strat| {
                let r = py_strat.call_method0("whoami");
                r.unwrap().extract().unwrap()
            })
        };
        let whoami_unimplemented: String = {
            let python_script = python_script("unimplemented").unwrap();
            let strat_wrapper =
                PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
            let strat = strat_wrapper.wrapped();
            strat.with_strat(|py_strat| {
                let r = py_strat.call_method0("whoami");
                r.unwrap().extract().unwrap()
            })
        };
        let whoami_calls2: String = {
            let python_script = python_script("calls").unwrap();
            let strat_wrapper =
                PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
            let strat = strat_wrapper.wrapped();
            strat.with_strat(|py_strat| {
                let r = py_strat.call_method0("whoami");
                r.unwrap().extract().unwrap()
            })
        };

        assert_ne!(whoami_unimplemented, whoami_calls);
        assert_ne!(whoami_unimplemented, whoami_calls2);
    }
}
