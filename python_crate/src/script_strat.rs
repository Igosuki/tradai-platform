use std::collections::{HashMap, HashSet};

use inline_python::Context;
use pyo3::indoc::formatdoc;
use pyo3::types::{PyDict, PyModule};
use pyo3::{IntoPy, PyObject, PyResult, Python};
use serde_json::Value;

use brokers::prelude::*;
use strategy::driver::{Strategy, StrategyInitContext};
use strategy::error::Error::BadConfiguration;
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::StrategyKey;

use crate::backtest::PyStrategyInitContext;
use crate::util::{register_tradai_module, PythonScript};
use crate::PyStrategyWrapper;

struct PyScriptStrategyProvider {
    context: Context,
}

impl PyScriptStrategyProvider {
    fn new(ctx: StrategyPluginContext, conf: HashMap<String, serde_json::Value>, python_script: PythonScript) -> Self {
        Self::try_new(ctx, conf, python_script).unwrap()
    }

    fn try_new(
        ctx: StrategyPluginContext,
        conf: HashMap<String, serde_json::Value>,
        python_script: PythonScript,
    ) -> Result<Self> {
        Python::with_gil(|py| {
            let context = Context::new_with_gil(py);
            register_tradai_module(py).unwrap();
            let dyn_mod_name = python_script.name;
            let m = PyModule::from_code(py, &python_script.code, &dyn_mod_name, &dyn_mod_name)?;
            match m.getattr("__strat_class__") {
                Ok(_) => {
                    let py_conf = pythonize::pythonize(py, &conf).unwrap();
                    let driver_ctx: PyStrategyInitContext = StrategyInitContext {
                        engine: ctx.engine.clone(),
                        db: ctx.db.clone(),
                    }
                    .into();
                    let py_ctx = driver_ctx.into_py(py);
                    let code = formatdoc! {
                        r#"import {dyn_mod_name}
                        print("loaded strategy class %s" % {dyn_mod_name}.__strat_class__.__name__)
                        from {dyn_mod_name} import __strat_class__ as Strat
                        strat = Strat(py_conf, py_ctx)
                    "#
                    };
                    let locals = PyDict::new(py);
                    locals.set_item("py_conf", py_conf)?;
                    locals.set_item("py_ctx", py_ctx)?;
                    py.run(&code, None, Some(locals))?;
                    if let Some(strat) = locals.get_item("strat") {
                        context.set_with_gil(py, "strat", strat);
                    }
                    Ok(Self { context })
                }
                Err(_) => {
                    return Err(BadConfiguration(format!(
                        "__strat_class__ must be initialized, check the documentation"
                    )));
                }
            }
        })
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
    crate::prepare();
    let options: PyScriptStrategyOptions = serde_json::from_value(conf)?;
    let script_content = std::fs::read_to_string(options.script_path)?;
    let python_script = PythonScript {
        name: "_dyn_strat_mod".to_string(),
        code: script_content,
    };
    let provider = PyScriptStrategyProvider::new(ctx, options.conf, python_script);
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
pub(crate) fn register_strat(py: Python, class: PyObject, module_name: Option<String>) -> PyResult<()> {
    let locals = PyDict::new(py);
    locals.set_item("theclass", class)?;
    let mod_name = module_name.unwrap_or("dyn_strat_mod".to_string());
    let code = format!(
        "import sys; setattr(sys.modules['{mod_name}'], '__strat_class__', theclass); __strat_class__ = theclass"
    );
    py.run(&code, None, Some(locals))
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
    pub(crate) fn flush(&self) {}
}

#[cfg(test)]
mod test {
    use brokers::exchange::Exchange;
    use inline_python::python;
    use pyo3::{IntoPy, PyObject, Python};
    use std::collections::HashMap;

    use crate::PyMarketEvent;
    use strategy::driver::Strategy;
    use strategy::plugin::StrategyPluginContext;
    use strategy_test_util::plugin::test_plugin_context;
    use util::test::test_dir;

    use crate::script_strat::PyScriptStrategyProvider;
    use crate::test_util::fixtures::default_order_book_event;
    use crate::util::PythonScript;

    fn python_script(name: &str) -> std::io::Result<PythonScript> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = format!("{}/python_scripts/{}.py", manifest_dir, name);
        std::fs::read_to_string(path).map(|content| PythonScript {
            code: content,
            name: name.to_string(),
        })
    }

    fn default_test_context() -> StrategyPluginContext {
        let test_dir = test_dir();
        test_plugin_context(test_dir, &[Exchange::Binance])
    }

    #[actix::test]
    async fn test_python_from_python() {
        let python_script = python_script("calls").unwrap();
        let strat_wrapper = PyScriptStrategyProvider::new(default_test_context(), HashMap::default(), python_script);
        let wrapper = strat_wrapper.wrapped();
        let whoami: String = wrapper.with_strat(|py_strat| {
            let r = py_strat.call_method0("whoami");
            r.unwrap().extract().unwrap()
        });
        assert_eq!(whoami, "CallStrat");
        let e = default_order_book_event();
        let py_e: PyMarketEvent = e.into();
        let py_e_o: PyObject = Python::with_gil(|py| py_e.into_py(py));
        strat_wrapper.context.run(python! {
            'wrapper.init()
            'wrapper.eval('py_e_o)
            'wrapper.models()
            'wrapper.channels()
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
