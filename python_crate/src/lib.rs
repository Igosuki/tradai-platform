/*!
The SDK that exports functionality from all other crates to Python.

# Overview

Currently all functionality is available through a common module named strategy defined by [`fn@strat`]
Currently, this is mostly a read only API that cannot modify the system outside of implementing strategies and defining models and signals.
For instance, overriding strategy driver behavior is not yet available to python.

 */

#![feature(used_with_arg)]
#![allow(
    clippy::wildcard_imports,
    clippy::used_underscore_binding,
    clippy::unnecessary_wraps,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::unused_self,
    clippy::missing_errors_doc
)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
#[macro_use]
extern crate pyo3;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate strum_macros;

use pyo3::prelude::*;

use py_strat::*;

use crate::brokerage::PyMarketEvent;
use crate::channel::PyChannel;
use crate::script_strat::*;
use crate::trading::*;

mod asyncio;
mod backtest;
mod brokerage;
mod candle;
mod channel;
mod db;
mod error;
mod json_cannonical;
mod model;
mod py_strat;
#[cfg(feature = "pyarrow")]
mod pyarrow;
pub mod script_strat;
mod ta;
mod test_util;
mod trading;
mod util;
mod uuid;
mod windowed_ta;

create_exception!(strat, ModelError, pyo3::exceptions::PyException);
create_exception!(strat, EvalError, pyo3::exceptions::PyException);

#[pymodule]
#[pyo3(name = "tradai")]
pub fn tradai(py: Python, m: &PyModule) -> PyResult<()> {
    // Core
    m.add_class::<PyStrategy>()?;
    m.add_class::<PyTradeSignal>()?;
    m.add_class::<PyMarketEvent>()?;
    m.add_class::<PyChannel>()?;
    m.add_class::<PyPositionKind>()?;
    m.add_class::<PyTradeKind>()?;
    m.add_class::<PyOperationKind>()?;
    m.add_class::<PyOrderType>()?;
    m.add_class::<PyExecutionInstruction>()?;
    m.add_class::<PyMarginSideEffect>()?;
    m.add_class::<PyAssetType>()?;
    m.add_class::<PyOrderEnforcement>()?;
    m.add_class::<LoggingStdout>()?;
    m.add("ModelError", py.get_type::<ModelError>())?;
    m.add("EvalError", py.get_type::<EvalError>())?;
    m.add_function(wrap_pyfunction!(signal, m)?)?;
    m.add_function(wrap_pyfunction!(register_strat, m)?)?;

    // Submodules
    m.add_wrapped(wrap_pymodule!(crate::backtest::backtest))?;
    m.add_wrapped(wrap_pymodule!(crate::uuid::uuid))?;
    m.add_wrapped(wrap_pymodule!(ta::ta))?;
    m.add_wrapped(wrap_pymodule!(windowed_ta::windowed_ta))?;
    m.add_wrapped(wrap_pymodule!(model::model))?;
    Ok(())
}

pub fn prepare() {
    pyo3::prepare_freethreaded_python();
    let mut builder = pyo3_asyncio::tokio::re_exports::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);
}
