#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate inline_python;
#[macro_use]
extern crate pyo3;
#[macro_use]
extern crate serde;

use pyo3::prelude::*;

use py_strat::*;

use crate::channel::PyChannel;
use crate::coinnect::PyMarketEvent;
use crate::trading::PyTradeSignal;

mod backtest;
mod channel;
mod coinnect;
mod error;
mod json_cannonical;
mod py_strat;
mod script_strat;
mod test_util;
mod trading;
mod util;

create_exception!(strat, ModelError, pyo3::exceptions::PyException);
create_exception!(strat, EvalError, pyo3::exceptions::PyException);

#[pymodule]
#[pyo3(name = "strat")]
pub fn strat(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyStrategy>()?;
    m.add_class::<PyTradeSignal>()?;
    m.add_class::<PyMarketEvent>()?;
    m.add_class::<PyChannel>()?;
    m.add("ModelError", py.get_type::<ModelError>())?;
    m.add("EvalError", py.get_type::<EvalError>())?;

    // Register backtest as a submodule
    let backtest = PyModule::new(py, "backtest")?;
    backtest::init_module(backtest)?;
    m.add_submodule(backtest)?;
    Ok(())
}
