#[macro_use]
extern crate pyo3;

use log::LevelFilter;
use pyo3_log::*;

#[pymodule]
#[pyo3(name = "strategy")]
fn strategy(py: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    Logger::new(py, Caching::LoggersAndLevels)?
        .filter(LevelFilter::Trace)
        .install()
        .expect("Someone installed a logger before us :-(");
    strategy_python::strat(py, m)?;
    Ok(())
}
