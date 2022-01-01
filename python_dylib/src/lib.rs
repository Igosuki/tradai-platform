/*!
This crate exists solely to export the `strategy_python` crate as a dynamic library for maturin,
and to hold pure python code that is to be packaged along with the library.

# Overview

A logger is pre-installed to output rust logs to python loggers

The exported cpython module is defined by [`fn@strategy`]

 */

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
