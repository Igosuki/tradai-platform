use pyo3::exceptions::PyException;
use pyo3::PyErr;
use thiserror::Error;

use strategy::error::Error as StrategyError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Strategy error {0:?}")]
    ExecutionError(#[from] StrategyError),
    #[error("{0}")]
    Common(String),
}

impl From<Error> for PyErr {
    fn from(err: Error) -> Self { PyException::new_err(err.to_string()) }
}
