use std::sync::Arc;

use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::PyResult;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use stats::indicators::macd_apo::MACDApo;
use stats::{Close, Next};
use strategy::models::IndicatorModel;

use crate::backtest::PyDb;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum TechnicalIndicator {
    MACDApo(MACDApo),
}

impl Next<f64> for TechnicalIndicator {
    type Output = ();

    fn next(&mut self, input: f64) -> Self::Output {
        match self {
            TechnicalIndicator::MACDApo(v) => v.next(input),
        };
    }
}

impl<R: Close> Next<&R> for TechnicalIndicator {
    type Output = ();

    fn next(&mut self, input: &R) -> Self::Output {
        match self {
            TechnicalIndicator::MACDApo(v) => v.next(input),
        };
    }
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct PyIndicator {
    inner: TechnicalIndicator,
}

#[doc = "The MACD Apo technical indicator"]
#[pyfunction(text_signature = "(short_window, long_window, /)")]
pub(crate) fn macd_apo(short_window: u32, long_window: u32) -> PyIndicator {
    PyIndicator {
        inner: TechnicalIndicator::MACDApo(MACDApo::new(long_window, short_window)),
    }
}

impl From<PyIndicator> for TechnicalIndicator {
    fn from(p: PyIndicator) -> Self { p.inner }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(macd_apo, m)?)?;
    Ok(())
}
