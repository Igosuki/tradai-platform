use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::PyResult;
use serde::{Deserialize, Serialize};

use stats::indicators::ppo::PercentPriceOscillator;
#[allow(unused_imports)]
use stats::*;
use stats::{Close, Next};

#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum TechnicalIndicator {
    PPO(PercentPriceOscillator),
}

impl Next<f64> for TechnicalIndicator {
    type Output = ();

    fn next(&mut self, input: f64) -> Self::Output {
        match self {
            TechnicalIndicator::PPO(v) => v.next(input),
        };
    }
}

impl<R: Close> Next<&R> for TechnicalIndicator {
    type Output = ();

    fn next(&mut self, input: &R) -> Self::Output {
        match self {
            TechnicalIndicator::PPO(v) => v.next(input.close()),
        };
    }
}

impl TechnicalIndicator {
    pub(crate) fn values(&self) -> Vec<f64> {
        match self {
            TechnicalIndicator::PPO(m) => vec![m.ppo],
        }
    }
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct PyIndicator {
    inner: TechnicalIndicator,
}

impl From<PyIndicator> for TechnicalIndicator {
    fn from(p: PyIndicator) -> Self { p.inner }
}

#[doc = "Absolute Price Oscillator technical indicator"]
#[pyfunction(text_signature = "(short_window, long_window, /)")]
pub(crate) fn ppo(short_window: u32, long_window: u32) -> PyIndicator {
    PyIndicator {
        inner: TechnicalIndicator::PPO(PercentPriceOscillator::new(long_window, short_window)),
    }
}

#[pymodule]
pub(crate) fn ta(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ppo, m)?)?;
    Ok(())
}
