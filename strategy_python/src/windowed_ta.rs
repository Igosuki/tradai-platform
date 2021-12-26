use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::PyResult;
use serde::{Deserialize, Serialize};

use stats::indicators::thresholds::Thresholds;
use stats::Next;

#[derive(Copy, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum WindowedTechnicalIndicator {
    Thresholds(Thresholds),
}

impl<'a, I> Next<I> for WindowedTechnicalIndicator
where
    I: Iterator<Item = &'a f64>,
{
    type Output = ();

    fn next(&mut self, input: I) -> Self::Output {
        match self {
            WindowedTechnicalIndicator::Thresholds(v) => v.next(input),
        };
    }
}

impl WindowedTechnicalIndicator {
    pub(crate) fn values(&self) -> Vec<f64> {
        match self {
            WindowedTechnicalIndicator::Thresholds(m) => vec![m.low, m.high],
        }
    }
}

#[derive(Clone)]
#[pyclass]
pub(crate) struct PyWindowedIndicator {
    inner: WindowedTechnicalIndicator,
}

impl From<PyWindowedIndicator> for WindowedTechnicalIndicator {
    fn from(p: PyWindowedIndicator) -> Self { p.inner }
}

#[doc = "The thresholds windowed technical indicator"]
#[pyfunction(text_signature = "(short_0, long_0, /)")]
pub(crate) fn thresholds(short_0: f64, long_0: f64) -> PyWindowedIndicator {
    PyWindowedIndicator {
        inner: WindowedTechnicalIndicator::Thresholds(Thresholds::new(short_0, long_0)),
    }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(thresholds, m)?)?;
    Ok(())
}
