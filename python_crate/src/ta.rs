use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyResult;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;

use stats::indicators::ppo::PercentPriceOscillator;
use stats::kline::Candle;
use stats::yata_prelude::dd::{IndicatorConfigDyn, IndicatorInstanceDyn};
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

#[pyclass]
pub(crate) struct PyYataIndicator {
    config: Box<dyn IndicatorConfigDyn<Candle> + Send>,
    instance: Option<Box<dyn IndicatorInstanceDyn<Candle> + Send>>,
}

impl PyYataIndicator {
    fn try_new(config_dict: &PyDict, mut config: Box<dyn IndicatorConfigDyn<Candle> + Send>) -> PyResult<Self> {
        for (k, v) in config_dict.iter() {
            let key: &str = k.extract()?;
            let value: String = v.extract()?;
            config
                .set(key, value)
                .map_err(|e| PyErr::new::<PyTypeError, _>(format!("bad indicator config {:?}", e)))?;
        }
        Ok(Self { config, instance: None })
    }
}

#[pymethods]
impl PyYataIndicator {
    fn name(&self) -> String { self.config.name().to_string() }
}

macro_rules! yata_indicator {
    ($name:ident, $doc:literal, $signature:literal, $struct:ident) => {
        #[doc = $doc]
        #[pyfunction(text_signature = $signature, kwds="**")]
        pub(crate) fn $name(py: Python, kwds: Option<&PyDict>) -> PyResult<PyYataIndicator> {
            let kwds = kwds.unwrap_or_else(|| PyDict::new(py));
            PyYataIndicator::try_new(kwds, Box::new(stats::yata_indicators::$struct::default()))
        }
    };
}

yata_indicator!(
    macd,
    "Moving average convergence/divergence (MACD)",
    "(ma1=None, ma2=None, signal=None, source=None)",
    MACD
);

yata_indicator!(
    stocho,
    "Stochastic Oscillator",
    "(period=None, ma=None, signal=None, zone=None)",
    StochasticOscillator
);

yata_indicator!(rsi, "Relative Strength Index", "(ma=None, zone=None, source=None)", RSI);

#[pymodule]
pub(crate) fn ta(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ppo, m)?)?;
    m.add_function(wrap_pyfunction!(macd, m)?)?;
    m.add_function(wrap_pyfunction!(stocho, m)?)?;
    m.add_function(wrap_pyfunction!(rsi, m)?)?;
    Ok(())
}
