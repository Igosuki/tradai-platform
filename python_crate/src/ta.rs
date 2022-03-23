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
    "Moving average convergence/divergence (MACD), see rust api stats::yata_indicators::MACD",
    "(ma1=None, ma2=None, signal=None, source=None)",
    MACD
);

yata_indicator!(
    stocho,
    "Stochastic Oscillator, see rust api stats::yata_indicators::StochasticOscillator",
    "(period=None, ma=None, signal=None, zone=None)",
    StochasticOscillator
);

yata_indicator!(
    rsi,
    "Relative Strength Index, see rust api stats::yata_indicators::RSI",
    "(ma=None, zone=None, source=None)",
    RSI
);

yata_indicator!(
    aroon,
    "Aroon, see rust api stats::yata_indicators::Aroon",
    "(period=None, signal_zone=None, over_zone_period=None)",
    Aroon
);

yata_indicator!(
    adi,
    "Average Directional Index, see rust api stats::yata_indicators::AverageDirectionalIndex",
    "(method1=None, method2=None, period1=None, zone=None)",
    AverageDirectionalIndex
);

yata_indicator!(
    awsmo,
    "Awesome Oscillator, see rust api stats::yata_indicators::AwesomeOscillator",
    "(ma1=None, ma2=None, source=None, left=None, right=None, conseq_peaks=None)",
    AwesomeOscillator
);

yata_indicator!(
    bb,
    "Bollinger Bands, see rust api stats::yata_indicators::BollingerBands",
    "(avg_size=None, sigma=None, source=None)",
    BollingerBands
);

yata_indicator!(
    chkmf,
    "Chaikin Money Flow, see rust api stats::yata_indicators::ChaikinMoneyFlow",
    "(size=None)",
    ChaikinMoneyFlow
);

yata_indicator!(
    chkno,
    "Chaikin Oscillator, see rust api stats::yata_indicators::ChaikinOscillator",
    "(ma1=None, ma2=None, window=None)",
    ChaikinOscillator
);

yata_indicator!(
    ckstp,
    "Chande Kroll Stop, see rust api stats::yata_indicators::ChandeKrollStop",
    "(ma=None, x=None, q=None, source=None)",
    ChandeKrollStop
);

yata_indicator!(
    cmo,
    "Chande Momentum Oscillator, see rust api stats::yata_indicators::ChandeMomentumOscillator",
    "(period=None, zone=None, source=None)",
    ChandeMomentumOscillator
);

yata_indicator!(
    cci,
    "Commodity Channel Index, see rust api stats::yata_indicators::CommodityChannelIndex",
    "(period=None, zone=None, source=None)",
    CommodityChannelIndex
);

yata_indicator!(
    cppc,
    "Coppock Curve, see rust api stats::yata_indicators::CoppockCurve",
    "(ma1=None, s3_ma=None, period2=None, period3=None, s2_left=None, s2_right=None, source=None)",
    CoppockCurve
);

yata_indicator!(
    trdsi,
    "Trend Strength Index, see rust api stats::yata_indicators::TrendStrengthIndex",
    "(period=None, zone=None, reverse_offset=None, source=None)",
    TrendStrengthIndex
);

yata_indicator!(
    trix,
    "Trix, see rust api stats::yata_indicators::Trix",
    "(period1=None, signal=None, source=None)",
    Trix
);

yata_indicator!(
    trusi,
    "True Strength Index, see rust api stats::yata_indicators::TrueStrengthIndex",
    "(period1=None, period2=None, period3=None, zone=None, source=None)",
    TrueStrengthIndex
);

yata_indicator!(
    wdscci,
    "Woodies Commodity Channel Index, see rust api stats::yata_indicators::WoodiesCCI",
    "(period1=None, period2=None, s1_lag=None, source=None)",
    WoodiesCCI
);

yata_indicator!(
    fshtsfm,
    "Fisher Transform, see rust api stats::yata_indicators::FisherTransform",
    "(period1=None, zone=None, signal=None, source=None)",
    FisherTransform
);

yata_indicator!(
    pcs,
    "Price Channel Strategy, see rust api stats::yata_indicators::PriceChannelStrategy",
    "(period=None, sigma=None)",
    PriceChannelStrategy
);

yata_indicator!(
    rvi,
    "Relative Vigor Index, see rust api stats::yata_indicators::RelativeVigorIndex",
    "(period1=None, period2=None, signal=None, zone=None)",
    RelativeVigorIndex
);

yata_indicator!(
    smiergo,
    "SMI Ergodic Indicator, see rust api stats::yata_indicators::SMIErgodicIndicator",
    "(period1=None, period2=None, signal=None, zone=None, source=None)",
    SMIErgodicIndicator
);

#[pymodule]
pub(crate) fn ta(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ppo, m)?)?;
    m.add_function(wrap_pyfunction!(macd, m)?)?;
    m.add_function(wrap_pyfunction!(stocho, m)?)?;
    m.add_function(wrap_pyfunction!(rsi, m)?)?;
    m.add_function(wrap_pyfunction!(aroon, m)?)?;
    m.add_function(wrap_pyfunction!(bb, m)?)?;
    m.add_function(wrap_pyfunction!(chkmf, m)?)?;
    m.add_function(wrap_pyfunction!(chkno, m)?)?;
    m.add_function(wrap_pyfunction!(ckstp, m)?)?;
    m.add_function(wrap_pyfunction!(cmo, m)?)?;
    m.add_function(wrap_pyfunction!(cci, m)?)?;
    m.add_function(wrap_pyfunction!(cppc, m)?)?;
    m.add_function(wrap_pyfunction!(trdsi, m)?)?;
    m.add_function(wrap_pyfunction!(trix, m)?)?;
    m.add_function(wrap_pyfunction!(trusi, m)?)?;
    m.add_function(wrap_pyfunction!(wdscci, m)?)?;
    m.add_function(wrap_pyfunction!(fshtsfm, m)?)?;
    m.add_function(wrap_pyfunction!(pcs, m)?)?;
    m.add_function(wrap_pyfunction!(rvi, m)?)?;
    m.add_function(wrap_pyfunction!(smiergo, m)?)?;

    Ok(())
}
