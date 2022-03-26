use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyModule;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyResult;
use serde::{Deserialize, Serialize};

use crate::candle::PyCandle;
use stats::indicators::ppo::PercentPriceOscillator;
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
pub(crate) struct PyIndicatorResult {
    pub(crate) signals: Vec<Option<f64>>,
    pub(crate) values: Vec<f64>,
    pub(crate) lengths: (u8, u8),
}

impl From<&IndicatorResult> for PyIndicatorResult {
    fn from(ir: &IndicatorResult) -> Self {
        PyIndicatorResult {
            signals: ir.signals().iter().map(|a| a.ratio()).collect::<Vec<Option<f64>>>(),
            values: ir.values().to_vec(),
            lengths: (ir.values_length(), ir.signals_length()),
        }
    }
}

#[pyclass]
pub(crate) struct PyYataIndicator {
    config: Box<dyn IndicatorConfigDyn<PyCandle>>,
    instance: Option<Box<dyn IndicatorInstanceDyn<PyCandle>>>,
}

impl PyYataIndicator {
    fn try_new(config_dict: &PyDict, mut config: Box<dyn IndicatorConfigDyn<PyCandle>>) -> PyResult<Self> {
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

    // TODO: better return a dataframe
    fn over(&self, candles: Vec<PyCandle>) -> PyResult<Vec<PyIndicatorResult>> {
        let results = self
            .config
            .over(&candles)
            .map_err(|e| PyErr::new::<PyTypeError, _>(format!("error going over values {:?}", e)))?;
        Ok(results.iter().map(Into::into).collect::<Vec<PyIndicatorResult>>())
    }

    fn init(&mut self, candle: PyCandle) -> PyResult<()> {
        let result: std::result::Result<Box<dyn IndicatorInstanceDyn<PyCandle>>, yata_prelude::Error> =
            self.config.init(&candle);
        self.instance =
            Some(result.map_err(|e| PyErr::new::<PyTypeError, _>(format!("error initializing indicator {:?}", e)))?);
        Ok(())
    }

    fn next(&mut self, candle: PyCandle) -> PyResult<PyIndicatorResult> {
        if let Some(indicator) = &mut self.instance {
            Ok((&indicator.next(&candle)).into())
        } else {
            Err(PyErr::new::<PyTypeError, _>(format!(
                "indicator not initialized {:?}",
                self.name()
            )))
        }
    }
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
    adidx,
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
    ccidx,
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

macro_rules! yata_method {
    ($name:ident, $doc:literal, $signature:literal, $method:ident) => {
        yata_method!(
            $name,
            $doc,
            $signature,
            $method,
            <stats::yata_methods::$method as stats::yata_prelude::Method>::Params,
            <stats::yata_methods::$method as stats::yata_prelude::Method>::Input
        );
    };
    ($name:ident, $doc:literal, $signature:literal, $method:ident, $valuety:ty) => {
        #[pyclass]
        pub(crate) struct $method {
            inner: stats::yata_methods::$method,
        }

        #[pymethods]
        impl $method {
            fn next(
                &mut self,
                value: $valuety,
            ) -> <stats::yata_methods::$method as stats::yata_prelude::Method>::Output {
                self.inner.next(&value)
            }
        }

        #[doc = $doc]
        #[pyfunction(text_signature = $signature, kwds="**")]
        pub(crate) fn $name(value: $valuety) -> PyResult<$method> {
            Ok($method {
                inner: stats::yata_methods::$method::new(&value)
                    .map_err(|e| PyErr::new::<PyTypeError, _>(format!("bad yata method init {:?}", e)))?,
            })
        }
    };
    ($name:ident, $doc:literal, $signature:literal, $method:ident, $paramsty:ty, $valuety:ty) => {
        #[pyclass]
        pub(crate) struct $method {
            inner: stats::yata_methods::$method,
        }

        #[pymethods]
        impl $method {
            fn next(
                &mut self,
                value: $valuety,
            ) -> <stats::yata_methods::$method as stats::yata_prelude::Method>::Output {
                self.inner.next(&value)
            }
        }

        #[doc = $doc]
        #[pyfunction(text_signature = $signature, kwds="**")]
        pub(crate) fn $name(_py: Python, params: $paramsty, value: $valuety) -> PyResult<$method> {
            Ok($method {
                inner: stats::yata_methods::$method::new(params, &value)
                    .map_err(|e| PyErr::new::<PyTypeError, _>(format!("bad yata method init {:?}", e)))?,
            })
        }
    };
}

yata_method!(
    adim,
    "Accumulation Distribution Index, see rust api stats::yata_methods::ADI",
    "(length: u32, candle: OHLCV) -> ADI",
    ADI,
    u32,
    crate::candle::PyCandle
);

// yata_method!(
//     collapsetf,
//     "Collapse Timeframe, see rust api stats::yata_methods::CollapseTimeframe",
//     "(length: u32, candle: OHLCV) -> CollapseTimeframe",
//     CollapseTimeframe<PyCandle>
// );

yata_method!(
    cci,
    "Commodity Channel Index, see rust api stats::yata_methods::CCI",
    "(length: u32, value: f64) -> CCI",
    CCI
);

yata_method!(
    convma,
    "Convolution Moving Average, see rust api stats::yata_methods::Conv",
    "(length: [f64], value: f64) -> Conv",
    Conv
);

// yata_method!(
//     cross,
//     "Cross, see rust api stats::yata_methods::Cross",
//     "(params: _, value: (f64, f64)) -> Cross",
//     Cross
// );

yata_method!(
    deriv,
    "Derivative, see rust api stats::yata_methods::Derivative",
    "(length: u32, value: f64) -> Derivative",
    Derivative
);

yata_method!(
    ema,
    "Exponantial Moving Average, see rust api stats::yata_methods::EMA",
    "(length: u32, value: f64) -> EMA",
    EMA
);

// yata_method!(
//     heikin,
//     "Heikin Ashi, see rust api stats::yata_methods::HeikinAshi",
//     "(length: u32, value: f64) -> HeikinAshi",
//     HeikinAshi,
//     PyCandle
// );

yata_method!(
    highlowdelta,
    "Highest Lowest Delta, see rust api stats::yata_methods::HighestLowestDelta",
    "(length: u32, value: f64) -> HighestLowestDelta",
    HighestLowestDelta
);

yata_method!(
    highidx,
    "Highest Index, see rust api stats::yata_methods::HighestIndex",
    "(length: u32, value: f64) -> HighestIndex",
    HighestIndex
);

yata_method!(
    lowidx,
    "Lowest Index, see rust api stats::yata_methods::LowestIndex",
    "(length: u32, value: f64) -> LowestIndex",
    LowestIndex
);

yata_method!(
    hma,
    "Hull Moving Average, see rust api stats::yata_methods::HMA",
    "(length: u32, value: f64) -> HMA",
    HMA
);

yata_method!(
    itgrl,
    "Hull Moving Average, see rust api stats::yata_methods::Integral",
    "(length: u32, value: f64) -> Integral",
    Integral
);

yata_method!(
    linreg,
    "Linear Regression, see rust api stats::yata_methods::LinReg",
    "(length: u32, value: f64) -> LinReg",
    LinReg
);

yata_method!(
    meanabsdev,
    "Mean Abs Dev, see rust api stats::yata_methods::MeanAbsDev",
    "(length: u32, value: f64) -> MeanAbsDev",
    MeanAbsDev
);

yata_method!(
    medianabsdev,
    "Median Abs Dev, see rust api stats::yata_methods::MedianAbsDev",
    "(length: u32, value: f64) -> MedianAbsDev",
    MedianAbsDev
);

yata_method!(
    momentum,
    "Momentum, see rust api stats::yata_methods::Momentum",
    "(length: u32, value: f64) -> Momentum",
    Momentum
);

// yata_method!(
//     past,
//     "Past, see rust api stats::yata_methods::Past",
//     "(length: u32, value: f64) -> Past",
//     Past
// );

yata_method!(
    ratechg,
    "Rate of Change, see rust api stats::yata_methods::RateOfChange",
    "(length: u32, value: f64) -> RateOfChange",
    RateOfChange
);

// yata_method!(
//     renko,
//     "Renko, see rust api stats::yata_methods::Renko",
//     "(length: u32, value: f64) -> Renko",
//     Renko
// );

// yata_method!(
//     reversalsignal,
//     "Reversal, see rust api stats::yata_methods::ReversalSignal",
//     "(period: usize, candle: OHLCV) -> ReversalSignal",
//     ReversalSignal
// );

// yata_method!(
//     uperreversalsignal,
//     "Reversal, see rust api stats::yata_methods::UpperReversalSignal",
//     "(period: usize, candle: OHLCV) -> UpperReversalSignal",
//     UpperReversalSignal
// );

// yata_method!(
//     lowerreversalsignal,
//     "Reversal, see rust api stats::yata_methods::LowerReversalSignal",
//     "(period: usize, candle: OHLCV) -> LowerReversalSignal",
//     LowerReversalSignal
// );

yata_method!(
    rma,
    "Running Moving Average, see rust api stats::yata_methods::RMA",
    "(length: u32, value: f64) -> RMA",
    RMA
);

yata_method!(
    sma,
    "Simple Moving Average, see rust api stats::yata_methods::SMA",
    "(length: u32, value: f64) -> SMA",
    SMA
);

yata_method!(
    smm,
    "Simple Moving Median, see rust api stats::yata_methods::SMM",
    "(length: u32, value: f64) -> SMM",
    SMM
);

yata_method!(
    stddev,
    "Moving Standard Deviation, see rust api stats::yata_methods::StDev",
    "(length: u32, value: f64) -> StDev",
    StDev
);

yata_method!(
    swma,
    "Symmetrically Weighted Moving Average, see rust api stats::yata_methods::SWMA",
    "(length: u32, value: f64) -> SWMA",
    SWMA
);

yata_method!(
    truerange,
    "True Range, see rust api stats::yata_methods::TR",
    "(params: null, value: Candle) -> TR",
    TR,
    PyCandle
);

yata_method!(
    trima,
    "Triangular Moving Average, see rust api stats::yata_methods::TRIMA",
    "(length: u32, value: f64) -> TRIMA",
    TRIMA
);

// yata_method!(
//     tsi,
//     "True Strength Index, see rust api stats::yata_methods::TSI",
//     "(params: u32, value: f64) -> TSI",
//     TSI
// );

yata_method!(
    vidya,
    "Variable Index Dynamic Average, see rust api stats::yata_methods::Vidya",
    "(params: u32, value: f64) -> Vidya",
    Vidya
);

yata_method!(
    volatility,
    "Volatility, see rust api stats::yata_methods::LinearVolatility",
    "(params: u32, value: f64) -> LinearVolatility",
    LinearVolatility
);

yata_method!(
    vwma,
    "Volume Weighted Moving Average, see rust api stats::yata_methods::VWMA",
    "(params: u32, value: f64) -> VWMA",
    VWMA
);

yata_method!(
    wma,
    "Weighted Moving Average, see rust api stats::yata_methods::WMA",
    "(params: u32, value: f64) -> WMA",
    WMA
);

yata_method!(
    wsma,
    "Wilder's Smoothing Average, see rust api stats::yata_methods::WSMA",
    "(params: u32, value: f64) -> WSMA",
    WSMA
);

#[pymodule]
pub(crate) fn ta(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(adidx, m)?)?;
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
    m.add_function(wrap_pyfunction!(ccidx, m)?)?;
    m.add_function(wrap_pyfunction!(cppc, m)?)?;
    m.add_function(wrap_pyfunction!(trdsi, m)?)?;
    m.add_function(wrap_pyfunction!(trix, m)?)?;
    m.add_function(wrap_pyfunction!(trusi, m)?)?;
    m.add_function(wrap_pyfunction!(wdscci, m)?)?;
    m.add_function(wrap_pyfunction!(fshtsfm, m)?)?;
    m.add_function(wrap_pyfunction!(pcs, m)?)?;
    m.add_function(wrap_pyfunction!(rvi, m)?)?;
    m.add_function(wrap_pyfunction!(smiergo, m)?)?;

    m.add_function(wrap_pyfunction!(trima, m)?)?;

    Ok(())
}
