use chrono::{DateTime, Utc};
use stats::yata_prelude::OHLCV;
use std::ops::Add;
use util::time::utc_zero;

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[pyclass(name = "Candle")]
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone, Copy)]
pub struct PyCandle {
    /// The time this candle was generated at
    pub event_time: DateTime<Utc>,
    /// The start time of this candle
    pub start_time: DateTime<Utc>,
    /// The close time of this candle
    pub end_time: DateTime<Utc>,
    /// Open price
    pub open: f64,
    /// Highest price within the interval
    pub high: f64,
    /// Lowest price within the interval
    pub low: f64,
    /// Close price
    pub close: f64,
    /// Traded volume in base asset
    pub volume: f64,
    /// Traded volume in quote asset
    pub quote_volume: f64,
    /// Trades count
    pub trade_count: u64,
    /// If the candle is closed (if the period is finished)
    pub is_final: bool,
}

impl Add<PyCandle> for PyCandle {
    type Output = PyCandle;

    fn add(self, other: PyCandle) -> Self {
        Self {
            high: self.high.max(other.high),
            low: self.low.min(other.low),
            close: other.close,
            volume: self.volume + other.volume,
            quote_volume: self.quote_volume + other.quote_volume,
            trade_count: self.trade_count + other.trade_count,
            end_time: other.end_time,
            event_time: other.event_time,
            ..self
        }
    }
}

impl PyCandle {
    pub fn new(
        price: f64,
        amount: f64,
        event_time: DateTime<Utc>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Self {
        Self {
            event_time,
            start_time,
            end_time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: amount,
            quote_volume: price * amount,
            trade_count: 1,
            is_final: false,
        }
    }
}

impl Default for PyCandle {
    fn default() -> Self {
        let epoch = utc_zero();
        Self {
            event_time: epoch,
            start_time: epoch,
            end_time: epoch,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            quote_volume: 0.0,
            trade_count: 0,
            is_final: false,
        }
    }
}

impl OHLCV for PyCandle {
    fn open(&self) -> f64 { self.open }

    fn high(&self) -> f64 { self.high }

    fn low(&self) -> f64 { self.low }

    fn close(&self) -> f64 { self.close }

    fn volume(&self) -> f64 { self.volume }
}
