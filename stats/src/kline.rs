use chrono::{DateTime, Utc};
use coinnect_rt::prelude::*;
use std::time::Duration;
use ta::*;

/// Klines is a set of candles with the same interval over a specific market
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
struct Klines {
    exchange: String,
    pair: Pair,
    interval: Duration,
    candles: Vec<Candle>,
}

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
struct Candle {
    /// The start time of this candle
    start_time: DateTime<Utc>,
    /// The close time of this candle
    end_time: DateTime<Utc>,
    /// Open price
    open: f64,
    /// Highest price within the interval
    high: f64,
    /// Lowest price within the interval
    low: f64,
    /// Close price
    close: f64,
    /// Traded volume in base asset
    volume: f64,
    /// Traded volume in quote asset
    quote_volume: f64,
    /// Trades count
    trade_count: u64,
}

impl Default for Candle {
    fn default() -> Self {
        Self {
            start_time: Utc::now(),
            end_time: Utc::now(),
            open: 1000.0,
            high: 1100.0,
            low: 900.0,
            close: 1050.0,
            volume: 1000000000.0,
            quote_volume: 1000000000.0,
            trade_count: 100,
        }
    }
}

impl Open for Candle {
    fn open(&self) -> f64 { self.open }
}

impl High for Candle {
    fn high(&self) -> f64 { self.high }
}

impl Low for Candle {
    fn low(&self) -> f64 { self.low }
}

impl Close for Candle {
    fn close(&self) -> f64 { self.close }
}

impl Volume for Candle {
    fn volume(&self) -> f64 { self.volume }
}

/// Defines the possible intervals that a [Candle] represents.
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
pub enum Interval {
    Minute1,
    Minute3,
    Minute5,
    Minute15,
    Minute30,
    Hour1,
    Hour2,
    Hour4,
    Hour6,
    Hour8,
    Hour12,
    Day1,
    Day3,
    Week1,
    Month1,
}

impl Interval {
    pub fn from_duration(d: Duration) -> Option<Interval> {
        let secs = d.as_secs();
        let i = if secs % (3600 * 24) == 0 {
            if secs == 3600 * 24 * 1 {
                Interval::Day1
            } else if secs == 3600 * 24 * 3 {
                Interval::Day3
            } else if secs == 3600 * 24 * 7 {
                Interval::Week1
            } else if secs == 3600 * 24 * 30 {
                Interval::Month1
            } else {
                return None;
            }
        } else if secs % 3600 == 0 {
            if secs == 3600 {
                Some(Interval::Hour1)
            } else if secs == 3600 * 2 {
                Some(Interval::Hour2)
            } else if secs == 3600 * 3 {
                Some(Interval::Hour3)
            } else if secs == 3600 * 4 {
                Some(Interval::Hour4)
            } else if secs == 3600 * 5 {
                Some(Interval::Hour5)
            } else if secs == 3600 * 6 {
                Some(Interval::Hour6)
            } else if secs == 3600 * 8 {
                Some(Interval::Hour8)
            } else if secs == 3600 * 12 {
                Some(Interval::Hour12)
            } else {
                return None;
            }
        } else if secs % 60 == 0 {
            if secs == 60 {
                Some(Interval::Minute1)
            } else if secs == 180 {
                Some(Interval::Minute3)
            } else if secs == 300 {
                Some(Interval::Minute5)
            } else if secs == 60 * 15 {
                Some(Interval::Minute15)
            } else if secs == 60 * 30 {
                Some(Interval::Minute30)
            } else {
                return None;
            }
        };
        Some(i)
    }
}

pub enum BarMerge {
    GapsOff,
    LookaheadOn,
}
