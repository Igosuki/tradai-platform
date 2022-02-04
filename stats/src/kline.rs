#![allow(dead_code)]

use chrono::{DateTime, Utc};
use std::time::Duration;
use ta::Next;
use yata::core::{ValueType, Window};
use yata::helpers::Merge;
use yata::prelude::{Sequence, OHLCV};

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone, Copy)]
pub struct Candle {
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

impl Merge<Candle> for Candle {
    fn merge(&self, other: &Candle) -> Self {
        Self {
            high: self.high.max(other.high),
            low: self.low.min(other.low),
            close: other.close,
            volume: self.volume + other.volume,
            quote_volume: self.quote_volume + other.quote_volume,
            trade_count: self.trade_count + other.trade_count,
            end_time: other.end_time,
            ..*self
        }
    }
}

impl Candle {
    pub fn new(price: f64, amount: f64, ts: DateTime<Utc>) -> Self {
        Self {
            start_time: ts,
            end_time: ts,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: amount,
            quote_volume: price * amount,
            trade_count: 1,
        }
    }
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

impl OHLCV for Candle {
    fn open(&self) -> ValueType { self.open }

    fn high(&self) -> ValueType { self.high }

    fn low(&self) -> ValueType { self.low }

    fn close(&self) -> ValueType { self.close }

    fn volume(&self) -> ValueType { self.volume }
}

/// Defines the possible intervals that a [Candle] represents.
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone, AsRefStr)]
pub enum Interval {
    Second1,
    Second3,
    Second5,
    Second15,
    Second30,
    Second45,
    Minute1,
    Minute3,
    Minute5,
    Minute15,
    Minute30,
    Minute45,
    Hour1,
    Hour2,
    Hour3,
    Hour4,
    Hour5,
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
        if secs % (3600 * 24) == 0 {
            if secs == 3600 * 24 {
                Some(Interval::Day1)
            } else if secs == 3600 * 24 * 3 {
                Some(Interval::Day3)
            } else if secs == 3600 * 24 * 7 {
                Some(Interval::Week1)
            } else if secs == 3600 * 24 * 30 {
                Some(Interval::Month1)
            } else {
                None
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
                None
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
            } else if secs == 60 * 45 {
                Some(Interval::Minute45)
            } else {
                None
            }
        } else if secs == 1 {
            Some(Interval::Second1)
        } else if secs == 3 {
            Some(Interval::Second3)
        } else if secs == 5 {
            Some(Interval::Second5)
        } else if secs == 15 {
            Some(Interval::Second15)
        } else if secs == 30 {
            Some(Interval::Second30)
        } else if secs == 45 {
            Some(Interval::Second45)
        } else {
            None
        }
    }
}

#[allow(dead_code)]
pub enum BarMerge {
    GapsOff,
    LookaheadOn,
}

/// Kline is a set of candles with the same interval over a specific market
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone)]
pub struct Kline {
    base_interval: Duration,
    candles: Window<Candle>,
}

impl Kline {
    /// Create a new kline with the minimum interval set at `base_interval`
    pub fn new(base_interval: Duration, capacity: u32) -> Self {
        if base_interval < Duration::from_secs(1) {
            panic!("Cannot have a candle duration < 1s");
        }
        Self {
            base_interval,
            candles: Window::from_parts(Vec::with_capacity(capacity as usize).into(), 0),
        }
    }

    /// Resamples the kline at [sample_interval]
    pub fn resample(&self, sample_interval: Duration) -> impl Iterator<Item = Candle> {
        if self.base_interval >= sample_interval {
            self.candles.iter()
        } else {
            let resample_size = sample_interval.as_secs() / self.base_interval.as_secs();
            self.candles.collapse_timeframe(resample_size as usize, true)
        }
    }

    /// Returns [`None`] if the kline duration is not a known interval
    pub fn interval(&self) -> Option<Interval> { Interval::from_duration(self.base_interval) }
}

impl<'a> Next<Candle> for Kline {
    type Output = &'a Candle;

    fn next(&mut self, input: Candle) -> Self::Output {
        match self.candles.newest() {
            None => self.candles.push(input),
            Some(candle)
                if candle.start_time.timestamp_millis() + self.base_interval.as_millis() as i64
                    > input.start_time.timestamp_millis() =>
            {
                let latest_candle = self.candles.last_mut().unwrap();
                *latest_candle = latest_candle.merge(&input);
            }
            _ => self.candles.push(input),
        }
        self.candles.newest()
    }
}

/// Price, Amount, Time
impl Next<(f64, f64, DateTime<Utc>)> for Kline {
    type Output = ();

    fn next(&mut self, input: (f64, f64, DateTime<Utc>)) -> Self::Output {
        let new_candle = Candle::new(input.0, input.1, input.2);
        match self.candles.last() {
            None => self.candles.push(new_candle),
            Some(candle)
                if candle.start_time.timestamp_millis() + self.base_interval.as_millis() as i64
                    > new_candle.start_time.timestamp_millis() =>
            {
                let latest_candle = self.candles.last_mut().unwrap();
                *latest_candle = latest_candle.merge(&new_candle);
            }
            _ => self.candles.push(new_candle),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::kline::{Interval, Kline};
    use std::time::Duration;

    #[test]
    fn test_basic_second_kline() {
        let kline = Kline::new("binance".to_string(), "BTC_USDT".to_string(), Duration::from_secs(1));
        assert_eq!(kline.interval(), Some(Interval::Second1));
    }
}
