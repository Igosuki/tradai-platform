#![allow(dead_code)]

use chrono::{DateTime, Datelike, Duration, DurationRound, TimeZone, Utc};
use std::ops::Mul;
use ta::Next;
use yata::core::{ValueType, Window};
use yata::helpers::Merge;
use yata::prelude::{Sequence, OHLCV};

/// Normalised OHLCV data from an [Interval] with the associated [DateTime] UTC timestamp;
#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone, Copy)]
pub struct Candle {
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
    pub is_closed: bool,
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
    pub fn new(price: f64, amount: f64, event_time: DateTime<Utc>, start_time: DateTime<Utc>) -> Self {
        Self {
            event_time,
            start_time,
            end_time: start_time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: amount,
            quote_volume: price * amount,
            trade_count: 1,
            is_closed: false,
        }
    }
}

impl Default for Candle {
    fn default() -> Self {
        Self {
            event_time: Utc::now(),
            start_time: Utc::now(),
            end_time: Utc::now(),
            open: 1000.0,
            high: 1100.0,
            low: 900.0,
            close: 1050.0,
            volume: 1000000000.0,
            quote_volume: 1000000000.0,
            trade_count: 100,
            is_closed: false,
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

#[derive(Debug, Deserialize, Serialize, PartialOrd, PartialEq, Clone, AsRefStr, Copy)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

/// Defines the possible intervals that a [Candle] represents.
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct SampleInterval {
    pub time_unit: TimeUnit,
    pub units: u32,
}

impl SampleInterval {
    fn truncate(&self, dt: DateTime<Utc>) -> DateTime<Utc> {
        let date_part = dt.date();
        let maybe_secs = match self.time_unit {
            TimeUnit::Second => Some(Duration::seconds(1)),
            TimeUnit::Minute => Some(Duration::minutes(1)),
            TimeUnit::Hour => Some(Duration::hours(1)),
            TimeUnit::Day => Some(Duration::days(1)),
            TimeUnit::Week => Some(Duration::days(1).mul(7)),
            _ => None,
        };
        if let Some(secs) = maybe_secs {
            dt.duration_trunc(secs).unwrap()
        } else {
            if self.time_unit == TimeUnit::Month {
                Utc.ymd(date_part.year(), date_part.month0(), 0).and_hms(0, 0, 0)
            } else {
                Utc.ymd(date_part.year(), 0, 0).and_hms(0, 0, 0)
            }
        }
    }

    fn as_secs(&self) -> i64 {
        match self.time_unit {
            TimeUnit::Second => Duration::seconds(1),
            TimeUnit::Minute => Duration::minutes(1),
            TimeUnit::Hour => Duration::hours(1),
            TimeUnit::Day => Duration::days(1),
            TimeUnit::Week => Duration::days(1).mul(7),
            TimeUnit::Month => Duration::days(1).mul(30),
            TimeUnit::Year => Duration::days(1).mul(365),
        }
        .num_seconds()
    }
}

#[allow(dead_code)]
pub enum BarMerge {
    GapsOff,
    LookaheadOn,
}

/// Kline is a set of candles with the same interval over a specific market
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Kline {
    base_interval: SampleInterval,
    candles: Window<Candle>,
}

impl Kline {
    /// Create a new kline with the minimum interval set at `base_interval`
    pub fn new(base_interval: SampleInterval, capacity: u32) -> Self {
        if base_interval.units <= 0 {
            panic!("Cannot have a candle duration < 0 time units");
        }
        Self {
            base_interval,
            candles: Window::from_parts(Vec::with_capacity(capacity as usize).into(), 0),
        }
    }

    /// Resamples the kline at [sample_interval]
    pub fn resample(&self, sample_interval: SampleInterval) -> Box<dyn Iterator<Item = Candle> + '_> {
        if self.base_interval.time_unit == sample_interval.time_unit
            && self.base_interval.units >= sample_interval.units
        {
            Box::new(self.candles.iter().copied())
        } else {
            let resample_size = sample_interval.as_secs() / self.base_interval.as_secs();
            Box::new(
                self.candles
                    .collapse_timeframe(resample_size as usize, true)
                    .into_iter(),
            )
        }
    }

    /// Returns [`None`] if the kline duration is not a known interval
    pub fn interval(&self) -> SampleInterval { self.base_interval }
}

impl Next<Candle> for Kline {
    type Output = Candle;

    fn next(&mut self, input: Candle) -> Self::Output {
        match self.candles.iter().last() {
            Some(candle)
                if self.base_interval.truncate(candle.start_time) > self.base_interval.truncate(input.start_time) =>
            {
                let mut latest_candle = *self.candles.oldest();
                //let latest_candle = self.candles.last_mut().unwrap();
                latest_candle = latest_candle.merge(&input);
                latest_candle.is_closed = true;
                latest_candle
            }
            _ => self.candles.push(input),
        }
    }
}

/// Price, Amount, Time
impl Next<(f64, f64, DateTime<Utc>)> for Kline {
    type Output = Candle;

    fn next(&mut self, input: (f64, f64, DateTime<Utc>)) -> Self::Output {
        let start_time = self.base_interval.truncate(input.2);
        let new_candle = Candle::new(input.0, input.1, input.2, start_time);
        Next::<Candle>::next(self, new_candle)
    }
}

#[cfg(test)]
mod test {
    use crate::kline::TimeUnit::Second;
    use crate::kline::{Kline, SampleInterval};

    #[test]
    fn test_basic_second_kline() {
        let _kline = Kline::new(
            SampleInterval {
                time_unit: Second,
                units: 1,
            },
            1000,
        );
        //assert_eq!(kline.interval(), SampleInterval { time_unit: }));
    }
}
