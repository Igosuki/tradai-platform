#![allow(dead_code)]

use chrono::{DateTime, Datelike, Duration, DurationRound, TimeZone, Utc};
use ringbuffer::{AllocRingBuffer, RingBuffer, RingBufferExt, RingBufferWrite};
use smallvec::SmallVec;
use std::ops::{Add, Mul};
use ta::Next;
use yata::core::ValueType;
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
    pub is_final: bool,
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

impl Default for Candle {
    fn default() -> Self {
        Self {
            event_time: Utc.timestamp_millis(0),
            start_time: Utc.timestamp_millis(0),
            end_time: Utc.timestamp_millis(0),
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
    pub fn new(time_unit: TimeUnit, units: u32) -> Self {
        match time_unit {
            TimeUnit::Second | TimeUnit::Minute => assert!(units <= 60),
            TimeUnit::Hour => assert!(units <= 24),
            TimeUnit::Day => assert!(units <= 31),
            TimeUnit::Week => assert!(units <= 52),
            TimeUnit::Month => assert!(units <= 12),
            _ => {}
        }
        Self { time_unit, units }
    }

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
        } else if self.time_unit == TimeUnit::Month {
            Utc.ymd(date_part.year(), date_part.month0(), 0).and_hms(0, 0, 0)
        } else {
            Utc.ymd(date_part.year(), 0, 0).and_hms(0, 0, 0)
        }
    }

    fn as_secs(&self) -> i64 {
        match self.time_unit {
            TimeUnit::Second => Duration::seconds(1),
            TimeUnit::Minute => Duration::minutes(1),
            TimeUnit::Hour => Duration::hours(1),
            TimeUnit::Day => Duration::days(1),
            TimeUnit::Week => Duration::weeks(1),
            TimeUnit::Month => Duration::days(1).mul(30),
            TimeUnit::Year => Duration::days(1).mul(365),
        }
        .num_seconds()
    }

    fn add(&self, to: DateTime<Utc>) -> DateTime<Utc> {
        match self.time_unit {
            TimeUnit::Second => to.add(Duration::seconds(self.units as i64)),
            TimeUnit::Minute => to.add(Duration::minutes(self.units as i64)),
            TimeUnit::Hour => to.add(Duration::hours(self.units as i64)),
            TimeUnit::Day => to.add(Duration::days(self.units as i64)),
            TimeUnit::Week => to.add(Duration::weeks(self.units as i64)),
            TimeUnit::Month => {
                let month = to.month();
                let year = to.year();
                if month == 12 {
                    to.with_year(year + 1).unwrap().with_month(self.units).unwrap()
                } else {
                    to.with_month(month + self.units).unwrap()
                }
            }
            TimeUnit::Year => to.with_year(to.year() + self.units as i32).unwrap(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum BarMerge {
    GapsOff,
    LookaheadOn,
}

/// Kline is a set of candles with the same interval over a specific market
#[derive(Debug, Clone)]
pub struct Kline {
    bar_merge: BarMerge,
    base_interval: SampleInterval,
    candles: AllocRingBuffer<Candle>,
}

impl Kline {
    /// Create a new kline with the minimum interval set at `base_interval`
    /// N.B. : capacity must be a power of 2
    pub fn new(base_interval: SampleInterval, capacity: usize) -> Self {
        if base_interval.units == 0 {
            panic!("Cannot have a candle duration 0 time units");
        }
        Self {
            bar_merge: BarMerge::GapsOff,
            base_interval,
            candles: AllocRingBuffer::with_capacity(capacity),
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
                    .to_vec()
                    .collapse_timeframe(resample_size as usize, true)
                    .into_iter(),
            )
        }
    }

    /// Returns [`None`] if the kline duration is not a known interval
    pub fn interval(&self) -> SampleInterval { self.base_interval }
}

impl Next<Candle> for Kline {
    type Output = SmallVec<[Candle; 2]>;

    fn next(&mut self, input: Candle) -> Self::Output {
        if self.candles.is_empty() {
            self.candles.push(input);
            return SmallVec::from_slice(&[input]);
        }
        match self.candles.get_mut((self.candles.len() - 1) as isize) {
            Some(candle) if candle.start_time < input.start_time => {
                candle.is_final = true;
                let v = SmallVec::from_slice(&[*candle, input]);
                self.candles.push(input);
                v
            }
            Some(candle) if candle.start_time >= input.start_time => {
                *candle = candle.merge(&input);
                SmallVec::from_slice(&[*candle])
            }
            _ => {
                self.candles.push(input);
                SmallVec::from_slice(&[input])
            }
        }
    }
}

/// Price, Amount, Time
impl Next<(f64, f64, DateTime<Utc>)> for Kline {
    type Output = SmallVec<[Candle; 2]>;

    fn next(&mut self, input: (f64, f64, DateTime<Utc>)) -> Self::Output {
        let start_time = self.base_interval.truncate(input.2);
        let end_time = self.base_interval.add(start_time);
        let new_candle = Candle::new(input.0, input.1, input.2, start_time, end_time);
        Next::<Candle>::next(self, new_candle)
    }
}

pub struct KlineIterator<'a>(&'a dyn Iterator<Item = Candle>);

impl<'a> IntoIterator for &'a Kline {
    type Item = &'a Candle;
    type IntoIter = impl Iterator<Item = &'a Candle>;

    fn into_iter(self) -> Self::IntoIter { self.candles.iter() }
}

#[cfg(test)]
mod test {
    use crate::kline::TimeUnit::Second;
    use crate::kline::{Candle, Kline, SampleInterval};
    use chrono::{Duration, Utc};
    use pretty_assertions::assert_eq;
    use std::ops::Add;
    use ta::Next;

    #[test]
    fn test_basic_second_kline() {
        let interval = SampleInterval::new(Second, 1);
        let mut kline = Kline::new(interval, 2_usize.pow(14));
        let candle1_time = Utc::now();
        let vec1 = kline.next((1.0, 2.0, candle1_time));
        let candle1 = vec1.first().unwrap();
        assert!(!candle1.is_final);
        let candle3_time = candle1_time.add(Duration::milliseconds(200));
        let vec2 = kline.next((3.5, 5.0, candle3_time));
        let candle2 = vec2.first().unwrap();
        assert!(!candle2.is_final);
        let candle3_time = candle1_time.add(Duration::seconds(1)).add(Duration::milliseconds(1));
        let vec3 = kline.next((6.0, 8.0, candle3_time));
        let candle3 = vec3.first().unwrap();
        assert!(!candle3.is_final);
        let kline_candles: Vec<Candle> = kline.into_iter().copied().collect::<Vec<Candle>>();
        let expected = vec![
            Candle {
                event_time: candle1_time,
                start_time: interval.truncate(candle1_time),
                end_time: interval.truncate(candle1_time).add(Duration::seconds(1)),
                open: 1.0,
                high: 3.5,
                low: 1.0,
                close: 3.5,
                volume: 7.0,
                quote_volume: 19.5,
                trade_count: 2,
                is_final: true,
            },
            Candle {
                event_time: candle3_time,
                start_time: interval.truncate(candle3_time),
                end_time: interval.truncate(candle3_time).add(Duration::seconds(1)),
                open: 6.0,
                high: 6.0,
                low: 6.0,
                close: 6.0,
                volume: 8.0,
                quote_volume: 48.0,
                trade_count: 1,
                is_final: false,
            },
        ];
        assert_eq!(kline_candles, expected);
        //assert_eq!(kline.interval(), SampleInterval { time_unit: }));
    }
}
