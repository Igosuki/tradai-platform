use std::mem;

use chrono::TimeZone;
use chrono::{DateTime, Duration, Timelike, Utc};
#[cfg(feature = "mock_time")]
use mock_instant::MockClock;

#[derive(Clone, Copy)]
pub enum DurationRangeType {
    Millis,
    Seconds,
    Days,
}

#[derive(Clone, Copy)]
pub struct DateRange(pub DateTime<Utc>, pub DateTime<Utc>, pub DurationRangeType, pub i64);

impl DateRange {
    pub fn by_day(from: DateTime<Utc>, to: DateTime<Utc>) -> Self { Self(from, to, DurationRangeType::Days, 1) }

    fn range(&self) -> Duration {
        match self.2 {
            DurationRangeType::Days => Duration::days(self.3),
            DurationRangeType::Seconds => Duration::seconds(self.3),
            DurationRangeType::Millis => Duration::milliseconds(self.3),
        }
    }

    /// Returns Some(upper) if the upper bound is within range of the current lower bound
    pub fn upper_bound_in_range(&self) -> Option<DateTime<Utc>> { (self.0 + self.range() > self.1).then_some(self.1) }
}

impl Iterator for DateRange {
    type Item = DateTime<Utc>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0 <= self.1 {
            let next = self.0 + self.range();
            Some(mem::replace(&mut self.0, next))
        } else {
            None
        }
    }
}

pub const TIMESTAMP_FORMAT: &str = "%Y%m%d %H:%M:%S";

#[must_use]
pub fn now_str() -> String {
    let now = Utc::now();
    now.format(TIMESTAMP_FORMAT).to_string()
}

pub const FILES_TIMESTAMP_FORMAT: &str = "%Y%m%d_%H:%M:%S";

#[must_use]
pub fn now_str_files() -> String {
    let now = Utc::now();
    now.format(FILES_TIMESTAMP_FORMAT).to_string()
}

/// Now simply returns the last clock if previously set
#[cfg(feature = "mock_time")]
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn now() -> DateTime<Utc> {
    // TODO : this should use the mock clock as an offset so we can still simulate passing nanoseconds
    Utc.timestamp_millis_opt(MockClock::time().as_millis() as i64).unwrap()
}

/// Uses chrono as the default if not mocking time
#[cfg(not(feature = "mock_time"))]
pub fn now() -> DateTime<Utc> { Utc::now() }

#[allow(clippy::cast_sign_loss)]
pub fn set_mock_time(t: DateTime<Utc>) {
    let d = std::time::Duration::from_millis(t.timestamp_millis() as u64);
    mock_instant::MockClock::set_time(d);
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct TimedData<T> {
    pub ts: DateTime<Utc>,
    #[serde(flatten)]
    pub value: T,
}

impl<T> TimedData<T> {
    pub fn new(ts: DateTime<Utc>, value: T) -> Self { Self { ts, value } }
}

pub type TimedVec<T> = Vec<TimedData<T>>;

pub fn get_unix_timestamp_ms() -> i64 { now().timestamp_millis() }

pub fn get_unix_timestamp_us() -> i64 { now().timestamp_nanos() }

pub fn utc_zero() -> DateTime<Utc> { return Utc.timestamp_millis_opt(0).unwrap(); }

#[inline]
pub fn utc_at_midnight(dt: DateTime<Utc>) -> DateTime<Utc> {
    return dt.with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap();
}
