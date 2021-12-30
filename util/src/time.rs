use std::mem;

#[cfg(feature = "mock_time")]
use chrono::TimeZone;
use chrono::{Date, DateTime, Duration, Utc};
#[cfg(feature = "mock_time")]
use mock_instant::MockClock;

#[derive(Clone, Copy)]
pub enum DurationRangeType {
    Millis,
    Seconds,
    Days,
}

#[derive(Clone, Copy)]
pub struct DateRange(pub Date<Utc>, pub Date<Utc>, pub DurationRangeType, pub i64);

pub struct DateRangeIter(DateRange);

impl Iterator for DateRangeIter {
    type Item = Date<Utc>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.0;
        if iter.0 <= iter.1 {
            let d = match iter.2 {
                DurationRangeType::Days => Duration::days(iter.3),
                DurationRangeType::Seconds => Duration::seconds(iter.3),
                DurationRangeType::Millis => Duration::milliseconds(iter.3),
            };
            let next = iter.0 + d;
            Some(mem::replace(&mut iter.0, next))
        } else {
            None
        }
    }
}

impl IntoIterator for DateRange {
    type Item = Date<Utc>;
    type IntoIter = DateRangeIter;
    fn into_iter(self) -> Self::IntoIter { DateRangeIter(self) }
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
    Utc.timestamp_millis(MockClock::time().as_millis() as i64)
}

/// Uses chrono as the default if not mocking time
#[cfg(not(feature = "mock_time"))]
pub fn now() -> DateTime<Utc> { Utc::now() }

#[allow(clippy::cast_sign_loss)]
pub fn set_current_time(t: DateTime<Utc>) {
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
