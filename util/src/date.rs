use std::mem;

use chrono::{Date, Duration, Utc};

#[derive(Clone)]
pub enum DurationRangeType {
    Millis,
    Seconds,
    Days,
}

#[derive(Clone)]
pub struct DateRange(pub Date<Utc>, pub Date<Utc>, pub DurationRangeType, pub i64);

impl Iterator for DateRange {
    type Item = Date<Utc>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0 <= self.1 {
            let d = match self.2 {
                DurationRangeType::Days => Duration::days(self.3),
                DurationRangeType::Seconds => Duration::seconds(self.3),
                DurationRangeType::Millis => Duration::milliseconds(self.3),
            };
            let next = self.0 + d;
            Some(mem::replace(&mut self.0, next))
        } else {
            None
        }
    }
}

pub const TIMESTAMP_FORMAT: &str = "%Y%m%d %H:%M:%S";

pub fn now_str() -> String {
    let now = Utc::now();
    now.format(TIMESTAMP_FORMAT).to_string()
}
