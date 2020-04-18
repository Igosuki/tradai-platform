use chrono::{Date, Duration, Utc};
use std::mem;

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
