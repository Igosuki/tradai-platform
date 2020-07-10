use chrono::Duration;
use chrono::{DateTime, Utc};

use std::ops::{Add, Mul};

/// TIme obsolescence is defined by last_time + (sample_freq * eval_freq) > current_time
pub fn is_eval_time_reached(
    current_time: DateTime<Utc>,
    last_time: DateTime<Utc>,
    sample_freq: Duration,
    eval_freq: i32,
) -> bool {
    let obsolete_time = last_time.add(sample_freq.mul(eval_freq));
    current_time.ge(&obsolete_time)
}
