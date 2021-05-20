use chrono::Duration;
use chrono::{DateTime, Utc};

use std::ops::{Add, Mul};
use crate::types::{StratEvent, StopEvent};

/// Time obsolescence is defined by last_time + (sample_freq * eval_freq) > current_time
pub fn is_eval_time_reached(
    current_time: DateTime<Utc>,
    last_time: DateTime<Utc>,
    sample_freq: Duration,
    eval_freq: i32,
) -> bool {
    let obsolete_time = last_time.add(sample_freq.mul(eval_freq));
    current_time.ge(&obsolete_time)
}

pub(crate) struct Stopper<T> {
    stop_gain: T,
    stop_loss: T
}

impl<T: std::cmp::PartialOrd + Copy> Stopper<T> {
    pub(crate) fn new(stop_gain: T, stop_loss: T) -> Self {
        Self {
            stop_gain, stop_loss
        }
    }

    pub(crate) fn should_stop(&self, ret: T) -> bool {
        ret > self.stop_gain || ret < self.stop_loss
    }

    pub(crate) fn maybe_stop(&self, ret: T) -> bool {
        if self.should_stop(ret) {
            let stop_type = if ret > self.stop_gain {
                StopEvent::GAIN
            } else if ret < self.stop_loss {
                StopEvent::LOSS
            } else {
                StopEvent::NA
            };
            StratEvent::Stop(stop_type).log();
            return true
        }
        false
    }
}
