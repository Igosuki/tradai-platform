use std::ops::{Add, Mul};

use chrono::Duration;
use chrono::{DateTime, Utc};

use crate::types::{StopEvent, StratEvent};

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

#[derive(Debug)]
pub(crate) struct Stopper<T> {
    stop_gain: T,
    stop_loss: T,
}

impl<T: std::cmp::PartialOrd + Copy> Stopper<T> {
    pub(crate) fn new(stop_gain: T, stop_loss: T) -> Self { Self { stop_gain, stop_loss } }

    pub(crate) fn should_stop(&self, ret: T) -> bool {
        let stop_type = if ret > self.stop_gain {
            Some(StopEvent::Gain)
        } else if ret < self.stop_loss {
            Some(StopEvent::Loss)
        } else {
            None
        };
        stop_type
            .map(|stop| {
                let strat_event = StratEvent::Stop { stop };
                strat_event.log();
                strat_event
            })
            .is_some()
    }
}
