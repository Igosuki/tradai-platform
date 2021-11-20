use crate::types::{StopEvent, StratEvent};

#[derive(Debug)]
pub struct Stopper<T> {
    stop_gain: T,
    stop_loss: T,
}

impl<T: std::cmp::PartialOrd + Copy> Stopper<T> {
    pub fn new(stop_gain: T, stop_loss: T) -> Self { Self { stop_gain, stop_loss } }

    /// Returns `Some(StopEvent)` if the stop conditions are matched, `None` otherwise
    pub fn should_stop(&self, ret: T) -> Option<StopEvent> {
        if ret > self.stop_gain {
            Some(StopEvent::Gain)
        } else if ret < self.stop_loss {
            Some(StopEvent::Loss)
        } else {
            None
        }
    }
}

#[allow(dead_code)]
pub fn maybe_log_stop(stop_event: Option<StopEvent>) {
    stop_event.map(|stop| {
        let strat_event = StratEvent::Stop { stop };
        strat_event.log();
        strat_event
    });
}
