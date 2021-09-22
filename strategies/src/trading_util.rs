use crate::types::{StopEvent, StratEvent};

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
