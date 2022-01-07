use crate::position::PositionKind;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StopEvent {
    Gain,
    Loss,
}

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

pub struct PositionStopper {
    pos_price: f64,
    pos_kind: PositionKind,
}

impl PositionStopper {
    pub fn new(pos_price: f64, pos_kind: PositionKind) -> Self { Self { pos_price, pos_kind } }

    pub fn should_stop(&self, new_stop_price: f64) -> bool {
        match self.pos_kind {
            PositionKind::Short => new_stop_price > self.pos_price,
            PositionKind::Long => new_stop_price < self.pos_price,
        }
    }
}
