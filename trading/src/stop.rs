use crate::position::PositionKind;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StopEvent {
    Gain,
    Loss,
    Trailing,
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

#[derive(Debug)]
pub struct TrailingStopper<T> {
    trailing_stop_start: T,
    trailing_stop_loss: T,
    stop_loss: T,
    last_top: Option<T>,
}

impl TrailingStopper<f64> {
    pub fn new(trailing_stop_start: f64, trailing_stop_loss: f64, stop_loss: f64) -> Self {
        Self {
            trailing_stop_start,
            stop_loss,
            trailing_stop_loss,
            last_top: None,
        }
    }

    /// Returns `Some(StopEvent)` if the stop conditions are matched, `None` otherwise
    pub fn should_stop(&mut self, ret: f64) -> Option<StopEvent> {
        if ret < self.stop_loss {
            eprintln!("stop loss triggered : ret < stop_loss ( {} < {} )", ret, self.stop_loss);
            return Some(StopEvent::Loss);
        }

        if self.last_top.is_none() && ret > self.trailing_stop_start {
            self.last_top = Some(ret);
        } else if let Some(last_top) = self.last_top.as_mut() {
            if ordered_float::OrderedFloat(ret) > ordered_float::OrderedFloat(*last_top) {
                *last_top = ret;
            }
            if ret < *last_top - self.trailing_stop_loss {
                eprintln!(
                    "trailing_stop triggered : ret < last stop - trailing_stop_loss ( {} < {} - {} )",
                    ret, *last_top, self.trailing_stop_loss
                );
                return Some(StopEvent::Trailing);
            }
        }
        None
    }

    pub fn reset(&mut self) { self.last_top = None; }
}
