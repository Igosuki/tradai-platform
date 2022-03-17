use crate::position::PositionKind;
use util::time::now;

pub trait Stopper<T> {
    fn should_stop(&self, next: T) -> Option<StopEvent>;
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StopEvent {
    Gain,
    Loss,
    TrailingStop,
}

#[derive(Debug)]
pub struct FixedStopper<T> {
    stop_gain: T,
    stop_loss: T,
}

impl<T: std::cmp::PartialOrd + Copy> FixedStopper<T> {
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

impl<T: std::cmp::PartialOrd + Copy> Stopper<T> for FixedStopper<T> {
    /// Returns `Some(StopEvent)` if the stop conditions are matched, `None` otherwise
    fn should_stop(&self, ret: T) -> Option<StopEvent> {
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

    pub fn should_stop(&self, new_stop_price: f64) -> Option<StopEvent> {
        (match self.pos_kind {
            PositionKind::Short => new_stop_price > self.pos_price,
            PositionKind::Long => new_stop_price < self.pos_price,
        })
        .then(|| StopEvent::Loss)
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
            debug!("stop loss triggered : ret < stop_loss ( {} < {} )", ret, self.stop_loss);
            return Some(StopEvent::Loss);
        }

        if self.last_top.is_none() && ret > self.trailing_stop_start {
            self.last_top = Some(ret);
        } else if let Some(last_top) = self.last_top.as_mut() {
            if ordered_float::OrderedFloat(ret) > ordered_float::OrderedFloat(*last_top) {
                *last_top = ret;
            }
            if ret < *last_top - self.trailing_stop_loss {
                debug!(
                    "trailing_stop triggered at ({}) : ret < last stop - trailing_stop_loss ( {} < {} - {} )",
                    now(),
                    ret,
                    *last_top,
                    self.trailing_stop_loss
                );
                return Some(StopEvent::TrailingStop);
            }
        }
        None
    }

    pub fn reset(&mut self) { self.last_top = None; }
}

#[cfg(test)]
mod test {
    use crate::position::PositionKind;
    use crate::stop::{FixedStopper, PositionStopper, StopEvent, TrailingStopper};

    #[test]
    fn test_fixed_stopper() {
        let stopper = FixedStopper::new(0.1, -0.1);
        assert_eq!(stopper.should_stop(0.2), Some(StopEvent::Gain));
        assert_eq!(stopper.should_stop(-0.2), Some(StopEvent::Loss));
        assert_eq!(stopper.should_stop(-0.01), None);
        assert_eq!(stopper.should_stop(0.01), None);
    }

    #[test]
    fn test_position_stopper() {
        let stopper = PositionStopper::new(100.0, PositionKind::Short);
        assert_eq!(stopper.should_stop(101.0), Some(StopEvent::Loss));
        assert_eq!(stopper.should_stop(99.0), None);
        let stopper = PositionStopper::new(100.0, PositionKind::Long);
        assert_eq!(stopper.should_stop(99.0), Some(StopEvent::Loss));
        assert_eq!(stopper.should_stop(101.0), None);
    }

    #[test]
    fn test_trailing_stopper() {
        let mut stopper = TrailingStopper::new(0.5, 0.01, -0.1);
        // reaches trailing stop start and then trail stops
        assert_eq!(stopper.should_stop(0.6), None);
        assert_eq!(stopper.should_stop(0.58), Some(StopEvent::TrailingStop));
        let mut stopper = TrailingStopper::new(0.5, 0.01, -0.1);
        // reaches the stop loss
        assert_eq!(stopper.should_stop(-0.2), Some(StopEvent::Loss));
    }
}
