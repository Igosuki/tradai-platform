use crate::types::BookPosition;
use chrono::{DateTime, Utc};
use math::moving_average::ExponentialMovingAverage;
use math::Next;
use log::Level::Trace;
use crate::models::Window;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinglePosRow {
    pub time: DateTime<Utc>,
    pub pos: BookPosition, // crypto_1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanRevertingModelValue {
    pub long_ema: ExponentialMovingAverage,
    pub short_ema: ExponentialMovingAverage,
    pub apo: f64,
}

impl MeanRevertingModelValue {
    pub fn new(long_window: u32, short_window: u32) -> MeanRevertingModelValue {
        MeanRevertingModelValue {
            long_ema: ExponentialMovingAverage::new(2.0, long_window).unwrap(),
            short_ema: ExponentialMovingAverage::new(2.0, short_window).unwrap(),
            apo: 0.0,
        }
    }
}

pub fn moving_average_apo(m: &MeanRevertingModelValue, row: &SinglePosRow) -> MeanRevertingModelValue {
    let mut m = m.clone();
    let long_ema = m.long_ema.next(row.pos.mid);
    let short_ema = m.short_ema.next(row.pos.mid);
    let apo = (short_ema - long_ema) / long_ema;
    if log_enabled!(Trace) {
        trace!("short_ema={},long_ema={}", short_ema, long_ema);
        trace!(
            "moving average : short {} long {} apo {}",
            short_ema,
            long_ema,
            apo
        );
    }
    m.apo = apo;
    m
}

pub fn threshold(_m: &f64, _rows: Window<f64>) -> f64 {
    0.0
}
