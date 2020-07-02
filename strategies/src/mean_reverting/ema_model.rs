use crate::model::BookPosition;
use crate::ob_double_window_model::DoubleWindowTable;
use chrono::{DateTime, Utc};
use math::iter::MeanExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinglePosRow {
    pub time: DateTime<Utc>,
    pub pos: BookPosition, // crypto_1
}

pub fn moving_average(i: &DoubleWindowTable<SinglePosRow>, window_size: usize) -> f64 {
    let mean = i.window(window_size).map(|l| l.pos.mid).mean();
    trace!("mean {}", mean);
    mean
}
