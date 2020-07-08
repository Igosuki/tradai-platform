use crate::mean_reverting::PosAndApo;
use crate::model::BookPosition;
use crate::ob_double_window_model::DoubleWindowTable;
use chrono::{DateTime, Utc};
use math::iter::{MovingAvgExt, MovingAvgType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinglePosRow {
    pub time: DateTime<Utc>,
    pub pos: BookPosition, // crypto_1
}

pub fn moving_average_apo(i: &DoubleWindowTable<PosAndApo>) -> f64 {
    let long_ema: f64 = i
        .window(i.long_window_size)
        .map(|spr| spr.row.pos.mid)
        .moving_avg(MovingAvgType::Exponential(2));
    let short_ema: f64 = i
        .window(i.short_window_size)
        .map(|spr| spr.row.pos.mid)
        .moving_avg(MovingAvgType::Exponential(2));
    let apo = (short_ema - long_ema) / long_ema;
    debug!(
        "moving average : short {} long {} apo {}",
        short_ema, long_ema, apo
    );
    apo
}
