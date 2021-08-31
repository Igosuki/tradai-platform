use crate::models::{IndicatorModel, Window};
use db::Storage;
use itertools::Itertools;
use math::indicators::macd_apo::MACDApo;
use math::iter::QuantileExt;
use ordered_float::OrderedFloat;
use std::cmp::{max, min};
use std::sync::Arc;

pub fn ema_indicator_model(
    pair: &str,
    db: Arc<dyn Storage>,
    short_window_size: u32,
    long_window_size: u32,
) -> IndicatorModel<MACDApo, f64> {
    let init = MACDApo::new(long_window_size, short_window_size);
    IndicatorModel::new(&format!("model_{}", pair), db, init)
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
pub struct ApoThresholds {
    short_0: f64,
    long_0: f64,
    long: f64,
    short: f64,
}

pub fn threshold(m: &ApoThresholds, wdw: Window<'_, f64>) -> ApoThresholds {
    let (threshold_short_iter, threshold_long_iter) = wdw.tee();
    let threshold_short = max(m.short_0.into(), OrderedFloat(threshold_short_iter.quantile(0.99))).into();
    let threshold_long = min(OrderedFloat(m.long), OrderedFloat(threshold_long_iter.quantile(0.01))).into();
    ApoThresholds {
        short: threshold_short,
        long: threshold_long,
        ..*m
    }
}
