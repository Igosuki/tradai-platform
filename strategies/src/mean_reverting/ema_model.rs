use crate::models::{IndicatorModel, Window};
use db::Storage;
use math::indicators::macd_apo::MACDApo;
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

pub fn threshold(_m: &f64, _rows: Window<f64>) -> f64 { 0.0 }
