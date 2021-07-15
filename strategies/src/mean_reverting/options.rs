use crate::types::OrderMode;
use chrono::Duration;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use parse_duration::parse;

#[derive(Clone, Debug, Deserialize)]
pub struct Options {
    pub pair: Pair,
    pub short_window_size: u32,
    pub long_window_size: u32,
    pub dry_mode: Option<bool>,
    pub sample_freq: String,
    pub initial_cap: f64,
    pub threshold_short: f64,
    pub threshold_long: f64,
    pub threshold_eval_freq: Option<i32>,
    pub dynamic_threshold: Option<bool>,
    pub threshold_window_size: Option<usize>,
    pub stop_loss: f64,
    pub stop_gain: f64,
    pub exchange: Exchange,
    pub order_mode: OrderMode,
}

impl Options {
    pub(super) fn dry_mode(&self) -> bool { self.dry_mode.unwrap_or(true) }

    pub(super) fn dynamic_threshold(&self) -> bool { self.dynamic_threshold.unwrap_or(true) }

    pub(super) fn sample_freq(&self) -> Duration { Duration::from_std(parse(&self.sample_freq).unwrap()).unwrap() }
}
