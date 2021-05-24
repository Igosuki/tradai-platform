use chrono::Duration;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use parse_duration::parse;

#[derive(Clone, Debug, Deserialize)]
pub struct Options {
    pub left: Pair,
    pub right: Pair,
    pub exchange: Exchange,
    pub beta_eval_freq: i32,
    pub beta_sample_freq: String,
    pub window_size: i32,
    pub threshold_long: f64,
    pub threshold_short: f64,
    pub stop_loss: f64,
    pub stop_gain: f64,
    pub initial_cap: f64,
    pub dry_mode: Option<bool>,
}

impl Options {
    pub(super) fn beta_sample_freq(&self) -> Duration {
        Duration::from_std(parse(&self.beta_sample_freq).unwrap()).unwrap()
    }

    pub(super) fn dry_mode(&self) -> bool { self.dry_mode.unwrap_or(true) }
}
