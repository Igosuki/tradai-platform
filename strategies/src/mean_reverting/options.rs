use chrono::Duration;
use coinnect_rt::types::Pair;
use parse_duration::parse;

#[derive(Clone, Debug, Deserialize)]
pub struct Options {
    pub pair: Pair,
    pub short_window_size: usize,
    pub long_window_size: usize,
    pub dry_mode: Option<bool>,
    pub sample_freq: String,
    pub initial_cap: f64,
    pub threshold_short: f64,
    pub threshold_long: f64,
    pub stop_loss: f64,
    pub stop_gain: f64,
}

impl Options {
    pub(super) fn dry_mode(&self) -> bool {
        self.dry_mode.unwrap_or(true)
    }

    pub(super) fn sample_freq(&self) -> Duration {
        Duration::from_std(parse(&self.sample_freq).unwrap()).unwrap()
    }
}
