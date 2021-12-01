use chrono::Duration;
use coinnect_rt::prelude::*;
use parse_duration::parse;
use trading::types::OrderConf;

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
    pub order_conf: OrderConf,
    pub max_pos_duration: Option<String>,
}

impl Options {
    pub(super) fn beta_sample_freq(&self) -> Duration {
        Duration::from_std(parse(&self.beta_sample_freq).unwrap()).unwrap_or_else(|_| Duration::minutes(1))
    }

    pub(super) fn max_pos_duration(&self) -> Duration {
        self.max_pos_duration
            .as_ref()
            .and_then(|s| Duration::from_std(parse(s).unwrap()).ok())
            .unwrap_or_else(|| Duration::days(3))
    }
}
