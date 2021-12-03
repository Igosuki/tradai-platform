use chrono::Duration;
use parse_duration::parse;

use coinnect_rt::prelude::*;
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

    pub fn new_test_default(exchange: Exchange, left_pair: Pair, right_pair: Pair) -> Self {
        Self {
            left: left_pair,
            right: right_pair,
            beta_eval_freq: 1000,
            beta_sample_freq: "1min".to_string(),
            window_size: 2000,
            exchange,
            threshold_long: -0.03,
            threshold_short: 0.03,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            order_conf: Default::default(),
            max_pos_duration: None,
        }
    }
}
