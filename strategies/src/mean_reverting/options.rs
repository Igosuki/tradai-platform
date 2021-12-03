use chrono::Duration;
use parse_duration::parse;

use coinnect_rt::prelude::*;
use trading::types::OrderConf;

#[derive(Clone, Debug, Deserialize)]
pub struct Options {
    pub pair: Pair,
    pub short_window_size: u32,
    pub long_window_size: u32,
    pub sample_freq: String,
    pub threshold_short: f64,
    pub threshold_long: f64,
    pub threshold_eval_freq: Option<i32>,
    pub dynamic_threshold: Option<bool>,
    pub threshold_window_size: Option<usize>,
    pub stop_loss: f64,
    pub stop_gain: f64,
    pub exchange: Exchange,
    pub order_conf: OrderConf,
}

impl Options {
    pub fn strat_key(&self) -> String { format!("{}_{}.{}", "mean_reverting", self.exchange, self.pair) }

    pub(crate) fn dynamic_threshold(&self) -> bool { self.dynamic_threshold.unwrap_or(true) }

    pub(crate) fn sample_freq(&self) -> Duration { Duration::from_std(parse(&self.sample_freq).unwrap()).unwrap() }

    #[cfg(test)]
    pub(crate) fn new_test_default(pair: &str, exchange: Exchange) -> Self {
        Self {
            pair: pair.into(),
            threshold_long: -0.01,
            threshold_short: 0.01,
            threshold_eval_freq: Some(1),
            dynamic_threshold: Some(true),
            threshold_window_size: Some(10000),
            stop_loss: -0.1,
            stop_gain: 0.075,
            short_window_size: 100,
            long_window_size: 1000,
            sample_freq: "1min".to_string(),
            exchange,
            order_conf: OrderConf::default(),
        }
    }
}
