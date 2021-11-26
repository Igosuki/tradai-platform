use chrono::Duration;
use parse_duration::parse;

use coinnect_rt::prelude::*;
use trading::signal::ExecutionInstruction;
use trading::types::OrderMode;

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
    /// Default is `OrderMode::Limit`
    pub order_mode: Option<OrderMode>,
    /// Default is None
    pub execution_instruction: Option<ExecutionInstruction>,
    /// Default is `AssetType::Spot`
    pub order_asset_type: Option<AssetType>,
    /// Start trading after first start
    pub start_trading: Option<bool>,
}

impl Options {
    pub fn strat_key(&self) -> String { format!("{}_{}.{}", "mean_reverting", self.exchange, self.pair) }

    pub(super) fn dry_mode(&self) -> bool { self.dry_mode.unwrap_or(true) }

    pub(crate) fn dynamic_threshold(&self) -> bool { self.dynamic_threshold.unwrap_or(true) }

    pub(crate) fn sample_freq(&self) -> Duration { Duration::from_std(parse(&self.sample_freq).unwrap()).unwrap() }

    pub(super) fn order_asset_type(&self) -> AssetType { self.order_asset_type.unwrap_or(AssetType::Spot) }

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
            initial_cap: 100.0,
            dry_mode: Some(true),
            short_window_size: 100,
            long_window_size: 1000,
            sample_freq: "1min".to_string(),
            exchange,
            order_mode: None,
            execution_instruction: None,
            order_asset_type: None,
            start_trading: Some(true),
        }
    }
}
