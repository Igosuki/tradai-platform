use chrono::Duration;
use parse_duration::parse;

use coinnect_rt::prelude::*;

use crate::types::{ExecutionInstruction, OrderMode};

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
    /// Default is `OrdereMode::Limit`
    pub order_mode: Option<OrderMode>,
    /// Default is None
    pub execution_instruction: Option<ExecutionInstruction>,
    /// Default is `AssetType::Spot`
    pub order_asset_type: Option<AssetType>,
}

impl Options {
    pub(super) fn dry_mode(&self) -> bool { self.dry_mode.unwrap_or(true) }

    pub(super) fn dynamic_threshold(&self) -> bool { self.dynamic_threshold.unwrap_or(true) }

    pub(super) fn sample_freq(&self) -> Duration { Duration::from_std(parse(&self.sample_freq).unwrap()).unwrap() }

    pub(super) fn order_asset_type(&self) -> AssetType { self.order_asset_type.unwrap_or(AssetType::Spot) }
}
