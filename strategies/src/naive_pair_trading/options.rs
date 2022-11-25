use std::collections::HashSet;

use chrono::Duration;
use itertools::Itertools;

use brokers::prelude::*;
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::StrategyKey;
use trading::types::OrderConf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Options {
    pub left: Pair,
    pub right: Pair,
    pub exchange: Exchange,
    pub beta_eval_freq: i32,
    #[serde(
        deserialize_with = "util::ser::string_duration_chrono",
        serialize_with = "util::ser::encode_duration_str"
    )]
    pub beta_sample_freq: Duration,
    pub window_size: i32,
    pub threshold_long: f64,
    pub threshold_short: f64,
    pub stop_loss: f64,
    pub stop_gain: f64,
    pub initial_cap: f64,
    pub order_conf: OrderConf,
    #[serde(
        deserialize_with = "util::ser::string_duration_chrono_opt",
        serialize_with = "util::ser::encode_duration_str_opt"
    )]
    pub max_pos_duration: Option<Duration>,
}

impl Options {
    pub fn new_test_default(exchange: Exchange, left_pair: Pair, right_pair: Pair) -> Self {
        Self {
            left: left_pair,
            right: right_pair,
            beta_eval_freq: 1000,
            beta_sample_freq: Duration::minutes(1),
            window_size: 2000,
            exchange,
            threshold_long: -0.03,
            threshold_short: 0.03,
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            order_conf: OrderConf::default(),
            max_pos_duration: None,
        }
    }

    pub(super) fn max_pos_duration(&self) -> Duration { self.max_pos_duration.unwrap_or_else(|| Duration::days(3)) }
}

impl StrategySettingsReplicator for Options {
    fn replicate_for_pairs(&self, pairs: HashSet<Pair>) -> Vec<serde_json::Value> {
        let pair_pairs = pairs.into_iter().permutations(2);
        pair_pairs
            .into_iter()
            .map(|pair_pair| {
                let mut new = self.clone();
                new.left = pair_pair[0].clone();
                new.right = pair_pair[1].clone();
                serde_json::to_value(new).unwrap()
            })
            .collect()
    }
}

impl StrategyOptions for Options {
    fn key(&self) -> StrategyKey { StrategyKey("naive_spread".to_string(), format!("{}_{}", self.left, self.right)) }
}
