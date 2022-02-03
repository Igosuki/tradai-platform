use std::collections::HashMap;

use prometheus::{CounterVec, GaugeVec, Opts, Registry};

use metrics::prelude::*;
use portfolio::portfolio::Portfolio;
use trading::position::Position;

use crate::naive_pair_trading::covar_model::LinearModelValue;

use super::covar_model::DualBookPosition;

lazy_static! {
    static ref METRIC_STORE: MetricStore<(String, String), NaiveStrategyMetrics> = MetricStore::new();
}

pub fn metric_store() -> &'static MetricStore<(String, String), NaiveStrategyMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

type PortfolioIndicatorFn = MetricProviderFn<Portfolio>;
type PositionIndicatorFn = MetricProviderFn<Position>;
type ModelIndicatorFn = MetricProviderFn<LinearModelValue>;

static RELATIVE_SPREAD: &str = "relative_spread";
static SPREAD_RATIO: &str = "spread_ratio";
static PREDICTED_RIGHT: &str = "predicted_right";
static LEFT_MID: &str = "left_mid";
static RIGHT_MID: &str = "right_mid";

#[derive(Clone)]
pub struct NaiveStrategyMetrics {
    gauges: HashMap<String, GaugeVec>,
    model_indicator_fns: Vec<ModelIndicatorFn>,
    error_counter: CounterVec,
}

impl NaiveStrategyMetrics {
    pub fn for_strat(registry: &Registry, left_pair: &str, right_pair: &str) -> NaiveStrategyMetrics {
        metric_store().get_or_create((left_pair.to_string(), right_pair.to_string()), || {
            Self::new_metrics(registry, left_pair, right_pair)
        })
    }

    fn new_metrics(registry: &Registry, left_pair: &str, right_pair: &str) -> NaiveStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();
        let mut const_labels = HashMap::new();
        const_labels.insert("left_pair".to_string(), left_pair.to_string());
        const_labels.insert("right_pair".to_string(), right_pair.to_string());
        {
            let pos_labels = &["pair", "as", "pos", "op"];
            let pos_gauge_names = vec!["position"];
            for gauge_name in pos_gauge_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .namespace("naive_pair_trading")
                    .const_labels(const_labels.clone());
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        {
            let pos_labels = &[];
            let right_mid_names = vec![LEFT_MID, RIGHT_MID, RELATIVE_SPREAD, PREDICTED_RIGHT, SPREAD_RATIO];
            for gauge_name in right_mid_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .namespace("naive_pair_trading")
                    .const_labels(const_labels.clone());
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        let portfolio_gauges: Vec<PortfolioIndicatorFn> = vec![
            ("value_strat".to_string(), Portfolio::value),
            ("pnl".to_string(), Portfolio::pnl),
        ];
        let position_gauges: Vec<PositionIndicatorFn> = vec![
            ("return".to_string(), |x| x.unreal_profit_loss),
            ("traded_price".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.price).unwrap_or(0.0)
            }),
            ("nominal_position".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.base_qty).unwrap_or(0.0)
            }),
        ];
        let model_gauges: Vec<ModelIndicatorFn> =
            vec![("beta".to_string(), |x| x.beta), ("alpha".to_string(), |x| x.alpha)];
        {
            let pos_labels = &[];
            for gauge_name in portfolio_gauges
                .iter()
                .map(|v| v.0.as_str())
                .chain(position_gauges.iter().map(|v| v.0.as_str()))
                .chain(model_gauges.iter().map(|v| v.0.as_str()))
            {
                let string = format!("state gauge for {}", gauge_name);
                let gauge_vec_opts = Opts::new(gauge_name, &string)
                    .namespace("naive_pair_trading")
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        let const_labels = labels! {"left_pair" => left_pair, "right_pair" => right_pair};
        let error_counter = register_counter_vec!(
            opts!("naive_strat_error", "Total number of process errors.", const_labels),
            &["kind"]
        )
        .unwrap();

        NaiveStrategyMetrics {
            gauges,
            model_indicator_fns: model_gauges.clone(),
            error_counter,
        }
    }

    pub(super) fn log_mid_price(&self, lr: &DualBookPosition) {
        if let Some(g) = self.gauges.get(LEFT_MID) {
            g.with_label_values(&[]).set(lr.left.mid);
        };
        if let Some(g) = self.gauges.get(RIGHT_MID) {
            g.with_label_values(&[]).set(lr.right.mid);
        };
    }

    pub(super) fn log_ratio(&self, spread: f64) {
        if let Some(g) = self.gauges.get(SPREAD_RATIO) {
            g.with_label_values(&[]).set(spread);
        };
    }

    pub(super) fn relative_spread(&self, spread: f64) {
        if let Some(g) = self.gauges.get(RELATIVE_SPREAD) {
            g.with_label_values(&[]).set(spread);
        };
    }

    pub(super) fn log_model(&self, model: &LinearModelValue) {
        self.log_all_with_providers(&self.model_indicator_fns, model, &[]);
    }

    pub(super) fn log_error(&self, label: &str) { self.error_counter.with_label_values(&[label]).inc(); }
}

impl MetricGaugeProvider<LinearModelValue> for NaiveStrategyMetrics {
    fn gauges(&self) -> &HashMap<String, GaugeVec> { &self.gauges }
}
