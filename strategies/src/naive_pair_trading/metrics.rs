use std::collections::HashMap;

use prometheus::{GaugeVec, Opts, Registry};

use metrics::store::MetricStore;

use crate::types::OperationKind;

use super::covar_model::DataRow;
use super::state::MovingState;
use super::state::Position;

lazy_static! {
    static ref METRIC_STORE: MetricStore<(String, String), NaiveStrategyMetrics> = { MetricStore::new() };
}

pub fn metric_store() -> &'static MetricStore<(String, String), NaiveStrategyMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

type StateGauge = (String, fn(&MovingState) -> f64);

#[derive(Clone)]
pub struct NaiveStrategyMetrics {
    gauges: HashMap<String, GaugeVec>,
    state_gauges: Vec<StateGauge>,
}

impl NaiveStrategyMetrics {
    pub fn for_strat(registry: &Registry, left_pair: &str, right_pair: &str) -> NaiveStrategyMetrics {
        metric_store().get_or_create((left_pair.to_string(), right_pair.to_string()), || {
            Self::new_metrics(registry, left_pair, right_pair)
        })
    }

    fn new_metrics(registry: &Registry, left_pair: &str, right_pair: &str) -> NaiveStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();

        {
            let pos_labels = &["pair", "as", "pos", "op"];
            let pos_gauge_names = vec!["position"];
            for gauge_name in pos_gauge_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .namespace("naive_pair_trading")
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        {
            let pos_labels = &[];
            let right_mid_names = vec!["left_mid", "right_mid"];
            for gauge_name in right_mid_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .namespace("naive_pair_trading")
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        let state_gauges: Vec<StateGauge> = vec![
            ("return".to_string(), |x| {
                x.short_position_return() + x.long_position_return()
            }),
            ("predicted_right".to_string(), |x| x.predicted_right()),
            ("relative_spread".to_string(), |x| x.res()),
            ("pnl".to_string(), |x| x.pnl()),
            ("nominal_position".to_string(), |x| x.nominal_position()),
            ("beta".to_string(), |x| x.beta_lr()),
        ];
        {
            let pos_labels = &[];
            for (gauge_name, _) in state_gauges.clone() {
                let string = format!("state gauge for {}", gauge_name.clone());
                let gauge_vec_opts = Opts::new(&gauge_name, &string)
                    .namespace("naive_pair_trading")
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        NaiveStrategyMetrics {
            gauges,
            state_gauges: state_gauges.clone(),
        }
    }

    pub(super) fn log_position(&self, pos: &Position, op: &OperationKind) {
        if let Some(g) = self.gauges.get("position") {
            g.with_label_values(&[&pos.left_pair, "left", pos.kind.as_ref(), op.as_ref()])
                .set(pos.left_price);
            g.with_label_values(&[&pos.right_pair, "right", pos.kind.as_ref(), op.as_ref()])
                .set(pos.right_price);
        }
    }

    pub(super) fn log_state(&self, state: &MovingState) {
        for (state_gauge_name, state_gauge_fn) in &self.state_gauges {
            if let Some(g) = self.gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }

    pub(super) fn log_row(&self, lr: &DataRow) {
        if let Some(g) = self.gauges.get("left_mid") {
            g.with_label_values(&[]).set(lr.left.mid)
        };
        if let Some(g) = self.gauges.get("right_mid") {
            g.with_label_values(&[]).set(lr.right.mid)
        };
    }
}
