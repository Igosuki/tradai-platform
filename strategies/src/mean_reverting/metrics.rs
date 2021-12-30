use std::collections::HashMap;

use prometheus::{CounterVec, GaugeVec, Registry};

use metrics::prelude::*;
use stats::indicators::macd_apo::MACDApo;
use trading::book::BookPosition;
use trading::position::{OperationKind, PositionKind};

type ModelIndicatorFn = (String, fn(&MACDApo) -> f64);

lazy_static! {
    static ref METRIC_STORE: MetricStore<String, MeanRevertingStrategyMetrics> = MetricStore::new();
}

pub fn metric_store() -> &'static MetricStore<String, MeanRevertingStrategyMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

#[derive(Clone)]
pub struct MeanRevertingStrategyMetrics {
    common_gauges: HashMap<String, GaugeVec>,
    model_indicator_fns: Vec<ModelIndicatorFn>,
    error_counter: CounterVec,
}

impl MeanRevertingStrategyMetrics {
    pub fn for_strat(registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        metric_store().get_or_create(pair.to_string(), || Self::new_metrics(registry, pair))
    }

    fn new_metrics(_registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();
        let const_labels = labels! {"pair" => pair};
        {
            let pos_labels = &["pos", "op"];
            let gauge_name = "mr_position";
            let gauge_vec = register_gauge_vec!(
                opts!(gauge_name, format!("gauge for {}", gauge_name), const_labels),
                pos_labels
            )
            .unwrap();
            gauges.insert(gauge_name.to_string(), gauge_vec);
        }

        {
            let gauge_name = "mr_mid";
            let gauge_vec =
                register_gauge_vec!(opts!(gauge_name, format!("gauge for {}", gauge_name), const_labels), &[
                ])
                .unwrap();
            gauges.insert(gauge_name.to_string(), gauge_vec);
        }

        let model_gauges: Vec<ModelIndicatorFn> = vec![("apo".to_string(), |x| x.apo)];
        let threshold_gauges: Vec<String> = vec!["mr_threshold_short".to_string(), "mr_threshold_long".to_string()];
        {
            for gauge_name in threshold_gauges.iter().chain(model_gauges.iter().map(|g| &g.0)) {
                let gauge_vec = register_gauge_vec!(
                    opts!(gauge_name, format!("gauge for {}", gauge_name.clone()), const_labels),
                    &[]
                )
                .unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
            }
        }

        let error_counter = register_counter_vec!(opts!("error", "Total number of process errors.", const_labels), &[
            "kind"
        ])
        .unwrap();

        MeanRevertingStrategyMetrics {
            common_gauges: gauges,
            model_indicator_fns: model_gauges.clone(),
            error_counter,
        }
    }

    pub(super) fn log_position(&self, pos_kind: PositionKind, op_kind: OperationKind, price: f64) {
        if let Some(g) = self.common_gauges.get("mr_position") {
            g.with_label_values(&[pos_kind.as_ref(), op_kind.as_ref()]).set(price);
        }
    }

    pub(super) fn log_thresholds(&self, threshold_short: f64, threshold_long: f64) {
        if let Some(g) = self.common_gauges.get("mr_threshold_short") {
            g.with_label_values(&[]).set(threshold_short);
        }
        if let Some(g) = self.common_gauges.get("mr_threshold_long") {
            g.with_label_values(&[]).set(threshold_long);
        }
    }
    pub(super) fn log_model(&self, model: &MACDApo) {
        for (gauge_name, model_gauge_fn) in &self.model_indicator_fns {
            if let Some(g) = self.common_gauges.get(gauge_name) {
                g.with_label_values(&[]).set(model_gauge_fn(model));
            }
        }
    }

    pub(super) fn log_pos(&self, pos: &BookPosition) {
        if let Some(g) = self.common_gauges.get("mr_mid") {
            g.with_label_values(&[]).set(pos.mid);
        };
    }

    pub(super) fn log_error(&self, label: &str) { self.error_counter.with_label_values(&[label]).inc(); }
}
