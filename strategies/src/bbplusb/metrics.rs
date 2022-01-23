use std::collections::HashMap;

use prometheus::{CounterVec, GaugeVec, Registry};

use metrics::prelude::*;
use stats::indicators::macd_apo::MACDApo;
use stats::{BollingerBands, BollingerBandsOutput, Next};
use trading::book::BookPosition;
use trading::position::{OperationKind, PositionKind};

type ModelIndicatorFn = (String, fn(&BollingerBandsOutput) -> f64);

lazy_static! {
    static ref METRIC_STORE: MetricStore<String, BBPlusMetrics> = MetricStore::new();
}

pub fn metric_store() -> &'static MetricStore<String, BBPlusMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

#[derive(Clone)]
pub struct BBPlusMetrics {
    common_gauges: HashMap<String, GaugeVec>,
    model_indicator_fns: Vec<ModelIndicatorFn>,
}

impl BBPlusMetrics {
    pub fn for_strat(registry: &Registry, pair: &str) -> BBPlusMetrics {
        metric_store().get_or_create(pair.to_string(), || Self::new_metrics(registry, pair))
    }

    fn new_metrics(_registry: &Registry, pair: &str) -> BBPlusMetrics {
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

        let model_gauges: Vec<ModelIndicatorFn> = vec![
            ("bbpb_sma".to_string(), |x| x.average),
            ("bbpb_lower".to_string(), |x| x.lower),
            ("bbpb_upper".to_string(), |x| x.upper),
        ];
        let other_gauges: Vec<String> = vec![
            "bbpb_price".to_string(),
            "bbpb_ob".to_string(),
            "bbpb_ob_close".to_string(),
            "bbpb_os".to_string(),
            "bbpb_os_close".to_string(),
            "bbpb_ppo".to_string(),
        ];
        {
            for gauge_name in other_gauges.iter().chain(model_gauges.iter().map(|g| &g.0)) {
                let gauge_vec = register_gauge_vec!(
                    opts!(gauge_name, format!("gauge for {}", gauge_name.clone()), const_labels),
                    &[]
                )
                .unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
            }
        }

        BBPlusMetrics {
            common_gauges: gauges,
            model_indicator_fns: model_gauges.clone(),
        }
    }

    pub(super) fn log_constants(&self, ob: f64, ob_close: f64, os: f64, os_close: f64) {
        if let Some(g) = self.common_gauges.get("bbpb_ob") {
            g.with_label_values(&[]).set(ob);
        }
        if let Some(g) = self.common_gauges.get("bbpb_ob_close") {
            g.with_label_values(&[]).set(ob_close);
        }
        if let Some(g) = self.common_gauges.get("bbpb_os") {
            g.with_label_values(&[]).set(os);
        }
        if let Some(g) = self.common_gauges.get("bbpb_ob_close") {
            g.with_label_values(&[]).set(os_close);
        }
    }

    pub(super) fn log_model(&self, model: &BollingerBandsOutput, ppo: f64) {
        for (gauge_name, model_gauge_fn) in &self.model_indicator_fns {
            if let Some(g) = self.common_gauges.get(gauge_name) {
                g.with_label_values(&[]).set(model_gauge_fn(model));
            }
        }
        if let Some(g) = self.common_gauges.get("bbpb_ppo") {
            g.with_label_values(&[]).set(ppo);
        }
    }

    pub(super) fn log_price(&self, price: f64) {
        if let Some(g) = self.common_gauges.get("bbplus_price_mid") {
            g.with_label_values(&[]).set(price);
        };
    }
}
