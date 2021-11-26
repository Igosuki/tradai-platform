use std::collections::HashMap;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use prometheus::{default_registry, CounterVec, GaugeVec, Registry};

lazy_static! {
    static ref METRICS: GenericDriverMetrics = { GenericDriverMetrics::new(default_registry()) };
}

#[derive(Clone)]
pub struct GenericDriverMetrics {
    lock_counters: GaugeVec,
    signal_errors: CounterVec,
    errors: CounterVec,
}

impl GenericDriverMetrics {
    fn new(_registry: &Registry) -> Self {
        let const_labels: HashMap<&str, &str> = HashMap::new();
        let lock_counters = {
            let pos_labels = &["xch", "pair"];
            let vec_name = "dr_pos_lock";
            register_gauge_vec!(
                opts!(vec_name, format!("gauge for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let signal_errors = {
            let pos_labels = &["xch", "pair"];
            let vec_name = "dr_sig_err";
            register_counter_vec!(
                opts!(vec_name, format!("counter for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let errors = {
            let pos_labels = &["err"];
            let vec_name = "dr_all_err";
            register_counter_vec!(
                opts!(vec_name, format!("counter for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        Self {
            lock_counters,
            signal_errors,
            errors,
        }
    }

    pub(super) fn log_lock(&self, xch: &Exchange, pair: &Pair) {
        self.lock_counters
            .with_label_values(&[xch.as_ref(), pair.as_ref()])
            .set(1.0);
    }

    pub(super) fn signal_error(&self, xch: &Exchange, pair: &Pair) {
        self.signal_errors
            .with_label_values(&[xch.as_ref(), pair.as_ref()])
            .inc();
    }

    pub(super) fn log_error(&self, e: &str) { self.errors.with_label_values(&[e]).inc(); }
}

pub fn get() -> &'static GenericDriverMetrics { &METRICS }
