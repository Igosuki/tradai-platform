#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;

use prometheus::GaugeVec;
use std::collections::HashMap;

pub mod error;
pub mod prom;
pub mod store;

pub type MetricProviderFn<T> = (String, fn(&T) -> f64);

pub trait MetricGaugeProvider<T> {
    fn gauges(&self) -> &HashMap<String, GaugeVec>;
    fn log_all_with_providers(&self, gauges: &[MetricProviderFn<T>], v: &T) {
        for (gauge_name, gauge_fn) in gauges {
            if let Some(g) = self.gauges().get(gauge_name) {
                g.with_label_values(&[]).set(gauge_fn(v))
            }
        }
    }
}
