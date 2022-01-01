/*!
Defines how to store application metrics

# Overview

Mostly just plumbing code to interact with the `prometheus` library

 */

#![allow(
    // noisy
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::let_underscore_drop,
    clippy::needless_pass_by_value
)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tracing;

use std::collections::HashMap;

use prometheus::GaugeVec;

pub mod error;
pub mod prom;
pub mod store;
pub mod prelude {
    pub use super::store::MetricStore;
    pub use super::{MetricGaugeProvider, MetricProviderFn};
}

pub type MetricProviderFn<T> = (String, fn(&T) -> f64);

pub trait MetricGaugeProvider<T> {
    fn gauges(&self) -> &HashMap<String, GaugeVec>;
    fn log_all_with_providers(&self, gauges: &[MetricProviderFn<T>], v: &T, labels: &[&str]) {
        for (gauge_name, gauge_fn) in gauges {
            if let Some(g) = self.gauges().get(gauge_name) {
                g.with_label_values(labels).set(gauge_fn(v));
            }
        }
    }
}

#[allow(clippy::implicit_hasher)]
pub fn make_gauges<T>(
    const_labels: HashMap<&str, &str>,
    labels: &[&str],
    pfns: &[MetricProviderFn<T>],
) -> HashMap<String, GaugeVec> {
    pfns.iter()
        .map(|(gauge_name, _)| {
            (
                gauge_name.clone(),
                register_gauge_vec!(
                    opts!(gauge_name, format!("gauge for {}", gauge_name), const_labels),
                    labels
                )
                .unwrap(),
            )
        })
        .collect()
}
