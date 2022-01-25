use hdrhistogram::{Counter, Histogram};
use opentelemetry::sdk::trace::Config;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use std::collections::HashMap;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// # Panics
///
/// Panics if the flame file cannot be created at './tracing.folded'
#[must_use]
pub fn setup_flame_subscriber() -> impl Drop {
    let (flame_layer, guard) = tracing_flame::FlameLayer::with_file("./tracing.folded").unwrap();
    tracing_subscriber::registry().with(flame_layer).init();
    guard
}

#[cfg(feature = "console_tracing")]
pub fn init_console_subscriber() {
    let console_layer = console_subscriber::spawn();
    let env_filter = tracing_subscriber::EnvFilter::from_default_env();
    tracing_subscriber::registry()
        .with(console_layer)
        .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .init();
}

pub fn init_tracing_env_subscriber() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .finish()
        .init();
}

// fn tracing_log_subscriber() -> Subscriber {
//     tracing_subscriber::fmt()
//         // filter spans/events with level TRACE or higher.
//         .with_max_level(Level::INFO)
//         // build but do not install the subscriber.
//         .finish()
// }

// use tracing_timing::group::ByName;
// use tracing_timing::{Histogram, LayerDowncaster};
// ///  ```rust
// ///  use util::trace::setup_timing_subscriber;
// /// fn do_stuff() {
// ///     let sid = setup_timing_subscriber();
// ///     // Compute things...
// ///     print_timings(sid);
// ///  }
// /// ```
// #[must_use]
// #[allow(clippy::missing_panics_doc)]
// pub fn setup_timing_subscriber() -> LayerDowncaster<ByName, ByName> {
//     let timing_subscriber = tracing_timing::Builder::default()
//         .events(tracing_timing::group::ByName)
//         .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
//     let sid = timing_subscriber.downcaster();
//     let l = tracing_log_subscriber().with(timing_subscriber);
//     l.init();
//     sid
// }

// #[allow(clippy::missing_panics_doc)]
// pub fn print_timings(sid: LayerDowncaster<ByName, ByName>) {
//     tracing::dispatcher::get_default(|dispatcher| {
//         let layer = sid.downcast(dispatcher).unwrap();
//         layer.force_synchronize();
//         layer.with_histograms(|hs| {
//             for (hkey, events_hs) in hs.iter_mut() {
//                 let keys: Vec<String> = events_hs.keys().map(ToString::to_string).collect();
//                 for k in keys {
//                     let h = &events_hs[k.as_str()];
//                     println!(" for {} in {} : {}", k, hkey, display_hist_percentiles(h));
//                 }
//             }
//         });
//     });
// }

#[must_use]
pub fn display_hist_percentiles<T: Counter>(h: &Histogram<T>) -> String {
    format!(
        "count: {}, mean: {:.1}µs, p50: {}µs, p90: {}µs, p99: {}µs, p999: {}µs, max: {}µs",
        h.len(),
        h.mean() / 1000.0,
        h.value_at_quantile(0.5) / 1_000,
        h.value_at_quantile(0.9) / 1_000,
        h.value_at_quantile(0.99) / 1_000,
        h.value_at_quantile(0.999) / 1_000,
        h.max() / 1_000,
    )
}

#[must_use]
#[allow(clippy::missing_panics_doc)]
pub fn microtime_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_max(60 * 60 * 1000 * 1000 * 1000, 2).unwrap()
}

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn microtime_percentiles<T: Counter>(h: &Histogram<T>) -> HashMap<String, f64> {
    hashmap! {
        "mean".to_string() => h.mean() / 1000.0,
        "median".to_string() => h.value_at_quantile(0.5) as f64 / 1_000_f64,
        "p90".to_string() => h.value_at_quantile(0.9) as f64 / 1_000_f64,
        "p99".to_string() =>h.value_at_quantile(0.99) as f64 / 1_000_f64,
        "p999".to_string() =>h.value_at_quantile(0.999) as f64 / 1_000_f64,
        "max".to_string() => h.max() as f64 / 1_000_f64,
        "count".to_string() => h.len() as f64
    }
}

/// # Panics
///
/// Panics if the open telemetry pipeline cannot be created
pub fn setup_opentelemetry(agent_endpoints: String, service_name: String, tags: HashMap<String, String>) {
    let tags: Vec<KeyValue> = tags.into_iter().map(|(k, v)| KeyValue::new(k, v)).collect();
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        .with_agent_endpoint(agent_endpoints)
        .with_trace_config(Config::default().with_resource(Resource::new(tags)))
        .install_simple()
        .unwrap();
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .finish()
        .with(opentelemetry)
        .try_init()
        .unwrap();
}
