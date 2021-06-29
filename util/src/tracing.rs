use opentelemetry::KeyValue;
use std::collections::HashMap;
use tracing::Level;
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_timing::group::ByName;
use tracing_timing::LayerDowncaster;

pub fn setup_flame_subscriber() -> impl Drop {
    let (flame_layer, _guard) = tracing_flame::FlameLayer::with_file("./tracing.folded").unwrap();
    tracing_subscriber::registry().with(flame_layer).init();
    _guard
}

fn tracing_log_subscriber() -> Subscriber {
    let collector = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(Level::INFO)
        // build but do not install the subscriber.
        .finish();
    collector
}

///  ```rust
///  fn do_stuff() {
///     let sid = setup_timing_subscriber();
///     // Compute things...
///     print_timings(sid);
///  }
/// ```
pub fn setup_timing_subscriber() -> LayerDowncaster<ByName, ByName> {
    let timing_subscriber = tracing_timing::Builder::default()
        .events(tracing_timing::group::ByName)
        .layer(|| tracing_timing::Histogram::new_with_max(1_000_000, 2).unwrap());
    let sid = timing_subscriber.downcaster();
    let l = tracing_log_subscriber().with(timing_subscriber);
    l.init();
    sid
}

pub fn print_timings(sid: LayerDowncaster<ByName, ByName>) {
    tracing::dispatcher::get_default(|dispatcher| {
        let layer = sid.downcast(dispatcher).unwrap();
        layer.force_synchronize();
        layer.with_histograms(|hs| {
            let hkeys: Vec<String> = hs.keys().map(|k| k.to_string()).collect();
            for hkey in hkeys {
                let events_hs = hs.get_mut(hkey.as_str()).unwrap();
                let keys: Vec<String> = events_hs.keys().map(|k| k.to_string()).collect();
                for k in keys {
                    let h = &events_hs[k.as_str()];
                    println!(
                        " for {} in {} : mean: {:.1}µs, p50: {}µs, p90: {}µs, p99: {}µs, p999: {}µs, max: {}µs",
                        k,
                        hkey,
                        h.mean() / 1000.0,
                        h.value_at_quantile(0.5) / 1_000,
                        h.value_at_quantile(0.9) / 1_000,
                        h.value_at_quantile(0.99) / 1_000,
                        h.value_at_quantile(0.999) / 1_000,
                        h.max() / 1_000,
                    );
                }
            }
        });
    });
}

pub fn setup_opentelemetry(agent_endpoints: String, service_name: String, tags: HashMap<String, String>) {
    let tags: Vec<KeyValue> = tags.into_iter().map(|(k, v)| KeyValue::new(k, v)).collect();
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name(service_name)
        .with_agent_endpoint(agent_endpoints)
        .with_tags(tags)
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
