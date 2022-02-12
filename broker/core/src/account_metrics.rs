use prometheus::{CounterVec, Opts};

use crate::exchange::Exchange;
use crate::metrics_util::MetricStore;

lazy_static! {
    static ref METRIC_STORE: MetricStore<Exchange, AccountMetrics> = { MetricStore::new() };
}

#[must_use]
pub fn metric_store() -> &'static MetricStore<Exchange, AccountMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

#[derive(Clone)]
pub struct AccountMetrics {
    stream_reconnects: CounterVec,
    send_errors: CounterVec,
}

impl AccountMetrics {
    #[must_use]
    pub fn for_exchange(xchg: Exchange) -> AccountMetrics {
        metric_store().get_or_create(xchg, || Self::new_metrics(xchg))
    }

    /// # Panics
    ///
    /// Panics if some metrics cannot register
    #[must_use]
    pub fn new_metrics(xchg: Exchange) -> AccountMetrics {
        let name = format!("{:?}", xchg);
        let stream_reconnect_opts = Opts::new(
            "account_stream_reconnects",
            "Total number of times the account stream restarted after a disconnection.",
        )
        .const_label("xchg", &name);
        let stream_reconnect_vec = CounterVec::new(stream_reconnect_opts, &[]).unwrap();
        let send_error_opts = Opts::new(
            "account_send_errors",
            "Total number of times the account stream restarted after a disconnection.",
        )
        .const_label("xchg", &name);
        let send_error_vec = CounterVec::new(send_error_opts, &[]).unwrap();

        prometheus::default_registry()
            .register(Box::new(stream_reconnect_vec.clone()))
            .unwrap();
        prometheus::default_registry()
            .register(Box::new(send_error_vec.clone()))
            .unwrap();
        AccountMetrics {
            stream_reconnects: stream_reconnect_vec,
            send_errors: send_error_vec,
        }
    }

    pub fn stream_reconnected(&self) { self.stream_reconnects.with_label_values(&[]).inc(); }

    pub fn send_error(&self) { self.send_errors.with_label_values(&[]).inc(); }
}
