use std::collections::HashMap;

use prometheus::{histogram_opts, labels, opts, register_counter_vec, register_histogram_vec, register_int_gauge_vec,
                 CounterVec, HistogramVec, IntGaugeVec};

pub const EVENT_LAG_BUCKETS: &[f64; 11] = &[
    1.0, 5.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

#[derive(Clone)]
pub struct FileLoggerMetrics {
    event_lag: IntGaugeVec,
    event_lag_hist: HistogramVec,
    writer_acquisition_failure: CounterVec,
    flush_failure: CounterVec,
    write_append_failure: CounterVec,
}

impl FileLoggerMetrics {
    fn new() -> Self {
        let const_labels: HashMap<&str, &str> = labels! {};
        let pos_labels = &[];
        Self {
            event_lag: register_int_gauge_vec!(
                opts!("event_lag", "current time vs event time", const_labels),
                pos_labels
            )
            .unwrap(),
            event_lag_hist: register_histogram_vec!(
                histogram_opts!(
                    "event_lag_hist",
                    "current time vs event time",
                    EVENT_LAG_BUCKETS.to_vec()
                ),
                pos_labels
            )
            .unwrap(),
            writer_acquisition_failure: register_counter_vec!(
                opts!(
                    "writer_acquisition_failure",
                    "failure to acquire a writer for an event",
                    const_labels
                ),
                pos_labels
            )
            .unwrap(),
            flush_failure: register_counter_vec!(
                opts!("flush_failure", "failure to flush the writer", const_labels),
                pos_labels
            )
            .unwrap(),
            write_append_failure: register_counter_vec!(
                opts!(
                    "write_append_failure",
                    "failure to append a value to the writer",
                    const_labels
                ),
                pos_labels
            )
            .unwrap(),
        }
    }

    pub(crate) fn event_lag(&self, millis: i64) {
        self.event_lag.with_label_values(&[]).set(millis);
        self.event_lag_hist.with_label_values(&[]).observe(millis as f64);
    }

    pub(crate) fn writer_acquisition_failure(&self) { self.writer_acquisition_failure.with_label_values(&[]).inc() }

    pub(crate) fn flush_failure(&self) { self.flush_failure.with_label_values(&[]).inc() }

    pub(crate) fn write_append_failure(&self) { self.write_append_failure.with_label_values(&[]).inc() }
}

lazy_static! {
    static ref LOGGER_METRICS: FileLoggerMetrics = FileLoggerMetrics::new();
}

pub fn metrics() -> &'static FileLoggerMetrics {
    lazy_static::initialize(&LOGGER_METRICS);
    &LOGGER_METRICS
}
