use std::collections::HashMap;

use prometheus::{labels, opts, register_counter_vec, register_int_gauge_vec, CounterVec, IntGaugeVec};

#[derive(Clone)]
pub struct FileLoggerMetrics {
    event_lag: IntGaugeVec,
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

    pub(super) fn event_lag(&self, millis: i64) { self.event_lag.with_label_values(&[]).set(millis) }

    pub(super) fn writer_acquisition_failure(&self) { self.writer_acquisition_failure.with_label_values(&[]).inc() }

    pub(super) fn flush_failure(&self) { self.flush_failure.with_label_values(&[]).inc() }

    pub(super) fn write_append_failure(&self) { self.write_append_failure.with_label_values(&[]).inc() }
}

lazy_static! {
    static ref LOGGER_METRICS: FileLoggerMetrics = FileLoggerMetrics::new();
}

pub fn metrics() -> &'static FileLoggerMetrics {
    lazy_static::initialize(&LOGGER_METRICS);
    &LOGGER_METRICS
}
