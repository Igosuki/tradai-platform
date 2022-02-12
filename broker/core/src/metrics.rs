use prometheus::{CounterVec, GaugeVec, IntCounterVec, Opts, Registry};
use strum_macros::AsRefStr;

use crate::exchange::Exchange;
use crate::metrics_util::MetricStore;

lazy_static! {
    static ref METRIC_STORE: MetricStore<Exchange, ExchangeMetrics> = { MetricStore::new() };
}

#[must_use]
pub fn metric_store() -> &'static MetricStore<Exchange, ExchangeMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

#[derive(Clone)]
pub struct ExchangeMetrics {
    broadcasts: GaugeVec,
    broadcast_failures: CounterVec,
    stream_reconnects: CounterVec,
    top_bid: GaugeVec,
    low_ask: GaugeVec,
    in_unsupported_pair: CounterVec,
    subscription_failures: IntCounterVec,
}

impl ExchangeMetrics {
    #[must_use]
    pub fn for_exchange(xchg: Exchange) -> ExchangeMetrics {
        metric_store().get_or_create(xchg, || Self::new_metrics(xchg))
    }

    #[must_use]
    pub fn new_metrics(xchg: Exchange) -> ExchangeMetrics {
        let name = format!("{:?}", xchg);
        let labels = &["pair", "channel"];
        let const_labels = labels! {"xchg" => &name};
        let gauge_vec = register_gauge_vec!(
            opts!("event_broadcast", "Broadcasted events rate gauge", const_labels),
            labels
        )
        .unwrap();
        let broadcast_failure_vec = register_counter_vec!(
            opts!(
                "broadcast_failures",
                "Total number of times the stream failed to broadcast a live event.",
                const_labels
            ),
            labels
        )
        .unwrap();
        let subscription_failure_vec = register_int_counter_vec!(
            opts!(
                "subscription_failures",
                "Total number of times the stream failed to subscribe.",
                const_labels
            ),
            labels
        )
        .unwrap();
        let stream_reconnect_vec = register_counter_vec!(
            opts!(
                "stream_reconnects",
                "Total number of times the stream restarted after a disconnection.",
                const_labels
            ),
            &[]
        )
        .unwrap();
        let in_unsupported_pair = register_counter_vec!(
            opts!(
                "in_unsupported_pairs",
                "Number of times an unsupported pair was seen in input",
                const_labels
            ),
            labels
        )
        .unwrap();
        let top_bid_vec =
            register_gauge_vec!(opts!("top_bid", "The latest registered top bid.", const_labels), labels).unwrap();
        let low_ask_vec = register_gauge_vec!(
            opts!("low_ask", "The latest registered lowest ask.", const_labels),
            labels
        )
        .unwrap();
        ExchangeMetrics {
            broadcasts: gauge_vec,
            broadcast_failures: broadcast_failure_vec,
            stream_reconnects: stream_reconnect_vec,
            top_bid: top_bid_vec,
            low_ask: low_ask_vec,
            in_unsupported_pair,
            subscription_failures: subscription_failure_vec,
        }
    }

    pub fn event_broadcasted(&self, pair: &str, channel: &str) {
        self.broadcasts.with_label_values(&[pair, channel]).inc();
    }

    pub fn broadcast_failure(&self, pair: &str, channel: &str) {
        self.broadcast_failures.with_label_values(&[pair, channel]).inc();
    }

    pub fn subscription_failure(&self, pair: &str, channel: &str) {
        self.subscription_failures.with_label_values(&[pair, channel]).inc();
    }

    pub fn in_unsupported_pair(&self, pair: &str, channel: &str) {
        self.in_unsupported_pair.with_label_values(&[pair, channel]).inc();
    }

    pub fn stream_reconnected(&self) { self.stream_reconnects.with_label_values(&[]).inc(); }

    pub fn top_bid(&self, price: f64, _volume: f64, pair: &str, channel: &str) {
        self.top_bid.with_label_values(&[pair, channel]).set(price);
    }

    pub fn lowest_ask(&self, price: f64, _volume: f64, pair: &str, channel: &str) {
        self.low_ask.with_label_values(&[pair, channel]).set(price);
    }
}

lazy_static! {
    static ref WS_METRIC_STORE: MetricStore<String, WsStreamMetrics> = { MetricStore::new() };
}

#[must_use]
pub fn ws_metric_store() -> &'static MetricStore<String, WsStreamMetrics> {
    lazy_static::initialize(&WS_METRIC_STORE);
    &WS_METRIC_STORE
}

#[derive(AsRefStr, Copy, Clone)]
pub enum WsStreamLifecycleEvent {
    #[strum(serialize = "started")]
    Started,
    #[strum(serialize = "stopped")]
    Stopped,
    #[strum(serialize = "restarting")]
    Restarting,
    #[strum(serialize = "finished")]
    Finished,
    #[strum(serialize = "connected")]
    Connected,
}

#[derive(AsRefStr, Clone, Copy)]
pub enum WsCommEvent {
    #[strum(serialize = "ping_send")]
    PingSend,
    #[strum(serialize = "pong_send")]
    PongSend,
    #[strum(serialize = "ping_recv")]
    PingRecv,
    #[strum(serialize = "pong_recv")]
    PongRecv,
    #[strum(serialize = "pong_send_fail")]
    PongSendFail,
    #[strum(serialize = "msg_recv")]
    MsgRecv,
    #[strum(serialize = "msg_send")]
    MsgSend,
    #[strum(serialize = "close_recv")]
    CloseRecv,
    #[strum(serialize = "conn_closed")]
    ConnClosed,
    #[strum(serialize = "unhandled_recv")]
    Unhandled,
}

#[derive(Clone)]
pub struct WsStreamMetrics {
    counters: CounterVec,
    comm_counters: CounterVec,
    backoff_gauge: GaugeVec,
    stale_gauge: GaugeVec,
}

static WS_LIFECYCLE_EVENT: &str = "ws_lifecycle_event";
static WS_COMM_EVENT: &str = "ws_comm_event";

impl WsStreamMetrics {
    #[must_use]
    pub fn for_name(registry: &Registry, name: &str) -> WsStreamMetrics {
        ws_metric_store().get_or_create(name.to_string(), || Self::new_metrics(registry, name))
    }

    /// # Panics
    ///
    /// If the metrics fail to register
    #[must_use]
    pub fn new_metrics(registry: &Registry, name: &str) -> WsStreamMetrics {
        let opts = Opts::new(
            "connection_backoff",
            "Backoff time (seconds) until the next connection retry",
        )
        .const_label("name", name);
        let backoff_gauge = GaugeVec::new(opts, &[]).unwrap();
        registry.register(Box::new(backoff_gauge.clone())).unwrap();
        let opts = Opts::new(
            "connection_stale",
            "If no message were received since the stale duration",
        )
        .const_label("name", name);
        let stale_gauge = GaugeVec::new(opts, &[]).unwrap();
        registry.register(Box::new(stale_gauge.clone())).unwrap();
        WsStreamMetrics {
            counters: WsStreamMetrics::counter_for(registry, WS_LIFECYCLE_EVENT, name),
            comm_counters: WsStreamMetrics::counter_for(registry, WS_COMM_EVENT, name),
            backoff_gauge,
            stale_gauge,
        }
    }

    fn counter_for(registry: &Registry, base_name: &str, name: &str) -> CounterVec {
        let opts = Opts::new(base_name, &format!("Websocket stream event : {}", base_name)).const_label("name", name);
        let vec = CounterVec::new(opts, &["event"]).unwrap();
        registry.register(Box::new(vec.clone())).unwrap();
        vec
    }

    pub(super) fn lifecycle_event(&self, e: WsStreamLifecycleEvent) {
        self.counters.with_label_values(&[e.as_ref()]).inc();
    }

    pub(super) fn comm_event(&self, e: WsCommEvent) { self.comm_counters.with_label_values(&[e.as_ref()]).inc(); }

    pub(super) fn conn_backoff(&self, time: f64) { self.backoff_gauge.with_label_values(&[]).set(time); }

    pub(super) fn stale(&self, is_stale: bool) {
        self.stale_gauge
            .with_label_values(&[])
            .set(if is_stale { 1.0 } else { 0.0 });
    }
}
