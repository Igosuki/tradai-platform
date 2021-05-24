use chrono::prelude::*;
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use std::path::PathBuf;

pub mod file_actor;
mod rotate;

/// Create a partition for this even in the form of a PathBuf
/// each partition has a key and value formatted like hdfs does
/// /k1=v1/k2=v2/...
/// Dates are formatted using strftime/Ymd
pub fn live_event_partitioner(le: &LiveEventEnveloppe) -> Option<PathBuf> {
    let exchange = format!("{:?}", le.0);
    match &le.1 {
        LiveEvent::LiveOrderbook(ob) => partition_path(&exchange, ob.timestamp, "order_books", &ob.pair.clone()),
        LiveEvent::LiveOrder(o) => partition_path(&exchange, o.event_ms, "orders", &o.pair),
        LiveEvent::LiveTrade(t) => partition_path(&exchange, t.event_ms, "trades", &t.pair),
        // No partitioning for this event
        _ => None,
    }
}

pub fn partition_path(exchange: &str, ts: i64, channel: &str, pair: &str) -> Option<PathBuf> {
    let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
    Some(
        PathBuf::new()
            .join(exchange)
            .join(channel)
            .join(format!("pr={}", pair))
            .join(format!("dt={}", dt_par)),
    )
}
