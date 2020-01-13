use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use std::path::PathBuf;
use chrono::prelude::*;

pub mod file_actor;
mod rotate;

/// Create a partition for this even in the form of a PathBuf
/// each partition has a key and value formatted like hdfs does
/// /k1=v1/k2=v2/...
/// Dates are formatted using strftime/Ymd
pub fn live_event_partitioner(le: &LiveEventEnveloppe) -> Option<PathBuf> {
    let exchange = format!("{:?}", le.0);
    match &le.1 {
        LiveEvent::LiveOrderbook(ob) => {
            let dt_par = Utc.timestamp_millis(ob.timestamp).format("%Y%m%d");
            Some(PathBuf::new().join(exchange).join("order_books").join("order_books").join(format!("pr={}", ob.pair)).join(format!("dt={}", dt_par)))
        }
        LiveEvent::LiveOrder(o) => {
            let dt_par = Utc.timestamp_millis(o.event_ms).format("%Y%m%d");
            Some(PathBuf::new().join(exchange).join("orders").join(format!("pr={}", o.pair)).join(format!("dt={}", dt_par)))
        }
        LiveEvent::LiveTrade(t) => {
            let dt_par = Utc.timestamp_millis(t.event_ms).format("%Y%m%d");
            Some(PathBuf::new().join(exchange).join("trades").join(format!("pr={}", t.pair)).join(format!("dt={}", dt_par)))
        }
        // No partitioning for this event
        _ => None
    }
}
