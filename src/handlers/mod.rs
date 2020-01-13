use coinnect_rt::types::{LiveEvent, Pair};
use std::path::PathBuf;
use chrono::prelude::*;

pub mod file_actor;
mod rotate;

pub fn liveEventPartitioner(le: &LiveEvent) -> Option<PathBuf> {
    match le {
        LiveEvent::LiveOrderbook(ob) => {
            let dt_par = Utc.timestamp_millis(ob.timestamp).format("%Y%m%d");
            Some(PathBuf::new().join("order_books").join(format!("pr={:?}", ob.pair)).join(format!("dt={}", dt_par)))
        }
        LiveEvent::LiveOrder(o) => {
            let dt_par = Utc.timestamp_millis(o.event_ms).format("%Y%m%d");
            Some(PathBuf::new().join("orders").join(format!("pr={:?}", o.pair)).join(format!("dt={}", dt_par)))
        }
        LiveEvent::LiveTrade(t) => {
            let dt_par = Utc.timestamp_millis(t.event_ms).format("%Y%m%d");
            Some(PathBuf::new().join("trades").join(format!("pr={:?}", t.pair)).join(format!("dt={}", dt_par)))
        }
        /// No partitioning for this event
        _ => None
    }
}
