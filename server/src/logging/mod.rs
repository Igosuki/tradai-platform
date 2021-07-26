use chrono::prelude::*;
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe, Pair};
use std::path::PathBuf;

pub mod file_actor;
mod rotate;

trait Partition {
    fn to_path(&self) -> PathBuf;

    fn is_expired(&self) -> bool;
}

trait Partitioner<T> {
    fn partition(&self, data: &T) -> Option<Box<dyn Partition>>;
}

#[derive(Clone)]
pub struct LiveEventPartitioner;

impl Partitioner<LiveEventEnveloppe> for LiveEventPartitioner {
    /// Create a partition for this event
    /// each partition has a key and value formatted like hdfs does
    /// /k1=v1/k2=v2/...
    /// Dates are formatted using strftime/Ymd
    fn partition(&self, data: &LiveEventEnveloppe) -> Option<Box<dyn Partition>> {
        let exchange = format!("{:?}", data.xch);
        match &data.e {
            LiveEvent::LiveOrderbook(ob) => Some(Box::new(LiveEventPartition::new(
                exchange,
                ob.timestamp,
                "order_books",
                ob.pair.clone(),
            ))),
            LiveEvent::LiveOrder(o) => Some(Box::new(LiveEventPartition::new(
                exchange,
                o.event_ms,
                "orders",
                o.pair.clone(),
            ))),
            LiveEvent::LiveTrade(t) => Some(Box::new(LiveEventPartition::new(
                exchange,
                t.event_ms,
                "trades",
                t.pair.clone(),
            ))),
            // No partitioning for this event
            _ => None,
        }
    }
}

struct LiveEventPartition {
    exchange: String,
    ts: i64,
    channel: &'static str,
    pair: Pair,
}

impl LiveEventPartition {
    fn new(exchange: String, ts: i64, channel: &'static str, pair: Pair) -> LiveEventPartition {
        LiveEventPartition {
            exchange,
            ts,
            channel,
            pair,
        }
    }
}

impl Partition for LiveEventPartition {
    fn to_path(&self) -> PathBuf {
        let dt_par = Utc.timestamp_millis(self.ts).format("%Y%m%d");
        PathBuf::new()
            .join(&self.exchange)
            .join(self.channel)
            .join(format!("pr={}", self.pair))
            .join(format!("dt={}", dt_par))
    }

    fn is_expired(&self) -> bool { todo!() }
}
