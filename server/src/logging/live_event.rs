use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use actix::Handler;
use avro_rs::Schema;
use chrono::{Duration, TimeZone, Utc};

use coinnect_rt::prelude::*;
use models::avro_gen::{self,
                       models::{LiveTrade as LT, Orderbook as OB}};

use super::file_actor::{AvroFileActor, Error, ToAvroSchema};
use super::{Partition, Partitioner};

#[derive(Clone)]
pub struct LiveEventPartitioner {
    grace_period: Duration,
}

impl LiveEventPartitioner {
    pub fn new(grace_period: Duration) -> Self { Self { grace_period } }
}

impl Partitioner<MarketEventEnvelope> for LiveEventPartitioner {
    /// Create a partition for this event
    /// each partition has a key and value formatted like hdfs does
    /// /k1=v1/k2=v2/...
    /// Dates are formatted using strftime/Ymd
    fn partition(&self, data: &MarketEventEnvelope) -> Option<Partition> {
        let exchange = format!("{:?}", data.xch);
        match &data.e {
            MarketEvent::Orderbook(ob) => Some((ob.timestamp, "order_books", ob.pair.clone())),
            MarketEvent::Trade(t) => Some((t.event_ms, "trades", t.pair.clone())),
            // No partitioning for this event
            _ => None,
        }
        .map(|(ts, channel, pair)| {
            let ts = Utc.timestamp_millis(ts);
            let dt_par = ts.format("%Y%m%d");
            let path = PathBuf::new()
                .join(&exchange)
                .join(channel)
                .join(format!("pr={}", pair))
                .join(format!("dt={}", dt_par));
            let date = ts.date();
            Partition::new(
                path,
                Some(date.and_hms(0, 0, 0).add(Duration::days(1)).add(self.grace_period)),
            )
        })
    }
}

impl ToAvroSchema for MarketEventEnvelope {
    fn schema(&self) -> Option<&'static Schema> {
        match &self.e {
            MarketEvent::Trade(_) => Some(&*avro_gen::models::LIVETRADE_SCHEMA),
            MarketEvent::Orderbook(_) => Some(&*avro_gen::models::ORDERBOOK_SCHEMA),
            _ => None,
        }
    }
}

impl Handler<Arc<MarketEventEnvelope>> for AvroFileActor<MarketEventEnvelope> {
    type Result = anyhow::Result<()>;

    fn handle(&mut self, msg: Arc<MarketEventEnvelope>, _ctx: &mut Self::Context) -> Self::Result {
        let rc = self.writer_for(&msg);
        if let Err(rc_err) = rc {
            self.metrics.writer_acquisition_failure();
            debug!("Could not acquire writer for partition {:?}", rc_err);
            return Err(anyhow!(rc_err));
        }
        let rc_ok = rc.unwrap();
        let mut writer = rc_ok.borrow_mut();
        let now = Utc::now();
        let appended = match &msg.e {
            MarketEvent::Trade(lt) => {
                let lt = LT {
                    pair: lt.pair.to_string(),
                    tt: lt.tt.into(),
                    price: lt.price,
                    event_ms: lt.event_ms,
                    amount: lt.amount,
                };
                self.metrics.event_lag(now.timestamp_millis() - lt.event_ms);
                self.append_log(&mut writer, lt)
            }
            MarketEvent::Orderbook(lt) => {
                self.metrics.event_lag(now.timestamp_millis() - lt.timestamp);
                let orderbook = OB {
                    pair: lt.pair.to_string(),
                    event_ms: lt.timestamp,
                    asks: lt.asks.iter().map(|(p, v)| vec![*p, *v]).collect(),
                    bids: lt.bids.iter().map(|(p, v)| vec![*p, *v]).collect(),
                };
                self.append_log(&mut writer, orderbook)
            }
            _ => Ok(0),
        };
        if let Err(e) = appended.and_then(|_| writer.flush().map_err(|_e| Error::WriterError)) {
            self.metrics.flush_failure();
            trace!("Failed to flush writer {:?}", e);
            return Err(anyhow!(e));
        }
        self.remove_expired_entries();
        Ok(())
    }
}
