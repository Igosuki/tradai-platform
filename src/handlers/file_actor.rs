use actix::{Actor, Context, Handler};
use coinnect_rt::bitstamp::models::Event;
use coinnect_rt::exchange_bot::DefaultWsActor;
use std::path::Path;
use coinnect_rt::types::LiveEvent;
use avro_rs::{Writer, Schema};
use crate::avro_gen::{self, models::{LiveTrade as LT, Orderbook as OB}};
use bigdecimal::ToPrimitive;
use std::borrow::Borrow;
use std::rc::Rc;
use std::cell::RefCell;
use std::fs::File;
use uuid::Uuid;
use bigdecimal::BigDecimal;

pub struct FileActorOptions {
    pub base_dir: String,
}

pub struct AvroFileActor {
    writer: Rc<RefCell<Writer<'static, File>>>,
}

impl AvroFileActor {
    pub fn new(options: &FileActorOptions) -> Self {
        let session_uuid = Uuid::new_v4();
        let base_path = Path::new(options.base_dir.as_str());
        let mut file = File::create(base_path.join(format!("part-{}.avro", session_uuid))).unwrap();
        let mut writer = Writer::new(&avro_gen::models::ORDERBOOK_SCHEMA, file);
        Self { writer: Rc::new(RefCell::new(writer)), }
    }
}

impl Actor for AvroFileActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Context<Self>) {
    }
}

impl Handler<LiveEvent> for AvroFileActor {
    type Result = ();

    fn handle(&mut self, msg: LiveEvent, ctx: &mut Self::Context) -> Self::Result {
        let mut rc = self.writer.borrow_mut();
        match msg {
            LiveEvent::LiveTrade(lt) => rc.append_ser(LT {
                pair: lt.pair,
                tt: lt.tt.into(),
                price: lt.price.to_f32().unwrap(),
                event_ms: lt.event_ms,
                amount: lt.amount,
            }),
            LiveEvent::LiveOrderbook(lt) => {
                let orderbook = OB {
                    pair: serde_json::to_string(&lt.pair).unwrap(),
                    event_ms: lt.timestamp,
                    asks: lt.asks.into_iter().map(|(p, v)| vec![p.to_f32().unwrap(), v.to_f32().unwrap()]).collect(),
                    bids: lt.bids.into_iter().map(|(p, v)| vec![p.to_f32().unwrap(), v.to_f32().unwrap()]).collect()
                };
                debug!("Avro bean {:?}", orderbook);
                match rc.append_ser(orderbook) {
                    Err(e) => {
                        debug!("Error writing avro bean {:?}", e);
                        Err(e)
                    },
                    _ => Ok(0)
                }
            },
            _ => Ok(0)
        };
        rc.flush();
    }
}
