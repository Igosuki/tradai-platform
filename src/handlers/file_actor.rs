use actix::{Actor, Context, Handler};
use coinnect_rt::bitstamp::models::Event;
use coinnect_rt::exchange_bot::DefaultWsActor;
use std::path::{Path, PathBuf};
use coinnect_rt::types::LiveEvent;
use avro_rs::{Writer, Schema};
use crate::avro_gen::{self, models::{LiveTrade as LT, Orderbook as OB}};
use bigdecimal::ToPrimitive;
use std::rc::Rc;
use std::cell::{RefCell, RefMut};
use std::fs::File;
use uuid::Uuid;
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::collections::hash_map::Entry;
use std::ops::Deref;
use std::fs;
use crate::handlers::rotate::{SizeAndExpirationPolicy, RotatingFile};
use chrono::{Utc, Duration};

type RotatingWriter = Writer<'static, RotatingFile<SizeAndExpirationPolicy>>;

type Partition = PathBuf;
type Partitioner = fn(&LiveEvent) -> Option<Partition>;

pub struct FileActorOptions {
    pub base_dir: String,
    /// Max file size in bytes
    pub max_file_size: u64,
    /// Max time before closing file
    pub max_file_time: Duration,
    /// Record partitioner
    pub partitioner: Partitioner,
}

#[derive(Debug)]
pub enum Error {
    NoWriterError,
    NoPartitionError,
    NoSchemaError,
    IOError(std::io::Error),
}

pub struct AvroFileActor {
    base_path: PathBuf,
    partitioner: Partitioner,
    writers: Rc<RefCell<HashMap<PathBuf, Rc<RefCell<RotatingWriter>>>>>,
    rotation_policy: SizeAndExpirationPolicy,
    session_uuid: Uuid,
}

const AVRO_EXTENSION : &str = "avro";

impl AvroFileActor {
    pub fn new(options: &FileActorOptions) -> Self {
        let base_path = Path::new(options.base_dir.as_str()).to_path_buf();
        Self {
            partitioner: options.partitioner,
            writers: Rc::new(RefCell::new(HashMap::new())),
            base_path,
            session_uuid: Uuid::new_v4(),
            rotation_policy: SizeAndExpirationPolicy {
                last_flush: Utc::now(),
                max_size_b: options.max_file_size,
                max_time_ms: options.max_file_time,
            },
        }
    }

    /// Finds the next incremental file name for this path
    fn next_file_part_name(previous: &PathBuf) -> Option<PathBuf> {
        let previous_name = previous.file_stem().and_then(|os_str| os_str.to_str())?;
        let i = previous_name.rfind("-")?;
        let (stem, num) = previous_name.split_at(i + 1);
        let next_name = format!("{}{:04}.{}", stem, num.parse::<i32>().ok()? + 1, AVRO_EXTENSION);
        let mut next = previous.clone();
        next.set_file_name(next_name);
        next.set_extension(AVRO_EXTENSION);
        Some(next)
    }

    /// Returns (creating it if necessary) the current rotating file writer for the partition
    fn writer_for(&mut self, e: &LiveEvent) -> Result<Rc<RefCell<RotatingWriter>>, Error> {
        let partition = (self.partitioner)(e).ok_or(Error::NoPartitionError)?;
        match self.writers.borrow_mut().entry(partition.clone()) {
            Entry::Vacant(v) => {
                let buf = self.base_path.join(partition);
                // Create base directory for partition if necessary
                fs::create_dir_all(&buf).map_err(|e| Error::IOError(e))?;

                // Rotating file
                let file_path = buf.join(format!("{}-{:04}.{}", self.session_uuid, 0, AVRO_EXTENSION));
                let file = RotatingFile::new(Box::new(file_path), self.rotation_policy.clone(), AvroFileActor::next_file_part_name).map_err(|e| Error::IOError(e))?;

                // Schema based avro file writer
                let schema = self.schema_for(e).ok_or(Error::NoSchemaError)?;
                let rc = Rc::new(RefCell::new(Writer::new(&schema, file)));
                let v = v.insert(rc.clone());
                Ok(rc)
            }
            Entry::Occupied(o) => Ok(o.get().clone())
        }
    }

    /// Lookup the avro schema for the event type
    fn schema_for(&self, e: &LiveEvent) -> Option<&'static Schema> {
        match e {
            LiveEvent::LiveTrade(_) => Some(&*avro_gen::models::LIVETRADE_SCHEMA),
            LiveEvent::LiveOrder(_) => Some(&*avro_gen::models::LIVEORDER_SCHEMA),
            LiveEvent::LiveOrderbook(_) => Some(&*avro_gen::models::ORDERBOOK_SCHEMA),
            _ => None
        }
    }
}

impl Actor for AvroFileActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Context<Self>) {}
}

impl Handler<LiveEvent> for AvroFileActor {
    type Result = ();

    fn handle(&mut self, msg: LiveEvent, ctx: &mut Self::Context) -> Self::Result {
        let rc = self.writer_for(&msg);
        if rc.is_err() {
            debug!("Could not acquire writer for partition {:?}", rc.err().unwrap());
            return;
        }
        let rc_ok = rc.unwrap();
        let mut writer = rc_ok.borrow_mut();
        match msg {
            LiveEvent::LiveTrade(lt) => writer.append_ser(LT {
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
                    bids: lt.bids.into_iter().map(|(p, v)| vec![p.to_f32().unwrap(), v.to_f32().unwrap()]).collect(),
                };
                debug!("Avro bean {:?}", orderbook);
                match writer.append_ser(orderbook) {
                    Err(e) => {
                        debug!("Error writing avro bean {:?}", e);
                        Err(e)
                    }
                    _ => Ok(0)
                }
            }
            _ => Ok(0)
        };
        writer.flush();
    }
}
