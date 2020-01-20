use actix::{Actor, Context, Handler};


use std::path::{Path, PathBuf};
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use avro_rs::{Writer, Schema, types::{Value, ToAvro}, Codec};
use crate::avro_gen::{self, models::{LiveTrade as LT, Orderbook as OB}};
use bigdecimal::ToPrimitive;
use std::rc::Rc;
use std::cell::{RefCell};

use uuid::Uuid;

use std::collections::HashMap;

use std::collections::hash_map::Entry;

use std::fs;
use crate::handlers::rotate::{SizeAndExpirationPolicy, RotatingFile};
use chrono::{Utc, Duration};
use std::fs::File;
use avro_rs::encode::encode;

type RotatingWriter = Writer<'static, RotatingFile<SizeAndExpirationPolicy>>;

type Partition = PathBuf;
type Partitioner = fn(&LiveEventEnveloppe) -> Option<Partition>;

use rand::random;
use std::io::Write;
use derive_more::Display;

pub struct FileActorOptions {
    pub base_dir: String,
    /// Max file size in bytes
    pub max_file_size: u64,
    /// Max time before closing file
    pub max_file_time: Duration,
    /// Record partitioner
    pub partitioner: Partitioner,
}

#[derive(Debug, Display)]
pub enum Error {
    NoWriterError,
    NoPartitionError,
    NoSchemaError,
    IOError(std::io::Error),
}

impl std::error::Error for Error {

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
    fn writer_for(&mut self, e: &LiveEventEnveloppe) -> Result<Rc<RefCell<RotatingWriter>>, Error> {
        let partition = (self.partitioner)(e).ok_or(Error::NoPartitionError)?;
        match self.writers.borrow_mut().entry(partition.clone()) {
            Entry::Vacant(v) => {
                let buf = self.base_path.join(partition);
                // Create base directory for partition if necessary
                fs::create_dir_all(&buf).map_err(|e| Error::IOError(e))?;

                let schema = self.schema_for(&e.1).ok_or(Error::NoSchemaError)?;

                // Rotating file
                let file_path = buf.join(format!("{}-{:04}.{}", self.session_uuid, 0, AVRO_EXTENSION));

                let mut marker = Vec::with_capacity(16);
                for _ in 0..16 {
                    marker.push(random::<u8>());
                }

                let file = RotatingFile::new(Box::new(file_path), self.rotation_policy.clone(), AvroFileActor::next_file_part_name, Some(avro_header(&schema, marker.clone())?)).map_err(|e| Error::IOError(e))?;

                // Schema based avro file writer
                let mut writer = Writer::new(&schema, file);
                writer.marker = marker;
                let rc = Rc::new(RefCell::new(writer));
                let _v = v.insert(rc.clone());
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

impl Handler<LiveEventEnveloppe> for AvroFileActor {
    type Result = ();

    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let rc = self.writer_for(&msg);
        if rc.is_err() {
            trace!("Could not acquire writer for partition {:?}", rc.err().unwrap());
            return;
        }
        let rc_ok = rc.unwrap();
        let mut writer = rc_ok.borrow_mut();
        match msg.1 {
            LiveEvent::LiveTrade(lt) => {
                let lt = LT {
                    pair: lt.pair,
                    tt: lt.tt.into(),
                    price: lt.price.to_f32().unwrap(),
                    event_ms: lt.event_ms,
                    amount: lt.amount,
                };
                trace!("Avro bean {:?}", lt);
                match writer.append_ser(lt) {
                    Err(e) => {
                        trace!("Error writing avro bean {:?}", e);
                        Err(e)
                    }
                    _ => Ok(0)
                }
            },
            LiveEvent::LiveOrderbook(lt) => {
                let orderbook = OB {
                    pair: serde_json::to_string(&lt.pair).unwrap(),
                    event_ms: lt.timestamp,
                    asks: lt.asks.into_iter().map(|(p, v)| vec![p.to_f32().unwrap(), v.to_f32().unwrap()]).collect(),
                    bids: lt.bids.into_iter().map(|(p, v)| vec![p.to_f32().unwrap(), v.to_f32().unwrap()]).collect(),
                };
                trace!("Avro bean {:?}", orderbook);
                match writer.append_ser(orderbook) {
                    Err(e) => {
                        trace!("Error writing avro bean {:?}", e);
                        Err(e)
                    }
                    _ => Ok(0)
                }
            }
            _ => Ok(0)
        };
        writer.flush().unwrap_or_else(|_| {
            trace!("Error flushing writer");
            0
        });
    }
}

const AVRO_OBJECT_HEADER: &[u8] = &[b'O', b'b', b'j', 1u8];

fn avro_header(schema: &Schema, marker: Vec<u8>) -> Result<Vec<u8>, Error> {
    let schema_bytes = serde_json::to_string(schema).map_err(|e| Error::NoSchemaError)?.into_bytes();

    let mut metadata = HashMap::with_capacity(2);
    metadata.insert("avro.schema", Value::Bytes(schema_bytes));
    metadata.insert("avro.codec", Codec::Null.avro());

    let mut header = Vec::new();
    header.extend_from_slice(AVRO_OBJECT_HEADER);
    encode(
        &metadata.avro(),
        &Schema::Map(Box::new(Schema::Bytes)),
        &mut header,
    );
    header.extend_from_slice(&marker);

    Ok(header)
}
