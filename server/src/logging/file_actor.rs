use actix::{Actor, Handler, Running, SyncContext};
use avro_rs::encode::encode;
use avro_rs::{
    types::{ToAvro, Value},
    Codec, Schema, Writer,
};
use chrono::Duration;
use derive_more::Display;
use log::Level::*;
use rand::random;
use std::cell::{RefCell, RefMut};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use uuid::Uuid;

use crate::logging::rotate::{RotatingFile, SizeAndExpirationPolicy};
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use models::avro_gen::{
    self,
    models::{LiveTrade as LT, Orderbook as OB},
};
use serde::Serialize;

type RotatingWriter = Writer<'static, RotatingFile<SizeAndExpirationPolicy>>;

type Partition = PathBuf;
type Partitioner = fn(&LiveEventEnveloppe) -> Option<Partition>;

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
    WriterError,
    NoPartitionError,
    NoSchemaError,
    IOError(std::io::Error),
}

impl std::error::Error for Error {}

pub struct AvroFileActor {
    base_path: PathBuf,
    partitioner: Partitioner,
    writers: Rc<RefCell<HashMap<PathBuf, Rc<RefCell<RotatingWriter>>>>>,
    rotation_policy: SizeAndExpirationPolicy,
    session_uuid: Uuid,
}

const AVRO_EXTENSION: &str = "avro";

impl AvroFileActor {
    pub fn new(options: &FileActorOptions) -> Self {
        let base_path = Path::new(options.base_dir.as_str()).to_path_buf();
        Self {
            partitioner: options.partitioner,
            writers: Rc::new(RefCell::new(HashMap::new())),
            base_path,
            session_uuid: Uuid::new_v4(),
            rotation_policy: SizeAndExpirationPolicy {
                last_flush: None,
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
        let next_name = format!(
            "{}{:04}.{}",
            stem,
            num.parse::<i32>().ok()? + 1,
            AVRO_EXTENSION
        );
        let mut next = previous.clone();
        next.set_file_name(next_name);
        next.set_extension(AVRO_EXTENSION);
        Some(next)
    }

    /// Returns (creating it if necessary) the current rotating file writer for the partition
    #[cfg_attr(feature = "flame_it", flame)]
    fn writer_for(&mut self, e: &LiveEventEnveloppe) -> Result<Rc<RefCell<RotatingWriter>>, Error> {
        let partition = (self.partitioner)(e).ok_or(Error::NoPartitionError)?;
        match self.writers.borrow_mut().entry(partition.clone()) {
            Entry::Vacant(v) => {
                let buf = self.base_path.join(partition);
                // Create base directory for partition if necessary
                fs::create_dir_all(&buf).map_err(|e| Error::IOError(e))?;

                let schema = self.schema_for(&e.1).ok_or(Error::NoSchemaError)?;

                // Rotating file
                let file_path =
                    buf.join(format!("{}-{:04}.{}", self.session_uuid, 0, AVRO_EXTENSION));

                let mut marker = Vec::with_capacity(16);
                for _ in 0..16 {
                    marker.push(random::<u8>());
                }

                let file = RotatingFile::new(
                    Box::new(file_path),
                    self.rotation_policy.clone(),
                    AvroFileActor::next_file_part_name,
                    Some(avro_header(&schema, marker.clone())?),
                )
                .map_err(|e| Error::IOError(e))?;

                // Schema based avro file writer
                let mut writer = Writer::new(&schema, file);
                writer.marker = marker;

                let rc = Rc::new(RefCell::new(writer));
                let _v = v.insert(rc.clone());
                Ok(rc)
            }
            Entry::Occupied(o) => Ok(o.get().clone()),
        }
    }

    /// Lookup the avro schema for the event type
    fn schema_for(&self, e: &LiveEvent) -> Option<&'static Schema> {
        match e {
            LiveEvent::LiveTrade(_) => Some(&*avro_gen::models::LIVETRADE_SCHEMA),
            LiveEvent::LiveOrder(_) => Some(&*avro_gen::models::LIVEORDER_SCHEMA),
            LiveEvent::LiveOrderbook(_) => Some(&*avro_gen::models::ORDERBOOK_SCHEMA),
            _ => None,
        }
    }

    fn append_log<S: std::fmt::Debug + Serialize>(
        writer: &mut RefMut<Writer<RotatingFile<SizeAndExpirationPolicy>>>,
        s: S,
    ) -> Result<i32, Error> {
        if log_enabled!(Trace) {
            trace!("Avro bean {:?}", s);
        }
        match writer.append_ser(s) {
            Err(e) => {
                if log_enabled!(Trace) {
                    trace!("Error writing avro bean {:?}", e);
                }
                Err(Error::WriterError)
            }
            _ => Ok(0),
        }
    }
}

impl Actor for AvroFileActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // ctx.set_mailbox_capacity(ctx.);
        info!("AvroFileActor : started");
    }
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("AvroFileActor : stopping");
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("AvroFileActor stopped, flushing writers...");
        let writers = &self.writers.borrow_mut();
        for (_k, v) in writers.iter() {
            v.borrow_mut().flush().unwrap_or_else(|_| {
                trace!("Error flushing writer");
                0
            });
        }
    }
}

impl Handler<LiveEventEnveloppe> for AvroFileActor {
    type Result = ();

    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let rc = self.writer_for(&msg);
        if rc.is_err() {
            if log_enabled!(Debug) {
                debug!(
                    "Could not acquire writer for partition {:?}",
                    rc.err().unwrap()
                );
            }
            return;
        }
        let rc_ok = rc.unwrap();
        let mut writer = rc_ok.borrow_mut();
        let appended = match msg.1 {
            LiveEvent::LiveTrade(lt) => {
                let lt = LT {
                    pair: lt.pair.to_string(),
                    tt: lt.tt.into(),
                    price: lt.price,
                    event_ms: lt.event_ms,
                    amount: lt.amount,
                };
                AvroFileActor::append_log(&mut writer, lt)
            }
            LiveEvent::LiveOrderbook(lt) => {
                let orderbook = OB {
                    pair: lt.pair.to_string(),
                    event_ms: lt.timestamp,
                    asks: lt.asks.iter().map(|(p, v)| vec![*p, *v]).collect(),
                    bids: lt.bids.iter().map(|(p, v)| vec![*p, *v]).collect(),
                };
                AvroFileActor::append_log(&mut writer, orderbook)
            }
            _ => Ok(0),
        };
        match appended.and_then(|_| writer.flush().map_err(|_e| Error::WriterError)) {
            Err(e) => trace!("Failed to flush writer {:?}", e),
            Ok(_) => (),
        }
    }
}

const AVRO_OBJECT_HEADER: &[u8] = &[b'O', b'b', b'j', 1u8];

fn avro_header(schema: &Schema, marker: Vec<u8>) -> Result<Vec<u8>, Error> {
    let schema_bytes = serde_json::to_string(schema)
        .map_err(|_e| Error::NoSchemaError)?
        .into_bytes();

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

#[cfg(test)]
mod test {
    use std::thread;

    use actix::SyncArbiter;
    use actix_rt::System;

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::Orderbook;
    use fs_extra::dir::get_dir_content;
    use tempdir;

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn actor(base_dir: &str) -> AvroFileActor {
        AvroFileActor::new(&FileActorOptions {
            max_file_size: 100_000,
            max_file_time: Duration::seconds(1),
            base_dir: String::from(base_dir),
            partitioner: crate::logging::live_event_partitioner,
        })
    }

    #[test]
    fn test_workflow() {
        init();
        let dir = tempdir::TempDir::new("s").unwrap();
        let x = dir.path().clone();
        let dir_str = String::from(x.as_os_str().to_str().unwrap());
        let new_dir = dir_str.clone();
        System::run(move || {
            let addr = SyncArbiter::start(1, move || actor(new_dir.clone().as_str()));
            let order_book_event = LiveEventEnveloppe(
                Exchange::Binance,
                LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: "BTC_USDT".into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                }),
            );
            println!("Sending...");
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            thread::sleep(std::time::Duration::from_secs(2));
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            thread::sleep(std::time::Duration::from_secs(2));
            for _ in 0..100000 {
                addr.do_send(order_book_event.clone());
            }
            System::current().stop();
        })
        .unwrap();
        let content = get_dir_content(x).unwrap();
        assert_eq!(content.files.len(), 2)
    }
}
