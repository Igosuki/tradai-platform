use std::cell::{RefCell, RefMut};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use actix::{Actor, SyncContext};
use avro_rs::encode;
use avro_rs::{types::Value, Codec, Schema, Writer};
use chrono::Duration;
use derive_more::Display;
use log::Level::*;
use rand::random;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::logging::rotate::{RotatingFile, SizeAndExpirationPolicy};
use crate::logging::{Partition, Partitioner};

type RotatingWriter = Writer<'static, RotatingFile<SizeAndExpirationPolicy>>;

pub struct FileActorOptions<T> {
    pub base_dir: String,
    /// Max file size in bytes
    pub max_file_size: u64,
    /// Max time before closing file
    pub max_file_time: Duration,
    /// Record partitioner
    pub partitioner: Rc<dyn Partitioner<T>>,
}

#[derive(Debug, Display, Error)]
pub enum Error {
    NoWriterError,
    WriterError,
    NoPartitionError,
    NoSchemaError,
    IOError(std::io::Error),
}

pub struct AvroFileActor<T> {
    base_path: PathBuf,
    partitioner: Rc<dyn Partitioner<T>>,
    writers: Rc<RefCell<HashMap<Partition, Rc<RefCell<RotatingWriter>>>>>,
    rotation_policy: SizeAndExpirationPolicy,
    session_uuid: Uuid,
}

const AVRO_EXTENSION: &str = "avro";

pub trait ToAvroSchema {
    fn schema(&self) -> Option<&'static Schema>;
}

impl<T: ToAvroSchema> AvroFileActor<T>
where
    T: 'static,
{
    pub fn new(options: &FileActorOptions<T>) -> Self {
        let base_path = Path::new(options.base_dir.as_str()).to_path_buf();
        Self {
            partitioner: options.partitioner.clone(),
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
    fn next_file_part_name(previous: &Path) -> Option<PathBuf> {
        let previous_name = previous.file_stem().and_then(|os_str| os_str.to_str())?;
        let i = previous_name.rfind('-')?;
        let (stem, num) = previous_name.split_at(i + 1);
        let next_name = format!("{}{:04}.{}", stem, num.parse::<i32>().ok()? + 1, AVRO_EXTENSION);
        let mut next = previous.to_path_buf();
        next.set_file_name(next_name);
        next.set_extension(AVRO_EXTENSION);
        Some(next)
    }

    /// Returns (creating it if necessary) the current rotating file writer for the partition
    #[cfg_attr(feature = "flame_it", flame)]
    pub(crate) fn writer_for(&mut self, e: &T) -> Result<Rc<RefCell<RotatingWriter>>, Error> {
        let partition = self.partitioner.partition(e).ok_or(Error::NoPartitionError)?;
        let path = partition.path.clone();
        match self.writers.borrow_mut().entry(partition) {
            Entry::Vacant(v) => {
                let buf = self.base_path.join(path);
                // Create base directory for partition if necessary
                fs::create_dir_all(&buf).map_err(Error::IOError)?;

                let schema = e.schema().ok_or(Error::NoSchemaError)?;

                // Rotating file
                let file_path = buf.join(format!("{}-{:04}.{}", self.session_uuid, 0, AVRO_EXTENSION));

                let mut marker = Vec::with_capacity(16);
                for _ in 0..16 {
                    marker.push(random::<u8>());
                }

                let file = RotatingFile::new(
                    Box::new(file_path),
                    self.rotation_policy.clone(),
                    AvroFileActor::<T>::next_file_part_name,
                    Some(avro_header(schema, marker.clone())?),
                )
                .map_err(Error::IOError)?;

                // Schema based avro file writer
                let mut writer = Writer::new(schema, file);
                writer.set_marker(marker);

                let rc = Rc::new(RefCell::new(writer));
                let _v = v.insert(rc.clone());
                Ok(rc)
            }
            Entry::Occupied(o) => Ok(o.get().clone()),
        }
    }

    pub(crate) fn remove_expired_entries(&self) {
        let mut ref_mut = self.writers.borrow_mut();
        ref_mut.retain(|partition, _| !partition.is_expired());
    }
}

impl<T: ToAvroSchema> Actor for AvroFileActor<T>
where
    T: 'static,
{
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("avro file logger started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("avro file logger stopped, flushing writers...");
        let writers = &self.writers.borrow_mut();
        for (_k, v) in writers.iter() {
            v.borrow_mut().flush().unwrap_or_else(|_| {
                trace!("error flushing writer");
                0
            });
        }
    }
}

/// Append an avro serializable to this writer
pub fn append_log<S: std::fmt::Debug + Serialize, W: Write>(
    writer: &mut RefMut<Writer<W>>,
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

const AVRO_OBJECT_HEADER: &[u8] = &[b'O', b'b', b'j', 1u8];

/// Get a byte array of avro file header with byte encoded metadata
fn avro_header(schema: &Schema, marker: Vec<u8>) -> Result<Vec<u8>, Error> {
    let schema_bytes = serde_json::to_string(schema)
        .map_err(|_e| Error::NoSchemaError)?
        .into_bytes();

    let mut metadata = HashMap::with_capacity(2);
    metadata.insert("avro.schema", Value::Bytes(schema_bytes));
    metadata.insert("avro.codec", Codec::Null.into());

    let mut header = Vec::new();
    header.extend_from_slice(AVRO_OBJECT_HEADER);
    encode(&metadata.into(), &Schema::Map(Box::new(Schema::Bytes)), &mut header);
    header.extend_from_slice(&marker);

    Ok(header)
}

#[cfg(test)]
mod test {
    use std::thread;

    use actix::SyncArbiter;
    use actix::System;
    use fs_extra::dir::get_dir_content;

    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::{LiveEvent, LiveEventEnvelope, Orderbook};

    use crate::logging::live_event::LiveEventPartitioner;

    use super::*;
    use std::sync::Arc;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    fn actor(base_dir: &str) -> AvroFileActor<LiveEventEnvelope> {
        AvroFileActor::new(&FileActorOptions {
            max_file_size: 100_000,
            max_file_time: Duration::seconds(1),
            base_dir: String::from(base_dir),
            partitioner: Rc::new(LiveEventPartitioner::new(Duration::seconds(10))),
        })
    }

    #[test]
    fn test_workflow() {
        init();
        let dir = tempdir::TempDir::new("s").unwrap();
        let x = dir.path();
        let dir_str = String::from(x.as_os_str().to_str().unwrap());
        let new_dir = dir_str;
        System::new().block_on(async move {
            let addr = SyncArbiter::start(1, move || actor(new_dir.clone().as_str()));
            let order_book_event = Arc::new(LiveEventEnvelope {
                xch: Exchange::Binance,
                e: LiveEvent::LiveOrderbook(Orderbook {
                    timestamp: chrono::Utc::now().timestamp(),
                    pair: "BTC_USDT".into(),
                    asks: vec![(0.1, 0.1), (0.2, 0.2)],
                    bids: vec![(0.1, 0.1), (0.2, 0.2)],
                    last_order_id: None,
                }),
            });
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
        });
        let content = get_dir_content(x).unwrap();
        assert_eq!(content.files.len(), 2)
    }
}
