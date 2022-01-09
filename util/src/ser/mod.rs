use std::fmt::Debug;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use crate::compress::Compression;
use byte_unit::Byte;
use chrono::Duration;
use serde::de::Error;
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::RwLock;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;

#[allow(dead_code)]
fn round_serialize<S>(x: f64, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{:.2}", x))
}

pub mod date_time_format {
    use chrono::{DateTime, TimeZone, Utc};
    use rust_decimal::prelude::ToPrimitive;
    use rust_decimal::Decimal;
    use serde::{self, Deserialize, Deserializer, Serializer};

    static DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(DATE_FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let pr = s.parse::<i64>();
        pr.map(|millis| Utc.timestamp_millis(millis))
            .or_else(|_| {
                Decimal::from_scientific(s.as_str())
                    .and_then(|d| d.to_i64().ok_or(rust_decimal::Error::ExceedsMaximumPossibleValue))
                    .map(|decimal| Utc.timestamp_millis(decimal))
            })
            .or_else(|_| Utc.datetime_from_str(&s, DATE_FORMAT))
            .map_err(serde::de::Error::custom)
    }
}

pub fn string_duration<'de, D>(deserializer: D) -> Result<core::time::Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    parse_duration::parse(&val).map_err(D::Error::custom)
}

pub fn decode_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::seconds(val))
}

pub fn decode_file_size<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    let size_bytes = Byte::from_str(val).map_err(|e| de::Error::custom(format!("{:?}", e)))?;
    Ok(size_bytes.get_bytes())
}

pub fn decode_duration_str<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    Duration::from_std(parse_duration::parse(&val).map_err(serde::de::Error::custom)?).map_err(serde::de::Error::custom)
}

pub fn encode_duration_str<S>(x: &Duration, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{}ms", x.num_milliseconds()))
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum F64Helper {
    Price(f64),
    Null,
}

pub fn parse_null_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let helper = Deserialize::deserialize(deserializer)?;
    match helper {
        F64Helper::Price(s) => Ok(s),
        F64Helper::Null => Ok(0.0),
    }
}

pub fn write_as_seq<P: AsRef<Path>, T: Serialize>(out_file: P, data: &[T]) -> Result<(), anyhow::Error> {
    let logs_f = std::fs::File::create(out_file)?;
    let mut serializer = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = serializer.serialize_seq(None)?;
    for models in data {
        seq.serialize_element(&models)?;
    }
    SerializeSeq::end(seq)?;
    Ok(())
}

pub struct StreamSerializerWriter<T> {
    out_file: PathBuf,
    compression: Compression,
    sink: UnboundedSender<T>,
    stream: RwLock<UnboundedReceiverStream<T>>,
    finish_tx: Sender<bool>,
    finish_rx: RwLock<Receiver<bool>>,
    finish_resp_tx: Sender<bool>,
    finish_resp_rx: RwLock<Receiver<bool>>,
}

impl<T: 'static + Serialize + Debug + Send> StreamSerializerWriter<T> {
    pub fn new<P: AsRef<Path>>(out_file: P) -> StreamSerializerWriter<T> {
        Self::new_with_compression(out_file, Compression::default())
    }

    pub fn new_with_compression<P: AsRef<Path>>(out_file: P, compression: Compression) -> StreamSerializerWriter<T> {
        let (sink, rcv) = tokio::sync::mpsc::unbounded_channel();
        let (finish_tx, finish_rx) = tokio::sync::mpsc::channel::<bool>(1);
        let (finish_resp_tx, finish_resp_rx) = tokio::sync::mpsc::channel::<bool>(1);
        let stream = UnboundedReceiverStream::new(rcv);
        Self {
            out_file: out_file.as_ref().to_path_buf(),
            compression,
            sink,
            stream: RwLock::new(stream),
            finish_rx: RwLock::new(finish_rx),
            finish_resp_tx,
            finish_tx,
            finish_resp_rx: RwLock::new(finish_resp_rx),
        }
    }

    /// Push a new value to be serialized and written
    pub fn push(&self, value: T) -> Result<(), SendError<T>> { self.sink.send(value) }

    /// Get the sink
    pub fn sink(&self) -> UnboundedSender<T> { self.sink.clone() }

    /// Ask the stream to finish and close the serializer, this will wait until the serializer has finished writing
    /// # Panics
    ///
    /// Will panic if the close channel is already closed
    pub async fn close(&self) {
        self.finish_tx.send(true).await.unwrap();
        let mut end = self.finish_resp_rx.write().await;
        end.recv().await.unwrap();
    }

    /// Open a new serializer to the file and start writing push values to it
    /// # Panics
    ///
    /// Will panic if `out_file` cannot be opened and written to
    pub async fn start(&self) {
        let logs_f = BufWriter::new(std::fs::File::create(&self.out_file).unwrap());
        let mut writer = self.compression.wrap_writer(logs_f);
        let mut serializer = serde_json::Serializer::new(&mut writer);
        let mut seq = serializer.serialize_seq(None).unwrap();
        let mut lock = self.stream.write().await;
        let mut finish_lock = self.finish_rx.write().await;
        'stream: loop {
            select! {
                biased;
                next = lock.next() => {
                    if let Some(value) = next {
                        tokio::task::block_in_place(|| {
                            seq.serialize_element(&value).unwrap();
                        });
                    } else {
                        break 'stream;
                    }
                }
                _ = finish_lock.recv() => break 'stream
            }
        }
        SerializeSeq::end(seq).unwrap();
        self.finish_resp_tx.send(true).await.unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::env::temp_dir;
    use std::io::BufReader;
    use std::sync::Arc;

    use futures::StreamExt;
    use serde::Serialize;

    use crate::ser::StreamSerializerWriter;

    #[derive(PartialEq, Debug, Serialize, Deserialize)]
    struct TestData {
        i: usize,
    }

    #[tokio::test]
    async fn stream_should_write_valid_json() {
        let file = temp_dir().join("file.json");

        let serializer = Arc::new(StreamSerializerWriter::<TestData>::new(file.clone()));
        let ser_ref = serializer.clone();
        tokio::spawn(async move { ser_ref.start().await });
        let count: usize = 10_000;
        let stream = stream! {
            for i in (0..count) {
                yield TestData { i }
            }
        };
        stream
            .map(|td| {
                serializer.push(td).unwrap();
                Ok(())
            })
            .forward(futures::sink::drain())
            .await
            .unwrap();
        serializer.close().await;
        let file = std::fs::File::open(&file).unwrap();
        let values: Vec<TestData> = serde_json::from_reader(BufReader::new(file)).unwrap();
        assert_eq!(values.len(), count);
        assert_eq!(values.last(), Some(&TestData { i: count - 1 }));
    }
}
