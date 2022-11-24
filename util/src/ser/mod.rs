use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::compress::Compression;
use byte_unit::Byte;
use chrono::Duration;
use serde::de::{DeserializeOwned, Error};
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::RwLock;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

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
        pr.map(|millis| Utc.timestamp_millis_opt(millis).unwrap())
            .or_else(|_| {
                Decimal::from_scientific(s.as_str())
                    .and_then(|d| d.to_i64().ok_or(rust_decimal::Error::ExceedsMaximumPossibleValue))
                    .map(|decimal| Utc.timestamp_millis_opt(decimal).unwrap())
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

pub fn string_duration_opt<'de, D>(deserializer: D) -> Result<Option<core::time::Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let val: Option<String> = Deserialize::deserialize(deserializer)?;
    val.map(|v| parse_duration::parse(&v).map_err(D::Error::custom))
        .transpose()
}

pub fn string_duration_chrono<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    parse_duration::parse(&val)
        .map(|v| chrono::Duration::milliseconds(v.as_millis() as i64))
        .map_err(D::Error::custom)
}

pub fn string_duration_chrono_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let val: Option<String> = Deserialize::deserialize(deserializer)?;
    val.map(|v| {
        parse_duration::parse(&v)
            .map(|v| chrono::Duration::milliseconds(v.as_millis() as i64))
            .map_err(D::Error::custom)
    })
    .transpose()
}

pub fn encode_duration_str<S>(x: &Duration, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{}ms", x.num_milliseconds()))
}

pub fn encode_duration_str_opt<S>(x: &Option<Duration>, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(x) = x {
        s.serialize_str(&format!("{}ms", x.num_milliseconds()))
    } else {
        s.serialize_none()
    }
}

pub fn decode_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::seconds(val))
}

pub fn decode_duration_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Some(Duration::seconds(val)))
}

pub fn decode_file_size<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    let size_bytes = Byte::from_str(val).map_err(|e| de::Error::custom(format!("{:?}", e)))?;
    Ok(size_bytes.get_bytes())
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

pub struct StreamSerializerWriter<T, S> {
    pub out_file: PathBuf,
    pub compression: Compression,
    sink: UnboundedSender<T>,
    stream: RwLock<UnboundedReceiverStream<T>>,
    finish_token: CancellationToken,
    finish_resp_tx: Sender<bool>,
    finish_resp_rx: RwLock<Receiver<bool>>,
    _phantom_data: PhantomData<S>,
}

impl<T: 'static + DeserializeOwned + Serialize + Debug + Send, S: JsonSerde + Send> StreamSerializerWriter<T, S> {
    pub fn new<P: AsRef<Path>>(out_file: P) -> StreamSerializerWriter<T, S> {
        Self::new_with_compression(out_file, Compression::default())
    }

    pub fn new_with_compression<P: AsRef<Path>>(out_file: P, compression: Compression) -> StreamSerializerWriter<T, S> {
        let (sink, rcv) = tokio::sync::mpsc::unbounded_channel();
        let (finish_resp_tx, finish_resp_rx) = tokio::sync::mpsc::channel::<bool>(1);
        Self {
            out_file: out_file.as_ref().to_path_buf(),
            compression,
            sink,
            stream: RwLock::new(UnboundedReceiverStream::new(rcv)),
            finish_token: CancellationToken::new(),
            finish_resp_tx,
            finish_resp_rx: RwLock::new(finish_resp_rx),
            _phantom_data: Default::default(),
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
        self.finish_token.cancel();
        let mut end = self.finish_resp_rx.write().await;
        end.recv().await.unwrap();
    }

    /// Open a new serializer to the file and start writing push values to it
    /// # Panics
    ///
    /// Will panic if `out_file` cannot be opened and written to
    pub async fn start(&self) {
        let out_file = self.compression.wrap_ext(&self.out_file);
        let logs_f = BufWriter::new(std::fs::File::create(out_file).unwrap());
        let mut writer = self.compression.wrap_writer(logs_f);
        let mut lock = self.stream.write().await;
        S::serialize_stream(&mut writer, &mut lock, self.finish_token.clone()).await;
        drop(writer);
        self.finish_resp_tx.send(true).await.unwrap();
    }

    pub fn read_all(&self) -> Result<Vec<T>, serde_json::Error> { S::deserialize(self.reader()) }

    pub fn reader(&self) -> Box<dyn BufRead> {
        let file_path = self.out_file.as_path();
        let file = self.compression.wrap_ext(file_path);
        let read = BufReader::new(File::open(file).unwrap());
        let reader = self.compression.wrap_reader(read);
        reader
    }
}

#[async_trait]
pub trait JsonSerde: Send {
    async fn serialize_stream<T: Serialize + Send, W: Write + Send>(
        mut writer: W,
        stream: &mut UnboundedReceiverStream<T>,
        token: CancellationToken,
    );

    fn deserialize<R: Read, T: DeserializeOwned>(read: R) -> Result<Vec<T>, serde_json::Error>;
}

pub struct SeqJsonSerde;

#[async_trait]
impl JsonSerde for SeqJsonSerde {
    async fn serialize_stream<T: Serialize + Send, W: Write + Send>(
        writer: W,
        stream: &mut UnboundedReceiverStream<T>,
        token: CancellationToken,
    ) {
        let mut serializer = serde_json::Serializer::new(writer);
        let mut seq = serializer.serialize_seq(None).unwrap();
        'stream: loop {
            select! {
                biased;
                next = stream.next() => {
                    if let Some(value) = next {
                        tokio::task::block_in_place(|| {
                            seq.serialize_element(&value).unwrap();
                        });
                    } else {
                        break 'stream;
                    }
                }
                _ = token.cancelled() => {
                    eprintln!("token cancelled !");
                    break 'stream
                }
            }
        }
        SerializeSeq::end(seq).unwrap();
    }

    fn deserialize<R: Read, T: DeserializeOwned>(read: R) -> Result<Vec<T>, serde_json::Error> {
        serde_json::from_reader(read)
    }
}

pub struct NdJsonSerde;

#[async_trait]
impl JsonSerde for NdJsonSerde {
    async fn serialize_stream<T: Serialize + Send, W: Write + Send>(
        mut writer: W,
        stream: &mut UnboundedReceiverStream<T>,
        token: CancellationToken,
    ) {
        'stream: loop {
            select! {
                biased;
                next = stream.next() => {
                    if let Some(value) = next {
                        tokio::task::block_in_place(|| {
                            serde_json::to_writer(&mut writer, &value).unwrap();
                            writer.write_all(&[b'\n']).unwrap();
                        });
                    } else {
                        break 'stream;
                    }
                }
                _ = token.cancelled() => break 'stream
            }
        }
    }

    fn deserialize<R: Read, T: DeserializeOwned>(read: R) -> Result<Vec<T>, serde_json::Error> {
        serde_json::Deserializer::from_reader(read).into_iter::<T>().collect()
    }
}

#[cfg(test)]
mod test {
    use std::env::temp_dir;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::ser::{JsonSerde, NdJsonSerde};
    use futures::StreamExt;
    use serde::Serialize;

    use crate::ser::{SeqJsonSerde, StreamSerializerWriter};

    #[derive(PartialEq, Debug, Serialize, Deserialize)]
    struct TestData {
        i: usize,
    }

    async fn test_write_valid_json<T: JsonSerde>() {
        let file = temp_dir().join("file.json");

        let serializer = Arc::new(StreamSerializerWriter::<TestData, SeqJsonSerde>::new(file.clone()));
        let ser_ref = serializer.clone();
        tokio::spawn(async move { ser_ref.start().await });
        let count: usize = 1000;
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
        // yield to the writing thread
        tokio::time::sleep(Duration::from_millis(10)).await;
        let closed = tokio::time::timeout(Duration::from_millis(200), serializer.close()).await;
        assert!(closed.is_ok());
        let file = std::fs::File::open(&file).unwrap();
        let read = BufReader::new(file);
        let values: Vec<TestData> = SeqJsonSerde::deserialize(read).unwrap();
        assert_eq!(values.len(), count);
        assert_eq!(values.last(), Some(&TestData { i: count - 1 }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_should_write_valid_json() { test_write_valid_json::<SeqJsonSerde>().await; }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_should_write_valid_ndjson() { test_write_valid_json::<NdJsonSerde>().await; }
}
