use std::io::BufWriter;
use std::path::Path;

use byte_unit::Byte;
use chrono::Duration;
use serde::de::Error;
use serde::ser::SerializeSeq;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[allow(dead_code)]
fn round_serialize<S>(x: &f64, s: S) -> std::result::Result<S::Ok, S::Error>
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
                Decimal::from_scientific(s.as_str()).map(|decimal| Utc.timestamp_millis(decimal.to_i64().unwrap()))
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

pub fn decode_file_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    let size_bytes = Byte::from_str(val).map_err(|e| de::Error::custom(format!("{:?}", e)))?;
    Ok(size_bytes.get_bytes() as u64)
}

pub fn decode_duration_str<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
    Duration::from_std(parse_duration::parse(&val).map_err(serde::de::Error::custom)?).map_err(serde::de::Error::custom)
}

pub fn write_as_seq<P: AsRef<Path>, T: Serialize>(out_file: P, data: &[T]) {
    let logs_f = std::fs::File::create(out_file).unwrap();
    let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = ser.serialize_seq(None).unwrap();
    for models in data {
        seq.serialize_element(&models).unwrap();
    }
    SerializeSeq::end(seq).unwrap();
}
