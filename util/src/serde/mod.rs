use serde::de::Error;
use serde::{Deserialize, Deserializer, Serializer};

#[allow(dead_code)]
fn round_serialize<S>(x: &f64, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{:.2}", x))
}

pub mod date_time_format {
    use crate::rust_decimal::prelude::ToPrimitive;
    use chrono::{DateTime, TimeZone, Utc};
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
