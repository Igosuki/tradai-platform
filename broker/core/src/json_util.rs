use bytes::buf::Reader;
use bytes::Buf;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::*;
use ext::ResultExt;

pub fn deserialize_json<T: DeserializeOwned>(json_string: &str) -> Result<T> {
    serde_json::from_str(json_string).err_into()
}

pub fn deserialize_json_s<T: DeserializeOwned>(slice: &[u8]) -> Result<T> { serde_json::from_slice(slice).err_into() }

pub fn deserialize_json_r<T: DeserializeOwned, B>(reader: Reader<B>) -> Result<T>
where
    B: Buf,
{
    serde_json::from_reader(reader).err_into()
}

pub fn deserialize_json_array(json_string: &str) -> Result<Map<String, Value>> {
    let data: Value = serde_json::from_str(json_string)?;
    if data.is_array() {
        let mut map = Map::new();
        map.insert("data".to_string(), data);
        Ok(map)
    } else {
        Err(Error::BadParse)
    }
}

/// Convert a JSON array into a map containing a Vec for the "data" key
pub fn deserialize_json_array_r<B>(reader: Reader<B>) -> Result<Map<String, Value>>
where
    B: Buf,
{
    let data: Value = serde_json::from_reader(reader)?;

    if data.is_array() {
        let mut map = Map::new();
        map.insert("data".to_string(), data);
        Ok(map)
    } else {
        Err(Error::BadParse)
    }
}

pub fn array_to_map(data: Value) -> Result<Map<String, Value>> {
    if data.is_array() {
        let mut map = Map::new();
        map.insert("data".to_string(), data);
        Ok(map)
    } else {
        Err(Error::BadParse)
    }
}

pub fn get_json_string<'a>(json_obj: &'a Value, key: &str) -> Result<&'a str> {
    json_obj
        .get(key)
        .ok_or_else(|| Error::MissingField(key.to_string()))?
        .as_str()
        .ok_or_else(|| Error::InvalidFieldFormat {
            value: key.to_string(),
            source: anyhow!("could not convert to string"),
        })
}

pub fn from_json_f64(json_obj: &Value, key: &str) -> Result<f64> {
    let num = json_obj.as_str().ok_or_else(|| Error::MissingField(key.to_string()))?;

    num.parse::<f64>().map_err(|e| Error::InvalidFieldFormat {
        value: key.to_string(),
        source: anyhow!(e),
    })
}
