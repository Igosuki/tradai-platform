
use config::{ConfigError, Config, File, Environment};

use chrono::Duration;
use serde::{Deserializer, Deserialize};
use byte_unit::Byte;
use serde::de;
use coinnect_rt::types::Pair;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use std::collections::HashMap;

fn decode_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where Duration: Sized,
          D: Deserializer<'de> {
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::seconds(val))
}

fn decode_file_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where D: Deserializer<'de> {
    let val : String = Deserialize::deserialize(deserializer)?;
    let size_bytes = Byte::from_str(val).map_err(|e| de::Error::custom(format!("{:?}", e)))?;
    Ok(size_bytes.get_bytes() as u64)
}

#[derive(Debug, Deserialize)]
pub struct FileRotation {
    /// Max file size in bytes
    #[serde(deserialize_with = "decode_file_size")]
    pub max_file_size: u64,
    /// Max time before closing file
    #[serde(deserialize_with = "decode_duration")]
    pub max_file_time: Duration,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub file_rotation: FileRotation,
    pub data_dir: String,
    pub __config_file: String,
    pub exchanges: HashMap<Exchange, ExchangeSettings>,
    pub keys: String,
}

impl Settings {
    pub fn new(env: String) -> Result<Self, ConfigError> {
        let config_file = format!("config/{}.yaml", env);
        let mut s = Config::new();

        s.merge(File::with_name(&config_file)).unwrap();

        // Add in a local configuration file
        // This file shouldn't be checked in to git
        s.merge(File::with_name("config/local.yaml").required(false))?;

        s.merge(Environment::with_prefix("TRADER"))?;

        // You may also programmatically change settings
        s.set("__config_file", config_file)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into()
    }
}
