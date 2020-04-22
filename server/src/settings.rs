use std::collections::{HashMap, HashSet};

use byte_unit::Byte;
use chrono::Duration;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::types::Pair;
use config::{Config, ConfigError, Environment, File};
use serde::de;
use serde::{Deserialize, Deserializer};
use strategies::Strategy;

fn decode_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    Duration: Sized,
    D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::seconds(val))
}

fn decode_file_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let val: String = Deserialize::deserialize(deserializer)?;
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
pub struct Port(pub u16);

/// Timeout in seconds.
impl Default for Port {
    fn default() -> Self {
        Port(8080)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct ApiSettings {
    #[serde(default)]
    pub port: Port,
}

fn default_as_false() -> bool {
    false
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub file_rotation: FileRotation,
    pub data_dir: String,
    pub __config_file: String,
    pub exchanges: HashMap<Exchange, ExchangeSettings>,
    pub keys: String,
    #[serde(default = "default_as_false")]
    pub profile_main: bool,
    #[serde(default)]
    pub api: ApiSettings,
    #[serde(default)]
    pub strategies: Vec<Strategy>,
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

    pub fn sanitize(&mut self) {
        for (xchg, xchg_settings) in self.exchanges.clone() {
            info!("{:?} : Checking exchange config...", xchg);
            let pairs: HashSet<Pair> = vec![xchg_settings.trades, xchg_settings.orderbook]
                .into_iter()
                .filter(|s| s.is_some())
                .map(|o| o.unwrap())
                .flat_map(|o| o.symbols)
                .collect();
            let pairs_fn: fn(&Pair) -> Option<&&str> = coinnect_rt::utils::pair_fn(xchg);
            let invalid_pairs: Vec<Pair> = pairs
                .clone()
                .into_iter()
                .filter(|&p| pairs_fn(&p).is_none())
                .collect();
            let valid_pairs: Vec<Pair> = pairs
                .clone()
                .into_iter()
                .filter(|&p| pairs_fn(&p).is_some())
                .collect();
            info!("Invalid pairs : {:?}", invalid_pairs);
            info!("Valid pairs : {:?}", valid_pairs);
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_deserialize() {}
}
