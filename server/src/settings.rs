use std::collections::{HashMap, HashSet};

use byte_unit::Byte;
use chrono::Duration;
use config::{Config, ConfigError, Environment, File};
use serde::de;
use serde::{Deserialize, Deserializer};

use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::types::Pair;
use strategies::StrategySettings;

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

#[derive(Debug, Deserialize, Clone)]
pub struct FileRotation {
    /// Max file size in bytes
    #[serde(deserialize_with = "decode_file_size")]
    pub max_file_size: u64,
    /// Max time before closing file
    #[serde(deserialize_with = "decode_duration")]
    pub max_file_time: Duration,
}

#[derive(Debug, Deserialize)]
pub struct Port(pub i32);

/// Timeout in seconds.
impl Default for Port {
    fn default() -> Self { Port(8080) }
}

#[derive(Debug, Deserialize, Default)]
pub struct ApiSettings {
    #[serde(default)]
    pub port: Port,
}

#[derive(Debug, Deserialize)]
pub struct OpenTelemetrySettings {
    pub agents: String,
    pub tags: HashMap<String, String>,
    pub service_name: String,
}

impl Default for OpenTelemetrySettings {
    fn default() -> Self {
        Self {
            agents: "127.0.0.1:6831".to_string(),
            tags: HashMap::new(),
            service_name: "default".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsSettings {
    pub username: String,
    pub password: String,
    pub host: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AvroFileLoggerSettings {
    pub file_rotation: FileRotation,
    pub basedir: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum OutputSettings {
    Nats(NatsSettings),
    AvroFileLogger(AvroFileLoggerSettings),
    Strategies,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum StreamSettings {
    Nats(NatsSettings),
    ExchangeBots,
}

fn default_as_false() -> bool { false }

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub __config_file: String,
    #[serde(default)]
    pub exchanges: HashMap<Exchange, ExchangeSettings>,
    #[serde(default)]
    pub streams: Vec<StreamSettings>,
    #[serde(default)]
    pub outputs: Vec<OutputSettings>,
    pub data_dir: String,
    pub keys: String,
    #[serde(default = "default_as_false")]
    pub profile_main: bool,
    #[serde(default)]
    pub api: ApiSettings,
    #[serde(default)]
    pub strategies: Vec<StrategySettings>,
    pub db_storage_path: String,
    pub prom_push_gw: String,
    pub prom_instance: String,
    #[serde(default)]
    pub telemetry: OpenTelemetrySettings,
}

impl Settings {
    pub fn new(config_file_name: String) -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name(&config_file_name)).unwrap();

        // Add in a local configuration file
        // This file shouldn't be checked in to git
        s.merge(File::with_name("config/local.yaml").required(false))?;

        s.merge(Environment::with_prefix("TRADER"))?;

        // You may also programmatically change settings
        s.set("__config_file", config_file_name)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into()
    }

    pub fn sanitize(&self) {
        for (xchg, xchg_settings) in self.exchanges.clone() {
            info!("{:?} : Checking exchange config...", xchg);
            let pairs: HashSet<Pair> = vec![
                xchg_settings.trades.map(|ts| ts.symbols),
                xchg_settings.orderbook.map(|obs| obs.symbols),
            ]
            .into_iter()
            .flatten()
            .flatten()
            .collect();
            let (invalid_pairs, valid_pairs): (_, Vec<Pair>) = pairs
                .clone()
                .into_iter()
                .partition(|p| coinnect_rt::pair::pair_to_symbol(&xchg, p).is_none());
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
