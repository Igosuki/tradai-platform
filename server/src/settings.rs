use std::collections::{HashMap, HashSet};

use chrono::Duration;
use config::{Config, ConfigError, Environment, File};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use coinnect_rt::prelude::*;
use db::DbOptions;
use metrics::prom::PrometheusOptions;
use portfolio::balance::BalanceReporterOptions;
use portfolio::margin::MarginAccountReporterOptions;
use strategy::actor::StrategyActorOptions;
use strategy::prelude::*;
use util::ser::{decode_duration, decode_duration_str, decode_file_size};

use crate::notify::DiscordNotifierOptions;

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
    #[serde(default)]
    pub cors: CorsMode,
    #[serde(default)]
    pub allowed_origins: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum CorsMode {
    Permissive,
    Restricted,
}

impl Default for CorsMode {
    fn default() -> Self { Self::Restricted }
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
    #[serde(deserialize_with = "decode_duration_str")]
    pub partitions_grace_period: Duration,
    pub parallelism: Option<usize>,
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
    AccountBots,
}

fn default_as_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
    pub version: String,
    pub sha: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub __config_file: String,
    #[serde(default)]
    pub exchanges: HashMap<Exchange, ExchangeSettings>,
    #[serde(default)]
    pub streams: Vec<StreamSettings>,
    #[serde(default)]
    pub outputs: Vec<OutputSettings>,
    pub keys: String,
    #[serde(default = "default_as_false")]
    pub profile_main: bool,
    #[serde(default)]
    pub api: ApiSettings,
    #[serde(default)]
    pub strategies: Vec<StrategyDriverSettings>,
    #[serde(default)]
    pub strategies_copy: Vec<StrategyCopySettings>,
    pub storage: DbOptions<String>,
    pub prometheus: PrometheusOptions,
    #[serde(default)]
    pub telemetry: OpenTelemetrySettings,
    pub balance_reporter: Option<BalanceReporterOptions>,
    pub margin_account_reporter: Option<MarginAccountReporterOptions>,
    pub version: Option<Version>,
    pub discord_notifier: Option<DiscordNotifierOptions>,
    pub connectivity_check_interval: Option<u64>,
    #[serde(default)]
    pub strat_actor: StrategyActorOptions,
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
            .map_into()
            .collect();
            let (invalid_pairs, valid_pairs): (_, Vec<Pair>) = pairs
                .clone()
                .into_iter()
                .partition(|p| coinnect_rt::pair::pair_to_symbol(&xchg, p).is_err());
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
