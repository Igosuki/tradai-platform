use std::env;
use config::{ConfigError, Config, File, Environment};
use crate::handlers::file_actor::FileActorOptions;
use chrono::Duration;
use serde::{Deserializer, Deserialize};

fn decode_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where Duration: Sized,
          D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::milliseconds(val))
}

#[derive(Debug, Deserialize)]
pub struct FileRotation {
    /// Max file size in bytes
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
