use std::collections::HashSet;
use std::ops::Sub;
use std::path::{Path, PathBuf};

use ::config::{Config, File};
use brokers::exchange::Exchange;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use parse_duration::parse;
use typed_builder::TypedBuilder;

use db::{DbEngineOptions, DbOptions, RocksDbOptions};
use strategy::settings::StrategyCopySettings;
use strategy::settings::StrategyDriverSettings;
use util::test::test_dir;
use util::time::DateRange;

use crate::backtest::init_brokerages;
use crate::{DataFormat, MarketEventDatasetType};

use crate::error::*;
use crate::report::ReportConfig;

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Period {
    Since { since: String },
    Interval { from: NaiveDate, to: Option<NaiveDate> },
}

impl Period {
    pub(crate) fn as_range(&self) -> DateRange {
        match self {
            Period::Since { since } => {
                let duration = Duration::from_std(parse(since).unwrap()).unwrap();
                let now = Utc::now();
                DateRange::by_day(now.sub(duration).date().and_hms(0, 0, 0), now.date().and_hms(0, 0, 0))
            }
            Period::Interval { from, to } => DateRange::by_day(
                Utc.from_utc_date(from).and_hms(0, 0, 0),
                Utc.from_utc_date(&to.unwrap_or_else(|| Utc::now().naive_utc().date()))
                    .and_hms(0, 0, 0),
            ),
        }
    }
}

#[derive(Deserialize, TypedBuilder)]
pub struct BacktestConfig {
    #[builder(default, setter(strip_option))]
    pub db_path: Option<PathBuf>,
    pub strats: Vec<StrategyDriverSettings>,
    pub strat_copy: Option<StrategyCopySettings>,
    pub fees: f64,
    pub period: Period,
    pub input_format: DataFormat,
    pub input_dataset: MarketEventDatasetType,
    pub coindata_cache_dir: Option<PathBuf>,
    #[builder(default, setter(strip_option))]
    pub sql_override: Option<String>,
    #[builder(default, setter(strip_option))]
    pub output_dir: Option<PathBuf>,
    #[serde(deserialize_with = "util::ser::decode_duration_str")]
    pub input_sample_rate: Duration,
    pub db_conf: Option<DbEngineOptions>,
    pub report: ReportConfig,
    pub runner_queue_size: Option<usize>,
}

impl BacktestConfig {
    /// # Panics
    ///
    /// if the config file does not exist
    pub fn new(config_file_name: String) -> Result<Self> {
        let mut s = Config::new();

        s.merge(File::with_name(&config_file_name)).unwrap();

        // You may also programmatically change settings
        s.set("__config_file", config_file_name)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into().map_err(Into::into)
    }

    pub fn output_dir(&self) -> PathBuf {
        self.output_dir.clone().unwrap_or_else(|| {
            let mut p = test_dir().into_path();
            p.push("results");
            p
        })
    }

    pub fn db_path(&self) -> PathBuf {
        let db_path = self.db_path.clone().unwrap_or_else(|| test_dir().into_path());
        if let Ok(true) = std::fs::try_exists(db_path.clone()) {
            std::fs::remove_dir_all(db_path.clone()).unwrap();
        }
        db_path
    }

    pub fn db_conf(&self) -> DbOptions<PathBuf> {
        let db_path = self.db_path();
        self.db_conf.as_ref().map_or_else(
            || default_db_conf(db_path.clone()),
            |eo| DbOptions::new_with_options(db_path.clone(), eo.clone()),
        )
    }

    //pub fn sample_rate(&self) -> Duration { Duration::from_std(parse(&self.input_sample_rate).unwrap()).unwrap() }

    pub(crate) fn coindata_cache_dir(&self) -> PathBuf {
        self.coindata_cache_dir
            .clone()
            .unwrap_or_else(|| Path::new(&std::env::var("COINDATA_CACHE_DIR").unwrap()).join("data"))
    }

    pub(crate) async fn all_strategy_settings(&self) -> Vec<StrategyDriverSettings> {
        let mut all_strategy_settings: Vec<StrategyDriverSettings> = vec![];
        all_strategy_settings.extend_from_slice(self.strats.as_slice());
        let mut exchanges: HashSet<Exchange> = HashSet::new();
        if let Some(copy) = self.strat_copy.as_ref() {
            exchanges.extend(copy.exchanges());
            all_strategy_settings.extend_from_slice(copy.all().unwrap().as_slice());
        }
        exchanges.insert(Exchange::Binance);
        init_brokerages(&exchanges.into_iter().collect::<Vec<Exchange>>()).await;
        all_strategy_settings
    }
}

fn default_db_conf<S: AsRef<Path>>(local_db_path: S) -> DbOptions<S> {
    DbOptions {
        path: local_db_path,
        engine: DbEngineOptions::RocksDb(
            RocksDbOptions::default()
                .max_log_file_size(10 * 1024 * 1024)
                .keep_log_file_num(2)
                .max_total_wal_size(10 * 1024 * 1024),
        ),
    }
}
