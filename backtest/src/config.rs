use std::ops::Sub;
use std::path::PathBuf;

use ::config::{Config, File};
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use parse_duration::parse;
use typed_builder::TypedBuilder;

use strategies::settings::StrategyDriverSettings;
use strategies::StrategyCopySettings;
use util::test::test_dir;
use util::time::{DateRange, DurationRangeType};

use crate::error::*;
use crate::{Dataset, DatasetInputFormat};

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
                DateRange(now.sub(duration).date(), now.date(), DurationRangeType::Days, 1)
            }
            Period::Interval { from, to } => DateRange(
                Utc.from_utc_date(from),
                Utc.from_utc_date(&to.unwrap_or_else(|| Utc::now().naive_utc().date())),
                DurationRangeType::Days,
                1,
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
    pub input_format: DatasetInputFormat,
    pub input_dataset: Dataset,
    pub data_dir: PathBuf,
    #[builder(default, setter(strip_option))]
    pub sql_override: Option<String>,
    #[builder(default, setter(strip_option))]
    pub output_dir: Option<PathBuf>,
    #[serde(deserialize_with = "util::serde::decode_duration_str")]
    pub input_sample_rate: Duration,
}

impl BacktestConfig {
    pub fn new(config_file_name: String) -> Result<Self> {
        let mut s = Config::new();

        s.merge(File::with_name(&config_file_name)).unwrap();

        // You may also programmatically change settings
        s.set("__config_file", config_file_name)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into().map_err(|e| e.into())
    }

    pub fn output_dir(&self) -> PathBuf {
        self.output_dir.clone().unwrap_or_else(|| {
            let mut p = test_dir().into_path();
            p.push("results");
            p
        })
    }
    //pub fn sample_rate(&self) -> Duration { Duration::from_std(parse(&self.input_sample_rate).unwrap()).unwrap() }
}
