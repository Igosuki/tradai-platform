#![feature(box_patterns)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use chrono::Duration;
use serde::{ser::SerializeSeq, Serializer};
use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use strategies::driver::StrategyDriver;
use strategies::input::partition_path;
use strategies::margin_interest_rates::test_util::mock_interest_rate_provider;
use strategies::order_manager::test_util::mock_manager;
use strategies::query::{DataQuery, DataResult};
use strategies::settings::StrategySettings;
use strategies::{Channel, DbOptions, ExchangeSettings};
use util::date::DateRange;
use util::test::test_dir;

use crate::datasources::avro_orderbook::{avro_orderbooks_df, events_from_avro_orderbooks};
use crate::datasources::csv_orderbook::{csv_orderbooks_df, events_from_csv_orderbooks};
pub use crate::{config::*, error::*};

mod config;
mod datasources;
mod error;

#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum OrderbookInputMode {
    AvroRaw {
        #[serde(deserialize_with = "util::serde::decode_duration_str")]
        sample_rate: Duration,
    },
    CsvDownsampled,
}

#[derive(Serialize, Deserialize, Default)]
struct BacktestReport {
    model_failures: u32,
}

pub struct Backtest {
    period: DateRange,
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    data_dir: PathBuf,
    output_dir: PathBuf,
    orderbook_input_mode: OrderbookInputMode,
}

impl Backtest {
    pub fn try_new(conf: &BacktestConfig) -> Result<Self> {
        let db_path = conf
            .db_path
            .clone()
            .unwrap_or_else(|| test_dir().into_path())
            .into_os_string()
            .into_string()
            .unwrap();
        info!("db_path = {:?}", db_path);
        let output_path = conf.output_dir.clone().unwrap_or_else(|| {
            let mut p = test_dir().into_path();
            p.push("results");
            p
        });
        info!("output_path = {:?}", db_path);
        let order_manager_addr = mock_manager(&db_path);
        let margin_interest_rate_provider_addr = mock_interest_rate_provider(conf.strat.exchange());
        let generic_strat = StrategySettings::Generic(Box::new(conf.strat.clone()));
        let strategy_settings = if conf.use_generic { &generic_strat } else { &conf.strat };
        let db_conf = DbOptions::new(db_path);
        let exchange_conf = ExchangeSettings {
            fees: conf.fees,
            trades: None,
            orderbook: None,
            orderbook_depth: None,
            use_margin_account: false,
            use_account: true,
            use_test: true,
        };
        let strategy_driver = strategies::settings::from_settings(
            &db_conf,
            &exchange_conf,
            &strategy_settings,
            Some(order_manager_addr),
            margin_interest_rate_provider_addr.clone(),
        );
        Ok(Self {
            period: conf.period.as_range(),
            strategy: Arc::new(Mutex::new(strategy_driver)),
            data_dir: conf.data_dir.clone(),
            orderbook_input_mode: conf.orderbook_input_mode.clone(),
            output_dir: output_path,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let mut strategy = self.strategy.lock().await;
        let chans = strategy.channels();
        let period = self.period.clone();
        let mut live_events = vec![];
        for chan in chans {
            match chan {
                Channel::Orderbooks { xch, pair } => {
                    let mut partitions = vec![];
                    for date in period.clone() {
                        let partition = partition_path(
                            &xch.to_string(),
                            date.and_hms_milli(0, 0, 0, 0).timestamp_millis(),
                            "order_books",
                            pair.as_ref(),
                        );
                        if let Some(partition) = partition {
                            let mut partition_file = self.data_dir.clone();
                            partition_file.push(partition);
                            partitions.push(partition_file.as_path().to_string_lossy().to_string());
                        }
                    }
                    match self.orderbook_input_mode {
                        OrderbookInputMode::AvroRaw { sample_rate } => {
                            let records = avro_orderbooks_df(partitions, sample_rate, false).await?;
                            live_events.extend(events_from_avro_orderbooks(xch, pair.clone(), records))
                        }
                        OrderbookInputMode::CsvDownsampled => {
                            let records = csv_orderbooks_df(partitions).await?;
                            live_events.extend(events_from_csv_orderbooks(xch, pair.clone(), records))
                        }
                    }
                }
                Channel::Trades { .. } => {
                    panic!("cannot yet read from trades");
                }
                Channel::Orders { .. } => {
                    panic!("cannot yet read from orders");
                }
            }
        }
        let mut all_models: Vec<Vec<(String, Option<serde_json::Value>)>> = vec![];
        let mut report = BacktestReport::default();
        for live_event in live_events {
            strategy.add_event(&live_event).await.unwrap();
            // If there is an ongoing operation, resolve orders
            let mut tries = 0;
            loop {
                if tries > 5 {
                    break;
                }
                let open_ops = strategy.data(DataQuery::OpenOperations);
                if matches!(
                    open_ops,
                    Ok(DataResult::NaiveOperation(box Some(_))) | Ok(DataResult::MeanRevertingOperation(box Some(_)))
                ) {
                    strategy.resolve_orders().await;
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    tries += 1;
                } else {
                    break;
                }
            }
            match strategy.data(DataQuery::Models) {
                Ok(DataResult::Models(models)) => all_models.push(models),
                _ => {
                    report.model_failures += 1;
                }
            }
        }
        std::fs::create_dir_all(self.output_dir.clone())?;
        write_models(self.output_dir.clone(), all_models);
        write_report(self.output_dir.clone(), report);

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(())
    }
}

fn write_report(output_dir: PathBuf, report: BacktestReport) {
    let mut file = output_dir;
    file.push("report.json");
    let logs_f = std::fs::File::create(file).unwrap();
    let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = ser.serialize_seq(None).unwrap();
    seq.serialize_element(&report).unwrap();
    seq.end().unwrap();
}

fn write_models(output_dir: PathBuf, all_models: Vec<Vec<(String, Option<serde_json::Value>)>>) {
    let mut file = output_dir;
    file.push("models.json");
    let logs_f = std::fs::File::create(file).unwrap();
    let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = ser.serialize_seq(None).unwrap();
    for models in all_models {
        let obj: BTreeMap<String, Option<serde_json::Value>> = models.into_iter().collect();
        seq.serialize_element(&obj).unwrap();
    }
    seq.end().unwrap();
}
