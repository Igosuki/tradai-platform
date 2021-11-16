#![feature(box_patterns)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, TimeZone, Utc};
use itertools::Itertools;
use tokio::sync::Mutex;

use ext::ResultExt;
use strategies::driver::StrategyDriver;
use strategies::margin_interest_rates::test_util::mock_interest_rate_provider;
use strategies::order_manager::test_util::mock_manager;
use strategies::query::{DataQuery, DataResult};
use strategies::settings::StrategySettings;
use strategies::types::{BookPosition, StratEvent};
use strategies::{Channel, DbOptions, Exchange, ExchangeSettings, LiveEvent, LiveEventEnvelope, Pair, StratEventLogger};
use util::test::test_dir;
use util::time::{now, DateRange};

use crate::datasources::orderbook::convert::events_from_orderbooks;
use crate::datasources::orderbook::csv_source::{csv_orderbooks_df, events_from_csv_orderbooks};
use crate::datasources::orderbook::raw_source::raw_orderbooks_df;
use crate::datasources::orderbook::sampled_source::sampled_orderbooks_df;
use crate::report::{BacktestReport, TimedData, TimedVec};
pub use crate::{config::*, error::*};

mod config;
mod datafusion_util;
mod datasources;
mod error;
mod report;

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Dataset {
    OrderbooksByMinute,
    OrderbooksBySecond,
    OrderbooksRaw,
    Trades,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DatasetInputFormat {
    Avro,
    Parquet,
    Csv,
}

impl ToString for DatasetInputFormat {
    fn to_string(&self) -> String {
        match self {
            DatasetInputFormat::Avro => "AVRO",
            DatasetInputFormat::Parquet => "PARQUET",
            DatasetInputFormat::Csv => "CSV",
        }
        .to_string()
    }
}

pub struct Backtest {
    period: DateRange,
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    strategy_events_logger: Arc<VecEventLogger>,
    data_dir: PathBuf,
    output_dir: PathBuf,
    input_format: DatasetInputFormat,
    dataset: Dataset,
    input_sample_rate: Duration,
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
        std::fs::remove_dir_all(db_path.clone()).unwrap();
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
            use_isolated_margin_account: false,
            isolated_margin_account_pairs: vec![],
        };

        let logger: Arc<VecEventLogger> = Arc::new(VecEventLogger::default());
        let strategy_driver = strategies::settings::from_settings(
            &db_conf,
            &exchange_conf,
            strategy_settings,
            Some(order_manager_addr),
            margin_interest_rate_provider_addr,
            Some(logger.clone()),
        );
        Ok(Self {
            period: conf.period.as_range(),
            strategy: Arc::new(Mutex::new(strategy_driver)),
            strategy_events_logger: logger,
            data_dir: conf.data_dir.clone(),
            input_format: conf.input_format.clone(),
            input_sample_rate: conf.input_sample_rate,
            output_dir: output_path,
            dataset: conf.input_dataset,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let chans = {
            let strategy = self.strategy.lock().await;
            strategy.channels()
        };

        let before_read = Instant::now();
        let live_events = self.read_channels(chans).await?;
        let elapsed = before_read.elapsed();
        info!(
            "read {} events in {}.{}s",
            live_events.len(),
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        let report = self.run_strategy(self.strategy.clone(), live_events.as_slice()).await;
        report.write_to(self.output_dir.clone())?;

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }

    async fn read_channels(&self, chans: Vec<Channel>) -> Result<Vec<LiveEventEnvelope>> {
        let mut live_events = vec![];
        for chan in chans {
            match chan {
                Channel::Orderbooks { xch, pair } => {
                    let partitions = self.dataset_partitions(self.period.clone(), xch, &pair);
                    let events = match self.dataset {
                        Dataset::OrderbooksByMinute | Dataset::OrderbooksBySecond => {
                            let records = sampled_orderbooks_df(
                                partitions,
                                Some(pair.to_string()),
                                &self.input_format.to_string(),
                            )
                            .await?;
                            events_from_orderbooks(xch, pair.clone(), records.get(pair.as_ref()).unwrap())
                        }
                        Dataset::OrderbooksRaw => match self.input_format {
                            DatasetInputFormat::Csv => {
                                let records = csv_orderbooks_df(partitions).await?;
                                events_from_csv_orderbooks(xch, pair.clone(), records.as_slice())
                            }
                            _ => {
                                let records = raw_orderbooks_df(
                                    partitions,
                                    self.input_sample_rate,
                                    false,
                                    &self.input_format.to_string(),
                                )
                                .await?;
                                events_from_orderbooks(xch, pair.clone(), records.as_slice())
                            }
                        },
                        _ => panic!("order books channel requires an order books dataset"),
                    };
                    live_events.extend(events);
                }
                Channel::Trades { .. } => {
                    panic!("cannot yet read from trades");
                }
                Channel::Orders { .. } => {
                    panic!("cannot yet read from orders");
                }
            }
        }
        Ok(live_events)
    }

    async fn run_strategy(
        &self,
        strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
        live_events: &[LiveEventEnvelope],
    ) -> BacktestReport {
        let mut strategy = strategy.lock().await;
        let mut report = BacktestReport::default();
        for live_event in live_events.iter().sorted_by_key(|le| le.e.time().timestamp_millis()) {
            util::time::set_current_time(live_event.e.time());
            strategy.add_event(live_event).await.unwrap();
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
            if let LiveEvent::LiveOrderbook(ob) = &live_event.e {
                let bp_try: Result<BookPosition> = ob.try_into().err_into();
                if let Ok(bp) = bp_try {
                    report.book_positions.push(TimedData::new(live_event.e.time(), bp));
                }
            }
            match strategy.data(DataQuery::Models) {
                Ok(DataResult::Models(models)) => report
                    .models
                    .push(TimedData::new(live_event.e.time(), models.into_iter().collect())),
                _ => {
                    report.model_failures += 1;
                }
            }
            match strategy.data(DataQuery::Indicators) {
                Ok(DataResult::Indicators(i)) => report.indicators.push(TimedData::new(live_event.e.time(), i)),
                _ => {
                    report.indicator_failures += 1;
                }
            }
        }
        let read = self.strategy_events_logger.get_events().await;
        report.events = read;
        report
    }

    fn dataset_partitions(&self, period: DateRange, xch: Exchange, pair: &Pair) -> Vec<String> {
        let mut partitions = vec![];
        for date in period {
            let ts = date.and_hms_milli(0, 0, 0, 0).timestamp_millis();
            let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
            let sub_partition = match self.dataset {
                Dataset::OrderbooksByMinute => vec![
                    format!("xch={}", xch),
                    format!("chan={}", "1mn_order_books"),
                    format!("dt={}", dt_par),
                ],
                Dataset::OrderbooksBySecond => vec![
                    format!("xch={}", xch),
                    format!("chan={}", "1s_order_books"),
                    format!("dt={}", dt_par),
                ],
                Dataset::OrderbooksRaw => vec![
                    xch.to_string(),
                    "order_books".to_string(),
                    format!("pr={}", pair),
                    format!("dt={}", dt_par),
                ],
                Dataset::Trades => vec![
                    xch.to_string(),
                    "trades".to_string(),
                    format!("pr={}", pair),
                    format!("dt={}", dt_par),
                ],
            };
            let mut partition_file = self.data_dir.clone();
            for part in sub_partition {
                partition_file.push(part);
            }
            partitions.push(partition_file.as_path().to_string_lossy().to_string());
        }
        partitions
    }
}

#[derive(Clone)]
pub struct VecEventLogger {
    events: Arc<Mutex<TimedVec<StratEvent>>>,
}

impl Default for VecEventLogger {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(vec![])),
        }
    }
}
impl VecEventLogger {
    pub async fn get_events(&self) -> TimedVec<StratEvent> {
        let read = self.events.lock().await;
        read.clone()
    }
}

#[async_trait]
impl StratEventLogger for VecEventLogger {
    async fn maybe_log(&self, event: Option<StratEvent>) {
        if let Some(e) = event {
            let mut write = self.events.lock().await;
            write.push(TimedData::new(now(), e));
        }
    }
}
