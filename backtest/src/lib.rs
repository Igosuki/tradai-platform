#![feature(box_patterns)]
#![feature(map_try_insert)]
#![feature(path_try_exists)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate serde_derive;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, TimeZone, Utc};
use futures::StreamExt;
use itertools::Itertools;
use tokio::sync::Mutex;

use db::{DbEngineOptions, RocksDbOptions};
use ext::ResultExt;
use strategies::driver::StrategyDriver;
use strategies::margin_interest_rates::test_util::mock_interest_rate_provider;
use strategies::query::{DataQuery, DataResult};
use strategies::types::StratEvent;
use strategies::{Channel, Coinnect, DbOptions, Exchange, ExchangeApi, ExchangeSettings, LiveEvent, LiveEventEnvelope,
                 Pair, StratEventLogger};
use trading::book::BookPosition;
use trading::order_manager::test_util::mock_manager;
use util::test::test_dir;
use util::time::{now, DateRange};

use crate::datasources::orderbook::convert::events_from_orderbooks;
use crate::datasources::orderbook::csv_source::{csv_orderbooks_df, events_from_csv_orderbooks};
use crate::datasources::orderbook::raw_source::raw_orderbooks_df;
use crate::datasources::orderbook::sampled_source::sampled_orderbooks_df;
use crate::report::{BacktestReport, GlobalReport, TimedData, TimedVec};
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

impl Dataset {
    fn partitions(&self, data_dir: PathBuf, period: DateRange, xch: Exchange, pair: &Pair) -> HashSet<String> {
        let mut partitions = HashSet::new();
        for date in period {
            let ts = date.and_hms_milli(0, 0, 0, 0).timestamp_millis();
            let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
            let sub_partition = match self {
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
            let mut partition_file = data_dir.clone();
            for part in sub_partition {
                partition_file.push(part);
            }
            partitions.insert(partition_file.as_path().to_string_lossy().to_string());
        }
        partitions
    }
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
    runners: Vec<BacktestRunner>,
    data_dir: PathBuf,
    output_dir: PathBuf,
    input_format: DatasetInputFormat,
    dataset: Dataset,
    input_sample_rate: Duration,
}

impl Backtest {
    pub async fn try_new(conf: &BacktestConfig) -> Result<Self> {
        let db_path = conf.db_path.clone().unwrap_or_else(|| test_dir().into_path());
        if let Ok(true) = std::fs::try_exists(db_path.clone()) {
            std::fs::remove_dir_all(db_path.clone()).unwrap();
        }
        let output_path = conf.output_dir();
        let mut all_strategy_settings = vec![];
        all_strategy_settings.extend_from_slice(conf.strats.as_slice());
        if let Some(copy) = conf.strat_copy.as_ref() {
            init_coinnect(copy.exchange()).await;
            all_strategy_settings.extend_from_slice(copy.all().unwrap().as_slice());
        }
        // TODO: factorize all of that shit into an 'Engine'
        let order_manager_addr = mock_manager(&db_path.clone());
        let runners: Vec<BacktestRunner> = all_strategy_settings
            .into_iter()
            .map(|settings| {
                let local_db_path = db_path.clone();
                let exchange_conf = ExchangeSettings::default_test(conf.fees);
                let margin_interest_rate_provider_addr = mock_interest_rate_provider(settings.exchange());

                let db_conf = DbOptions {
                    path: local_db_path,
                    engine: DbEngineOptions::RocksDb(
                        RocksDbOptions::default()
                            .max_log_file_size(10 * 1024 * 1024)
                            .keep_log_file_num(2)
                            .max_total_wal_size(10 * 1024 * 1024),
                    ),
                };

                let logger: Arc<VecEventLogger> = Arc::new(VecEventLogger::default());
                let strategy_driver = strategies::settings::from_settings(
                    &db_conf,
                    &exchange_conf,
                    &settings,
                    Some(order_manager_addr.clone()),
                    margin_interest_rate_provider_addr,
                    Some(logger.clone()),
                );
                info!("Creating strategy : {:?}", settings.key());
                BacktestRunner {
                    strategy_events_logger: logger,
                    strategy: Arc::new(Mutex::new(strategy_driver)),
                }
            })
            .collect();
        info!("Created {} strategy runners", runners.len());
        Ok(Self {
            period: conf.period.as_range(),
            runners,
            data_dir: conf.data_dir.clone(),
            input_format: conf.input_format.clone(),
            input_sample_rate: conf.input_sample_rate,
            output_dir: output_path,
            dataset: conf.input_dataset,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let chans = {
            futures::future::join_all(self.runners.iter().map(|r| async {
                let reader = r.strategy.lock().await;
                reader.channels()
            }))
            .await
        }
        .into_iter()
        .flatten()
        .collect();

        let before_read = Instant::now();
        let live_events = self.read_channels(chans).await?;
        let elapsed = before_read.elapsed();
        info!(
            "read {} events in {}.{}s",
            live_events.iter().map(|(_, v)| v.len()).sum::<usize>(),
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        let futs = self.runners.iter().map(|r| async {
            let strat_chans = {
                let strat = r.strategy.lock().await;
                strat.channels()
            };
            let events: Vec<LiveEventEnvelope> = strat_chans
                .into_iter()
                .map(|c| live_events.get(&(c.exchange(), c.pair())))
                .flatten()
                .cloned()
                .flatten()
                .collect();
            r.run(events.as_slice()).await
        });
        let mut global_report = GlobalReport::new(self.output_dir.clone());
        let reports: Vec<BacktestReport> = futures::future::join_all(futs).await;
        for report in reports {
            global_report.add_report(report)
        }
        global_report.write().unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }

    async fn read_channels(
        &self,
        chans: HashSet<Channel>,
    ) -> Result<HashMap<(Exchange, Pair), Vec<LiveEventEnvelope>>> {
        let mut live_events = HashMap::new();
        match self.dataset {
            Dataset::OrderbooksByMinute | Dataset::OrderbooksBySecond => {
                let partitions: HashSet<String> = chans
                    .iter()
                    .filter(|c| matches!(c, Channel::Orderbooks { .. }))
                    .map(|c| match c {
                        Channel::Orderbooks { xch, pair } => {
                            self.dataset
                                .partitions(self.data_dir.clone(), self.period.clone(), *xch, pair)
                        }
                        _ => HashSet::new(),
                    })
                    .flatten()
                    .collect();
                let records = sampled_orderbooks_df(partitions, &self.input_format.to_string()).await?;
                let xch = chans.iter().last().unwrap().exchange();
                let events = events_from_orderbooks(xch, records.as_slice());
                live_events.extend(events.into_iter().map(|e| ((xch, e.e.pair()), e)).into_group_map());
            }
            Dataset::OrderbooksRaw => {
                for chan in chans {
                    if let Channel::Orderbooks { xch, pair } = chan {
                        let partitions =
                            self.dataset
                                .partitions(self.data_dir.clone(), self.period.clone(), xch, &pair);
                        let events = match self.input_format {
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
                                events_from_orderbooks(xch, records.as_slice())
                            }
                        };
                        live_events.extend(events.into_iter().map(|e| ((xch, e.e.pair()), e)).into_group_map());
                    }
                }
            }
            _ => panic!("order books channel requires an order books dataset"),
        }

        Ok(live_events)
    }

    pub async fn gen_report(conf: &BacktestConfig) {
        let mut output_dir = conf.output_dir();
        output_dir.push("latest");
        let dir_list = std::fs::read_dir(output_dir.clone()).unwrap();
        let mut global_report = GlobalReport::new(output_dir.clone());
        let fetches = futures::stream::iter(dir_list.into_iter().map(|file| async {
            let dir_entry = file.unwrap();
            if dir_entry.metadata().unwrap().is_dir() {
                let report_os_string = dir_entry.file_name();
                let report_dir = report_os_string.to_str().unwrap();
                info!("Reading report at {}", report_dir);
                Some(BacktestReport::from_files(report_dir, dir_entry.path()))
            } else {
                None
            }
        }))
        .buffer_unordered(10)
        .filter_map(futures::future::ready)
        .collect::<Vec<BacktestReport>>();
        for report in fetches.await {
            global_report.add_report(report);
        }
        global_report.write_global_report(output_dir.as_path()).unwrap();
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

struct BacktestRunner {
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    strategy_events_logger: Arc<VecEventLogger>,
}

impl BacktestRunner {
    async fn run(&self, live_events: &[LiveEventEnvelope]) -> BacktestReport {
        let mut strategy = self.strategy.lock().await;
        let mut report = BacktestReport::new(strategy.key().await);
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
}

async fn init_coinnect(xch: Exchange) {
    let mut exchange_apis: HashMap<Exchange, Arc<dyn ExchangeApi>> = HashMap::new();
    let manager = Coinnect::new_manager();
    let api = manager.build_public_exchange_api(&xch, false).await.unwrap();
    exchange_apis.insert(xch, api);
    Coinnect::load_pair_registries(Arc::new(exchange_apis)).await.unwrap();
}
