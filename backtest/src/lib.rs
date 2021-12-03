#![feature(box_patterns)]
#![feature(map_try_insert)]
#![feature(path_try_exists)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tracing;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use chrono::Duration;
use futures::{stream, StreamExt};
use itertools::Itertools;
use tokio::sync::Mutex;

use db::{DbEngineOptions, RocksDbOptions};
use strategies::settings::StrategyDriverSettings;
use strategies::{Channel, Coinnect, DbOptions, Exchange, ExchangeApi, ExchangeSettings, MarketEventEnvelope, Pair};
use trading::engine::mock_engine;
use util::test::test_dir;
use util::time::DateRange;

use crate::datasources::orderbook::convert::events_from_orderbooks;
use crate::datasources::orderbook::csv_source::{csv_orderbooks_df, events_from_csv_orderbooks};
use crate::datasources::orderbook::raw_source::raw_orderbooks_df;
use crate::datasources::orderbook::sampled_source::sampled_orderbooks_df;
use crate::report::{BacktestReport, GlobalReport, TimedData, VecEventLogger};
use crate::runner::BacktestRunner;
pub use crate::{config::*,
                dataset::{Dataset, DatasetInputFormat},
                error::*};

mod config;
mod datafusion_util;
mod dataset;
mod datasources;
mod error;
mod report;
mod runner;

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
        let mut all_strategy_settings: Vec<StrategyDriverSettings> = vec![];
        all_strategy_settings.extend_from_slice(conf.strats.as_slice());
        if let Some(copy) = conf.strat_copy.as_ref() {
            init_coinnect(copy.exchange()).await;
            all_strategy_settings.extend_from_slice(copy.all().unwrap().as_slice());
        }
        let exchanges: Vec<Exchange> = all_strategy_settings.iter().map(|s| s.exchange()).collect();
        let mock_engine = Arc::new(mock_engine(db_path.clone(), &exchanges));
        let runners: Vec<BacktestRunner> = all_strategy_settings
            .into_iter()
            .map(|settings| {
                let local_db_path = db_path.clone();
                let exchange_conf = ExchangeSettings::default_test(conf.fees);
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
                let strategy_driver = strategies::settings::from_driver_settings(
                    &db_conf,
                    &exchange_conf,
                    &settings,
                    mock_engine.clone(),
                    Some(logger.clone()),
                );
                info!("Creating strategy : {:?}", settings.key());
                BacktestRunner::new(Arc::new(Mutex::new(strategy_driver)), logger)
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
        let all_chans = stream::iter(&self.runners)
            .map(BacktestRunner::channels)
            .buffer_unordered(10)
            .collect::<Vec<Vec<Channel>>>()
            .await
            .into_iter()
            .flatten()
            .collect();

        let before_read = Instant::now();
        let live_events = self.read_channels(all_chans).await?;
        let elapsed = before_read.elapsed();
        info!(
            "read {} events in {}.{}s",
            live_events.iter().map(|(_, v)| v.len()).sum::<usize>(),
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        let futs = self.runners.iter().map(|r| async {
            let strat_chans = r.channels().await;
            let events: Vec<MarketEventEnvelope> = strat_chans
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
    ) -> Result<HashMap<(Exchange, Pair), Vec<MarketEventEnvelope>>> {
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

async fn init_coinnect(xch: Exchange) {
    let mut exchange_apis: HashMap<Exchange, Arc<dyn ExchangeApi>> = HashMap::new();
    let manager = Coinnect::new_manager();
    let api = manager.build_public_exchange_api(&xch, false).await.unwrap();
    exchange_apis.insert(xch, api);
    Coinnect::load_pair_registries(Arc::new(exchange_apis)).await.unwrap();
}
