/*!
Defines a backtesting tool for all strategies available through `StrategyPlugin`

# Overview

A backtest spawns all configured strategies, feeds them market events from available datasources, and
outputs the state of each strategy to reports after every iteration.
Additionally, the strategies are ranked by PnL in a global report.

If using persistent storage, the databases can be re-used by a trading server directly to run a strategy
with the same configuration as the backtest.

 */

#![allow(
    clippy::wildcard_imports,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::unused_self,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
#![feature(box_patterns)]
#![feature(map_try_insert)]
#![feature(path_try_exists)]
#![feature(exact_size_is_empty)]
#![feature(associated_type_defaults)]
// TODO: https://github.com/rust-lang/rust/issues/47384
#![allow(clippy::single_component_path_imports)]

#[macro_use]
extern crate async_stream;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tokio;
#[macro_use]
extern crate tracing;
extern crate core;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tokio_util::sync::CancellationToken;

use db::DbOptions;
// TODO: https://github.com/rust-lang/rust/issues/47384
use brokers::prelude::*;
use strategy::prelude::*;
// TODO: https://github.com/rust-lang/rust/issues/47384
use trading::engine::{mock_engine, TradingEngine};
use util::time::TimedData;

use crate::dataset::DatasetReader;
use crate::datasources::orderbook::{flat_orderbooks_df, raw_orderbooks_df, sampled_orderbooks_df};
use crate::report::{BacktestReport, GlobalReport, ReportConfig, StreamWriterLogger};
use crate::runner::BacktestRunner;
pub use crate::{config::*,
                dataset::{DataFormat, MarketEventDatasetType},
                error::*};
use brokers::broker::{Broker, ChannelMessageBroker};
use strategy::plugin::plugin_registry;

mod config;
mod datafusion_util;
mod dataset;
mod datasources;
mod error;
mod report;
mod runner;

pub struct Backtest {
    runners: Vec<Arc<RwLock<BacktestRunner>>>,
    output_dir: PathBuf,
    dataset: DatasetReader,
    stop_token: CancellationToken,
    report_conf: ReportConfig,
}

impl Backtest {
    /// # Panics
    ///
    /// if copying strats and spawning runners fail
    pub async fn try_new(conf: &BacktestConfig) -> Result<Self> {
        let output_path = conf.output_dir();
        let all_strategy_settings = all_strategy_settings(conf).await;
        let db_conf = conf.db_conf();
        let mock_engine = Arc::new(mock_engine(db_conf.path.clone(), &[Exchange::Binance]));
        let stop_token = CancellationToken::new();
        let runners: Vec<_> = tokio_stream::iter(all_strategy_settings)
            .map(|s| {
                spawn_runner(
                    stop_token.clone(),
                    conf.runner_queue_size,
                    db_conf.clone(),
                    mock_engine.clone(),
                    s,
                )
            })
            .buffer_unordered(10)
            .collect()
            .await;
        info!("Created {} strategy runners", runners.len());
        Ok(Self {
            stop_token,
            runners,
            output_dir: output_path,
            dataset: DatasetReader {
                input_format: conf.input_format.clone(),
                period: conf.period.as_range(),
                ds_type: conf.input_dataset,
                base_dir: conf.coindata_cache_dir(),
                input_sample_rate: conf.input_sample_rate,
            },
            report_conf: conf.report.clone(),
        })
    }

    /// # Panics
    ///
    /// Writing the global report fails
    pub async fn run(&mut self) -> Result<()> {
        let mut broker = ChannelMessageBroker::new();
        for runner in &self.runners {
            let runner = runner.read().await;
            for channel in runner.channels().await {
                broker.register(channel, runner.event_sink());
            }
        }

        // Start runners
        let (reports_tx, mut reports_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut global_report = GlobalReport::new_with(
            self.output_dir.clone(),
            self.report_conf.parallelism,
            self.report_conf.compression,
        );
        let num_runners = self.spawn_runners(&global_report, reports_tx).await;
        // Read input datasets
        let before_read = Instant::now();
        self.dataset.read_market_events(broker).await?;
        tokio::time::sleep(Duration::from_secs(10)).await;
        self.stop_token.cancel();
        let elapsed = before_read.elapsed();
        info!(
            "processed all market events in {}.{}s",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );

        info!("Awaiting reports...");
        while global_report.len() < num_runners {
            if let Ok(Some(report)) = tokio::time::timeout(Duration::from_secs(30), reports_rx.recv()).await {
                global_report.add_report(report);
            } else {
                break;
            }
        }
        info!("Writing reports...");
        global_report.write().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    }

    /// # Panics
    ///
    /// typically if loading old backtest data fails
    pub async fn gen_report(conf: &BacktestConfig) {
        let mut output_dir = conf.output_dir();
        output_dir.push("latest");
        let dir_list = std::fs::read_dir(output_dir.clone()).unwrap();
        let mut global_report = GlobalReport::new(output_dir.clone());
        let fetches = futures::stream::iter(dir_list.into_iter().map(|file| async {
            let dir_entry = file.unwrap();
            if dir_entry.metadata().unwrap().is_dir() {
                let string = dir_entry.file_name();
                let key = string.to_str().unwrap();
                info!("Reading report at {}", key);
                Some(BacktestReport::reload(key, output_dir.clone(), conf.report.compression).await)
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
        global_report.write_global_report(output_dir.as_path());
    }

    async fn spawn_runners(&self, global_report: &GlobalReport, tx: UnboundedSender<BacktestReport>) -> usize {
        for runner in &self.runners {
            let reports_tx = tx.clone();
            let runner = runner.clone();
            let output_dir = global_report.output_dir.clone();
            let compression = self.report_conf.compression;
            tokio::task::spawn(async move {
                let mut runner = runner.write().await;
                let report = runner.run(output_dir, compression).await;
                reports_tx.send(report).unwrap();
            });
        }
        self.runners.len()
    }
}

async fn init_coinnect(xchs: &[Exchange]) {
    let exchange_apis = Brokerages::public_apis(xchs).await;
    Brokerages::load_pair_registries(&exchange_apis).await.unwrap();
}

async fn spawn_runner(
    stop_token: CancellationToken,
    sink_size: Option<usize>,
    db_conf: DbOptions<PathBuf>,
    engine: Arc<TradingEngine>,
    settings: StrategyDriverSettings,
) -> Arc<RwLock<BacktestRunner>> {
    let logger: Arc<StreamWriterLogger<TimedData<StratEvent>>> = Arc::new(StreamWriterLogger::new());
    let logger2 = logger.clone();
    let strategy_driver = task::spawn_blocking(move || {
        let plugin = plugin_registry().get(settings.strat.strat_type.as_str()).unwrap();
        strategy::settings::from_driver_settings(plugin, &db_conf, &settings, engine, Some(logger.clone())).unwrap()
    })
    .await
    .unwrap();
    let runner = BacktestRunner::new(Arc::new(Mutex::new(strategy_driver)), logger2, stop_token, sink_size);
    Arc::new(RwLock::new(runner))
}

async fn all_strategy_settings(conf: &BacktestConfig) -> Vec<StrategyDriverSettings> {
    let mut all_strategy_settings: Vec<StrategyDriverSettings> = vec![];
    all_strategy_settings.extend_from_slice(conf.strats.as_slice());
    let mut exchanges: HashSet<Exchange> = HashSet::new();
    if let Some(copy) = conf.strat_copy.as_ref() {
        exchanges.extend(copy.exchanges());
        all_strategy_settings.extend_from_slice(copy.all().unwrap().as_slice());
    }
    exchanges.insert(Exchange::Binance);
    init_coinnect(&exchanges.into_iter().collect::<Vec<Exchange>>()).await;
    all_strategy_settings
}
