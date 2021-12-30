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

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::broadcast::Sender;
use tokio::sync::{Mutex, RwLock};
use tokio::task;

use db::{DbEngineOptions, DbOptions, RocksDbOptions};
// TODO: https://github.com/rust-lang/rust/issues/47384
#[allow(unused_imports)]
use strategies::mean_reverting;
#[allow(unused_imports)]
use strategies::naive_pair_trading;
use strategy::coinnect::prelude::*;
use strategy::plugin::plugin_registry;
use strategy::prelude::*;
#[allow(unused_imports)]
use strategy_python::script_strat;
use trading::engine::{mock_engine, TradingEngine};
use util::test::test_dir;
use util::time::TimedData;

use crate::coinnect::broker::{Broker, ChannelMessageBroker};
use crate::dataset::Dataset;
use crate::datasources::orderbook::convert::events_from_orderbooks;
use crate::datasources::orderbook::csv_source::{csv_orderbooks_df, events_from_csv_orderbooks};
use crate::datasources::orderbook::raw_source::raw_orderbooks_df;
use crate::datasources::orderbook::sampled_source::sampled_orderbooks_df;
use crate::report::{BacktestReport, GlobalReport, StreamWriterLogger};
use crate::runner::BacktestRunner;
pub use crate::{config::*,
                dataset::{DatasetInputFormat, DatasetType},
                error::*};

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
    dataset: Dataset,
    stop_tx: Sender<bool>,
}

impl Backtest {
    /// # Panics
    ///
    /// if copying strats and spawning runners fail
    pub async fn try_new(conf: &BacktestConfig) -> Result<Self> {
        let db_path = conf.db_path.clone().unwrap_or_else(|| test_dir().into_path());
        if let Ok(true) = std::fs::try_exists(db_path.clone()) {
            std::fs::remove_dir_all(db_path.clone()).unwrap();
        }
        let output_path = conf.output_dir();
        let mut all_strategy_settings: Vec<StrategyDriverSettings> = vec![];
        all_strategy_settings.extend_from_slice(conf.strats.as_slice());
        if let Some(copy) = conf.strat_copy.as_ref() {
            init_coinnect(&copy.exchanges()).await;
            all_strategy_settings.extend_from_slice(copy.all().unwrap().as_slice());
        }
        //let exchanges: Vec<Exchange> = all_strategy_settings.iter().map(|s| s.exchange()).collect();
        let exchanges = vec![Exchange::Binance];
        let mock_engine = Arc::new(mock_engine(db_path.clone(), &exchanges));
        let (stop_tx, _) = tokio::sync::broadcast::channel(1);
        let db_conf = conf.db_conf.as_ref().map_or_else(
            || default_db_conf(db_path.clone()),
            |eo| DbOptions::new_with_options(db_path.clone(), eo.clone()),
        );
        let runners: Vec<_> = tokio_stream::iter(all_strategy_settings.into_iter())
            .map(|s| spawn_runner(stop_tx.clone(), db_conf.clone(), mock_engine.clone(), s))
            .buffer_unordered(10)
            .collect()
            .await;
        info!("Created {} strategy runners", runners.len());
        Ok(Self {
            stop_tx,
            runners,
            output_dir: output_path,
            dataset: Dataset {
                input_format: conf.input_format.clone(),
                period: conf.period.as_range(),
                ds_type: conf.input_dataset,
                base_dir: conf.data_dir.clone(),
                input_sample_rate: conf.input_sample_rate,
            },
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
        let mut global_report = GlobalReport::new(self.output_dir.clone());
        for runner in &self.runners {
            let reports_tx = reports_tx.clone();
            let runner = runner.clone();
            let output_dir = global_report.output_dir.clone();
            tokio::task::spawn(async move {
                let mut runner = runner.write().await;
                let report = runner.run(output_dir).await;
                reports_tx.send(report).unwrap();
            });
        }
        // Read input datasets
        let before_read = Instant::now();
        self.dataset.read_channels(broker).await?;
        let sent = self.stop_tx.send(true).unwrap();
        info!("Closed {} runners", sent);
        let elapsed = before_read.elapsed();
        info!(
            "processed all market events in {}.{}s",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );

        info!("Awaiting {} reports...", sent);
        while global_report.len() < sent {
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
                Some(BacktestReport::reload(key, output_dir.clone()).await)
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
}

async fn init_coinnect(xchs: &[Exchange]) {
    let exchange_apis: DashMap<Exchange, Arc<dyn ExchangeApi>> = DashMap::new();
    let manager = Coinnect::new_manager();
    for xch in xchs {
        let api = manager.build_public_exchange_api(xch, false).await.unwrap();
        exchange_apis.insert(*xch, api);
    }
    Coinnect::load_pair_registries(Arc::new(exchange_apis)).await.unwrap();
}

async fn spawn_runner(
    stop_tx: Sender<bool>,
    db_conf: DbOptions<PathBuf>,
    engine: Arc<TradingEngine>,
    settings: StrategyDriverSettings,
) -> Arc<RwLock<BacktestRunner>> {
    let receiver = stop_tx.subscribe();
    let logger: Arc<StreamWriterLogger<TimedData<StratEvent>>> = Arc::new(StreamWriterLogger::new());
    let logger2 = logger.clone();
    let strategy_driver = task::spawn_blocking(move || {
        let plugin = plugin_registry().get(settings.strat.strat_type.as_str()).unwrap();
        strategy::settings::from_driver_settings(plugin, &db_conf, &settings, engine, Some(logger.clone())).unwrap()
    })
    .await
    .unwrap();
    let runner = BacktestRunner::new(Arc::new(Mutex::new(strategy_driver)), logger2, receiver);
    Arc::new(RwLock::new(runner))
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
