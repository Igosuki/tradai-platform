use chrono::{Date, Utc};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use brokers::broker::{Broker, ChannelMessageBroker};
use brokers::exchange::Exchange;
use brokers::types::MarketEventEnvelope;
use brokers::Brokerages;
use db::{get_or_create, DbOptions};
use futures::StreamExt;
use strategy::driver::{StratProviderRef, Strategy, StrategyInitContext};
use strategy::prelude::{GenericDriver, GenericDriverOptions, PortfolioOptions};
use strategy::Channel;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use trading::engine::mock_engine;
use util::compress::Compression;
use util::time::{DateRange, DurationRangeType};

use crate::config::BacktestConfig;
use crate::dataset::DatasetReader;
use crate::error::*;
use crate::report::{BacktestReport, GlobalReport, ReportConfig};
use crate::runner::BacktestRunner;
use crate::{DataFormat, MarketEventDatasetType};

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
        let all_strategy_settings = conf.all_strategy_settings().await;
        let db_conf = conf.db_conf();
        let mock_engine = Arc::new(mock_engine(db_conf.path.clone(), &[Exchange::Binance]));
        let stop_token = CancellationToken::new();
        let runners: Vec<_> = tokio_stream::iter(all_strategy_settings)
            .map(|s| BacktestRunner::spawn_with_conf(conf.runner_queue_size, db_conf.clone(), mock_engine.clone(), s))
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
    pub async fn run(&mut self) -> Result<GlobalReport> {
        let broker = build_msg_broker(&self.runners).await;

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
        Ok(global_report)
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
            let stop_token = self.stop_token.clone();
            tokio::task::spawn(async move {
                let mut runner = runner.write().await;
                let report = runner.run(output_dir, compression, stop_token).await;
                reports_tx.send(report).unwrap();
            });
        }
        self.runners.len()
    }
}

pub(crate) async fn init_brokerages(xchs: &[Exchange]) {
    let exchange_apis = Brokerages::public_apis(xchs).await;
    Brokerages::load_pair_registries(&exchange_apis).await.unwrap();
}

pub struct BacktestRange {
    from: Date<Utc>,
    to: Date<Utc>,
}

impl BacktestRange {
    pub fn new(from: Date<Utc>, to: Date<Utc>) -> Self { Self { from, to } }
}

/// Take a dataframe, a period, a strategy provider and configuration and launch a backtest
#[allow(clippy::too_many_lines)]
pub async fn backtest_single<'a>(
    test_name: &'a str,
    provider: StratProviderRef,
    date_range: &'a BacktestRange,
    providers: &'a [Exchange],
    starting_cash: f64,
    fees_rate: f64,
) -> Result<BacktestReport> {
    let path = util::test::test_dir();

    let engine = Arc::new(mock_engine(path.path(), providers));
    let test_results_dir = backtest_results_dir(test_name);
    let options = DbOptions::new(path);
    let db = get_or_create(&options, "", vec![]);
    let strat = provider(StrategyInitContext {
        engine: engine.clone(),
        db: db.clone(),
    });
    let generic_options = GenericDriverOptions {
        portfolio: PortfolioOptions {
            fees_rate,
            initial_quote_cash: starting_cash,
        },
        start_trading: None,
        dry_mode: None,
    };
    let channels = <dyn Strategy>::channels(strat.as_ref());
    for channel in &channels {
        brokers::pair::register_pair_default(
            channel.exchange(),
            &channel.pair().to_string().replace('_', ""),
            &channel.pair(),
        );
    }
    let driver = Box::new(GenericDriver::try_new(channels, db, &generic_options, strat, engine, None).unwrap());
    let runner_ref = BacktestRunner::spawn_with_driver(None, driver).await;
    let broker = build_msg_broker(&[runner_ref.clone()]).await;

    let stop_token = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let stop_token_a = stop_token.clone();
    tokio::spawn(async move {
        let mut runner = runner_ref.write().await;
        let report = runner.run(test_results_dir, Compression::none(), stop_token_a).await;
        report.finish().await.unwrap();
        tx.send(report).await.unwrap();
    });

    let dataset = DatasetReader {
        input_format: DataFormat::Parquet,
        ds_type: MarketEventDatasetType::Trades,
        base_dir: util::test::data_cache_dir(),
        period: DateRange(date_range.from, date_range.to, DurationRangeType::Days, 1),
        input_sample_rate: chrono::Duration::seconds(1),
    };
    dataset.read_market_events(broker).await?;

    stop_token.cancel();
    rx.recv().await.ok_or(crate::error::Error::AnyhowError(anyhow!(
        "Did not receive a backtest report"
    )))
}

async fn build_msg_broker(
    runners: &[Arc<RwLock<BacktestRunner>>],
) -> ChannelMessageBroker<Channel, MarketEventEnvelope> {
    let mut broker = ChannelMessageBroker::new();
    for runner in runners {
        let runner = runner.read().await;
        for channel in runner.channels().await {
            broker.register(channel, runner.event_sink());
        }
    }
    broker
}

/// The base directory of backtest results
/// # Panics
///
/// Panics if the test results directory cannot be created
#[must_use]
pub fn backtest_results_dir(test_name: &str) -> String {
    let base_path = std::env::var("TRADAI_BACKTESTS_OUT_DIR").unwrap_or("backtest_results".to_string());
    let test_results_dir = format!("{}/{}", base_path, test_name);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    test_results_dir
}
