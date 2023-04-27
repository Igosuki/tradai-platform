use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use itertools::Itertools;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use brokers::broker::{Broker, ChannelMessageBroker};
use brokers::exchange::Exchange;
use brokers::plugin::gather_plugins;
use brokers::types::MarketEventEnvelope;
use brokers::types::{MarketChannel, MarketChannelTopic};
use brokers::Brokerages;
use db::{get_or_create, DbOptions};
use strategy::driver::{StratProviderRef, Strategy, StrategyInitContext};
use strategy::prelude::{GenericDriver, GenericDriverOptions, PortfolioOptions};
use trading::engine::mock_engine;
use util::compress::Compression;
use util::time::DateRange;

use crate::config::BacktestConfig;
use crate::dataset::{DatasetCatalog, DatasetReader};
use crate::error::*;
use crate::report::{BacktestReport, GlobalReport, ReportConfig};
use crate::runner::BacktestRunner;

pub(crate) async fn init_brokerages(xchs: &[Exchange]) {
    let exchange_apis = Brokerages::public_apis(xchs).await;
    Brokerages::load_pair_registries(&exchange_apis).await.unwrap();
}

async fn build_msg_broker(
    runners: &[Arc<RwLock<BacktestRunner>>],
) -> ChannelMessageBroker<MarketChannelTopic, MarketEventEnvelope> {
    let mut broker = ChannelMessageBroker::new();
    for runner in runners {
        let runner = runner.read().await;
        for channel in runner.channels().await {
            broker.register(channel.into(), runner.event_sink());
        }
    }
    broker
}

async fn get_channels(runners: &[Arc<RwLock<BacktestRunner>>]) -> Vec<MarketChannel> {
    let mut channels = vec![];
    for runner in runners {
        let runner = runner.read().await;
        channels.extend(runner.channels().await);
    }
    channels
}

/// The base directory of backtest results
/// # Panics
///
/// Panics if the test results directory cannot be created
#[must_use]
pub fn backtest_results_dir(test_name: &str) -> String {
    let base_path = std::env::var("TRADAI_BACKTESTS_OUT_DIR")
        .or_else(|_| std::env::var("CARGO_MANIFEST_DIR"))
        .unwrap_or_else(|_| "".to_string());
    let test_results_dir = Path::new(&base_path).join("backtest_results").join(test_name);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    format!("{}", test_results_dir.as_path().display())
}

pub struct Backtest {
    runners: Vec<Arc<RwLock<BacktestRunner>>>,
    output_dir: PathBuf,
    dataset: DatasetReader,
    stop_token: CancellationToken,
    report_conf: ReportConfig,
    period: DateRange,
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
            .map(|s| {
                BacktestRunner::spawn_with_conf(
                    conf.runner_queue_size,
                    conf.report_sample_rate
                        .map(|d| chrono::Duration::milliseconds(d.as_millis() as i64)),
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
            period: conf.period.as_range(),
            output_dir: output_path,
            dataset: DatasetReader {
                catalog: DatasetCatalog::default_basedir(conf.coindata_cache_dir()),
            },
            report_conf: conf.report.clone(),
        })
    }

    /// # Panics
    ///
    /// Writing the global report fails
    pub async fn run(&mut self) -> Result<GlobalReport> {
        let broker = build_msg_broker(&self.runners).await;
        let channels = get_channels(&self.runners).await;
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
        self.dataset.stream_with_broker(&channels, &broker, self.period).await?;
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

        tokio::time::sleep(Duration::from_secs(1)).await;
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

async fn build_runner(
    report_sample_freq: Option<chrono::Duration>,
    provider: StratProviderRef,
    starting_cash: Option<f64>,
    fees_rate: Option<f64>,
) -> Arc<RwLock<BacktestRunner>> {
    let path = util::test::test_dir();
    let brokerages = gather_plugins();
    let providers = brokerages.keys().copied().collect_vec();
    let engine = Arc::new(mock_engine(path.path(), &providers));
    let options = DbOptions::new(path);
    let db = get_or_create(&options, "", vec![]);
    let strat = provider(StrategyInitContext {
        engine: engine.clone(),
        db: db.clone(),
    });
    let generic_options = GenericDriverOptions {
        portfolio: PortfolioOptions {
            fees_rate: fees_rate.unwrap_or(0.001),
            initial_quote_cash: starting_cash.unwrap_or(100.0),
        },
        start_trading: None,
        dry_mode: None,
    };
    let channels = <dyn Strategy>::channels(strat.as_ref());
    for channel in &channels {
        brokers::pair::register_pair_default(
            channel.exchange(),
            &channel.pair().to_string().replace('_', ""),
            channel.pair(),
        );
    }
    let logger = BacktestRunner::strat_event_logger(None);
    let logger2 = logger.clone();
    let driver =
        Box::new(GenericDriver::try_new(channels, db, &generic_options, strat, engine, Some(logger2)).unwrap());
    let runner_ref = BacktestRunner::spawn_with_driver(None, report_sample_freq, logger, driver).await;

    runner_ref
}

async fn start_bt(
    test_name: &str,
    runner_ref: Arc<RwLock<BacktestRunner>>,
) -> (Receiver<BacktestReport>, CancellationToken) {
    let test_results_dir = backtest_results_dir(test_name);

    let stop_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let stop_token_a = stop_token.clone();
    tokio::task::spawn_local(async move {
        let mut runner = runner_ref.write().await;
        let report = runner.run(test_results_dir, Compression::none(), stop_token_a).await;
        report.finish().await.unwrap();
        tx.send(report).await.unwrap();
    });
    (rx, stop_token)
}

/// Backtest over a period of time, reading data from the required channels
pub async fn backtest_with_range<'a>(
    test_name: &str,
    provider: StratProviderRef,
    dt_range: DateRange,
    starting_cash: Option<f64>,
    fees_rate: Option<f64>,
    data_catalog: Option<DatasetCatalog>,
) -> Result<BacktestReport> {
    let runner_ref = build_runner(None, provider, starting_cash, fees_rate).await;
    let broker = build_msg_broker(&[runner_ref.clone()]).await;
    let channels = get_channels(&[runner_ref.clone()]).await;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let (mut rx, stop_token) = start_bt(test_name, runner_ref).await;
            let catalog = data_catalog.unwrap_or_else(DatasetCatalog::default_prod);
            let dataset = DatasetReader { catalog };
            dataset.stream_with_broker(&channels, &broker, dt_range).await?;
            stop_token.cancel();
            rx.recv()
                .await
                .ok_or_else(|| Error::AnyhowError(anyhow!("Did not receive a backtest report")))
        })
        .await
}

/// Load market events over the provided range
pub async fn load_market_events(
    channels: Vec<MarketChannel>,
    dt_range: DateRange,
    mapper: Option<DatasetCatalog>,
) -> Result<Vec<MarketEventEnvelope>> {
    let catalog = mapper.unwrap_or_else(DatasetCatalog::default_prod);
    let mut market_events = vec![];
    let reader = DatasetReader { catalog };
    for c in channels {
        market_events.extend(reader.read_all_events(&[c], dt_range).await?);
    }
    market_events.sort_by(|me1, me2| me1.e.time().timestamp_millis().cmp(&me2.e.time().timestamp_millis()));
    Ok(market_events)
}

/// Load market events over the provided range
pub async fn load_market_events_df(
    channels: Vec<MarketChannel>,
    dt_range: DateRange,
    mapper: Option<DatasetCatalog>,
) -> Result<Vec<RecordBatch>> {
    let catalog = mapper.unwrap_or_else(DatasetCatalog::default_prod);
    let mut market_events = vec![];
    let reader = DatasetReader { catalog };
    for c in channels {
        let read_events: Vec<_> = reader
            .read_all_events_df(&[c], dt_range)
            .await?
            .into_iter()
            .filter(|rb| rb.num_rows() > 0)
            .collect();
        market_events.extend(read_events);
    }
    Ok(market_events)
}

/// Backtest over the given event stream, reading data from the required channels
pub async fn backtest_with_events(
    test_name: &str,
    provider: StratProviderRef,
    events: Vec<MarketEventEnvelope>,
    starting_cash: Option<f64>,
    fees_rate: Option<f64>,
) -> Result<BacktestReport> {
    let runner_ref = build_runner(None, provider, starting_cash, fees_rate).await;
    let sink = async {
        let runner = runner_ref.read().await;
        runner.event_sink()
    }
    .await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let (mut rx, stop_token) = start_bt(test_name, runner_ref).await;
            for event in events {
                sink.send(event).await.unwrap();
            }
            stop_token.cancel();
            rx.recv()
                .await
                .ok_or_else(|| Error::AnyhowError(anyhow!("Did not receive a backtest report")))
        })
        .await
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::ops::Add;
    use std::sync::Arc;

    use chrono::{DateTime, Duration, NaiveDate, Utc};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::util::pretty;

    use brokers::exchange::Exchange;
    use brokers::pair::register_pair_default;
    use brokers::prelude::MarketEventEnvelope;
    use brokers::types::{Candle, MarketChannel, MarketChannelTopic, MarketChannelType, MarketEvent, SecurityType,
                         Symbol};
    use stats::kline::{Resolution, TimeUnit};
    use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
    use strategy::models::io::SerializedModel;
    use util::time::DateRange;

    use crate::report::BacktestReport;
    use crate::{backtest_with_range, load_market_events, load_market_events_df, DatasetCatalog};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
        register_pair_default(Exchange::Binance, "BTCUSDT", "BTC_USDT");
    }

    fn default_trades_range() -> DateRange {
        let dt = DateTime::from_utc(
            NaiveDate::from_ymd_opt(2022, 1, 22)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        );
        DateRange::by_day(dt, dt.add(Duration::days(1)))
    }

    fn default_orderbooks_range() -> DateRange {
        let dt = DateTime::from_utc(
            NaiveDate::from_ymd_opt(2022, 3, 14)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        );
        DateRange::by_day(dt, dt.add(Duration::days(1)))
    }

    fn default_symbol() -> Symbol { Symbol::new("BTC_USDT".into(), SecurityType::Crypto, Exchange::Binance) }

    #[actix_rt::test]
    async fn load_order_books() {
        init();
        let events = load_market_events(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Orderbooks)
                .tick_rate(Some(Duration::milliseconds(200)))
                .build()],
            default_orderbooks_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 73);

        let all_events_are_channel_type = events.iter().find(|e| !matches!(e.e, MarketEvent::Orderbook(_)));
        assert!(
            all_events_are_channel_type.is_none(),
            "{:?}",
            all_events_are_channel_type
        );
    }

    #[actix_rt::test]
    async fn load_trades() {
        init();
        let events = load_market_events(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Trades)
                .build()],
            default_trades_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 100);
        let all_events_are_channel_type = events.iter().find(|e| !matches!(e.e, MarketEvent::Trade(_)));
        assert!(
            all_events_are_channel_type.is_none(),
            "{:?}",
            all_events_are_channel_type
        );
    }

    #[actix_rt::test]
    async fn load_candles() {
        init();
        let events = load_market_events(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Candles)
                .resolution(Some(Resolution::new(TimeUnit::MilliSecond, 200)))
                .build()],
            default_trades_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        let final_candles_count = events
            .iter()
            .filter(|e| {
                matches!(e, MarketEventEnvelope {
                    e: MarketEvent::TradeCandle(Candle { is_final: true, .. }),
                    ..
                })
            })
            .count();
        assert_eq!(final_candles_count, 8);
        let all_events_are_channel_type = events.iter().find(|e| !matches!(e.e, MarketEvent::TradeCandle(_)));
        assert!(
            all_events_are_channel_type.is_none(),
            "{:?}",
            all_events_are_channel_type
        );
    }

    #[actix_rt::test]
    async fn load_order_books_df() {
        init();
        let events = load_market_events_df(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Orderbooks)
                .tick_rate(Some(Duration::milliseconds(200)))
                .build()],
            default_orderbooks_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 2);
        let rb = events.first().unwrap();
        assert_eq!(rb.num_rows(), 73);
    }

    #[actix_rt::test]
    async fn load_trades_df() {
        init();
        let events = load_market_events_df(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Trades)
                .build()],
            default_trades_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 2);
        let rb = events.first().unwrap();
        assert_eq!(rb.num_rows(), 100);
    }

    // TODO: this should give 8 candles, but since we don't use ids and there are several candles in the same ms, there are 15 candles
    #[actix_rt::test]
    async fn load_candles_df() {
        init();
        let events = load_market_events_df(
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Candles)
                .build()],
            default_trades_range(),
            Some(DatasetCatalog::default_test()),
        )
        .await
        .unwrap();
        let rb = concat_batches(&events.first().unwrap().schema(), &events).unwrap();
        info!("candles = {:?}", pretty::print_batches(&[rb.clone()]));
        assert_eq!(rb.num_rows(), 8);
    }

    struct TestStrategy(Vec<MarketChannel>, Vec<MarketChannelTopic>, usize);

    #[async_trait]
    impl Strategy for TestStrategy {
        fn key(&self) -> String { "test".to_string() }

        fn init(&mut self) -> strategy::error::Result<()> { Ok(()) }

        async fn eval(
            &mut self,
            e: &MarketEventEnvelope,
            _ctx: &DefaultStrategyContext,
        ) -> strategy::error::Result<Option<TradeSignals>> {
            if self.1.contains(&e.into()) {
                self.2 += 1;
            }
            Ok(None)
        }

        fn model(&self) -> SerializedModel { Default::default() }

        fn channels(&self) -> HashSet<MarketChannel> {
            let mut channels = HashSet::new();
            for c in &self.0 {
                channels.insert(c.clone());
            }
            channels
        }
    }

    fn backtest_range(
        test_name: &str,
        channels: Vec<MarketChannel>,
        range: DateRange,
    ) -> crate::error::Result<BacktestReport> {
        init();
        actix::System::with_tokio_rt(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Default Tokio runtime could not be created.")
        })
        .block_on(async {
            let backtest_fn = |channels: Vec<MarketChannel>, range: DateRange| {
                backtest_with_range(
                    test_name,
                    Arc::new(move |_| {
                        Box::new(TestStrategy(
                            channels.clone(),
                            channels.iter().map(Into::into).collect(),
                            0,
                        ))
                    }),
                    range,
                    Some(100.0),
                    Some(Exchange::default_fees()),
                    Some(DatasetCatalog::default_test()),
                )
            };
            backtest_fn(channels, range).await
        })
    }

    #[test]
    fn backtest_range_candles() {
        let report = backtest_range(
            "backtest_range_candles",
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Candles)
                .resolution(Some(Resolution::new(TimeUnit::MilliSecond, 200)))
                .build()],
            default_trades_range(),
        )
        .unwrap();
        let candles = report.candles_ss.read_all().unwrap();
        assert_eq!(candles.len(), 8);
    }

    #[test]
    fn backtest_range_trades() {
        let report = backtest_range(
            "backtest_range_trades",
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Trades)
                .build()],
            default_trades_range(),
        )
        .unwrap();
        let candles = report.candles_ss.read_all().unwrap();
        assert_eq!(candles.len(), 0);
        let ticks = report.market_stats_ss.read_all().unwrap();
        assert_eq!(ticks.len(), 100);
    }

    #[test]
    fn backtest_range_orderbooks() {
        let report = backtest_range(
            "backtest_range_orderbooks",
            vec![MarketChannel::builder()
                .symbol(default_symbol())
                .r#type(MarketChannelType::Orderbooks)
                .tick_rate(Some(Duration::milliseconds(200)))
                .build()],
            default_orderbooks_range(),
        )
        .unwrap();
        let candles = report.candles_ss.read_all().unwrap();
        assert_eq!(candles.len(), 0);
        let ticks = report.market_stats_ss.read_all().unwrap();
        assert_eq!(ticks.len(), 73);
    }
}
