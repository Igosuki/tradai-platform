use chrono::{Date, Utc};
use datafusion::record_batch::RecordBatch;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use brokers::broker::{Broker, ChannelMessageBroker};
use brokers::exchange::Exchange;
use brokers::types::MarketEventEnvelope;
use brokers::Brokerages;
use db::{get_or_create, DbOptions};
use futures::StreamExt;
use stats::kline::TimeUnit;
use strategy::driver::{StratProviderRef, Strategy, StrategyInitContext};
use strategy::prelude::{GenericDriver, GenericDriverOptions, PortfolioOptions};
use strategy::Channel;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use trading::engine::mock_engine;
use util::compress::Compression;
use util::time::{DateRange, DurationRangeType};

use crate::config::BacktestConfig;
use crate::dataset::{default_data_catalog, DatasetCatalog, DatasetReader};
use crate::error::*;
use crate::report::{BacktestReport, GlobalReport, ReportConfig};
use crate::runner::BacktestRunner;

pub(crate) async fn init_brokerages(xchs: &[Exchange]) {
    let exchange_apis = Brokerages::public_apis(xchs).await;
    Brokerages::load_pair_registries(&exchange_apis).await.unwrap();
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
    let base_path = std::env::var("TRADAI_BACKTESTS_OUT_DIR")
        .or(std::env::var("CARGO_MANIFEST_DIR"))
        .unwrap_or("".to_string());
    let test_results_dir = Path::new(&base_path).join("backtest_results").join(test_name);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    format!("{}", test_results_dir.as_path().display())
}

#[derive(Copy, Clone)]
pub struct BacktestRange {
    from: Date<Utc>,
    to: Date<Utc>,
}

impl BacktestRange {
    pub fn new(from: Date<Utc>, to: Date<Utc>) -> Self { Self { from, to } }
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
            .map(|s| BacktestRunner::spawn_with_conf(conf.runner_queue_size, db_conf.clone(), mock_engine.clone(), s))
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
                input_format: conf.input_format.clone(),
                ds_type: conf.input_dataset,
                base_dir: conf.coindata_cache_dir(),
                input_sample_rate: conf.input_sample_rate,
                candle_resolution_period: TimeUnit::Minute,
                candle_resolution_unit: 15,
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
        self.dataset.stream_broker(&broker, self.period).await?;
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

async fn build_runner<'a>(
    provider: StratProviderRef,
    providers: &'a [Exchange],
    starting_cash: f64,
    fees_rate: f64,
) -> Arc<RwLock<BacktestRunner>> {
    let path = util::test::test_dir();

    let engine = Arc::new(mock_engine(path.path(), providers));
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

    runner_ref.clone()
}

async fn start_bt(
    test_name: &str,
    runner_ref: Arc<RwLock<BacktestRunner>>,
) -> (Receiver<BacktestReport>, CancellationToken) {
    let test_results_dir = backtest_results_dir(test_name);

    let stop_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let stop_token_a = stop_token.clone();
    tokio::spawn(async move {
        let mut runner = runner_ref.write().await;
        let report = runner.run(test_results_dir, Compression::none(), stop_token_a).await;
        report.finish().await.unwrap();
        tx.send(report).await.unwrap();
    });
    (rx, stop_token.clone())
}

/// Backtest over a period of time, reading data from the required channels
pub async fn backtest_with_range<'a>(
    test_name: &'a str,
    provider: StratProviderRef,
    date_range: BacktestRange,
    providers: &'a [Exchange],
    starting_cash: f64,
    fees_rate: f64,
    data_mapper: Option<DatasetCatalog>,
) -> Result<BacktestReport> {
    let runner_ref = build_runner(provider, providers, starting_cash, fees_rate).await;
    let broker = build_msg_broker(&[runner_ref.clone()]).await;
    let (mut rx, stop_token) = start_bt(test_name, runner_ref).await;
    let data_mapper = data_mapper.unwrap_or_else(|| default_data_catalog());
    let period = DateRange(date_range.from, date_range.to, DurationRangeType::Days, 1);
    for c in broker.subjects() {
        let dataset = data_mapper.get_reader(c);
        dataset.stream_broker(&broker, period).await?;
    }
    stop_token.cancel();
    rx.recv().await.ok_or(crate::error::Error::AnyhowError(anyhow!(
        "Did not receive a backtest report"
    )))
}

/// Load market events over the provided range
pub async fn load_market_events(
    channels: Vec<Channel>,
    date_range: &BacktestRange,
    mapper: Option<DatasetCatalog>,
) -> Result<Vec<MarketEventEnvelope>> {
    let data_mapper = mapper.unwrap_or_else(|| default_data_catalog());
    let period = DateRange(date_range.from, date_range.to, DurationRangeType::Days, 1);
    let mut market_events = vec![];
    for c in channels {
        let dataset = data_mapper.get_reader(&c);
        market_events.extend(dataset.read_all_events(&[c], period).await?);
    }
    market_events.sort_by(|me1, me2| me1.e.time().timestamp_millis().cmp(&me2.e.time().timestamp_millis()));
    Ok(market_events)
}

/// Load market events over the provided range
pub async fn load_market_events_df(
    channels: Vec<Channel>,
    date_range: &BacktestRange,
    mapper: Option<DatasetCatalog>,
) -> Result<Vec<RecordBatch>> {
    let data_mapper = mapper.unwrap_or_else(|| default_data_catalog());
    let period = DateRange(date_range.from, date_range.to, DurationRangeType::Days, 1);
    let mut market_events = vec![];
    for c in channels {
        let dataset = data_mapper.get_reader(&c);
        market_events.extend(dataset.read_all_events_df(&[c], period).await?);
    }
    Ok(market_events)
}

/// Backtest over the given event stream, reading data from the required channels
pub async fn backtest_with_events<'a>(
    test_name: &'a str,
    provider: StratProviderRef,
    events: Vec<MarketEventEnvelope>,
    providers: &'a [Exchange],
    starting_cash: f64,
    fees_rate: f64,
) -> Result<BacktestReport> {
    let runner_ref = build_runner(provider, providers, starting_cash, fees_rate).await;
    let sink = async {
        let runner = runner_ref.read().await;
        runner.event_sink()
    }
    .await;
    let (mut rx, stop_token) = start_bt(test_name, runner_ref).await;
    for event in events {
        sink.send(event).await.unwrap();
    }
    stop_token.cancel();
    rx.recv().await.ok_or(crate::error::Error::AnyhowError(anyhow!(
        "Did not receive a backtest report"
    )))
}

#[cfg(test)]
mod test {
    use crate::dataset::default_test_data_catalog;
    use crate::{backtest_with_range, load_market_events, load_market_events_df, BacktestRange};
    use brokers::exchange::Exchange;
    use brokers::pair::register_pair_default;
    use brokers::prelude::MarketEventEnvelope;
    use brokers::types::MarketEvent;
    use chrono::{Date, NaiveDate, Utc};
    use std::collections::HashSet;
    use std::sync::Arc;
    use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
    use strategy::models::io::SerializedModel;
    use strategy::Channel;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
        register_pair_default(Exchange::Binance, "BTCUSDT", "BTC_USDT");
    }

    fn default_trades_date() -> Date<Utc> { Date::from_utc(NaiveDate::from_ymd(2022, 01, 22), Utc) }

    fn default_orderbooks_date() -> Date<Utc> { Date::from_utc(NaiveDate::from_ymd(2021, 12, 13), Utc) }

    #[actix_rt::test]
    async fn load_order_books() {
        init();
        let date = Date::from_utc(NaiveDate::from_ymd(2021, 12, 13), Utc);
        let events = load_market_events(
            vec![Channel::Orderbooks {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(date, date),
            Some(default_test_data_catalog()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 100);
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
            vec![Channel::Trades {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(default_trades_date(), default_trades_date()),
            Some(default_test_data_catalog()),
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
            vec![Channel::Candles {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(default_trades_date(), default_trades_date()),
            Some(default_test_data_catalog()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 8);
        let all_events_are_channel_type = events.iter().find(|e| !matches!(e.e, MarketEvent::CandleTick(_)));
        assert!(
            all_events_are_channel_type.is_none(),
            "{:?}",
            all_events_are_channel_type
        );
    }

    #[actix_rt::test]
    async fn load_order_books_df() {
        init();
        let date = Date::from_utc(NaiveDate::from_ymd(2021, 12, 13), Utc);
        let events = load_market_events_df(
            vec![Channel::Orderbooks {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(date, date),
            Some(default_test_data_catalog()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        let rb = events.first().unwrap();
        assert_eq!(rb.num_rows(), 100);
    }

    #[actix_rt::test]
    async fn load_trades_df() {
        init();
        let events = load_market_events_df(
            vec![Channel::Trades {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(default_trades_date(), default_trades_date()),
            Some(default_test_data_catalog()),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        let rb = events.first().unwrap();
        assert_eq!(rb.num_rows(), 100);
    }

    // TODO: this should give 8 candles, but since we don't use ids and there are several candles in the same ms, there are 15 candles
    #[actix_rt::test]
    #[ignore]
    async fn load_candles_df() {
        init();
        let events = load_market_events_df(
            vec![Channel::Candles {
                xch: Exchange::Binance,
                pair: "BTC_USDT".into(),
            }],
            &BacktestRange::new(default_trades_date(), default_trades_date()),
            Some(default_test_data_catalog()),
        )
        .await
        .unwrap();
        info!("candles = {:?}", datafusion::arrow_print::write(&events.clone()));
        assert_eq!(events.len(), 1);
        let rb = events.first().unwrap();
        assert_eq!(rb.num_rows(), 8);
    }

    struct TestStrategy(Vec<Channel>, usize);

    #[async_trait]
    impl Strategy for TestStrategy {
        fn key(&self) -> String { "test".to_string() }

        fn init(&mut self) -> strategy::error::Result<()> { Ok(()) }

        async fn eval(
            &mut self,
            e: &MarketEventEnvelope,
            _ctx: &DefaultStrategyContext,
        ) -> strategy::error::Result<Option<TradeSignals>> {
            if self.0.contains(&Channel::from(e)) {
                self.1 += 1;
            }
            Ok(None)
        }

        fn model(&self) -> SerializedModel { Default::default() }

        fn channels(&self) -> HashSet<Channel> {
            let mut channels = HashSet::new();
            for c in &self.0 {
                channels.insert(c.clone());
            }
            channels
        }
    }

    #[test]
    fn backtest_range() {
        init();
        actix::System::with_tokio_rt(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Default Tokio runtime could not be created.")
        })
        .block_on(async {
            let backtest_fn = |channels: Vec<Channel>, range: BacktestRange| {
                backtest_with_range(
                    "backtest_range",
                    Arc::new(move |_| Box::new(TestStrategy(channels.clone(), 0))),
                    range,
                    &[Exchange::Binance],
                    100.0,
                    Exchange::default_fees(),
                    Some(default_test_data_catalog()),
                )
            };
            let report = backtest_fn(
                vec![Channel::Candles {
                    xch: Exchange::Binance,
                    pair: "BTC_USDT".into(),
                }],
                BacktestRange::new(default_trades_date(), default_trades_date()),
            )
            .await
            .unwrap();
            let candles = report.candles_ss.read_all().unwrap();
            assert_eq!(candles.len(), 8);

            let report = backtest_fn(
                vec![Channel::Trades {
                    xch: Exchange::Binance,
                    pair: "BTC_USDT".into(),
                }],
                BacktestRange::new(default_trades_date(), default_trades_date()),
            )
            .await
            .unwrap();
            let candles = report.candles_ss.read_all().unwrap();
            assert_eq!(candles.len(), 0);
            let ticks = report.market_stats_ss.read_all().unwrap();
            assert_eq!(ticks.len(), 100);

            let report = backtest_fn(
                vec![Channel::Orderbooks {
                    xch: Exchange::Binance,
                    pair: "BTC_USDT".into(),
                }],
                BacktestRange::new(default_orderbooks_date(), default_trades_date()),
            )
            .await
            .unwrap();
            let candles = report.candles_ss.read_all().unwrap();
            assert_eq!(candles.len(), 0);
            let ticks = report.market_stats_ss.read_all().unwrap();
            assert_eq!(ticks.len(), 100);
        });
    }
}
