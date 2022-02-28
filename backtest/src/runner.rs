use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use strategy::plugin::plugin_registry;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use brokers::prelude::{MarketEvent, MarketEventEnvelope};
use db::DbOptions;
use strategy::driver::StrategyDriver;
use strategy::event::{close_events, open_events};
use strategy::prelude::StrategyDriverSettings;
use strategy::query::{DataQuery, DataResult};
use strategy::types::{OperationEvent, StratEvent, TradeEvent};
use strategy::MarketChannel;
use trading::engine::TradingEngine;
use util::compress::Compression;
use util::time::{set_current_time, TimedData};
use util::trace::{display_hist_percentiles, microtime_histogram, microtime_percentiles};

use crate::report::{BacktestReport, StreamWriterLogger};

const DEFAULT_RUNNER_SINK_SIZE: usize = 1000;

pub(crate) struct BacktestRunner {
    driver: Arc<Mutex<Box<dyn StrategyDriver>>>,
    events_logger: Arc<StreamWriterLogger<TimedData<StratEvent>>>,
    events_stream: Receiver<MarketEventEnvelope>,
    events_sink: Sender<MarketEventEnvelope>,
}

impl BacktestRunner {
    pub fn new(
        strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
        strategy_events_logger: Arc<StreamWriterLogger<TimedData<StratEvent>>>,
        sink_size: Option<usize>,
    ) -> Self {
        let (events_sink, events_stream) =
            channel::<MarketEventEnvelope>(sink_size.unwrap_or(DEFAULT_RUNNER_SINK_SIZE));
        Self {
            driver: strategy,
            events_logger: strategy_events_logger,
            events_stream,
            events_sink,
        }
    }

    pub(crate) async fn spawn_with_conf(
        sink_size: Option<usize>,
        db_conf: DbOptions<PathBuf>,
        engine: Arc<TradingEngine>,
        settings: StrategyDriverSettings,
    ) -> Arc<RwLock<Self>> {
        let logger = Self::strat_event_logger(sink_size);
        let logger2 = logger.clone();
        let strategy_driver = task::spawn_blocking(move || {
            debug!("plugin_registry() = {:?}", plugin_registry());
            let plugin = plugin_registry().get(settings.strat.strat_type.as_str()).unwrap();
            strategy::settings::from_driver_settings(plugin, &db_conf, &settings, engine, Some(logger.clone())).unwrap()
        })
        .await
        .unwrap();
        let runner = Self::new(Arc::new(Mutex::new(strategy_driver)), logger2, sink_size);
        Arc::new(RwLock::new(runner))
    }

    pub(crate) async fn spawn_with_driver(
        sink_size: Option<usize>,
        events_logger: Arc<StreamWriterLogger<TimedData<StratEvent>>>,
        driver: Box<dyn StrategyDriver>,
    ) -> Arc<RwLock<Self>> {
        let runner = Self::new(Arc::new(Mutex::new(driver)), events_logger, sink_size);
        Arc::new(RwLock::new(runner))
    }

    pub(crate) fn strat_event_logger(sink_size: Option<usize>) -> Arc<StreamWriterLogger<TimedData<StratEvent>>> {
        Arc::new(StreamWriterLogger::<TimedData<StratEvent>>::new(sink_size))
    }

    pub(crate) async fn channels(&self) -> HashSet<MarketChannel> {
        let reader = self.driver.lock().await;
        reader.channels()
    }

    pub(crate) fn event_sink(&self) -> Sender<MarketEventEnvelope> { self.events_sink.clone() }

    pub(crate) async fn run<P: AsRef<Path>>(
        &mut self,
        output_dir: P,
        report_compression: Compression,
        stop_token: CancellationToken,
    ) -> BacktestReport {
        let key = {
            let strategy = self.driver.lock().await;
            strategy.key().await
        };

        // Start report
        let mut report = BacktestReport::new(output_dir, key.clone(), report_compression);
        report.start().await.unwrap();
        let mut execution_hist = microtime_histogram();

        // Subscribe report to events
        let mut sub = self.events_logger.subscription();
        let events_sink = report.strat_event_sink();
        tokio::spawn(async move {
            while let Some(Ok(e)) = sub.next().await {
                for event in simplify_pos_events(e) {
                    events_sink.send(event).unwrap();
                }
            }
        });
        // Main loop
        let mut driver = self.driver.lock().await;
        'main: loop {
            tokio::select! {
                biased;

                market_event = self.events_stream.recv() => {
                    if market_event.is_none() {
                        break 'main;
                    }
                    let start = Instant::now();
                    let market_event = market_event.unwrap();
                    set_current_time(market_event.e.time());
                    driver.on_market_event(&market_event).await.unwrap();
                    // If there is an ongoing operation, resolve orders
                    let mut tries = 0;
                    'resolve: loop {
                        if tries > 3 {
                            debug!("reached maximum tries");
                            break 'resolve;
                        }
                        if driver.is_locked().await {
                            driver.resolve_orders().await;
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            tries += 1;
                        } else {
                            break 'resolve;
                        }
                    }

                    // #[allow(clippy::needless_borrow)]
                    // report.push_market_stat(TimedData::new(market_event.e.time(), (&market_event.e).into()));
                    if let MarketEvent::CandleTick(candle) = &market_event.e {
                        if candle.is_final {
                            report.push_candle(TimedData::new(market_event.e.time(), candle.clone()));
                            match driver.query(DataQuery::Models).await {
                                Ok(DataResult::Models(models)) => report
                                    .push_model(TimedData::new(market_event.e.time(), models.into_iter().collect())),
                                _ => {
                                    report.failures += 1;
                                }
                            }
                            match driver.query(DataQuery::Indicators).await {
                                Ok(DataResult::Indicators(i)) => report.push_snapshot(TimedData::new(market_event.e.time(), i)),
                                _ => {
                                    report.failures += 1;
                                }
                            }
                        }
                    }
                    execution_hist += start.elapsed().as_nanos() as u64;
                },
                _ = stop_token.cancelled() => {
                    info!("Closing {}.", key);
                    break 'main;
                }
            }
        }

        info!(
            "{} event loop stats : {}",
            report.key,
            display_hist_percentiles(&execution_hist)
        );
        report.execution_hist = microtime_percentiles(&execution_hist);
        report
    }
}

fn simplify_pos_events(event: TimedData<StratEvent>) -> Vec<TimedData<StratEvent>> {
    match event.value {
        StratEvent::OpenPosition(pos) => open_events(&pos).map(op_and_trade_to_strat).unwrap_or_default(),
        StratEvent::ClosePosition(pos) => close_events(&pos).map(op_and_trade_to_strat).unwrap_or_default(),
        _ => vec![event],
    }
}

fn op_and_trade_to_strat((op, trade): (OperationEvent, TradeEvent)) -> Vec<TimedData<StratEvent>> {
    vec![TimedData::new(op.at, StratEvent::PositionSummary { op, trade })]
}
