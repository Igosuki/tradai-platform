use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use strategy::coinnect::prelude::{MarketEvent, MarketEventEnvelope};
use strategy::driver::StrategyDriver;
use strategy::event::{close_events, open_events};
use strategy::query::{DataQuery, DataResult};
use strategy::types::{OperationEvent, StratEvent, TradeEvent};
use strategy::Channel;
use util::compress::Compression;
use util::time::{set_current_time, TimedData};
use util::trace::{display_hist_percentiles, microtime_histogram, microtime_percentiles};

use crate::report::StreamWriterLogger;
use crate::BacktestReport;

const DEFAULT_RUNNER_SINK_SIZE: usize = 1000;

pub(crate) struct BacktestRunner {
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    strategy_events_logger: Arc<StreamWriterLogger<TimedData<StratEvent>>>,
    events_stream: Receiver<MarketEventEnvelope>,
    events_sink: Sender<MarketEventEnvelope>,
    stop_token: CancellationToken,
}

impl BacktestRunner {
    pub fn new(
        strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
        strategy_events_logger: Arc<StreamWriterLogger<TimedData<StratEvent>>>,
        stop_token: CancellationToken,
        sink_size: Option<usize>,
    ) -> Self {
        let (events_sink, events_stream) =
            channel::<MarketEventEnvelope>(sink_size.unwrap_or(DEFAULT_RUNNER_SINK_SIZE));
        Self {
            strategy,
            strategy_events_logger,
            events_stream,
            events_sink,
            stop_token,
        }
    }

    pub(crate) async fn channels(&self) -> HashSet<Channel> {
        let reader = self.strategy.lock().await;
        reader.channels()
    }

    pub(crate) fn event_sink(&self) -> Sender<MarketEventEnvelope> { self.events_sink.clone() }

    pub(crate) async fn run<P: AsRef<Path>>(
        &mut self,
        output_dir: P,
        report_compression: Compression,
    ) -> BacktestReport {
        let key = {
            let strategy = self.strategy.lock().await;
            strategy.key().await
        };

        // Start report
        let mut report = BacktestReport::new(output_dir, key.clone(), report_compression);
        report.start().await.unwrap();
        let mut execution_hist = microtime_histogram();

        // Subscribe report to events
        let mut sub = self.strategy_events_logger.subscription();
        let events_sink = report.strat_event_sink();
        tokio::spawn(async move {
            while let Some(Ok(e)) = sub.next().await {
                for event in simplify_pos_events(e) {
                    events_sink.send(event).unwrap();
                }
            }
        });
        // Main loop
        let mut driver = self.strategy.lock().await;
        'main: loop {
            select! {
                biased;

                market_event = self.events_stream.recv() => {
                    if market_event.is_none() {
                        break 'main;
                    }
                    let start = Instant::now();
                    let market_event = market_event.unwrap();
                    set_current_time(market_event.e.time());
                    driver.add_event(&market_event).await.unwrap();
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
                    if let MarketEvent::Orderbook(_) = &market_event.e {
                        report.push_market_stat(TimedData::new(market_event.e.time(), (&market_event.e).into()));
                    }
                    match driver.data(DataQuery::Models).await {
                        Ok(DataResult::Models(models)) => report
                            .push_model(TimedData::new(market_event.e.time(), models.into_iter().collect())),
                        _ => {
                            report.failures += 1;
                        }
                    }
                    match driver.data(DataQuery::Indicators).await {
                        Ok(DataResult::Indicators(i)) => report.push_snapshot(TimedData::new(market_event.e.time(), i)),
                        _ => {
                            report.failures += 1;
                        }
                    }
                    execution_hist += start.elapsed().as_nanos() as u64;
                },
                _ = self.stop_token.cancelled() => {
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
    vec![
        TimedData::new(op.at, StratEvent::Operation(op)),
        TimedData::new(trade.at, StratEvent::Trade(trade)),
    ]
}
