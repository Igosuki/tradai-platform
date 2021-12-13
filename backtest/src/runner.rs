use std::collections::HashSet;
use std::sync::Arc;

use ext::ResultExt;
use strategy::coinnect::prelude::{MarketEvent, MarketEventEnvelope};
use strategy::driver::StrategyDriver;
use strategy::query::{DataQuery, DataResult};
use strategy::Channel;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use trading::book::BookPosition;
use util::time::set_current_time;

use crate::error::*;
use crate::report::VecEventLogger;
use crate::{BacktestReport, TimedData};

pub(crate) struct BacktestRunner {
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    strategy_events_logger: Arc<VecEventLogger>,
    events_stream: UnboundedReceiver<MarketEventEnvelope>,
    events_sink: UnboundedSender<MarketEventEnvelope>,
    close_sink: tokio::sync::broadcast::Receiver<bool>,
}

impl BacktestRunner {
    pub fn new(
        strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
        strategy_events_logger: Arc<VecEventLogger>,
        close_sink: tokio::sync::broadcast::Receiver<bool>,
    ) -> Self {
        let (events_sink, events_stream) = unbounded_channel::<MarketEventEnvelope>();
        Self {
            strategy_events_logger,
            strategy,
            events_stream,
            events_sink,
            close_sink,
        }
    }

    pub(crate) async fn channels(&self) -> HashSet<Channel> {
        let reader = self.strategy.lock().await;
        reader.channels()
    }

    pub(crate) fn event_sink(&self) -> UnboundedSender<MarketEventEnvelope> { self.events_sink.clone() }

    pub(crate) async fn run(&mut self) -> BacktestReport {
        let key = {
            let strategy = self.strategy.lock().await;
            strategy.key().await
        };
        let mut report = BacktestReport::new(key.clone());
        'main: loop {
            select! {
                _ = self.close_sink.recv() => {
                    info!("Closing {}", key);
                    break 'main;
                },
                market_event = self.events_stream.recv() => {
                    if market_event.is_none() {
                        break 'main;
                    }
                    let market_event = market_event.unwrap();
                    set_current_time(market_event.e.time());
                    let mut driver = self.strategy.lock().await;
                    driver.add_event(&market_event).await.unwrap();
                    // If there is an ongoing operation, resolve orders
                    let mut tries = 0;
                    loop {
                        if tries > 5 {
                            break;
                        }
                        let open_ops = driver.data(DataQuery::OpenPositions).await;
                        match open_ops {
                            Ok(DataResult::OpenPositions(positions)) if !positions.is_empty() => {
                                driver.resolve_orders().await;
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                tries += 1;
                            }
                            _ => break,
                        }
                    }
                    if let MarketEvent::Orderbook(ob) = &market_event.e {
                        let bp_try: Result<BookPosition> = ob.try_into().err_into();
                        if let Ok(bp) = bp_try {
                            report.book_positions.push(TimedData::new(market_event.e.time(), bp));
                        }
                    }
                    match driver.data(DataQuery::Models).await {
                        Ok(DataResult::Models(models)) => report
                            .models
                            .push(TimedData::new(market_event.e.time(), models.into_iter().collect())),
                        _ => {
                            report.model_failures += 1;
                        }
                    }
                    match driver.data(DataQuery::Indicators).await {
                        Ok(DataResult::Indicators(i)) => report.indicators.push(TimedData::new(market_event.e.time(), i)),
                        _ => {
                            report.indicator_failures += 1;
                        }
                    }
                }
            }
        }
        let read = self.strategy_events_logger.get_events().await;
        report.events = read;
        report
    }
}
