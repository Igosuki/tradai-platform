use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use tokio::sync::Mutex;

use ext::ResultExt;
use strategy::coinnect::prelude::{MarketEvent, MarketEventEnvelope};
use strategy::driver::StrategyDriver;
use strategy::query::{DataQuery, DataResult};
use strategy::Channel;
use trading::book::BookPosition;

use crate::error::*;
use crate::report::VecEventLogger;
use crate::{BacktestReport, TimedData};

pub(crate) struct BacktestRunner {
    strategy: Arc<Mutex<Box<dyn StrategyDriver>>>,
    strategy_events_logger: Arc<VecEventLogger>,
}

impl BacktestRunner {
    pub fn new(strategy: Arc<Mutex<Box<dyn StrategyDriver>>>, strategy_events_logger: Arc<VecEventLogger>) -> Self {
        Self {
            strategy_events_logger,
            strategy,
        }
    }

    pub(crate) async fn channels(&self) -> HashSet<Channel> {
        let reader = self.strategy.lock().await;
        reader.channels()
    }

    pub(crate) async fn run(&self, live_events: &[MarketEventEnvelope]) -> BacktestReport {
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
                let open_ops = strategy.data(DataQuery::OpenPositions).await;
                match open_ops {
                    Ok(DataResult::OpenPositions(positions)) if !positions.is_empty() => {
                        strategy.resolve_orders().await;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        tries += 1;
                    }
                    _ => break,
                }
            }
            if let MarketEvent::Orderbook(ob) = &live_event.e {
                let bp_try: Result<BookPosition> = ob.try_into().err_into();
                if let Ok(bp) = bp_try {
                    report.book_positions.push(TimedData::new(live_event.e.time(), bp));
                }
            }
            match strategy.data(DataQuery::Models).await {
                Ok(DataResult::Models(models)) => report
                    .models
                    .push(TimedData::new(live_event.e.time(), models.into_iter().collect())),
                _ => {
                    report.model_failures += 1;
                }
            }
            match strategy.data(DataQuery::Indicators).await {
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
