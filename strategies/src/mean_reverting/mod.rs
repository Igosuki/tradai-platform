use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use brokers::prelude::*;
use brokers::types::{SecurityType, Symbol};
use db::Storage;
use model::MeanRevertingModel;
use options::Options;
use portfolio::portfolio::Portfolio;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::error::*;
use strategy::models::io::{IterativeModel, SerializedModel};
use strategy::models::Sampler;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::prelude::*;
use strategy::{MarketChannel, MarketChannelType, StratEventLoggerRef};
use trading::book::BookPosition;
use trading::position::{OperationKind, PositionKind};
use trading::signal::{new_trade_signal, TradeSignal};
use trading::stop::FixedStopper;
use trading::types::OrderConf;
use util::time::TimedData;

use self::metrics::MeanRevertingStrategyMetrics;

mod metrics;
pub mod model;
pub mod options;
#[cfg(test)]
mod tests;

pub fn provide_strat(name: &str, ctx: StrategyPluginContext, conf: serde_json::Value) -> Result<Box<dyn Strategy>> {
    let options: Options = serde_json::from_value(conf)?;
    Ok(Box::new(MeanRevertingStrategy::new(
        ctx.db,
        name.to_string(),
        &options,
        ctx.logger,
    )))
}

inventory::submit! {
    StrategyPlugin::new("mean_reverting", provide_options::<Options>, provide_strat)
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MeanRevertingStrategy {
    key: String,
    exchange: Exchange,
    pair: Pair,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    model: MeanRevertingModel,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    last_threshold_time: DateTime<Utc>,
    stopper: FixedStopper<f64>,
    sampler: Sampler,
    last_book_pos: Option<BookPosition>,
    logger: Option<StratEventLoggerRef>,
    order_conf: OrderConf,
}

impl MeanRevertingStrategy {
    /// # Panics
    ///
    /// if the strategy fails to load
    pub fn new(db: Arc<dyn Storage>, strat_key: String, n: &Options, logger: Option<StratEventLoggerRef>) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let model = MeanRevertingModel::new(n, db);
        let mut strat = Self {
            key: strat_key,
            exchange: n.exchange,
            pair: n.pair.clone(),
            sample_freq: n.sample_freq,
            last_sample_time: Utc.timestamp_millis(0),
            model,
            threshold_eval_freq: n.threshold_eval_freq,
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: FixedStopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
            sampler: Sampler::new(n.sample_freq, Utc.timestamp_millis(0)),
            last_book_pos: None,
            logger,
            order_conf: n.order_conf.clone(),
        };
        if let Err(e) = strat.load() {
            error!("{}", e);
            panic!("Could not load models");
        }
        strat
    }

    fn load(&mut self) -> strategy::error::Result<()> {
        self.model.try_load()?;
        if self.model.is_loaded() {
            Ok(())
        } else {
            Err(Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        }
    }

    fn make_signal(
        &self,
        trace_id: Uuid,
        event_time: DateTime<Utc>,
        operation_kind: OperationKind,
        position_kind: PositionKind,
        price: f64,
        qty: Option<f64>,
    ) -> TradeSignal {
        new_trade_signal(
            self.pair.clone(),
            self.exchange,
            &self.order_conf,
            event_time,
            trace_id,
            operation_kind,
            position_kind,
            price,
            qty,
        )
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn eval_latest(
        &self,
        lr: &BookPosition,
        portfolio: &Portfolio,
    ) -> strategy::error::Result<Option<TradeSignal>> {
        self.metrics.log_pos(lr);

        let ppo = self.model.ppo().expect("model required");

        let thresholds = self.model.thresholds();
        let threshold_short = thresholds.0;
        let threshold_long = thresholds.1;

        let signal = match portfolio.open_position(self.exchange, self.pair.clone()) {
            Some(pos) => {
                // TODO: move this logic to a single place in the code which can be reused
                let maybe_stop = self.stopper.should_stop(pos.unreal_profit_loss);
                if let Some(logger) = &self.logger {
                    if let Some(stop) = maybe_stop {
                        logger.log(TimedData::new(lr.event_time, StratEvent::Stop(stop))).await;
                    }
                }
                // Possibly close a short position
                if pos.is_short() && (ppo < 0.0 || maybe_stop.is_some()) {
                    Some(self.make_signal(
                        lr.trace_id,
                        lr.event_time,
                        OperationKind::Close,
                        PositionKind::Short,
                        lr.ask,
                        None,
                    ))
                }
                // Possibly close a long position
                else if pos.is_long() && (ppo > 0.0 || maybe_stop.is_some()) {
                    Some(self.make_signal(
                        lr.trace_id,
                        lr.event_time,
                        OperationKind::Close,
                        PositionKind::Long,
                        lr.bid,
                        None,
                    ))
                } else {
                    None
                }
            }
            None if (ppo > threshold_short) && lr.ask > 0.0 => {
                // Possibly open a short position
                let qty = Some(portfolio.value() / lr.ask);
                Some(self.make_signal(
                    lr.trace_id,
                    lr.event_time,
                    OperationKind::Open,
                    PositionKind::Short,
                    lr.ask,
                    qty,
                ))
            }
            None if (ppo < threshold_long) && lr.bid > 0.0 => {
                // Possibly open a long position
                let qty = Some(portfolio.value() / lr.bid);
                Some(self.make_signal(
                    lr.trace_id,
                    lr.event_time,
                    OperationKind::Open,
                    PositionKind::Long,
                    lr.bid,
                    qty,
                ))
            }
            _ => None,
        };
        Ok(signal)
    }

    fn can_eval(&self) -> bool { self.model.is_loaded() }

    fn parse_book_position(event: &MarketEventEnvelope) -> Option<BookPosition> {
        match &event.e {
            MarketEvent::Orderbook(ob) => ob.try_into().ok(),
            _ => None,
        }
    }
}

#[async_trait]
impl Strategy for MeanRevertingStrategy {
    fn key(&self) -> String { self.key.clone() }

    fn init(&mut self) -> Result<()> { self.load() }

    async fn eval(&mut self, e: &MarketEventEnvelope, ctx: &DefaultStrategyContext) -> Result<Option<TradeSignals>> {
        if let Err(e) = self.model.next_model(e) {
            self.metrics.log_error(e.short_name());
            return Ok(None);
        }
        let t = self.model.thresholds();
        self.metrics.log_thresholds(t.0, t.1);
        if let Some(ppo) = self.model.ppo_value() {
            self.metrics.log_model(&ppo);
        }

        let mut signals = TradeSignals::new();
        if !self.can_eval() {
            return Ok(None);
        }
        let book_pos = Self::parse_book_position(e);
        if book_pos.is_none() {
            return Ok(None);
        };
        self.last_book_pos = book_pos;
        match self.eval_latest(&book_pos.unwrap(), ctx.portfolio).await {
            Ok(Some(signal)) => {
                self.metrics.log_position(signal.pos_kind, signal.op_kind, signal.price);
                signals.push(signal);
            }
            Err(e) => self.metrics.log_error(e.short_name()),
            _ => {}
        }
        Ok(Some(signals))
    }

    fn model(&self) -> SerializedModel { self.model.values() }

    fn channels(&self) -> HashSet<MarketChannel> {
        vec![MarketChannel::builder()
            .symbol(Symbol::new(self.pair.clone(), SecurityType::Crypto, self.exchange))
            .r#type(MarketChannelType::Orderbooks)
            .tick_rate(Some(chrono::Duration::milliseconds(60000)))
            .build()]
        .into_iter()
        .collect()
    }
}
