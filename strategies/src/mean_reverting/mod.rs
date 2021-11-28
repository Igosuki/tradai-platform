use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use coinnect_rt::prelude::*;
use coinnect_rt::types::MarginSideEffect;
use db::Storage;
use portfolio::portfolio::{Portfolio, PortfolioRepoImpl};
use portfolio::risk::DefaultMarketRiskEvaluator;
#[cfg(test)]
use stats::indicators::macd_apo::MACDApo;
use trading::book::BookPosition;
use trading::engine::TradingEngine;

use crate::driver::StrategyDriver;
use crate::error::Result;
use crate::generic::Strategy;
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::model::MeanRevertingModel;
use crate::mean_reverting::options::Options;
use crate::models::io::IterativeModel;
use crate::models::Sampler;
use crate::query::{DataQuery, DataResult, ModelReset, MutableField, Mutation, StrategyIndicators};
use crate::{Channel, StratEvent, StratEventLogger, StrategyStatus};
use trading::position::{OperationKind, PositionKind};
use trading::signal::TradeSignal;
use trading::stop::Stopper;
use trading::types::{OrderMode, TradeKind};
use util::time::now;

mod metrics;
pub mod model;
pub mod options;
pub mod state;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct OrderConf {
    dry_mode: bool,
    order_mode: OrderMode,
    asset_type: AssetType,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MeanRevertingStrategy {
    key: String,
    exchange: Exchange,
    pair: Pair,
    fees_rate: f64,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    model: MeanRevertingModel,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    last_threshold_time: DateTime<Utc>,
    stopper: Stopper<f64>,
    sampler: Sampler,
    last_book_pos: Option<BookPosition>,
    logger: Option<Arc<dyn StratEventLogger>>,
    portfolio: Portfolio,
    is_trading: bool,
    order_conf: OrderConf,
    engine: Arc<TradingEngine>,
}

impl MeanRevertingStrategy {
    pub fn new(
        db: Arc<dyn Storage>,
        strat_key: String,
        fees_rate: f64,
        n: &Options,
        engine: Arc<TradingEngine>,
        logger: Option<Arc<dyn StratEventLogger>>,
    ) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let model = MeanRevertingModel::new(n, db.clone());
        let portfolio = Portfolio::try_new(
            n.initial_cap,
            fees_rate,
            strat_key.clone(),
            Arc::new(PortfolioRepoImpl::new(db)),
            Arc::new(DefaultMarketRiskEvaluator::default()),
            engine.interest_rate_provider.clone(),
        )
        .unwrap();
        let mut strat = Self {
            key: strat_key,
            exchange: n.exchange,
            pair: n.pair.clone(),
            fees_rate,
            sample_freq: n.sample_freq(),
            last_sample_time: Utc.timestamp_millis(0),
            model,
            threshold_eval_freq: n.threshold_eval_freq,
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
            sampler: Sampler::new(n.sample_freq(), Utc.timestamp_millis(0)),
            last_book_pos: None,
            logger,
            portfolio,
            is_trading: true,
            order_conf: OrderConf {
                dry_mode: n.dry_mode(),
                order_mode: n.order_mode(),
                asset_type: n.order_asset_type(),
            },
            engine,
        };
        if let Err(e) = strat.load() {
            error!("{}", e);
            panic!("Could not load models");
        }
        strat
    }

    fn load(&mut self) -> crate::error::Result<()> {
        self.model.try_load()?;
        if !self.model.is_loaded() {
            Err(crate::error::Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn is_trading(&self) -> bool { self.is_trading }

    #[cfg(test)]
    fn model_value(&self) -> Option<MACDApo> { self.model.apo_value() }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.portfolio.set_value(v)?,
            MutableField::Pnl => self.portfolio.set_pnl(v)?,
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn trade_signal(
        pair: Pair,
        exchange: Exchange,
        order_conf: &OrderConf,
        event_time: DateTime<Utc>,
        trace_id: Uuid,
        op_kind: OperationKind,
        pos_kind: PositionKind,
        price: f64,
        qty: f64,
    ) -> TradeSignal {
        let trade_kind = match (pos_kind, op_kind) {
            (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeKind::Sell,
            (PositionKind::Long, OperationKind::Open) | (PositionKind::Short, OperationKind::Close) => TradeKind::Buy,
        };
        let margin_side_effect = if order_conf.asset_type.is_margin() && pos_kind == PositionKind::Short {
            if op_kind == OperationKind::Open {
                Some(MarginSideEffect::MarginBuy)
            } else {
                Some(MarginSideEffect::AutoRepay)
            }
        } else {
            None
        };
        let (order_type, enforcement) = match order_conf.order_mode {
            OrderMode::Limit => (OrderType::Limit, Some(OrderEnforcement::FOK)),
            OrderMode::Market => (OrderType::Market, None),
        };
        TradeSignal {
            trace_id,
            pos_kind,
            op_kind,
            trade_kind,
            event_time,
            signal_time: now(),
            price,
            qty,
            pair,
            exchange,
            instructions: None,
            dry_mode: order_conf.dry_mode,
            order_type,
            enforcement,
            asset_type: Some(order_conf.asset_type),
            side_effect: margin_side_effect,
        }
    }

    fn make_signal(
        &self,
        trace_id: Uuid,
        event_time: DateTime<Utc>,
        operation_kind: OperationKind,
        position_kind: PositionKind,
        price: f64,
        qty: f64,
    ) -> TradeSignal {
        Self::trade_signal(
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

    /// Returns the order qty for the wanted position
    pub(super) fn open_signal_qty(&self, position_kind: PositionKind, bp: &BookPosition) -> Option<f64> {
        match position_kind {
            PositionKind::Short if bp.ask > 0.0 => Some(self.portfolio.value() / bp.ask),
            PositionKind::Long if bp.bid > 0.0 => Some(self.portfolio.value() / bp.bid),
            _ => None,
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn eval_latest(&mut self, lr: &BookPosition) -> Result<Option<TradeSignal>> {
        let apo = self.model.apo().expect("model required");

        let thresholds = self.model.thresholds();
        let threshold_short = thresholds.0;
        let threshold_long = thresholds.1;

        let signal = match self.portfolio.open_position(self.exchange, self.pair.clone()) {
            Some(pos) => {
                let maybe_stop = self.stopper.should_stop(pos.result_profit_loss);
                if let Some(logger) = &self.logger {
                    logger
                        .maybe_log(maybe_stop.as_ref().map(|e| StratEvent::Stop { stop: *e }))
                        .await;
                }
                let last_open_order = pos.open_order.as_ref().unwrap();
                // Possibly close a short position
                if (pos.is_short() && apo < 0.0) || maybe_stop.is_some() {
                    //let interest_fees = self.interest_fees_since_open().await?;
                    let interest_fees = 0.0;
                    let qty = (last_open_order.total_executed_qty / (1.0 - self.fees_rate)) + interest_fees;
                    Some(self.make_signal(
                        lr.trace_id,
                        lr.event_time,
                        OperationKind::Close,
                        PositionKind::Short,
                        lr.ask,
                        qty,
                    ))
                }
                // Possibly close a long position
                else if (pos.is_long() && apo > 0.0) || maybe_stop.is_some() {
                    let qty = last_open_order.total_executed_qty - last_open_order.base_fees();
                    Some(self.make_signal(
                        lr.trace_id,
                        lr.event_time,
                        OperationKind::Close,
                        PositionKind::Long,
                        lr.bid,
                        qty,
                    ))
                } else {
                    None
                }
            }
            None if (apo > threshold_short) => {
                // Possibly open a short position
                info!("Entering short position with threshold {}", threshold_short);
                let qty = self.open_signal_qty(PositionKind::Short, lr);
                Some(self.make_signal(
                    lr.trace_id,
                    lr.event_time,
                    OperationKind::Open,
                    PositionKind::Short,
                    lr.bid,
                    qty.unwrap(),
                ))
            }
            None if (apo < threshold_long) => {
                // Possibly open a long position
                info!("Entering long position with threshold {}", threshold_long);
                let qty = self.open_signal_qty(PositionKind::Long, lr);
                Some(self.make_signal(
                    lr.trace_id,
                    lr.event_time,
                    OperationKind::Open,
                    PositionKind::Long,
                    lr.ask,
                    qty.unwrap(),
                ))
            }
            None => None,
        };
        Ok(signal)
    }

    fn can_eval(&self) -> bool { self.model.is_loaded() }

    fn parse_book_position(&self, event: &MarketEventEnvelope) -> Option<BookPosition> {
        match &event.e {
            MarketEvent::Orderbook(ob) => ob.try_into().ok(),
            _ => None,
        }
    }

    async fn process_event(&mut self, event: &MarketEventEnvelope) {
        // A model is available
        if let Err(e) = self.update_model(event).await {
            self.metrics.log_error(e.short_name());
            return;
        }
        let pos = self.parse_book_position(event);
        if pos.is_none() {
            return;
        }
        let pos = pos.unwrap();
        self.last_book_pos = Some(pos.clone());
        if let Err(_e) = self.portfolio.update_from_market(event).await {
            // TODO: log err
        }
        if self.can_eval() {
            if self.is_trading() {
                match self.eval_latest(&pos).await {
                    Ok(Some(_signal)) => {
                        // TODO : execute the order
                        //self.metrics.log_position(signal., &op.kind);
                    }
                    Err(e) => self.metrics.log_error(e.short_name()),
                    _ => {}
                }
            }
            self.metrics
                .log_portfolio(self.exchange, self.pair.clone(), &self.portfolio);
        }
        self.metrics.log_is_trading(self.is_trading());
        self.metrics.log_pos(&pos);
    }

    pub(crate) fn handles(&self, e: &MarketEventEnvelope) -> bool {
        self.exchange == e.xch
            && match &e.e {
                MarketEvent::Orderbook(ob) => ob.pair == self.pair,
                _ => false,
            }
    }

    pub(crate) fn status(&self) -> StrategyStatus {
        if self.is_trading {
            StrategyStatus::Running
        } else {
            StrategyStatus::NotTrading
        }
    }

    pub(crate) fn indicators(&self) -> StrategyIndicators {
        StrategyIndicators {
            value: self.portfolio.value(),
            pnl: self.portfolio.pnl(),
            current_return: self.portfolio.returns().last().map(|l| l.1).unwrap_or(0.0),
        }
    }
}

#[async_trait]
impl StrategyDriver for MeanRevertingStrategy {
    async fn key(&self) -> String { self.key.to_owned() }

    #[tracing::instrument(skip(self, le), level = "trace")]
    async fn add_event(&mut self, le: &MarketEventEnvelope) -> Result<()> {
        if !self.handles(le) {
            return Ok(());
        }
        self.process_event(le).await;

        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            // DataQuery::OperationHistory => Ok(DataResult::MeanRevertingOperations(self.get_operations())),
            // DataQuery::OpenOperations => Ok(DataResult::MeanRevertingOperation(Box::new(
            //     self.get_ongoing_op().cloned(),
            // ))),
            //DataQuery::CancelOngoingOp => Ok(DataResult::Success(self.cancel_ongoing_op()?)),
            //DataQuery::State => Ok(DataResult::State(serde_json::to_string(&self.state.vars).unwrap())),
            DataQuery::Status => Ok(DataResult::Status(self.status())),
            DataQuery::Models => Ok(DataResult::Models(self.model.values())),
            DataQuery::Indicators => Ok(DataResult::Indicators(self.indicators())),
            DataQuery::PositionHistory => {
                unimplemented!()
            }
            DataQuery::OpenPositions => {
                unimplemented!()
            }
            DataQuery::CancelOngoingOp => {
                unimplemented!()
            }
        }
    }

    fn mutate(&mut self, m: Mutation) -> Result<()> {
        match m {
            Mutation::State(m) => self.change_state(m.field, m.value),
            Mutation::Model(ModelReset { name, .. }) => self.model.reset(name),
        }
    }

    fn channels(&self) -> Vec<Channel> {
        vec![Channel::Orderbooks {
            xch: self.exchange,
            pair: self.pair.clone(),
        }]
    }

    fn stop_trading(&mut self) { self.is_trading = false; }

    fn resume_trading(&mut self) { self.is_trading = true; }

    async fn resolve_orders(&mut self) {
        // TODO : probably bad performance
        let locked_ids: Vec<String> = self
            .portfolio
            .locks()
            .iter()
            .map(|(_k, v)| v.order_id.clone())
            .collect();
        for lock in locked_ids {
            match self.engine.order_executor.get_order(lock.as_str()).await {
                Ok((order, _)) => {
                    if let Err(_e) = self.portfolio.update_position(order) {
                        // log err
                    }
                }
                Err(_e) => {
                    //log err
                }
            }
        }
    }
    //
    // async fn resolve_orders(&mut self) {
    //     // If a position is taken, resolve pending operations
    //     // In case of error return immediately as no trades can be made until the position is resolved
    //     if let Some(operation) = self.state.ongoing_op().cloned() {
    //         match self.state.resolve_pending_operations(&operation).await {
    //             Ok(resolution) => {
    //                 self.metrics.log_error(resolution.as_ref());
    //                 trace!("pending operation resolution {}", resolution.as_ref());
    //             }
    //             Err(e) => {
    //                 self.metrics.log_error(e.short_name());
    //                 trace!("pending operation resolution error {}", e.short_name());
    //             }
    //         }
    //         if self.state.ongoing_op().is_none() && self.state.is_trading() {
    //             if let Some(bp) = self.last_book_pos.clone() {
    //                 if let Err(e) = self.eval_latest(&bp).await {
    //                     self.metrics.log_error(e.short_name());
    //                 }
    //             }
    //         }
    //     }
    // }
}

#[async_trait]
impl crate::generic::Strategy for MeanRevertingStrategy {
    fn key(&self) -> String { self.key.to_owned() }

    fn init(&mut self) -> Result<()> { self.load() }

    async fn eval(&mut self, e: &MarketEventEnvelope) -> Result<Vec<TradeSignal>> {
        let mut signals = vec![];
        if !self.can_eval() {
            return Ok(signals);
        }
        let book_pos = self.parse_book_position(e);
        if book_pos.is_none() {
            return Ok(vec![]);
        };
        self.last_book_pos = book_pos.clone();
        if self.is_trading() {
            match self.eval_latest(&book_pos.unwrap()).await {
                Ok(Some(signal)) => {
                    self.metrics.log_position(signal.pos_kind, signal.op_kind, signal.price);
                    signals.push(signal);
                }
                Err(e) => self.metrics.log_error(e.short_name()),
                _ => {}
            }
        }
        self.metrics
            .log_portfolio(self.exchange, self.pair.clone(), &self.portfolio);
        Ok(signals)
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn update_model(&mut self, e: &MarketEventEnvelope) -> Result<()> {
        self.model.next_model(e)?;
        let t = self.model.thresholds();
        self.metrics.log_thresholds(t.0, t.1);
        if let Some(apo) = self.model.apo_value() {
            self.metrics.log_model(apo);
        }
        Ok(())
    }

    fn model(&self) -> Vec<(String, Option<serde_json::Value>)> { self.model.values() }

    fn channels(&self) -> HashSet<Channel> {
        let mut hs = HashSet::new();
        hs.extend((self as &dyn StrategyDriver).channels());
        hs
    }

    // fn indicators(&self) -> StrategyIndicators {
    //     StrategyIndicators {
    //         current_return: self.state.position_return(),
    //         pnl: self.state.pnl(),
    //         value: self.state.value_strat(),
    //     }
    // }
}
