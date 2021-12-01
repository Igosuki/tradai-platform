use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use coinnect_rt::prelude::*;
use db::Storage;
use options::Options;
use portfolio::portfolio::{Portfolio, PortfolioRepoImpl};
use portfolio::risk::DefaultMarketRiskEvaluator;
use stats::Next;
use trading::book::BookPosition;
use trading::engine::TradingEngine;
use trading::order_manager::types::StagedOrder;
use trading::position::{OperationKind, PositionKind};
use trading::signal::{new_trade_signal, TradeSignal};
use trading::stop::Stopper;
use trading::types::OrderConf;
use util::time::now;
use uuid::Uuid;

use crate::driver::StrategyDriver;
use crate::error::*;
use crate::naive_pair_trading::covar_model::{DualBookPosition, LinearModelValue, LinearSpreadModel};
use crate::query::{ModelReset, MutableField, Mutation, StrategyIndicators};
use crate::{Channel, DataQuery, DataResult, StratEvent, StratEventLogger, StrategyStatus};

use self::metrics::NaiveStrategyMetrics;

pub mod covar_model;
pub mod metrics;
pub mod options;

#[cfg(test)]
mod tests;

pub struct NaiveTradingStrategy {
    key: String,
    exchange: Exchange,
    res_threshold_long: f64,
    res_threshold_short: f64,
    model: LinearSpreadModel,
    right_pair: Pair,
    left_pair: Pair,
    metrics: Arc<NaiveStrategyMetrics>,
    last_left: Option<BookPosition>,
    last_right: Option<BookPosition>,
    portfolio: Portfolio,
    is_trading: bool,
    order_conf: OrderConf,
    engine: Arc<TradingEngine>,
    stopper: Stopper<f64>,
    logger: Option<Arc<dyn StratEventLogger>>,
}

impl NaiveTradingStrategy {
    pub fn new(
        db: Arc<dyn Storage>,
        strat_key: String,
        fees_rate: f64,
        n: &Options,
        engine: Arc<TradingEngine>,
        logger: Option<Arc<dyn StratEventLogger>>,
    ) -> Self {
        let metrics = NaiveStrategyMetrics::for_strat(prometheus::default_registry(), &n.left, &n.right);
        let portfolio = Portfolio::try_new(
            n.initial_cap,
            fees_rate,
            strat_key.clone(),
            Arc::new(PortfolioRepoImpl::new(db.clone())),
            Arc::new(DefaultMarketRiskEvaluator::default()),
            engine.interest_rate_provider.clone(),
        )
        .unwrap();
        let mut model = LinearSpreadModel::new(
            db,
            &format!("{}_{}", &n.left, &n.right),
            n.window_size as usize,
            n.beta_sample_freq(),
            n.beta_eval_freq,
        );
        if let Err(e) = model.try_load() {
            error!("{}", e);
            panic!("Could not load naive strategy model");
        }
        Self {
            key: strat_key,
            exchange: n.exchange,
            res_threshold_long: n.threshold_long,
            res_threshold_short: n.threshold_short,
            model,
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            right_pair: n.right.clone(),
            left_pair: n.left.clone(),
            metrics: Arc::new(metrics),
            last_left: None,
            last_right: None,
            portfolio,
            is_trading: true,
            order_conf: n.order_conf.clone(),
            engine,
            logger,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn make_signal(
        &self,
        pair: Pair,
        trace_id: Uuid,
        event_time: DateTime<Utc>,
        operation_kind: OperationKind,
        position_kind: PositionKind,
        price: f64,
        qty: Option<f64>,
    ) -> TradeSignal {
        new_trade_signal(
            pair,
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

    async fn eval_latest(&mut self, lr: &DualBookPosition) -> Result<Option<[TradeSignal; 2]>> {
        if !self.portfolio.has_any_open_position() && self.model.should_eval(lr.time) {
            self.model.update()?;
        }

        let beta = self.model.beta().unwrap();
        if beta <= 0.0 {
            return Ok(None);
        }
        let ratio = match self.calc_pred_ratio(lr) {
            None => return Ok(None),
            Some(ratio) => ratio,
        };
        self.metrics.log_ratio(ratio);

        let left_pos = self.portfolio.open_position(self.exchange, self.left_pair.clone());
        let right_pos = self.portfolio.open_position(self.exchange, self.right_pair.clone());
        let signals: Option<[TradeSignal; 2]> = match (left_pos, right_pos) {
            (None, None) => {
                // Possibly open a short position for the right and long for the left
                if ratio > self.res_threshold_short {
                    let spread = self.portfolio.value() / (lr.left.ask * beta);
                    self.metrics.relative_spread(spread);
                    Some([
                        self.make_signal(
                            self.left_pair.clone(),
                            lr.left.trace_id,
                            lr.left.event_time,
                            OperationKind::Open,
                            PositionKind::Long,
                            lr.left.ask,
                            Some(spread * beta),
                        ),
                        self.make_signal(
                            self.right_pair.clone(),
                            lr.right.trace_id,
                            lr.right.event_time,
                            OperationKind::Open,
                            PositionKind::Short,
                            lr.right.bid,
                            Some(spread),
                        ),
                    ])
                }
                // Possibly open a long position for the right and short for the left
                else if ratio <= self.res_threshold_long {
                    let spread = self.portfolio.value() / lr.right.ask;
                    self.metrics.relative_spread(spread);
                    Some([
                        self.make_signal(
                            self.left_pair.clone(),
                            lr.left.trace_id,
                            lr.left.event_time,
                            OperationKind::Open,
                            PositionKind::Short,
                            lr.left.bid,
                            Some(spread * beta),
                        ),
                        self.make_signal(
                            self.right_pair.clone(),
                            lr.right.trace_id,
                            lr.right.event_time,
                            OperationKind::Open,
                            PositionKind::Long,
                            lr.right.ask,
                            Some(spread),
                        ),
                    ])
                } else {
                    None
                }
            }
            (Some(left_pos), Some(right_pos)) => {
                let ret = self.portfolio.current_return();
                let maybe_stop = self.stopper.should_stop(ret);
                if let Some(logger) = &self.logger {
                    logger
                        .maybe_log(maybe_stop.as_ref().map(|e| StratEvent::Stop { stop: *e }))
                        .await;
                }
                // Possibly close a short position
                if right_pos.is_short()
                    && left_pos.is_long()
                    && ((ratio <= self.res_threshold_short && ratio < 0.0) || maybe_stop.is_some())
                {
                    self.model.update()?;
                    Some([
                        self.make_signal(
                            self.left_pair.clone(),
                            lr.left.trace_id,
                            lr.left.event_time,
                            OperationKind::Close,
                            PositionKind::Long,
                            lr.left.bid,
                            None,
                        ),
                        self.make_signal(
                            self.right_pair.clone(),
                            lr.right.trace_id,
                            lr.right.event_time,
                            OperationKind::Close,
                            PositionKind::Short,
                            lr.right.ask,
                            None,
                        ),
                    ])
                }
                // Possibly close a long position
                else if right_pos.is_long()
                    && left_pos.is_short()
                    && ((ratio >= self.res_threshold_long && ratio > 0.0) || maybe_stop.is_some())
                {
                    self.model.update()?;
                    Some([
                        self.make_signal(
                            self.left_pair.clone(),
                            lr.left.trace_id,
                            lr.left.event_time,
                            OperationKind::Close,
                            PositionKind::Short,
                            lr.left.ask,
                            None,
                        ),
                        self.make_signal(
                            self.right_pair.clone(),
                            lr.right.trace_id,
                            lr.right.event_time,
                            OperationKind::Close,
                            PositionKind::Long,
                            lr.right.bid,
                            None,
                        ),
                    ])
                } else {
                    None
                }
            }
            _ => None,
        };

        Ok(signals)
    }

    /// Ratio of prediction vs mid price
    fn calc_pred_ratio(&mut self, lr: &DualBookPosition) -> Option<f64> {
        self.predict_right(lr)
            .map(|predicted_right| (lr.right.mid - predicted_right) / lr.right.mid)
    }

    /// Predict the value of right price
    fn predict_right(&self, lr: &DualBookPosition) -> Option<f64> { self.model.predict(lr.left.mid) }

    fn can_eval(&self) -> bool {
        let has_position = self.portfolio.has_any_open_position();
        self.model.has_model() && (has_position || self.model.is_obsolete())
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn process_dual_bp(&mut self, dual_bp: &DualBookPosition) {
        if self.can_eval() {
            match self.eval_latest(dual_bp).await {
                Ok(Some(signals)) => {
                    self.metrics.log_signals(&signals[0], &signals[1]);
                    let mut orders = vec![];
                    for signal in signals {
                        let order = self.portfolio.maybe_convert(&signal).await;
                        if let Ok(Some(order)) = order {
                            orders.push(order);
                        }
                    }
                    if orders.len() != 2 {
                        return;
                    }
                    for order in orders {
                        let exchange = order.xch;
                        let pair = order.pair.clone();
                        if let Err(e) = self
                            .engine
                            .order_executor
                            .stage_order(StagedOrder { request: order })
                            .await
                        {
                            // TODO : keep result and immediatly try to close (or retry) failed orders
                            self.metrics.log_error(e.short_name());
                            error!(err = %e, "failed to stage order");
                            if let Err(e) = self.portfolio.unlock_position(exchange, pair) {
                                self.metrics.log_error(e.short_name());
                                error!(err = %e, "failed to unlock position");
                            }
                        }
                    }
                }
                Err(e) => self.metrics.log_error(e.short_name()),
                _ => {}
            }
            self.metrics.log_portfolio(
                self.exchange,
                self.left_pair.clone(),
                self.right_pair.clone(),
                &self.portfolio,
            );
        }
        self.metrics.log_mid_price(dual_bp);
        match self.model.next(dual_bp) {
            Err(e) => self.metrics.log_error(e.short_name()),
            Ok(Some(lm)) => self.metrics.log_model(&lm),
            _ => {}
        }
    }

    #[allow(dead_code)]
    fn model_value(&self) -> Option<LinearModelValue> { self.model.value() }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.portfolio.set_value(v)?,
            MutableField::Pnl => self.portfolio.set_pnl(v)?,
        }
        Ok(())
    }

    fn last_dual_bp(&self) -> Option<DualBookPosition> {
        self.last_left.as_ref().and_then(|l| {
            self.last_right.as_ref().map(|r| DualBookPosition {
                time: now(),
                left: *l,
                right: *r,
            })
        })
    }
}

#[async_trait]
impl StrategyDriver for NaiveTradingStrategy {
    async fn key(&self) -> String { self.key.to_owned() }

    async fn add_event(&mut self, le: &MarketEventEnvelope) -> Result<()> {
        if let MarketEvent::Orderbook(ob) = &le.e {
            let string = ob.pair.clone();
            if string == self.left_pair {
                self.last_left = ob.try_into().ok();
            } else if string == self.right_pair {
                self.last_right = ob.try_into().ok();
            }
        } else {
            return Ok(());
        }
        if let Err(e) = self.portfolio.update_from_market(le).await {
            // TODO: in metrics
            error!(err = %e, "failed to update portfolio from market");
        }
        if let (Some(l), Some(r)) = (self.last_left, self.last_right) {
            let x = DualBookPosition {
                left: l,
                right: r,
                time: now(),
            };
            self.process_dual_bp(&x).await;
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::Status => Ok(DataResult::Status(StrategyStatus::Running)),
            DataQuery::Models => Err(Error::FeatureNotImplemented),
            DataQuery::Indicators => Ok(DataResult::Indicators(StrategyIndicators::default())),
            DataQuery::PositionHistory => unimplemented!(),
            DataQuery::OpenPositions => unimplemented!(),
            DataQuery::CancelOngoingOp => unimplemented!(),
        }
    }

    fn mutate(&mut self, m: Mutation) -> Result<()> {
        match m {
            Mutation::State(m) => self.change_state(m.field, m.value),
            Mutation::Model(ModelReset { name, .. }) if name.is_none() || name == Some("lm".to_string()) => {
                self.model.reset()
            }
            _ => {
                unreachable!()
            }
        }
    }

    fn channels(&self) -> Vec<Channel> {
        vec![
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.left_pair.clone(),
            },
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.right_pair.clone(),
            },
        ]
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
        for lock in &locked_ids {
            match self.engine.order_executor.get_order(lock).await {
                Ok((order, _)) => {
                    if let Err(e) = self.portfolio.update_position(order) {
                        // TODO: in metrics
                        error!(err = %e, "failed to update portfolio position from order");
                    }
                }
                Err(e) => {
                    // TODO: in metrics
                    error!(err = %e, "failed to query order");
                }
            }
        }
        if !locked_ids.is_empty() && self.portfolio.locks().is_empty() {
            if let Some(dbp) = self.last_dual_bp() {
                if let Err(e) = self.eval_latest(&dbp).await {
                    // TODO: in metrics
                    error!(err = %e, "failed to eval after unlocking portfolio");
                }
            }
        }
    }
}
