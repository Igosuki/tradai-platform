use std::cmp::max;
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use uuid::Uuid;

use options::Options;
use stats::Next;
use strategy::coinnect::prelude::*;
use strategy::db::Storage;
use strategy::driver::{DefaultStrategyContext, Strategy};
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin};
use strategy::prelude::*;
use strategy::types::StratEvent;
use strategy::Portfolio;
use strategy::{Channel, StratEventLogger};
use trading::book::BookPosition;
use trading::engine::TradingEngine;
use trading::position::{OperationKind, PositionKind};
use trading::signal::{new_trade_signal, TradeSignal};
use trading::stop::Stopper;
use trading::types::OrderConf;
use util::time::now;

use self::covar_model::{DualBookPosition, LinearModelValue, LinearSpreadModel};
use self::metrics::NaiveStrategyMetrics;

pub mod covar_model;
pub mod metrics;
pub mod options;

#[cfg(test)]
mod tests;

inventory::submit! {
    StrategyPlugin::new("naive_spread", provide_options::<Options>, |name, ctx, conf| {
        let options: Options = serde_json::from_value(conf)?;
        Ok(Box::new(NaiveTradingStrategy::new(ctx.db, name.to_string(), &options, ctx.engine, ctx.logger)))
    })
}

pub struct NaiveTradingStrategy {
    key: String,
    exchange: Exchange,
    res_threshold_long: f64,
    res_threshold_short: f64,
    max_pos_duration: Duration,
    model: LinearSpreadModel,
    right_pair: Pair,
    left_pair: Pair,
    metrics: Arc<NaiveStrategyMetrics>,
    last_left: Option<BookPosition>,
    last_right: Option<BookPosition>,
    order_conf: OrderConf,
    #[allow(dead_code)]
    engine: Arc<TradingEngine>,
    stopper: Stopper<f64>,
    logger: Option<Arc<dyn StratEventLogger>>,
}

impl NaiveTradingStrategy {
    pub fn new(
        db: Arc<dyn Storage>,
        strat_key: String,
        n: &Options,
        engine: Arc<TradingEngine>,
        logger: Option<Arc<dyn StratEventLogger>>,
    ) -> Self {
        let metrics = NaiveStrategyMetrics::for_strat(prometheus::default_registry(), &n.left, &n.right);
        let model = LinearSpreadModel::new(
            db,
            &format!("{}_{}", &n.left, &n.right),
            n.window_size as usize,
            n.beta_sample_freq(),
            n.beta_eval_freq,
        );

        Self {
            key: strat_key,
            exchange: n.exchange,
            res_threshold_long: n.threshold_long,
            res_threshold_short: n.threshold_short,
            max_pos_duration: n.max_pos_duration(),
            model,
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            right_pair: n.right.clone(),
            left_pair: n.left.clone(),
            metrics: Arc::new(metrics),
            last_left: None,
            last_right: None,
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

    async fn eval_latest(&self, lr: &DualBookPosition, portfolio: &Portfolio) -> Result<Option<[TradeSignal; 2]>> {
        let beta = self.model.beta().unwrap();
        if beta <= 0.0 {
            return Ok(None);
        }
        let ratio = match self.calc_pred_ratio(lr.left.mid, lr.right.mid) {
            None => return Ok(None),
            Some(ratio) => ratio,
        };
        self.metrics.log_ratio(ratio);

        let left_pos = portfolio.open_position(self.exchange, self.left_pair.clone());
        let right_pos = portfolio.open_position(self.exchange, self.right_pair.clone());
        let signals: Option<[TradeSignal; 2]> = match (left_pos, right_pos) {
            (None, None) => {
                // Possibly open a short position for the right and long for the left
                if ratio > self.res_threshold_short {
                    let spread = portfolio.value() / (lr.left.ask * beta);
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
                    let spread = portfolio.value() / lr.right.ask;
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
                let ret = portfolio.current_return();
                let maybe_stop = self.stopper.should_stop(ret);
                if let Some(logger) = &self.logger {
                    logger
                        .maybe_log(maybe_stop.as_ref().map(|e| StratEvent::Stop { stop: *e }))
                        .await;
                }
                let max_open_time_reached =
                    now() - max(left_pos.meta.open_at, right_pos.meta.open_at) > self.max_pos_duration;
                // Possibly close a short position
                if right_pos.is_short()
                    && left_pos.is_long()
                    && ((ratio <= self.res_threshold_short && ratio < 0.0)
                        || maybe_stop.is_some()
                        || max_open_time_reached)
                {
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
                    && ((ratio >= self.res_threshold_long && ratio > 0.0)
                        || maybe_stop.is_some()
                        || max_open_time_reached)
                {
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
    fn calc_pred_ratio(&self, left_price: f64, right_price: f64) -> Option<f64> {
        self.predict_right(left_price)
            .map(|predicted_right| (right_price - predicted_right) / right_price)
    }

    /// Predict the value of right price
    fn predict_right(&self, price: f64) -> Option<f64> { self.model.predict(price) }

    fn can_eval(&self, portfolio: &Portfolio) -> bool {
        let has_position = portfolio.has_any_open_position();
        self.model.has_model() && (has_position || self.model.is_obsolete())
    }

    #[allow(dead_code)]
    fn model_value(&self) -> Option<LinearModelValue> { self.model.value() }
}

#[async_trait]
impl Strategy for NaiveTradingStrategy {
    fn key(&self) -> String { self.key.to_owned() }

    fn init(&mut self) -> Result<()> {
        self.model.try_load().map_err(|e| {
            error!("{}", e);
            e
        })
    }

    async fn eval(
        &mut self,
        le: &MarketEventEnvelope,
        ctx: &DefaultStrategyContext,
    ) -> Result<Option<Vec<TradeSignal>>> {
        if let MarketEvent::Orderbook(ob) = &le.e {
            let string = ob.pair.clone();
            if string == self.left_pair {
                self.last_left = ob.try_into().ok();
            } else if string == self.right_pair {
                self.last_right = ob.try_into().ok();
            }
        } else {
            return Ok(None);
        }
        if let (Some(l), Some(r)) = (self.last_left, self.last_right) {
            let dbp = DualBookPosition {
                left: l,
                right: r,
                time: now(),
            };
            if !ctx.portfolio.has_any_open_position() && self.model.should_eval(dbp.time) {
                self.model.update()?;
            }
            if self.can_eval(ctx.portfolio) {
                if let Some(signals) = self.eval_latest(&dbp, ctx.portfolio).await? {
                    return Ok(Some(signals.into()));
                }
            }
        }
        Ok(None)
    }

    async fn update_model(&mut self, le: &MarketEventEnvelope) -> Result<()> {
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
        if let (Some(l), Some(r)) = (self.last_left, self.last_right) {
            let dbp = DualBookPosition {
                left: l,
                right: r,
                time: now(),
            };
            self.metrics.log_mid_price(&dbp);
            match self.model.next(&dbp) {
                Err(e) => self.metrics.log_error(e.short_name()),
                Ok(Some(lm)) => self.metrics.log_model(&lm),
                _ => {}
            }
        }
        Ok(())
    }

    fn model(&self) -> Vec<(String, Option<Value>)> { self.model.serialized() }

    fn channels(&self) -> HashSet<Channel> {
        let channels = vec![
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.left_pair.clone(),
            },
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.right_pair.clone(),
            },
        ];
        let mut hs = HashSet::new();
        hs.extend(channels);
        hs
    }
}
