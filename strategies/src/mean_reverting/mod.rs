use std::cmp::{max, min};
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use actix::Addr;
use chrono::{DateTime, Duration, TimeZone, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{AssetType, LiveEvent, LiveEventEnvelope, Pair};
use db::{get_or_create, DbOptions};
use ext::ResultExt;
use math::iter::QuantileExt;

use crate::error::Result;
use crate::generic::{InputEvent, TradeSignal};
use crate::mean_reverting::ema_model::ema_indicator_model;
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation, Position};
use crate::models::{IndicatorModel, Sampler};
use crate::models::{Model, WindowedModel};
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, FieldMutation, MutableField};
use crate::types::{BookPosition, PositionKind};
use crate::util::Stopper;
use crate::{Channel, StrategyDriver, StrategyStatus};
use math::indicators::macd_apo::MACDApo;
use std::collections::HashSet;

mod ema_model;
mod metrics;
pub mod options;
pub mod state;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinglePosRow {
    pub time: DateTime<Utc>,
    pub pos: BookPosition, // crypto_1
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MeanRevertingStrategy {
    exchange: Exchange,
    pair: Pair,
    fees_rate: f64,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    state: MeanRevertingState,
    model: IndicatorModel<MACDApo, f64>,
    threshold_table: Option<WindowedModel<f64, f64>>,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    last_threshold_time: DateTime<Utc>,
    stopper: Stopper<f64>,
    sampler: Sampler,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new<S: AsRef<Path>>(db_opts: &DbOptions<S>, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let strat_db_path = format!("{}_{}.{}", MEAN_REVERTING_DB_KEY, n.exchange.to_string(), n.pair);
        let db = get_or_create(db_opts, strat_db_path, vec![]);
        let state = MeanRevertingState::new(n, fees_rate, db.clone(), om);
        let ema_model = ema_indicator_model(n.pair.as_ref(), db.clone(), n.short_window_size, n.long_window_size);
        let threshold_table = if n.dynamic_threshold() {
            n.threshold_window_size.map(|thresold_window_size| {
                WindowedModel::new(
                    &format!("thresholds_{}", n.pair.as_ref()),
                    db.clone(),
                    thresold_window_size,
                    Some(thresold_window_size * 2),
                    ema_model::threshold,
                )
            })
        } else {
            None
        };

        let mut strat = Self {
            exchange: n.exchange,
            pair: n.pair.clone(),
            fees_rate,
            sample_freq: n.sample_freq(),
            last_sample_time: Utc.timestamp_millis(0),
            state,
            model: ema_model,
            threshold_table,
            threshold_eval_freq: n.threshold_eval_freq,
            threshold_short_0: n.threshold_short,
            threshold_long_0: n.threshold_long,
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
            sampler: Sampler::new(n.sample_freq(), Utc.timestamp_millis(0)),
        };
        if let Err(e) = strat.load() {
            error!("{}", e);
            panic!("Could not loaded models");
        }
        strat
    }

    fn load(&mut self) -> crate::error::Result<()> {
        {
            self.model.try_load()?;
            if let Some(_model_time) = self.model.last_model_time() {
                //self.sampler.set_last_time(model_time);
            }
        }
        {
            if let Some(threshold_table) = &mut self.threshold_table {
                threshold_table.try_load()?;
            }
        }
        if !self.models_loaded() {
            Err(crate::error::Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn models_loaded(&self) -> bool {
        self.model.is_loaded()
            && self
                .threshold_table
                .as_ref()
                .map(|t| t.is_loaded())
                .unwrap_or_else(|| false)
    }

    #[cfg(test)]
    fn model_value(&self) -> Option<MACDApo> { self.model.value() }

    fn log_state(&self) { self.metrics.log_state(&self.state); }

    fn get_operations(&self) -> Vec<Operation> { self.state.get_operations() }

    fn get_ongoing_op(&self) -> &Option<Operation> { self.state.ongoing_op() }

    fn cancel_ongoing_op(&mut self) -> bool { self.state.cancel_ongoing_op() }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> { self.state.change_state(field, v) }

    fn short_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::Short,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    fn long_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::Long,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn maybe_eval_threshold(&mut self, current_time: DateTime<Utc>) {
        if let (Some(threshold_table), Some(threshold_eval_freq)) = (&self.threshold_table, self.threshold_eval_freq) {
            if crate::util::is_eval_time_reached(
                current_time,
                self.last_threshold_time,
                self.sample_freq,
                threshold_eval_freq,
            ) && threshold_table.is_filled()
            {
                let wdw = threshold_table.window();
                let (threshold_short_iter, threshold_long_iter) = wdw.tee();
                self.state.set_threshold_short(
                    max(
                        self.threshold_short_0.into(),
                        OrderedFloat(threshold_short_iter.quantile(0.99)),
                    )
                    .into(),
                );
                tracing::trace!(target: "threshold_events", "set_threshold_short");
                self.state.set_threshold_long(
                    min(
                        OrderedFloat(self.threshold_long_0),
                        OrderedFloat(threshold_long_iter.quantile(0.01)),
                    )
                    .into(),
                );
                tracing::trace!(target: "threshold_events", "set_threshold_long");
                self.metrics.log_thresholds(&self.state);
                tracing::trace!(target: "threshold_events", "log_thresholds");
                self.last_threshold_time = current_time;
            }
        }
    }

    fn apo(&self) -> Option<f64> { self.model.value().map(|m| m.apo) }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn eval_latest(&mut self, lr: &BookPosition) -> Result<&Option<Operation>> {
        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        self.state.resolve_pending_operations(lr).await?;

        if self.state.no_position_taken() {
            self.state.update_units(lr);
        }

        let apo = self.apo().expect("model required");

        // Possibly open a short position
        if (apo > self.state.threshold_short()) && self.state.no_position_taken() {
            info!(
                "Entering short position with threshold {}",
                self.state.threshold_short()
            );
            let position = self.short_position(lr.bid, lr.event_time);
            self.state.open(position).await?;
        }
        // Possibly close a short position
        else if self.state.is_short() {
            self.state.set_position_return(lr.ask);
            if (apo < 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.short_position(lr.ask, lr.event_time);
                self.state.close(position).await?;
            }
        }
        // Possibly open a long position
        else if (apo < self.state.threshold_long()) && self.state.no_position_taken() {
            info!("Entering long position with threshold {}", self.state.threshold_long());
            let position = self.long_position(lr.ask, lr.event_time);
            self.state.open(position).await?;
        }
        // Possibly close a long position
        else if self.state.is_long() {
            self.state.set_position_return(lr.bid);
            if (apo > 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.long_position(lr.bid, lr.event_time);
                self.state.close(position).await?;
            }
        }
        Ok(self.state.ongoing_op())
    }

    fn can_eval(&self) -> bool { self.models_loaded() }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn process_row(&mut self, row: &SinglePosRow) {
        let should_sample = crate::util::is_eval_time_reached(row.time, self.last_sample_time, self.sample_freq, 1);
        // A model is available
        if should_sample {
            self.last_sample_time = row.time;
            if let Some(apo) = self.apo() {
                if let Some(t) = self.threshold_table.as_mut() {
                    t.push(&apo)
                }
            }
            let model_update = self.model.update(row.pos.mid).err_into().and_then(|_| {
                self.model
                    .value()
                    .ok_or_else(|| crate::error::Error::ModelLoadError("no mean reverting model value".to_string()))
                    .map(|m| self.metrics.log_model(m))
            });
            if model_update.is_err() {
                self.metrics.log_error("model_update");
            }
        }
        if self.can_eval() {
            self.maybe_eval_threshold(row.time);
            if self.state.is_trading() {
                match self.eval_latest(&row.pos).await {
                    Ok(Some(op)) => {
                        let op = op.clone();
                        self.metrics.log_position(&op.pos, &op.kind);
                    }
                    Err(e) => self.metrics.log_error(e.short_name()),
                    _ => {}
                }
            }
            self.log_state();
        }
        self.metrics.log_is_trading(self.state.is_trading());
        self.metrics.log_row(row);
    }

    pub(crate) fn handles(&self, e: &LiveEventEnvelope) -> bool {
        self.exchange == e.xch
            && match &e.e {
                LiveEvent::LiveOrderbook(ob) => ob.pair == self.pair,
                _ => false,
            }
    }

    pub(crate) fn status(&self) -> StrategyStatus {
        if self.state.is_trading() {
            StrategyStatus::Running
        } else {
            StrategyStatus::NotTrading
        }
    }
}

#[async_trait]
impl StrategyDriver for MeanRevertingStrategy {
    async fn add_event(&mut self, le: &LiveEventEnvelope) -> Result<()> {
        if !self.handles(le) {
            return Ok(());
        }
        if let LiveEvent::LiveOrderbook(ob) = &le.e {
            let book_pos = ob.try_into().ok();
            if let Some(pos) = book_pos {
                let x = SinglePosRow {
                    time: Utc.timestamp_millis(ob.timestamp),
                    pos,
                };
                self.process_row(&x).await;
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::OperationHistory => Some(DataResult::MeanRevertingOperations(self.get_operations())),
            DataQuery::OpenOperations => Some(DataResult::MeanRevertingOperation(Box::new(
                self.get_ongoing_op().clone(),
            ))),
            DataQuery::CancelOngoingOp => Some(DataResult::OperationCanceled(self.cancel_ongoing_op())),
            DataQuery::State => Some(DataResult::State(serde_json::to_string(&self.state).unwrap())),
            DataQuery::Status => Some(DataResult::Status(self.status())),
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> { self.change_state(m.field, m.value) }

    fn channels(&self) -> Vec<Channel> {
        vec![Channel::Orderbooks {
            xch: self.exchange,
            pair: self.pair.clone(),
        }]
    }
}

#[async_trait]
impl crate::generic::Strategy for MeanRevertingStrategy {
    fn init(&mut self) -> Result<()> { self.load() }

    async fn eval(&mut self, e: &crate::generic::InputEvent) -> Result<Vec<crate::generic::TradeSignal>> {
        let mut signals = vec![];
        if !self.can_eval() {
            return Ok(signals);
        }
        let book_pos = match e {
            InputEvent::BookPosition(bp) => bp,
            _ => return Ok(signals),
        };
        if self.state.is_trading() {
            match self.eval_latest(book_pos).await {
                Ok(Some(op)) => {
                    let op = op.clone();
                    self.metrics.log_position(&op.pos, &op.kind);
                    signals.push(TradeSignal {
                        position_kind: op.pos.kind,
                        operation_kind: op.kind,
                        trade_kind: op.trade.kind,
                        price: op.trade.price,
                        pair: self.pair.clone(),
                        exchange: self.exchange,
                        instructions: op.instructions,
                        dry_mode: op.trade.dry_mode,
                        asset_type: AssetType::Spot,
                    });
                }
                Err(e) => self.metrics.log_error(e.short_name()),
                _ => {}
            }
        }
        self.log_state();
        Ok(signals)
    }

    async fn update_model(&mut self, e: &crate::generic::InputEvent) -> Result<()> {
        let book_pos = match e {
            InputEvent::BookPosition(bp) => bp,
            _ => return Ok(()),
        };
        if !self.sampler.sample(book_pos.event_time) {
            return Ok(());
        }
        self.model
            .update(book_pos.mid)
            .err_into()
            .and_then(|_| {
                self.model
                    .value()
                    .ok_or_else(|| crate::error::Error::ModelLoadError("no mean reverting model value".to_string()))
                    .map(|m| self.metrics.log_model(m))
            })
            .map_err(|_| {
                self.metrics.log_error("model_update");
            })
            .unwrap();
        if let Some(apo) = self.model.value().map(|m| m.apo) {
            if let Some(t) = self.threshold_table.as_mut() {
                t.push(&apo);
                if t.is_filled() {
                    t.update_model().unwrap();
                    // Put this in model WindowFn
                    let wdw = t.window();
                    let (threshold_short_iter, threshold_long_iter) = wdw.tee();
                    self.state.set_threshold_short(
                        max(
                            self.threshold_short_0.into(),
                            OrderedFloat(threshold_short_iter.quantile(0.99)),
                        )
                        .into(),
                    );
                    tracing::trace!(target: "threshold_events", "set_threshold_short");
                    self.state.set_threshold_long(
                        min(
                            OrderedFloat(self.threshold_long_0),
                            OrderedFloat(threshold_long_iter.quantile(0.01)),
                        )
                        .into(),
                    );
                    tracing::trace!(target: "threshold_events", "set_threshold_long");
                    self.metrics.log_thresholds(&self.state);
                    tracing::trace!(target: "threshold_events", "log_thresholds");
                }
            }
        }
        Ok(())
    }

    fn models(&self) -> Vec<(&str, Option<serde_json::Value>)> {
        vec![
            ("apo", self.model.ser()),
            ("thresholds", self.threshold_table.as_ref().and_then(|t| t.ser())),
        ]
    }

    fn channels(&self) -> HashSet<Channel> {
        let mut hs = HashSet::new();
        hs.extend((self as &dyn StrategyDriver).channels());
        hs
    }
}
