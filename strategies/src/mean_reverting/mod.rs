use actix::Addr;
use chrono::{DateTime, Duration, TimeZone, Utc};
use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe, Pair};
use db::{get_or_create, Storage};
use itertools::Itertools;
use math::iter::QuantileExt;
use ordered_float::OrderedFloat;
use std::cmp::{max, min};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::Result;
use crate::mean_reverting::ema_model::{MeanRevertingModelValue, SinglePosRow};
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation, Position};
use crate::models::IndicatorModel;
use crate::models::WindowedModel;
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, FieldMutation, MutableField};
use crate::types::PositionKind;
use crate::util::Stopper;
use crate::{Channel, StrategyInterface, StrategyStatus};

mod ema_model;
mod metrics;
pub mod options;
pub mod state;
#[cfg(test)]
mod tests;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MeanRevertingStrategy {
    exchange: Exchange,
    pair: Pair,
    fees_rate: f64,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    state: MeanRevertingState,
    model: IndicatorModel<MeanRevertingModelValue, SinglePosRow>,
    threshold_table: Option<WindowedModel<f64, f64>>,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    dynamic_threshold: bool,
    last_threshold_time: DateTime<Utc>,
    stopper: Stopper<f64>,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new<S: AsRef<Path>>(db_path: S, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let mut pb: PathBuf = PathBuf::from(db_path.as_ref());
        let strat_db_path = format!("{}_{}.{}", MEAN_REVERTING_DB_KEY, n.exchange.to_string(), n.pair);
        pb.push(strat_db_path);
        let db = get_or_create(pb.as_path(), vec![]);
        let state = MeanRevertingState::new(n, fees_rate, db.clone(), om);
        let ema_model = Self::make_model(n.pair.as_ref(), db.clone(), n.short_window_size, n.long_window_size);
        let threshold_table = n.threshold_window_size.map(|thresold_window_size| {
            WindowedModel::new(
                &format!("thresholds_{}", n.pair.as_ref()),
                db.clone(),
                thresold_window_size,
                Some(thresold_window_size * 2),
                ema_model::threshold,
            )
        });

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
            dynamic_threshold: n.dynamic_threshold(),
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
        };
        if let Err(e) = strat.load() {
            error!("{}", e);
            panic!("Could not loaded models");
        }
        strat
    }

    fn load(&mut self) -> crate::error::Result<()> {
        {
            self.model.try_loading_model()?;
            if let Some(lm) = self.model.value() {
                self.state.set_apo(lm.apo);
            }
        }
        {
            if let Some(threshold_table) = &mut self.threshold_table {
                threshold_table.try_loading_model()?;
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

    pub fn make_model(
        pair: &str,
        db: Arc<dyn Storage>,
        short_window_size: u32,
        long_window_size: u32,
    ) -> IndicatorModel<MeanRevertingModelValue, SinglePosRow> {
        let init = MeanRevertingModelValue::new(long_window_size, short_window_size);
        IndicatorModel::new(&format!("model_{}", pair), db, init, ema_model::moving_average_apo)
    }

    #[cfg(test)]
    fn model_value(&self) -> Option<MeanRevertingModelValue> { self.model.value() }

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
        if !self.dynamic_threshold {
            return;
        }
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

    #[tracing::instrument(skip(self), level = "debug")]
    async fn eval_latest(&mut self, lr: &SinglePosRow) -> Result<&Option<Operation>> {
        if self.state.no_position_taken() {
            self.state.update_units(&lr.pos);
        }

        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        self.state.resolve_pending_operations(&lr.pos).await?;

        // Possibly open a short position
        if (self.state.apo() > self.state.threshold_short()) && self.state.no_position_taken() {
            info!(
                "Entering short position with threshold {}",
                self.state.threshold_short()
            );
            let position = self.short_position(lr.pos.bid, lr.time);
            self.state.open(position).await?;
        }
        // Possibly close a short position
        else if self.state.is_short() {
            self.state.set_position_return(lr.pos.ask);
            if (self.state.apo() < 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.short_position(lr.pos.ask, lr.time);
                self.state.close(position).await?;
            }
        }
        // Possibly open a long position
        else if (self.state.apo() < self.state.threshold_long()) && self.state.no_position_taken() {
            info!("Entering long position with threshold {}", self.state.threshold_long());
            let position = self.long_position(lr.pos.ask, lr.time);
            self.state.open(position).await?;
        }
        // Possibly close a long position
        else if self.state.is_long() {
            self.state.set_position_return(lr.pos.bid);
            if (self.state.apo() > 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.long_position(lr.pos.bid, lr.time);
                self.state.close(position).await?;
            }
        }
        Ok(self.state.ongoing_op())
    }

    fn can_eval(&self) -> bool { self.models_loaded() }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn process_row(&mut self, row: &SinglePosRow) {
        let should_sample = crate::util::is_eval_time_reached(row.time, self.last_sample_time, self.sample_freq, 1);
        // A model is available
        if should_sample {
            self.last_sample_time = row.time;
            let last_apo = self.state.apo();
            if let Some(t) = self.threshold_table.as_mut() {
                t.push(&last_apo)
            }
            let model_update = self
                .model
                .update_model(row.clone())
                .map_err(|e| e.into())
                .and_then(|_| {
                    self.model
                        .value()
                        .ok_or_else(|| crate::error::Error::ModelLoadError("no mean reverting model value".to_string()))
                })
                .map(|m| self.state.set_apo(m.apo));
            if model_update.is_err() {
                self.metrics.log_error("model_update");
            }
        }
        if self.can_eval() {
            self.maybe_eval_threshold(row.time);
            if self.state.is_trading() {
                match self.eval_latest(row).await {
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

    pub(crate) fn handles(&self, e: &LiveEventEnveloppe) -> bool {
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
impl StrategyInterface for MeanRevertingStrategy {
    async fn add_event(&mut self, le: LiveEventEnveloppe) -> Result<()> {
        if !self.handles(&le) {
            return Ok(());
        }
        if let LiveEvent::LiveOrderbook(ob) = le.e {
            let book_pos = ob.try_into().ok();
            if let Some(pos) = book_pos {
                let now = Utc::now();
                let x = SinglePosRow { time: now, pos };
                self.process_row(&x).await;
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::Operations => Some(DataResult::MeanRevertingOperations(self.get_operations())),
            DataQuery::CurrentOperation => Some(DataResult::MeanRevertingOperation(Box::new(
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
