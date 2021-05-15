use actix::Addr;
use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use coinnect_rt::types::{LiveEvent, Pair};
use db::Db;
use itertools::Itertools;
use std::convert::TryInto;
use std::path::PathBuf;

use crate::mean_reverting::ema_model::{MeanRevertingModelValue, SinglePosRow};
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation, Position};
use crate::model::{PositionKind, StopEventType, StratEvent, StopEvent};
use crate::ob_double_window_model::WindowTable;
use crate::ob_indicator_model::IndicatorModel;
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, FieldMutation, MutableField};
use crate::StrategyInterface;
use math::iter::QuantileExt;
use ordered_float::OrderedFloat;
use std::cmp::{max, min};
use std::sync::Arc;

mod ema_model;
mod metrics;
pub mod options;
pub mod state;
#[cfg(test)]
mod tests;

pub struct MeanRevertingStrategy {
    pair: Pair,
    fees_rate: f64,
    sample_freq: Duration,
    last_sample_time: DateTime<Utc>,
    state: MeanRevertingState,
    model: IndicatorModel<MeanRevertingModelValue, SinglePosRow>,
    threshold_table: Option<WindowTable<f64>>,
    #[allow(dead_code)]
    last_row_time_at_eval: DateTime<Utc>,
    #[allow(dead_code)]
    last_row_process_time: DateTime<Utc>,
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_eval_window_size: Option<usize>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    #[allow(dead_code)]
    dynamic_threshold: bool,
    last_threshold_time: DateTime<Utc>,
    stop_loss: f64,
    stop_gain: f64,
    #[allow(dead_code)]
    long_window_size: u32,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new(db_path: &str, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics =
            MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let mut pb = PathBuf::from(db_path);
        let strat_db_path = format!("{}_{}", MEAN_REVERTING_DB_KEY, n.pair);
        pb.push(strat_db_path);
        let db_name = format!("{}", n.pair);
        let db = Db::new(&pb.to_str().unwrap(), db_name);
        let state = MeanRevertingState::new(n, db, om);
        let dynamic_threshold_enabled = n.dynamic_threshold();
        let _max_size = if dynamic_threshold_enabled {
            n.threshold_window_size
                .map(|t| max(t, 2 * n.long_window_size as usize))
        } else {
            None
        };

        let model = Self::make_model_table(
            n.pair.as_ref(),
            pb.to_str().unwrap(),
            n.short_window_size,
            n.long_window_size,
        );
        let threshold_table = n
            .threshold_window_size
            .map(|t| Self::make_window_table(n.pair.as_ref(), pb.to_str().unwrap(), t));

        Self {
            pair: n.pair.clone(),
            fees_rate,
            sample_freq: n.sample_freq(),
            last_sample_time: Utc.timestamp_millis(0),
            last_row_time_at_eval: Utc.timestamp_millis(0),
            last_row_process_time: Utc.timestamp_millis(0),
            state,
            model,
            threshold_table,
            threshold_eval_freq: n.threshold_eval_freq,
            threshold_eval_window_size: n.threshold_window_size,
            threshold_short_0: n.threshold_short,
            threshold_long_0: n.threshold_long,
            dynamic_threshold: dynamic_threshold_enabled,
            last_threshold_time: Utc.timestamp_millis(0),
            stop_loss: n.stop_loss,
            metrics: Arc::new(metrics),
            long_window_size: n.long_window_size,
            stop_gain: n.stop_gain,
        }
    }

    pub fn make_model_table(
        pair: &str,
        db_path: &str,
        short_window_size: u32,
        long_window_size: u32,
    ) -> IndicatorModel<MeanRevertingModelValue, SinglePosRow> {
        let init = MeanRevertingModelValue::new(long_window_size, short_window_size);
        IndicatorModel::new(
            &pair,
            db_path,
            init,
            Box::new(ema_model::moving_average_apo),
        )
    }

    pub fn make_window_table(pair: &str, db_path: &str, window_size: usize) -> WindowTable<f64> {
        WindowTable::new(
            &pair,
            db_path,
            window_size,
            Some(window_size * 2),
            Box::new(ema_model::threshold),
        )
    }

    #[allow(dead_code)]
    fn model_value(&self) -> Option<MeanRevertingModelValue> {
        self.model.model().map(|m| m.value)
    }

    fn log_state(&self) {
        self.metrics.log_state(&self.state);
    }

    fn get_operations(&self) -> Vec<Operation> {
        self.state.get_operations()
    }

    fn get_ongoing_op(&self) -> &Option<Operation> {
        self.state.ongoing_op()
    }

    fn cancel_ongoing_op(&mut self) -> bool {
        self.state.cancel_ongoing_op()
    }

    fn dump_db(&self) -> Vec<String> {
        self.state.dump_db()
    }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        self.state.change_state(field, v)
    }

    fn short_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::SHORT,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    fn long_position(&self, price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::LONG,
            price,
            time,
            pair: self.pair.to_string(),
        }
    }

    fn maybe_eval_threshold(&mut self, current_time: DateTime<Utc>) {
        if let Some(threshold_table) = &self.threshold_table {
            if let (Some(threshold_eval_freq), Some(threshold_window_size)) =
            (self.threshold_eval_freq, self.threshold_eval_window_size)
            {
                if crate::util::is_eval_time_reached(
                    current_time,
                    self.last_threshold_time,
                    self.sample_freq,
                    threshold_eval_freq,
                ) && threshold_table.len() > threshold_window_size
                {
                    let wdw = threshold_table.window(threshold_window_size);
                    let (threshold_short_iter, threshold_long_iter) = wdw.tee();
                    self.state.set_threshold_short(
                        max(
                            self.threshold_short_0.into(),
                            OrderedFloat(threshold_short_iter.quantile(0.99)),
                        )
                            .into(),
                    );
                    self.state.set_threshold_long(
                        min(
                            OrderedFloat(self.threshold_long_0),
                            OrderedFloat(threshold_long_iter.quantile(0.01)),
                        )
                            .into(),
                    );
                    self.metrics.log_thresholds(&self.state);
                }
            }
        }
    }

    async fn eval_latest(&mut self, lr: &SinglePosRow) -> Result<Operation> {
        self.maybe_eval_threshold(lr.time);

        if self.state.no_position_taken() {
            self.state.update_units(&lr.pos, self.fees_rate);
        }

        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        self.state
            .resolve_pending_operations(&lr.pos)
            .await?;

        // Possibly open a short position
        if (self.state.apo() > self.state.threshold_short()) && self.state.no_position_taken() {
            info!(
                "Entering short position with threshold {}",
                self.state.threshold_short()
            );
            let position = self.short_position(lr.pos.bid, lr.time);
            return self.state.open(position, self.fees_rate).await
        }

        // Possibly close a short position
        if self.state.is_short() {
            self.state
                .set_short_position_return(self.fees_rate, lr.pos.ask);
            if (self.state.apo() < 0.0) || self.should_stop(&PositionKind::SHORT) {
                self.maybe_log_stop_loss(PositionKind::SHORT);
                let position = self.short_position(lr.pos.ask, lr.time);
                return self.state.close(position, self.fees_rate).await
            }
        }

        // Possibly open a long position
        if (self.state.apo() < self.state.threshold_long()) && self.state.no_position_taken() {
            info!(
                "Entering long position with threshold {}",
                self.state.threshold_long()
            );
            let position = self.long_position(lr.pos.ask, lr.time);
            return self.state.open(position, self.fees_rate).await
        }

        // Possibly close a long position
        if self.state.is_long() {
            self.state
                .set_long_position_return(self.fees_rate, lr.pos.bid);
            if (self.state.apo() > 0.0) || self.should_stop(&PositionKind::LONG) {
                self.maybe_log_stop_loss(PositionKind::LONG);
                let position = self.long_position(lr.pos.bid, lr.time);
                return self.state.close(position, self.fees_rate).await
            }
        }

        Err(anyhow!("Evaluation led to nothing"))
    }

    fn maybe_log_stop_loss(&self, pk: PositionKind) {
        if self.should_stop(&pk) {
            let ret = self.return_value(&pk);
            let stop_type = if ret > self.stop_gain {
                StopEventType::GAIN
            } else if ret < self.stop_loss {
                StopEventType::LOSS
            } else {
                StopEventType::NA
            };
            let event = StratEvent::Stop(StopEvent {
                ty: stop_type,
                pos: pk,
            });
            info!("{}", serde_json::to_string(&event).unwrap());
        }
    }

    fn return_value(&self, pk: &PositionKind) -> f64 {
        match pk {
            PositionKind::SHORT => self.state.short_position_return(),
            PositionKind::LONG => self.state.long_position_return(),
        }
    }

    fn should_stop(&self, pk: &PositionKind) -> bool {
        let ret = self.return_value(pk);
        ret > self.stop_gain || ret < self.stop_loss
    }

    fn can_eval(&self) -> bool {
        self.model.is_ready()
    }

    async fn process_row(&mut self, row: &SinglePosRow) {
        if self.model.model().is_some() {
            self.last_row_time_at_eval = self
                .model
                .last_model_time()
                .unwrap_or_else(|| Utc.timestamp_millis(0));
        }
        let should_sample =
            crate::util::is_eval_time_reached(row.time, self.last_sample_time, self.sample_freq, 1);
        // A model is available
        let can_eval = self.can_eval();
        if should_sample {
            self.last_sample_time = row.time;
            // TODO: log error
            self.model.update_model(row).unwrap();
            if let Some(m) = self.model.value() {
                trace!("apo {}", m.apo);
                self.state.set_apo(m.apo);
            }
            self.last_row_time_at_eval = self.last_sample_time;
        }
        if can_eval {
            match self.eval_latest(row).await {
                Ok(op) => self.metrics.log_position(&op.pos, &op.kind),
                Err(e) => trace!("{}", e)
            }
            self.log_state();
        }
        self.metrics.log_row(&row);
    }
}

#[async_trait]
impl StrategyInterface for MeanRevertingStrategy {
    async fn add_event(&mut self, le: LiveEvent) -> anyhow::Result<()> {
        if let LiveEvent::LiveOrderbook(ob) = le {
            let book_pos = ob.try_into().ok();
            if let Some(pos) = book_pos {
                let now = Utc::now();
                let x = SinglePosRow { time: now, pos };
                self.process_row(&x).await;
                self.last_row_process_time = now;
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::Operations => {
                Some(DataResult::MeanRevertingOperations(self.get_operations()))
            }
            DataQuery::Dump => Some(DataResult::Dump(self.dump_db())),
            DataQuery::CurrentOperation => Some(DataResult::MeanRevertingOperation(
                self.get_ongoing_op().clone(),
            )),
            DataQuery::CancelOngoingOp => Some(DataResult::OngongOperationCancelation(
                self.cancel_ongoing_op(),
            )),
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> {
        self.change_state(m.field, m.value)
    }
}
