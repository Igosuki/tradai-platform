use actix::Addr;
use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use coinnect_rt::types::{LiveEvent, Pair};
use itertools::Itertools;
use std::convert::TryInto;
use std::path::{Path, PathBuf};

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
use crate::{Channel, StrategyInterface};
use coinnect_rt::exchange::Exchange;
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
    #[allow(dead_code)]
    last_row_time_at_eval: DateTime<Utc>,
    #[allow(dead_code)]
    last_row_process_time: DateTime<Utc>,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    #[allow(dead_code)]
    dynamic_threshold: bool,
    last_threshold_time: DateTime<Utc>,
    stopper: Stopper<f64>,
    #[allow(dead_code)]
    long_window_size: u32,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new<S: AsRef<Path>>(db_path: S, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let mut pb: PathBuf = PathBuf::from(db_path.as_ref());
        let strat_db_path = format!("{}_{}", MEAN_REVERTING_DB_KEY, n.pair);
        pb.push(strat_db_path);
        let state = MeanRevertingState::new(n, pb.clone(), om);

        // EMA Model
        let model = Self::make_model(
            n.pair.as_ref(),
            pb.to_str().unwrap(),
            n.short_window_size,
            n.long_window_size,
        );
        // Threshold model
        let threshold_table = n.threshold_window_size.map(|thresold_window_size| {
            WindowedModel::new(
                n.pair.as_ref(),
                pb.to_str().unwrap(),
                thresold_window_size,
                Some(thresold_window_size * 2),
                ema_model::threshold,
            )
        });

        Self {
            exchange: n.exchange.clone(),
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
            threshold_short_0: n.threshold_short,
            threshold_long_0: n.threshold_long,
            dynamic_threshold: n.dynamic_threshold(),
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
            long_window_size: n.long_window_size,
        }
    }

    pub fn make_model<S: AsRef<Path>>(
        pair: &str,
        db_path: S,
        short_window_size: u32,
        long_window_size: u32,
    ) -> IndicatorModel<MeanRevertingModelValue, SinglePosRow> {
        let init = MeanRevertingModelValue::new(long_window_size, short_window_size);
        IndicatorModel::new(&pair, db_path, init, ema_model::moving_average_apo)
    }

    #[allow(dead_code)]
    fn model_value(&self) -> Option<MeanRevertingModelValue> { self.model.value() }

    fn log_state(&self) { self.metrics.log_state(&self.state); }

    fn get_operations(&self) -> Vec<Operation> { self.state.get_operations() }

    fn get_ongoing_op(&self) -> &Option<Operation> { self.state.ongoing_op() }

    fn cancel_ongoing_op(&mut self) -> bool { self.state.cancel_ongoing_op() }

    fn dump_db(&self) -> Vec<String> { self.state.dump_db() }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> { self.state.change_state(field, v) }

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
                tracing::debug!(target: "threshold_events", "set_threshold_short");
                self.state.set_threshold_long(
                    min(
                        OrderedFloat(self.threshold_long_0),
                        OrderedFloat(threshold_long_iter.quantile(0.01)),
                    )
                    .into(),
                );
                tracing::debug!(target: "threshold_events", "set_threshold_long");
                self.metrics.log_thresholds(&self.state);
                tracing::debug!(target: "threshold_events", "log_thresholds");
                self.last_threshold_time = current_time;
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn eval_latest(&mut self, lr: &SinglePosRow) -> Result<Operation> {
        self.maybe_eval_threshold(lr.time);

        if self.state.no_position_taken() {
            self.state.update_units(&lr.pos, self.fees_rate);
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
            return self.state.open(position, self.fees_rate).await;
        }

        // Possibly close a short position
        if self.state.is_short() {
            self.state.set_short_position_return(self.fees_rate, lr.pos.ask);
            if (self.state.apo() < 0.0) || self.stopper.maybe_stop(self.return_value(&PositionKind::SHORT)) {
                let position = self.short_position(lr.pos.ask, lr.time);
                return self.state.close(position, self.fees_rate).await;
            }
        }

        // Possibly open a long position
        if (self.state.apo() < self.state.threshold_long()) && self.state.no_position_taken() {
            info!("Entering long position with threshold {}", self.state.threshold_long());
            let position = self.long_position(lr.pos.ask, lr.time);
            return self.state.open(position, self.fees_rate).await;
        }

        // Possibly close a long position
        if self.state.is_long() {
            self.state.set_long_position_return(self.fees_rate, lr.pos.bid);
            if (self.state.apo() > 0.0) || self.stopper.maybe_stop(self.return_value(&PositionKind::LONG)) {
                let position = self.long_position(lr.pos.bid, lr.time);
                return self.state.close(position, self.fees_rate).await;
            }
        }

        Err(anyhow!("Evaluation led to nothing"))
    }

    fn return_value(&self, pk: &PositionKind) -> f64 {
        match pk {
            PositionKind::SHORT => self.state.short_position_return(),
            PositionKind::LONG => self.state.long_position_return(),
        }
    }

    fn can_eval(&self) -> bool { self.model.value().is_some() }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn process_row(&mut self, row: &SinglePosRow) {
        self.last_row_time_at_eval = self.model.last_model_time().unwrap_or_else(|| Utc.timestamp_millis(0));
        let should_sample = crate::util::is_eval_time_reached(row.time, self.last_sample_time, self.sample_freq, 1);
        // A model is available
        if should_sample {
            self.last_sample_time = row.time;
            // TODO: log error
            let last_apo = self.state.apo();
            self.threshold_table.as_mut().map(|t| t.push(&last_apo));
            self.model
                .update_model(row.clone())
                .map(|_| {
                    if let Some(m) = self.model.value() {
                        trace!("apo {}", m.apo);
                        self.state.set_apo(m.apo);
                    }
                })
                .unwrap();
            self.last_row_time_at_eval = self.last_sample_time;
        }
        if self.can_eval() {
            match self.eval_latest(row).await {
                Ok(op) => self.metrics.log_position(&op.pos, &op.kind),
                Err(e) => trace!("{}", e),
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
            DataQuery::Operations => Some(DataResult::MeanRevertingOperations(self.get_operations())),
            DataQuery::Dump => Some(DataResult::Dump(self.dump_db())),
            DataQuery::CurrentOperation => Some(DataResult::MeanRevertingOperation(self.get_ongoing_op().clone())),
            DataQuery::CancelOngoingOp => Some(DataResult::OngongOperationCancelation(self.cancel_ongoing_op())),
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> { self.change_state(m.field, m.value) }

    fn channels(&self) -> Vec<Channel> {
        vec![Channel::Orderbooks {
            xch: self.exchange.clone(),
            pair: self.pair.clone(),
        }]
    }
}
