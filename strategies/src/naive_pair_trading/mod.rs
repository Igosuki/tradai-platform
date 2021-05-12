use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use log::Level::Debug;
use std::convert::TryInto;
use std::ops::{Add, Mul, Sub};
use std::sync::Arc;

pub mod covar_model;
pub mod metrics;
pub mod options;
pub mod state;

#[cfg(test)]
mod tests;

use crate::model::{BookPosition, PositionKind};
use crate::naive_pair_trading::covar_model::DataRow;
use crate::naive_pair_trading::state::Operation;
use crate::ob_linear_model::LinearModelTable;
use crate::order_manager::OrderManager;
use crate::query::{FieldMutation, MutableField};
use crate::{DataQuery, DataResult, StrategyInterface};
use actix::Addr;
use coinnect_rt::types::LiveEvent;
use db::Db;
use metrics::NaiveStrategyMetrics;
use options::Options;
use state::{MovingState, Position};

const LM_AGE_CUTOFF_RATIO: f64 = 0.0013;

pub struct NaiveTradingStrategy {
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    stop_gain: f64,
    beta_eval_window_size: i32,
    beta_eval_freq: i32,
    beta_sample_freq: Duration,
    state: MovingState,
    data_table: LinearModelTable<DataRow>,
    pub right_pair: String,
    pub left_pair: String,
    metrics: Arc<NaiveStrategyMetrics>,
    last_row_process_time: DateTime<Utc>,
    last_sample_time: DateTime<Utc>,
    last_row_time_at_eval: DateTime<Utc>,
    last_left: Option<BookPosition>,
    last_right: Option<BookPosition>,
}

impl NaiveTradingStrategy {
    pub fn new(db_path: &str, fees_rate: f64, n: &Options, om: Addr<OrderManager>) -> Self {
        let metrics =
            NaiveStrategyMetrics::for_strat(prometheus::default_registry(), &n.left, &n.right);
        let strat_db_path = format!("{}/naive_pair_trading_{}_{}", db_path, n.left, n.right);
        let db_name = format!("{}_{}", n.left, n.right);
        let db = Db::new(&strat_db_path, db_name);
        Self {
            fees_rate,
            res_threshold_long: n.threshold_long,
            res_threshold_short: n.threshold_short,
            stop_loss: n.stop_loss,
            stop_gain: n.stop_gain,
            beta_eval_window_size: n.window_size,
            beta_eval_freq: n.beta_eval_freq,
            state: MovingState::new(n.initial_cap, db, om, n.dry_mode()),
            data_table: Self::make_lm_table(
                &n.left,
                &n.right,
                &strat_db_path,
                n.window_size as usize,
            ),
            right_pair: n.right.to_string(),
            left_pair: n.left.to_string(),
            last_row_process_time: Utc.timestamp_millis(0),
            last_sample_time: Utc.timestamp_millis(0),
            last_row_time_at_eval: Utc.timestamp_millis(0),
            metrics: Arc::new(metrics),
            last_left: None,
            last_right: None,
            beta_sample_freq: n.beta_sample_freq(),
        }
    }

    pub fn make_lm_table(
        left_pair: &str,
        right_pair: &str,
        db_path: &str,
        window_size: usize,
    ) -> LinearModelTable<DataRow> {
        LinearModelTable::new(
            &format!("{}_{}", left_pair, right_pair),
            db_path,
            window_size,
            Box::new(covar_model::beta),
            Box::new(covar_model::alpha),
        )
    }

    fn maybe_log_stop_loss(&self, pk: PositionKind) {
        if self.should_stop(&pk) {
            let ret = self.return_value(&pk);
            let expr = if ret > self.stop_gain {
                "gain"
            } else if ret < self.stop_loss {
                "loss"
            } else {
                "n/a"
            };
            info!(
                "---- Stop-{} executed ({} position) ----",
                expr,
                match pk {
                    PositionKind::SHORT => "short",
                    PositionKind::LONG => "long",
                },
            )
        }
    }

    fn should_eval(&self, event_time: DateTime<Utc>) -> bool {
        let model_time = self.last_row_time_at_eval;
        let mt_obsolescence = if self.state.beta_lr() < 0.0 {
            // When beta is negative the evaluation frequency is ten times lower
            model_time.add(self.beta_sample_freq.mul(self.beta_eval_freq / 10))
        } else {
            // Model obsolescence is defined here as event time being greater than the sample window
            model_time.add(self.beta_sample_freq.mul(self.beta_eval_freq))
        };
        let is_model_obsolete = event_time.ge(&mt_obsolescence);
        if is_model_obsolete && log_enabled!(Debug) {
            debug!(
                "model obsolete, eval time reached : {} > {} with model_time = {}, beta_val = {}",
                event_time,
                mt_obsolescence,
                model_time,
                self.state.beta_lr()
            );
        }
        is_model_obsolete
    }

    fn set_model_from_table(&mut self) {
        let lmb = self.data_table.model();
        lmb.map(|lm| {
            self.state.set_beta(lm.beta);
            self.state.set_alpha(lm.alpha);
        });
    }

    fn eval_linear_model(&mut self) {
        if let Err(e) = self.data_table.update_model() {
            error!("Error saving model : {:?}", e);
        }
        self.set_model_from_table();
        self.last_row_time_at_eval = self.last_sample_time;
    }

    fn predict(&self, bp: &BookPosition) -> f64 {
        self.data_table
            .predict(self.state.alpha(), self.state.beta(), bp)
    }

    fn update_spread(&mut self, row: &DataRow) {
        self.set_long_spread(row.right.ask);
        self.set_short_spread(row.left.ask);
    }

    fn set_long_spread(&mut self, traded_price: f64) {
        self.state.set_units_to_buy_long_spread(
            self.state.value_strat() / (traded_price * (1.0 + self.fees_rate)),
        );
    }

    fn set_short_spread(&mut self, traded_price: f64) {
        self.state.set_units_to_buy_short_spread(
            self.state.value_strat() / (traded_price * self.state.beta() * (1.0 + self.fees_rate)),
        );
    }

    fn short_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::SHORT,
            right_price,
            left_price,
            time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn long_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::LONG,
            right_price,
            left_price,
            time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn should_stop(&self, pk: &PositionKind) -> bool {
        let ret = self.return_value(pk);
        ret > self.stop_gain || ret < self.stop_loss
    }

    fn return_value(&self, pk: &PositionKind) -> f64 {
        match pk {
            PositionKind::SHORT => self.state.short_position_return(),
            PositionKind::LONG => self.state.long_position_return(),
        }
    }

    async fn eval_latest(&mut self, lr: &DataRow) {
        if self.state.no_position_taken() {
            if self.should_eval(lr.time) {
                self.eval_linear_model();
            }
            self.update_spread(lr);
        }

        self.state.set_beta_lr();
        self.state.set_predicted_right(self.predict(&lr.left));
        self.state
            .set_res((lr.right.mid - self.state.predicted_right()) / lr.right.mid);

        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        if self
            .state
            .resolve_pending_operations(&lr.left, &lr.right)
            .await
            .is_err()
        {
            return;
        }

        if self.state.beta_lr() <= 0.0 {
            return;
        }

        // Possibly open a short position
        if (self.state.res() > self.res_threshold_short) && self.state.no_position_taken() {
            let position = self.short_position(lr.right.bid, lr.left.ask, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a short position
        if self.state.is_short() {
            self.state
                .set_short_position_return(self.fees_rate, lr.right.ask, lr.left.bid);
            if (self.state.res() <= self.res_threshold_short && self.state.res() < 0.0)
                || self.should_stop(&PositionKind::SHORT)
            {
                self.maybe_log_stop_loss(PositionKind::SHORT);
                let position = self.short_position(lr.right.ask, lr.left.bid, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                self.metrics.log_position(&op.pos, &op.kind);
                self.eval_linear_model();
            }
        }

        // Possibly open a long position
        if self.state.res() <= self.res_threshold_long && self.state.no_position_taken() {
            let position = self.long_position(lr.right.ask, lr.left.bid, lr.time);
            let op = self.state.open(position, self.fees_rate).await;
            self.metrics.log_position(&op.pos, &op.kind);
        }

        // Possibly close a long position
        if self.state.is_long() {
            self.state
                .set_long_position_return(self.fees_rate, lr.right.bid, lr.left.ask);
            if (self.state.res() >= self.res_threshold_long && self.state.res() > 0.0)
                || self.should_stop(&PositionKind::LONG)
            {
                self.maybe_log_stop_loss(PositionKind::LONG);
                let position = self.long_position(lr.right.bid, lr.left.ask, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                self.metrics.log_position(&op.pos, &op.kind);
                self.eval_linear_model();
            }
        }
    }

    fn can_eval(&self) -> bool {
        let has_model = self.data_table.has_model();
        let has_position = !self.state.no_position_taken();
        let is_model_obsolete = self
            .data_table
            .model()
            .map(|m| {
                m.at.gt(&Utc::now().sub(
                    self.beta_sample_freq
                        .mul((self.beta_eval_freq as f64 * (1.0 + LM_AGE_CUTOFF_RATIO)) as i32),
                ))
            })
            .unwrap_or(false);
        has_model && (has_position || is_model_obsolete)
    }

    async fn process_row(&mut self, row: &DataRow) {
        if self.data_table.try_loading_model() {
            self.set_model_from_table();
            self.last_row_time_at_eval = self
                .data_table
                .last_model_time()
                .unwrap_or_else(|| Utc.timestamp_millis(0));
        }
        let time = self.last_sample_time.add(self.beta_sample_freq);
        let should_sample = row.time.gt(&time) || row.time == time;
        // A model is available
        let can_eval = self.can_eval();
        if should_sample {
            self.last_sample_time = row.time;
        }
        if can_eval {
            self.eval_latest(row).await;
            self.log_state();
        }
        self.metrics.log_row(&row);
        if should_sample {
            self.data_table.push(row);
        }

        // No model and there are enough samples
        if !can_eval && self.data_table.len() >= self.beta_eval_window_size as usize {
            self.eval_linear_model();
            self.update_spread(row);
        }
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
}

#[async_trait]
impl StrategyInterface for NaiveTradingStrategy {
    async fn add_event(&mut self, le: LiveEvent) -> anyhow::Result<()> {
        if let LiveEvent::LiveOrderbook(ob) = le {
            let string = ob.pair.clone();
            if string == self.left_pair {
                self.last_left = ob.try_into().ok();
            } else if string == self.right_pair {
                self.last_right = ob.try_into().ok();
            }
        }
        let now = Utc::now();
        if now.gt(&self
            .last_row_process_time
            .add(chrono::Duration::milliseconds(200)))
        {
            if let (Some(l), Some(r)) = (self.last_left.clone(), self.last_right.clone()) {
                let x = DataRow {
                    left: l,
                    right: r,
                    time: now,
                };
                self.process_row(&x).await;
                self.last_row_process_time = now;
            }
        }
        Ok(())
    }

    fn data(&mut self, q: DataQuery) -> Option<DataResult> {
        match q {
            DataQuery::Operations => Some(DataResult::NaiveOperations(self.get_operations())),
            DataQuery::Dump => Some(DataResult::Dump(self.dump_db())),
            DataQuery::CurrentOperation => {
                Some(DataResult::NaiveOperation(self.get_ongoing_op().clone()))
            }
            DataQuery::CancelOngoingOp => Some(DataResult::OngongOperationCancelation(
                self.cancel_ongoing_op(),
            )),
        }
    }

    fn mutate(&mut self, m: FieldMutation) -> Result<()> {
        self.change_state(m.field, m.value)
    }
}
