use std::convert::TryInto;
use std::ops::{Add, Mul, Sub};
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};

use coinnect_rt::prelude::*;
use db::{get_or_create, DbOptions, Storage};
use options::Options;
use state::{MovingState, Position};
use trading::book::BookPosition;
use trading::order_manager::OrderExecutor;
use trading::position::PositionKind;

use crate::driver::StrategyDriver;
use crate::error::*;
use crate::models::{Model, WindowedModel};
use crate::naive_pair_trading::covar_model::{DualBookPosition, LinearModelValue};
use crate::naive_pair_trading::state::Operation;
use crate::query::{ModelReset, MutableField, Mutation, StrategyIndicators};
use crate::{Channel, DataQuery, DataResult, StrategyStatus};

use self::metrics::NaiveStrategyMetrics;

pub mod covar_model;
pub mod metrics;
pub mod options;
pub mod state;

#[cfg(test)]
mod tests;

const LM_AGE_CUTOFF_RATIO: f64 = 0.0013;

pub struct NaiveTradingStrategy {
    key: String,
    exchange: Exchange,
    fees_rate: f64,
    res_threshold_long: f64,
    res_threshold_short: f64,
    stop_loss: f64,
    stop_gain: f64,
    beta_eval_freq: i32,
    beta_sample_freq: Duration,
    state: MovingState,
    data_table: WindowedModel<DualBookPosition, LinearModelValue>,
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
    pub fn new<S: AsRef<Path>>(
        db_opts: &DbOptions<S>,
        fees_rate: f64,
        n: &Options,
        om: Arc<dyn OrderExecutor>,
    ) -> Self {
        let metrics = NaiveStrategyMetrics::for_strat(prometheus::default_registry(), &n.left, &n.right);
        let strat_key = format!("naive_pair_trading_{}_{}", n.left, n.right);
        let db = get_or_create(db_opts, strat_key.clone(), vec![]);
        let mut strat = Self {
            key: strat_key,
            exchange: n.exchange,
            fees_rate,
            res_threshold_long: n.threshold_long,
            res_threshold_short: n.threshold_short,
            stop_loss: n.stop_loss,
            stop_gain: n.stop_gain,
            beta_eval_freq: n.beta_eval_freq,
            state: MovingState::new(n, db.clone(), om),
            data_table: Self::make_lm_table(&n.left, &n.right, db, n.window_size as usize),
            right_pair: n.right.to_string(),
            left_pair: n.left.to_string(),
            last_row_process_time: Utc.timestamp_millis(0),
            last_sample_time: Utc.timestamp_millis(0),
            last_row_time_at_eval: Utc.timestamp_millis(0),
            metrics: Arc::new(metrics),
            last_left: None,
            last_right: None,
            beta_sample_freq: n.beta_sample_freq(),
        };
        if let Err(e) = strat.load() {
            error!("{}", e);
            panic!("Could not load models");
        }
        strat
    }

    pub fn load(&mut self) -> crate::error::Result<()> {
        self.data_table.try_load()?;
        self.set_model_from_table();
        self.last_row_time_at_eval = self
            .data_table
            .last_model_time()
            .unwrap_or_else(|| Utc.timestamp_millis(0));
        debug!("Loaded model time at {}", self.last_row_time_at_eval);
        if !self.models_loaded() {
            Err(crate::error::Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn models_loaded(&self) -> bool { self.data_table.is_loaded() }

    pub fn make_lm_table(
        left_pair: &str,
        right_pair: &str,
        db: Arc<dyn Storage>,
        window_size: usize,
    ) -> WindowedModel<DualBookPosition, LinearModelValue> {
        WindowedModel::new(
            &format!("{}_{}", left_pair, right_pair),
            db,
            window_size,
            None,
            covar_model::linear_model,
            None,
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
            info!("---- Stop-{} executed ({} position) ----", expr, match pk {
                PositionKind::Short => "short",
                PositionKind::Long => "long",
            },)
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
        if is_model_obsolete {
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
        if let Some(lm) = self.data_table.model() {
            self.state.set_beta(lm.value.beta);
            self.state.set_alpha(lm.value.alpha);
        }
    }

    fn eval_linear_model(&mut self) {
        if let Err(e) = self.data_table.update_model() {
            error!("Error saving model : {:?}", e);
        }
        self.set_model_from_table();
        self.last_row_time_at_eval = self.last_sample_time;
    }

    fn predict(&self, bp: &BookPosition) -> f64 { covar_model::predict(self.state.alpha(), self.state.beta(), bp.mid) }

    fn short_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::Short,
            right_price,
            left_price,
            time,
            right_pair: self.right_pair.to_string(),
            left_pair: self.left_pair.to_string(),
        }
    }

    fn long_position(&self, right_price: f64, left_price: f64, time: DateTime<Utc>) -> Position {
        Position {
            kind: PositionKind::Long,
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
            PositionKind::Short => self.state.short_position_return(),
            PositionKind::Long => self.state.long_position_return(),
        }
    }

    async fn eval_latest(&mut self, lr: &DualBookPosition) {
        if self.state.ongoing_op().is_some() {
            return;
        }

        if self.state.no_position_taken() {
            if self.should_eval(lr.time) {
                self.eval_linear_model();
            }
            self.state.update_spread(lr);
        }

        self.state.set_beta_lr();
        self.state.set_predicted_right(self.predict(&lr.left));
        self.state
            .set_res((lr.right.mid - self.state.predicted_right()) / lr.right.mid);

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
                || self.should_stop(&PositionKind::Short)
            {
                self.maybe_log_stop_loss(PositionKind::Short);
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
                || self.should_stop(&PositionKind::Long)
            {
                self.maybe_log_stop_loss(PositionKind::Long);
                let position = self.long_position(lr.right.bid, lr.left.ask, lr.time);
                let op = self.state.close(position, self.fees_rate).await;
                self.metrics.log_position(&op.pos, &op.kind);
                self.eval_linear_model();
            }
        }
    }

    fn can_eval(&self) -> bool {
        let has_model = self.data_table.has_model() && self.models_loaded();
        let has_position = !self.state.no_position_taken();
        // Check that model is more recent than sample freq * (eval frequency + CUTOFF_RATIO%)
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

    #[tracing::instrument(skip(self), level = "trace")]
    async fn process_row(&mut self, row: &DualBookPosition) {
        let time = self.last_sample_time.add(self.beta_sample_freq);
        let should_sample = row.time.ge(&time);
        // A model is available
        let mut can_eval = self.can_eval();
        if should_sample {
            self.last_sample_time = row.time;
        }
        // No model and there are enough samples
        if !can_eval && self.data_table.is_filled() {
            self.eval_linear_model();
            self.state.update_spread(row);
            can_eval = self.can_eval();
        }
        if can_eval {
            self.eval_latest(row).await;
            self.log_state();
        }
        self.metrics.log_row(row);
        if should_sample {
            debug!("Sample at {} ({} rows)", row.time, self.data_table.len());
            self.data_table.push(row);
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn model_value(&self) -> Option<LinearModelValue> { self.data_table.model().map(|m| m.value) }

    fn log_state(&self) { self.metrics.log_state(&self.state); }

    fn get_operations(&self) -> Vec<Operation> { self.state.get_operations() }

    fn get_ongoing_op(&self) -> Option<&Operation> { self.state.ongoing_op() }

    fn cancel_ongoing_op(&mut self) -> bool { self.state.cancel_ongoing_op() }

    fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> { self.state.change_state(field, v) }
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
        let now = Utc::now();
        if now.gt(&self.last_row_process_time.add(chrono::Duration::milliseconds(200))) {
            if let (Some(l), Some(r)) = (self.last_left.clone(), self.last_right.clone()) {
                let x = DualBookPosition {
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

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::OperationHistory => Ok(DataResult::NaiveOperations(self.get_operations())),
            DataQuery::OpenOperations => Ok(DataResult::NaiveOperation(Box::new(self.get_ongoing_op().cloned()))),
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(self.cancel_ongoing_op())),
            DataQuery::State => Ok(DataResult::State(serde_json::to_string(&self.state).unwrap())),
            DataQuery::Status => Ok(DataResult::Status(StrategyStatus::Running)),
            DataQuery::Models => Err(Error::FeatureNotImplemented),
            DataQuery::Indicators => Ok(DataResult::Indicators(StrategyIndicators::default())),
        }
    }

    fn mutate(&mut self, m: Mutation) -> Result<()> {
        match m {
            Mutation::State(m) => self.change_state(m.field, m.value)?,
            Mutation::Model(ModelReset { name, .. }) => {
                if name == Some("lm".to_string()) || name.is_none() {
                    self.data_table.wipe()?
                }
            }
        }
        Ok(())
    }

    fn channels(&self) -> Vec<Channel> {
        vec![
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.left_pair.clone().into(),
            },
            Channel::Orderbooks {
                xch: self.exchange,
                pair: self.right_pair.clone().into(),
            },
        ]
    }

    fn stop_trading(&mut self) { self.state.stop_trading(); }

    fn resume_trading(&mut self) { self.state.resume_trading(); }

    async fn resolve_orders(&mut self) {
        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        if let Some(operation) = self.state.ongoing_op().cloned() {
            match self.state.resolve_pending_operations(&operation).await {
                Ok(resolution) => self.metrics.log_error(resolution.as_ref()),
                Err(e) => self.metrics.log_error(e.short_name()),
            }
            if self.state.ongoing_op().is_none()
                && self.state.is_trading()
                && self.last_right.is_some()
                && self.last_left.is_some()
            {
                self.eval_latest(&DualBookPosition {
                    left: self.last_left.as_ref().unwrap().clone(),
                    right: self.last_right.as_ref().unwrap().clone(),
                    time: Utc::now(),
                })
                .await;
            }
        }
    }
}
