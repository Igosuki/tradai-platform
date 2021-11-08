use std::collections::HashSet;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use actix::Addr;
use chrono::{DateTime, Duration, TimeZone, Utc};

use coinnect_rt::margin_interest_rates::MarginInterestRateProvider;
use coinnect_rt::prelude::*;
use db::{get_or_create, DbOptions};
#[cfg(test)]
use math::indicators::macd_apo::MACDApo;

use crate::driver::StrategyDriver;
use crate::error::Result;
use crate::generic::{InputEvent, Strategy, TradeSignal};
use crate::mean_reverting::metrics::MeanRevertingStrategyMetrics;
use crate::mean_reverting::model::MeanRevertingModel;
use crate::mean_reverting::options::Options;
use crate::mean_reverting::state::{MeanRevertingState, Operation, Position};
use crate::models::io::IterativeModel;
use crate::models::Sampler;
use crate::order_manager::OrderManager;
use crate::query::{DataQuery, DataResult, ModelReset, MutableField, Mutation};
use crate::trading_util::Stopper;
use crate::types::{BookPosition, PositionKind};
use crate::{Channel, StrategyStatus};

mod metrics;
pub mod model;
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
    model: MeanRevertingModel,
    #[derivative(Debug = "ignore")]
    metrics: Arc<MeanRevertingStrategyMetrics>,
    threshold_eval_freq: Option<i32>,
    threshold_short_0: f64,
    threshold_long_0: f64,
    last_threshold_time: DateTime<Utc>,
    stopper: Stopper<f64>,
    sampler: Sampler,
    last_book_pos: Option<BookPosition>,
}

static MEAN_REVERTING_DB_KEY: &str = "mean_reverting";

impl MeanRevertingStrategy {
    pub fn new<S: AsRef<Path>>(
        db_opts: &DbOptions<S>,
        fees_rate: f64,
        n: &Options,
        om: Addr<OrderManager>,
        mirp: Addr<MarginInterestRateProvider>,
    ) -> Self {
        let metrics = MeanRevertingStrategyMetrics::for_strat(prometheus::default_registry(), &n.pair);
        let strat_db_path = format!("{}_{}.{}", MEAN_REVERTING_DB_KEY, n.exchange, n.pair);
        let db = get_or_create(db_opts, strat_db_path, vec![]);
        let state = MeanRevertingState::new(n, fees_rate, db.clone(), om, mirp).unwrap();
        let model = MeanRevertingModel::new(n, db.clone());
        let mut strat = Self {
            exchange: n.exchange,
            pair: n.pair.clone(),
            fees_rate,
            sample_freq: n.sample_freq(),
            last_sample_time: Utc.timestamp_millis(0),
            state,
            model,
            threshold_eval_freq: n.threshold_eval_freq,
            threshold_short_0: n.threshold_short,
            threshold_long_0: n.threshold_long,
            last_threshold_time: Utc.timestamp_millis(0),
            stopper: Stopper::new(n.stop_gain, n.stop_loss),
            metrics: Arc::new(metrics),
            sampler: Sampler::new(n.sample_freq(), Utc.timestamp_millis(0)),
            last_book_pos: None,
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

    #[cfg(test)]
    #[allow(dead_code)]
    fn model_value(&self) -> Option<MACDApo> { self.model.apo_value() }

    fn log_state(&self) { self.metrics.log_state(&self.state); }

    fn get_operations(&self) -> Vec<Operation> { self.state.get_operations() }

    fn get_ongoing_op(&self) -> Option<&Operation> { self.state.ongoing_op() }

    fn cancel_ongoing_op(&mut self) -> Result<bool> { self.state.cancel_ongoing_op() }

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

    #[tracing::instrument(skip(self), level = "trace")]
    async fn eval_latest(&mut self, lr: &BookPosition) -> Result<Option<&Operation>> {
        if self.state.ongoing_op().is_some() {
            return Ok(None);
        }

        if self.state.no_position_taken() {
            self.state.update_units(lr);
        }

        let apo = self.model.apo().expect("model required");

        // Possibly open a short position
        let thresholds = self.thresholds();
        let threshold_short = thresholds.0;
        let threshold_long = thresholds.1;
        if (apo > threshold_short) && self.state.no_position_taken() {
            info!("Entering short position with threshold {}", threshold_short);
            let position = self.short_position(lr.bid, lr.event_time);
            self.state.open(position).await?;
        }
        // Possibly close a short position
        else if self.state.is_short() {
            self.state.set_position_return(lr.ask).await?;
            if (apo < 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.short_position(lr.ask, lr.event_time);
                self.state.close(position).await?;
            }
        }
        // Possibly open a long position
        else if (apo < threshold_long) && self.state.no_position_taken() {
            info!("Entering long position with threshold {}", threshold_long);
            let position = self.long_position(lr.ask, lr.event_time);
            self.state.open(position).await?;
        }
        // Possibly close a long position
        else if self.state.is_long() {
            self.state.set_position_return(lr.bid).await?;
            if (apo > 0.0) || self.stopper.should_stop(self.state.position_return()) {
                let position = self.long_position(lr.bid, lr.event_time);
                self.state.close(position).await?;
            }
        }
        Ok(self.state.ongoing_op())
    }

    fn can_eval(&self) -> bool { self.model.is_loaded() }

    async fn process_row(&mut self, row: &SinglePosRow) {
        // A model is available
        if let Err(e) = self.update_model(&InputEvent::BookPosition(row.pos.clone())).await {
            self.metrics.log_error(e.short_name());
            return;
        }
        self.last_book_pos = Some(row.pos.clone());
        if self.can_eval() {
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

    fn thresholds(&self) -> (f64, f64) {
        self.model
            .thresholds()
            .unwrap_or((self.threshold_short_0, self.threshold_long_0))
    }
}

#[async_trait]
impl StrategyDriver for MeanRevertingStrategy {
    #[tracing::instrument(skip(self, le), level = "trace")]
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

    fn data(&mut self, q: DataQuery) -> Result<DataResult> {
        match q {
            DataQuery::OperationHistory => Ok(DataResult::MeanRevertingOperations(self.get_operations())),
            DataQuery::OpenOperations => Ok(DataResult::MeanRevertingOperation(Box::new(
                self.get_ongoing_op().cloned(),
            ))),
            DataQuery::CancelOngoingOp => Ok(DataResult::Success(self.cancel_ongoing_op()?)),
            DataQuery::State => Ok(DataResult::State(serde_json::to_string(&self.state.vars).unwrap())),
            DataQuery::Status => Ok(DataResult::Status(self.status())),
            DataQuery::Models => Ok(DataResult::Models(self.model.values())),
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

    fn stop_trading(&mut self) { self.state.stop_trading().unwrap(); }

    fn resume_trading(&mut self) { self.state.resume_trading().unwrap(); }

    async fn resolve_orders(&mut self) {
        // If a position is taken, resolve pending operations
        // In case of error return immediately as no trades can be made until the position is resolved
        if let Some(operation) = self.state.ongoing_op().cloned() {
            match self.state.resolve_pending_operations(&operation).await {
                Ok(resolution) => {
                    self.metrics.log_error(resolution.as_ref());
                    trace!("pending operation resolution {}", resolution.as_ref());
                }
                Err(e) => {
                    self.metrics.log_error(e.short_name());
                    trace!("pending operation resolution error {}", e.short_name());
                }
            }
            if self.state.ongoing_op().is_none() && self.state.is_trading() {
                if let Some(bp) = self.last_book_pos.clone() {
                    if let Err(e) = self.eval_latest(&bp).await {
                        self.metrics.log_error(e.short_name());
                    }
                }
            }
        }
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

    #[tracing::instrument(skip(self), level = "trace")]
    async fn update_model(&mut self, e: &crate::generic::InputEvent) -> Result<()> { self.model.next_model(e) }

    fn models(&self) -> Vec<(String, Option<serde_json::Value>)> { self.model.values() }

    fn channels(&self) -> HashSet<Channel> {
        let mut hs = HashSet::new();
        hs.extend((self as &dyn StrategyDriver).channels());
        hs
    }
}
