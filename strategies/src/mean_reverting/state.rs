use std::panic;
use std::sync::Arc;

use actix::Addr;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::Level::Debug;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::margin_interest_rates::{GetInterestRate, MarginInterestRateProvider};
use coinnect_rt::types::{AssetType, InterestRate};
use db::{Storage, StorageExt};
use ext::ResultExt;

use crate::error::{Error, Result};
use crate::mean_reverting::options::Options;
use crate::order_manager::types::{OrderDetail, OrderStatus, Rejection, Transaction, TransactionStatus};
use crate::order_manager::{OrderManager, OrderResolution, TransactionService};
use crate::query::MutableField;
use crate::repos::OperationsRepository;
use crate::types::{BookPosition, ExecutionInstruction, OperationEvent, OrderMode, StratEvent, TradeEvent,
                   TradeOperation};
use crate::types::{OperationKind, PositionKind, TradeKind};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, juniper::GraphQLObject)]
pub struct Position {
    pub kind: PositionKind,
    pub price: f64,
    pub time: DateTime<Utc>,
    pub pair: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Operation {
    pub id: String,
    pub kind: OperationKind,
    pub pos: Position,
    pub transaction: Option<Transaction>,
    pub order_detail: Option<OrderDetail>,
    pub total_interests: Option<f64>,
    pub trade: TradeOperation,
    pub instructions: Option<ExecutionInstruction>,
}

impl Operation {
    fn new(
        pos: Position,
        op_kind: OperationKind,
        qty: f64,
        dry_mode: bool,
        order_mode: OrderMode,
        asset_type: AssetType,
    ) -> Self {
        let trade_kind = match (&pos.kind, &op_kind) {
            (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeKind::Sell,
            (PositionKind::Long, OperationKind::Open) | (PositionKind::Short, OperationKind::Close) => TradeKind::Buy,
        };
        Operation {
            id: Uuid::new_v4().to_string(),
            pos: pos.clone(),
            kind: op_kind,
            transaction: None,
            order_detail: None,
            total_interests: None,
            trade: TradeOperation {
                id: Some(TradeOperation::new_id()),
                price: pos.price,
                qty,
                pair: pos.pair,
                kind: trade_kind,
                dry_mode,
                mode: order_mode,
                asset_type,
            },
            instructions: None,
        }
    }

    fn is_open(&self) -> bool { matches!(self.kind, OperationKind::Open) }

    fn is_close(&self) -> bool { matches!(self.kind, OperationKind::Close) }
}

impl Operation {
    pub fn value(&self) -> f64 { self.trade.qty * self.trade.price }

    pub fn is_resolved(&self) -> bool {
        match &self.transaction {
            Some(trr) => trr.is_filled(),
            _ => false,
        }
    }

    pub fn operation_event(&self) -> OperationEvent {
        OperationEvent {
            op: self.kind,
            pos: self.pos.kind.clone(),
            at: self.pos.time,
        }
    }

    pub fn trade_event(&self) -> TradeEvent {
        TradeEvent {
            op: self.trade.kind.clone(),
            qty: self.trade.qty,
            pair: self.pos.pair.clone(),
            price: self.pos.price,
            strat_value: self.value(),
            at: self.pos.time,
        }
    }

    fn log(&self) {
        StratEvent::Operation(self.operation_event()).log();
        StratEvent::Trade(self.trade_event()).log();
    }
}

pub(crate) static STATE_KEY: &str = "state";

#[derive(Debug, Serialize)]
pub(super) struct MeanRevertingState {
    // Persist
    position: Option<PositionKind>,
    position_return: f64,
    is_trading: bool,
    last_open_order: Option<OrderDetail>,
    ongoing_op: Option<Operation>,
    state_key: String,
    units_to_buy: f64,
    units_to_sell: f64,
    vars: TransientState,
    // Conf
    dry_mode: bool,
    order_mode: OrderMode,
    execution_instruction: Option<ExecutionInstruction>,
    order_asset_type: AssetType,
    fees_rate: f64,
    exchange: Exchange,
    // Access
    #[serde(skip_serializing)]
    db: Arc<dyn Storage>,
    #[serde(skip_serializing)]
    ts: TransactionService,
    #[serde(skip_serializing)]
    mirp: Addr<MarginInterestRateProvider>,
    #[serde(skip_serializing)]
    operations_repo: OperationsRepository,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TransientState {
    pub value_strat: f64,
    pub pnl: f64,
    pub traded_price: f64,
    #[serde(default)]
    pub nominal_position: f64,
    pub ongoing_op: Option<String>,
    pub threshold_short: f64,
    pub threshold_long: f64,
    pub previous_value_strat: f64,
}

impl MeanRevertingState {
    pub fn new(
        options: &Options,
        fees_rate: f64,
        db: Arc<dyn Storage>,
        om: Addr<OrderManager>,
        mirp: Addr<MarginInterestRateProvider>,
    ) -> MeanRevertingState {
        db.ensure_table(STATE_KEY).unwrap();
        let mut state = MeanRevertingState {
            exchange: options.exchange,
            position: None,
            position_return: 0.0,
            units_to_buy: 0.0,
            units_to_sell: 0.0,
            db: db.clone(),
            state_key: format!("{}", options.pair),
            ts: TransactionService::new(om),
            ongoing_op: None,
            dry_mode: options.dry_mode(),
            order_mode: options.order_mode.unwrap_or(OrderMode::Limit),
            execution_instruction: options.execution_instruction,
            order_asset_type: options.order_asset_type(),
            is_trading: true,
            fees_rate,
            last_open_order: None,
            mirp,
            operations_repo: OperationsRepository::new(db),
            vars: TransientState {
                pnl: options.initial_cap,
                value_strat: options.initial_cap,
                threshold_long: options.threshold_long,
                threshold_short: options.threshold_short,
                ..TransientState::default()
            },
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let previous_state: Option<TransientState> = self.db.get(STATE_KEY, &self.state_key).ok();
        if let Some(ps) = previous_state {
            if let Some(id) = ps.ongoing_op.as_ref() {
                self.ongoing_op = self.get_operation(id).ok();
            }
            self.vars = ps;
        }
        let ops: Vec<Operation> = self.get_operations();
        let last_unrejected_op = ops.iter().sorted_by(|p1, p2| p2.pos.time.cmp(&p1.pos.time)).find(|o| {
            !matches!(
                o.order_detail,
                Some(OrderDetail {
                    status: OrderStatus::Rejected,
                    ..
                })
            )
        });
        if let Some(o) = last_unrejected_op {
            if matches!(o.kind, OperationKind::Open) {
                self.set_position(o.pos.kind.clone());
                self.last_open_order = o.order_detail.clone();
            }
        }
    }

    pub(super) fn no_position_taken(&self) -> bool { self.position.is_none() }

    pub(super) fn is_long(&self) -> bool { matches!(self.position, Some(PositionKind::Long)) }

    pub(super) fn is_short(&self) -> bool { matches!(self.position, Some(PositionKind::Short)) }

    pub(super) fn set_position(&mut self, k: PositionKind) { self.position = Some(k); }

    fn set_ongoing_op(&mut self, op: Option<Operation>) {
        self.vars.ongoing_op = op.as_ref().map(|o| o.id.clone());
        self.ongoing_op = op;
    }

    pub fn ongoing_op(&self) -> Option<&Operation> { self.ongoing_op.as_ref() }

    pub(super) fn traded_price(&self) -> f64 { self.vars.traded_price }

    pub(super) fn set_pnl(&mut self) { self.vars.pnl = self.vars.value_strat; }

    pub(super) fn pnl(&self) -> f64 { self.vars.pnl }

    pub(super) fn value_strat(&self) -> f64 { self.vars.value_strat }

    pub(super) fn previous_value_strat(&self) -> f64 { self.vars.previous_value_strat }

    #[allow(dead_code)]
    pub(super) fn nominal_position(&self) -> f64 { self.vars.nominal_position }

    pub(super) async fn interest_fees_since_open(&self) -> Result<f64> {
        Ok(match &self.last_open_order {
            Some(order) if order.asset_type == AssetType::Margin && order.borrowed_amount.is_some() => {
                let interest_rate = self.get_interest_rate(self.exchange, order.base_asset.clone()).await?;
                order.total_interest(interest_rate)
            }
            _ => 0.0,
        })
    }

    pub(super) async fn set_position_return(&mut self, current_price: f64) -> Result<()> {
        match self.position {
            Some(PositionKind::Long) => {
                self.position_return = self.vars.nominal_position
                    * (current_price * (1.0 - self.fees_rate) - self.vars.traded_price)
                    / (self.vars.nominal_position * self.vars.traded_price)
            }
            Some(PositionKind::Short) => {
                let interest_fees = self.interest_fees_since_open().await?;
                self.position_return = (self.vars.nominal_position
                    * (self.vars.traded_price - current_price * (1.0 + self.fees_rate))
                    - interest_fees)
                    / (self.vars.nominal_position * self.vars.traded_price)
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn position_return(&self) -> f64 { self.position_return }

    pub(super) fn update_units(&mut self, bp: &BookPosition) {
        self.set_units_to_buy(self.vars.value_strat / bp.ask * (1.0 + self.fees_rate));
        self.set_units_to_sell(self.vars.value_strat / bp.bid * (1.0 - self.fees_rate));
    }

    fn set_units_to_buy(&mut self, v: f64) { self.units_to_buy = v; }

    fn set_units_to_sell(&mut self, v: f64) { self.units_to_sell = v; }

    pub fn set_threshold_short(&mut self, v: f64) { self.vars.threshold_short = v; }

    pub fn set_threshold_long(&mut self, v: f64) { self.vars.threshold_long = v; }

    pub fn threshold_short(&self) -> f64 { self.vars.threshold_short }

    pub fn threshold_long(&self) -> f64 { self.vars.threshold_long }

    pub fn stop_trading(&mut self) { self.is_trading = false; }

    pub fn resume_trading(&mut self) { self.is_trading = true; }

    async fn clear_ongoing_operation(&mut self, last_price: f64, cummulative_qty: f64) -> Result<()> {
        match self.ongoing_op.clone() {
            Some(o) if o.is_close() => {
                self.update_close_value(self.vars.previous_value_strat, &o.pos.kind, last_price)
                    .await?;
                self.set_pnl();
                self.clear_open_position();
            }
            Some(o) if o.is_open() => {
                self.vars.traded_price = last_price;
                self.vars.nominal_position = cummulative_qty;
                if o.kind == OperationKind::Open {
                    self.last_open_order = o.order_detail;
                }
                self.update_open_value(self.vars.previous_value_strat, &o.pos.kind, last_price);
            }
            _ => {}
        }
        self.set_ongoing_op(None);
        self.save()
    }

    pub fn cancel_ongoing_op(&mut self) -> Result<bool> {
        match &self.ongoing_op {
            None => Ok(false),
            Some(op) => {
                // If there was no transaction, insert a rejection
                if op.transaction.as_ref().is_none() {
                    let mut op = op.clone();
                    op.transaction = Some(Transaction {
                        id: op
                            .transaction
                            .as_ref()
                            .map(|tr| tr.id.clone())
                            .unwrap_or_else(|| op.id.clone()),
                        status: TransactionStatus::Rejected(Rejection::Cancelled(Some("auto".to_string()))),
                    });
                    self.save_operation(&op)?;
                }
                let previous_vars = self.vars.clone();
                if op.is_open() {
                    self.clear_open_position();
                }
                self.vars.value_strat = self.vars.previous_value_strat;
                self.set_ongoing_op(None);
                self.save().map_err(|e| {
                    self.vars = previous_vars;
                    e
                })?;

                Ok(true)
            }
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub(super) async fn resolve_pending_operations(&mut self, ongoing_op: &Operation) -> Result<OrderResolution> {
        if ongoing_op.order_detail.is_none() {
            return Err(Error::NoTransactionInOperation);
        }
        let order_detail = ongoing_op.order_detail.as_ref().unwrap();
        let (new_order, transaction, resolution) = self.ts.resolve_pending_order(order_detail).await?;
        error!("{:?} {:?} {:?}", new_order, transaction, resolution);
        match resolution {
            OrderResolution::Filled => {
                self.clear_ongoing_operation(new_order.weighted_price, new_order.total_executed_qty)
                    .await?;
            }
            OrderResolution::Retryable | OrderResolution::Cancelled => {
                self.cancel_ongoing_op()?;
            }
            OrderResolution::NoChange => {}
            OrderResolution::BadRequest | OrderResolution::Rejected => {
                let mut new_op = ongoing_op.clone();
                new_op.order_detail = Some(new_order.clone());
                new_op.transaction = transaction;
                self.set_ongoing_op(Some(new_op.clone()));
                self.save_operation(&new_op)?;
                self.stop_trading();
            }
        };
        Ok(resolution)
    }

    fn update_nominal_position(&mut self, position_kind: &PositionKind) {
        match position_kind {
            PositionKind::Short => {
                self.vars.nominal_position = self.units_to_sell;
            }
            PositionKind::Long => {
                self.vars.nominal_position = self.units_to_buy;
            }
        }
    }

    fn update_open_value(&mut self, previous_value_strat: f64, kind: &PositionKind, price: f64) {
        match kind {
            PositionKind::Short => {
                self.vars.value_strat = previous_value_strat + self.vars.nominal_position * price;
            }
            PositionKind::Long => {
                self.vars.value_strat =
                    previous_value_strat - self.vars.nominal_position * price * (1.0 + self.fees_rate);
            }
        }
    }

    async fn update_close_value(&mut self, previous_value_strat: f64, kind: &PositionKind, price: f64) -> Result<()> {
        match kind {
            PositionKind::Short => {
                let interest_fees = self.interest_fees_since_open().await?;
                self.vars.value_strat = previous_value_strat - self.vars.nominal_position * price - interest_fees;
                if let Some(mut operation) = self.ongoing_op.as_mut() {
                    operation.total_interests = Some(interest_fees);
                }
            }
            PositionKind::Long => {
                self.vars.value_strat =
                    previous_value_strat + self.vars.nominal_position * price * (1.0 - self.fees_rate);
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn open(&mut self, pos: Position) -> Result<Operation> {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.vars.traded_price = pos.price;
        self.vars.previous_value_strat = self.vars.value_strat;
        self.update_nominal_position(&position_kind);
        self.update_open_value(self.vars.value_strat, &position_kind, pos.price);
        let mut op = Operation::new(
            pos,
            OperationKind::Open,
            self.vars.nominal_position,
            self.dry_mode,
            self.order_mode,
            self.order_asset_type,
        );
        self.stage_operation(&mut op).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn close(&mut self, pos: Position) -> Result<Operation> {
        self.vars.previous_value_strat = self.vars.value_strat;
        self.update_close_value(self.vars.value_strat, &pos.kind, pos.price)
            .await?;
        let mut op = Operation::new(
            pos,
            OperationKind::Close,
            self.vars.nominal_position,
            self.dry_mode,
            self.order_mode,
            self.order_asset_type,
        );
        self.stage_operation(&mut op).await
    }

    fn clear_open_position(&mut self) {
        self.last_open_order = None;
        self.position = None;
        self.position_return = 0.0;
        self.vars.nominal_position = 0.0;
    }

    fn get_operation(&self, id: &str) -> Result<Operation> { self.operations_repo.get(id) }

    fn save_operation(&self, op: &Operation) -> Result<()> { self.operations_repo.put(&op.id, op) }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stage_operation(&mut self, op: &mut Operation) -> Result<Operation> {
        self.save_operation(op)?;
        self.set_ongoing_op(Some(op.clone()));
        self.ts
            .stage_trade(&op.trade)
            .await
            .and_then(|order| {
                op.order_detail = Some(order);
                self.save_operation(op)?;
                self.set_ongoing_op(Some(op.clone()));
                op.log();
                self.save()?;
                self.log_indicators();
                Ok(op.clone())
            })
            .map_err(|e| match self.cancel_ongoing_op() {
                Ok(_) => e,
                Err(e) => e,
            })
    }

    fn save(&mut self) -> Result<()> { self.db.put(STATE_KEY, &self.state_key, &self.vars).err_into() }

    pub fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.vars.value_strat = v,
            MutableField::NominalPosition => self.vars.nominal_position = v,
            MutableField::Pnl => self.vars.pnl = v,
            MutableField::PreviousValueStrat => self.vars.previous_value_strat = v,
        }
        self.save()
    }

    pub fn get_operations(&self) -> Vec<Operation> { self.operations_repo.all() }

    pub(crate) fn is_trading(&self) -> bool { self.is_trading }

    fn log_indicators(&mut self) {
        if log_enabled!(Debug) {
            debug!("{}", serde_json::to_string(&self.vars).unwrap());
        }
    }

    async fn get_interest_rate(&self, exchange: Exchange, asset: String) -> Result<InterestRate> {
        self.mirp
            .send(GetInterestRate { asset, exchange })
            .await
            .map_err(|_| Error::InterestRateProviderMailboxError)?
            .err_into()
    }
}
