use std::ops::Sub;
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

use crate::error::{Error, Result};
use crate::mean_reverting::options::Options;
use crate::order_manager::{OrderManager, TransactionService};
use crate::order_types::{OrderDetail, Rejection, Transaction, TransactionStatus};
use crate::query::MutableField;
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
        let margin_interest_rate = if asset_type == AssetType::Margin {
            // TODO: fetch the margin interest rate from the exchange
            Some(0.0)
        } else {
            None
        };
        Operation {
            id: Uuid::new_v4().to_string(),
            pos: pos.clone(),
            kind: op_kind,
            transaction: None,
            order_detail: None,
            trade: TradeOperation {
                price: pos.price,
                qty,
                pair: pos.pair,
                kind: trade_kind,
                dry_mode,
                mode: order_mode,
                asset_type,
                margin_interest_rate,
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

pub(crate) static OPERATIONS_KEY: &str = "orders";

pub(crate) static STATE_KEY: &str = "state";

#[derive(Debug, Serialize)]
pub(super) struct MeanRevertingState {
    position: Option<PositionKind>,
    value_strat: f64,
    nominal_position: f64,
    traded_price: f64,
    position_return: f64,
    units_to_buy: f64,
    units_to_sell: f64,
    pnl: f64,
    threshold_short: f64,
    threshold_long: f64,
    #[serde(skip_serializing)]
    db: Arc<dyn Storage>,
    key: String,
    #[serde(skip_serializing)]
    ts: TransactionService,
    ongoing_op: Option<Operation>,
    /// Remote operations are ran dry, meaning no actual action will be performed when possible
    dry_mode: bool,
    order_mode: OrderMode,
    execution_instruction: Option<ExecutionInstruction>,
    order_asset_type: AssetType,
    is_trading: bool,
    fees_rate: f64,
    previous_value_strat: f64,
    last_open_order: Option<OrderDetail>,
    #[serde(skip_serializing)]
    mirp: Addr<MarginInterestRateProvider>,
    exchange: Exchange,
}

#[derive(Serialize, Deserialize)]
struct TransientState {
    value_strat: f64,
    pnl: f64,
    traded_price: f64,
    nominal_position: Option<f64>,
    ongoing_op: Option<String>,
    units_to_buy: f64,
    units_to_sell: f64,
    threshold_short: f64,
    threshold_long: f64,
    previous_value_strat: f64,
}

impl From<&mut MeanRevertingState> for TransientState {
    fn from(trs: &mut MeanRevertingState) -> Self {
        TransientState {
            value_strat: trs.value_strat,
            pnl: trs.pnl,
            nominal_position: Some(trs.nominal_position),
            ongoing_op: trs.ongoing_op.as_ref().map(|o| o.id.clone()),
            units_to_buy: trs.units_to_buy,
            traded_price: trs.traded_price,
            units_to_sell: trs.units_to_sell,
            threshold_short: trs.threshold_short,
            threshold_long: trs.threshold_long,
            previous_value_strat: trs.previous_value_strat,
        }
    }
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
        db.ensure_table(OPERATIONS_KEY).unwrap();
        let mut state = MeanRevertingState {
            exchange: options.exchange,
            position: None,
            value_strat: options.initial_cap,
            nominal_position: 0.0,
            traded_price: 0.0,
            position_return: 0.0,
            units_to_buy: 0.0,
            units_to_sell: 0.0,
            pnl: options.initial_cap,
            threshold_short: options.threshold_short,
            threshold_long: options.threshold_long,
            db,
            key: format!("{}", options.pair),
            ts: TransactionService::new(om),
            ongoing_op: None,
            dry_mode: options.dry_mode(),
            order_mode: options.order_mode.unwrap_or(OrderMode::Limit),
            execution_instruction: options.execution_instruction,
            order_asset_type: options.order_asset_type(),
            is_trading: true,
            fees_rate,
            previous_value_strat: options.initial_cap,
            last_open_order: None,
            mirp,
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let previous_state: Option<TransientState> = self.db.get(STATE_KEY, &self.key).ok();
        if let Some(ps) = previous_state {
            if let Some(id) = &ps.ongoing_op {
                self.ongoing_op = self.get_operation(id);
            }
            self.load_from(ps);
        }
        let ops: Vec<Operation> = self.get_operations();
        let last_unrejected_op = ops.iter().sorted_by(|p1, p2| p2.pos.time.cmp(&p1.pos.time)).find(|o| {
            !matches!(
                o.transaction,
                Some(Transaction {
                    status: TransactionStatus::Rejected(_),
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

    fn load_from(&mut self, ps: TransientState) {
        self.set_units_to_sell(ps.units_to_sell);
        self.set_units_to_buy(ps.units_to_buy);
        self.set_threshold_short(ps.threshold_short);
        self.set_threshold_long(ps.threshold_long);
        self.value_strat = ps.value_strat;
        self.pnl = ps.pnl;
        self.traded_price = ps.traded_price;
        if let Some(np) = ps.nominal_position {
            self.nominal_position = np;
        }
        self.previous_value_strat = ps.previous_value_strat;
    }

    pub(super) fn no_position_taken(&self) -> bool { self.position.is_none() }

    pub(super) fn is_long(&self) -> bool { matches!(self.position, Some(PositionKind::Long)) }

    pub(super) fn is_short(&self) -> bool { matches!(self.position, Some(PositionKind::Short)) }

    pub(super) fn set_position(&mut self, k: PositionKind) { self.position = Some(k); }

    fn set_ongoing_op(&mut self, op: Option<Operation>) { self.ongoing_op = op; }

    pub fn ongoing_op(&self) -> &Option<Operation> { &self.ongoing_op }

    pub(super) fn traded_price(&self) -> f64 { self.traded_price }

    pub(super) fn set_pnl(&mut self) { self.pnl = self.value_strat; }

    pub(super) fn pnl(&self) -> f64 { self.pnl }

    pub(super) fn value_strat(&self) -> f64 { self.value_strat }

    pub(super) fn previous_value_strat(&self) -> f64 { self.previous_value_strat }

    #[allow(dead_code)]
    pub(super) fn nominal_position(&self) -> f64 { self.nominal_position }

    pub(super) async fn set_position_return(&mut self, current_price: f64) {
        match self.position {
            Some(PositionKind::Long) => {
                self.position_return = self.nominal_position
                    * (current_price * (1.0 - self.fees_rate) - self.traded_price)
                    / (self.nominal_position * self.traded_price)
            }
            Some(PositionKind::Short) => {
                let interest_fees = match &self.last_open_order {
                    Some(order) if self.order_asset_type == AssetType::Margin => {
                        let interest_rate = self.get_interest_rate(self.exchange, order.base_asset.clone()).await;
                        let hours_elapsed = Utc::now().sub(order.created_at);
                        interest_rate
                            .map(|ir| ir.resolve(order.borrowed_amount.unwrap(), hours_elapsed.num_hours()))
                            .unwrap_or(0.0)
                    }
                    _ => 0.0,
                };
                self.position_return = (self.nominal_position
                    * (self.traded_price - current_price * (1.0 + self.fees_rate))
                    - interest_fees)
                    / (self.nominal_position * self.traded_price)
            }
            _ => {}
        }
    }

    pub(super) fn position_return(&self) -> f64 { self.position_return }

    pub(super) fn update_units(&mut self, bp: &BookPosition) {
        self.units_to_buy = self.value_strat / bp.ask * (1.0 + self.fees_rate);
        self.units_to_sell = self.value_strat / bp.bid * (1.0 - self.fees_rate);
    }

    fn set_units_to_buy(&mut self, v: f64) { self.units_to_buy = v; }

    fn set_units_to_sell(&mut self, v: f64) { self.units_to_sell = v; }

    pub fn set_threshold_short(&mut self, v: f64) { self.threshold_short = v; }

    pub fn set_threshold_long(&mut self, v: f64) { self.threshold_long = v; }

    pub fn threshold_short(&self) -> f64 { self.threshold_short }

    pub fn threshold_long(&self) -> f64 { self.threshold_long }

    pub fn stop_trading(&mut self) { self.is_trading = false; }

    pub fn resume_trading(&mut self) { self.is_trading = true; }

    fn clear_ongoing_operation(&mut self, last_price: f64, cummulative_qty: f64) {
        match self.ongoing_op.clone() {
            Some(o) if o.is_close() => {
                self.update_close_value(self.previous_value_strat, &o.pos.kind, last_price);
                self.set_pnl();
                self.clear_position();
            }
            Some(o) if o.is_open() => {
                self.traded_price = last_price;
                self.nominal_position = cummulative_qty;
                if o.kind == OperationKind::Open {
                    self.last_open_order = o.order_detail;
                }
                self.update_open_value(self.previous_value_strat, &o.pos.kind, last_price);
            }
            _ => {}
        }
        self.set_ongoing_op(None);
        self.save();
    }

    pub fn cancel_ongoing_op(&mut self) -> bool {
        match &self.ongoing_op {
            None => false,
            Some(op) => {
                let mut op = op.clone();
                self.value_strat = self.previous_value_strat;
                if op.is_open() {
                    self.clear_position();
                }
                op.transaction = Some(Transaction {
                    id: op
                        .transaction
                        .as_ref()
                        .map(|tr| tr.id.clone())
                        .unwrap_or_else(|| op.id.clone()),
                    status: TransactionStatus::Rejected(Rejection::Cancelled(Some("canceled by strategy".to_string()))),
                });
                self.save_operation(&op);
                self.ongoing_op = None;
                self.save();
                true
            }
        }
    }

    #[tracing::instrument(skip(self), level = "trace")]
    pub(super) async fn resolve_pending_operations(&mut self, current_bp: &BookPosition) -> Result<()> {
        if self.ongoing_op.is_none() {
            return Ok(());
        }
        let ongoing_op = self.ongoing_op.as_ref().unwrap();
        if ongoing_op.order_detail.is_none() {
            return Err(Error::NoTransactionInOperation);
        }
        let order_detail = ongoing_op.order_detail.as_ref().unwrap();
        let (new_order, resolution) = self.ts.resolve_pending_order(order_detail).await?;
        let mut new_op = ongoing_op.clone();
        new_op.order_detail = Some(new_order.clone());
        let result = match resolution {
            Ok(_) => {
                self.clear_ongoing_operation(new_order.weighted_price, new_order.total_executed_qty);
                Ok(())
            }
            Err(e) => {
                if matches!(e, Error::OperationRestaged) {
                    let current_price = self.new_price(current_bp, &ongoing_op.kind)?;
                    new_op.trade.with_new_price(current_price);
                    if let Ok(order_detail) = self.ts.stage_trade(&new_op.trade).await {
                        new_op.order_detail = Some(order_detail);
                    }
                }
                if matches!(e, Error::OperationBadRequest) {
                    self.stop_trading();
                }
                self.set_ongoing_op(Some(new_op.clone()));
                Err(e)
            }
        };
        self.save_operation(&new_op);
        result
    }

    fn update_nominal_position(&mut self, position_kind: &PositionKind) {
        match position_kind {
            PositionKind::Short => {
                self.nominal_position = self.units_to_sell;
            }
            PositionKind::Long => {
                self.nominal_position = self.units_to_buy;
            }
        }
    }

    fn update_open_value(&mut self, previous_value_strat: f64, kind: &PositionKind, price: f64) {
        match kind {
            PositionKind::Short => {
                self.value_strat = previous_value_strat + self.nominal_position * price;
            }
            PositionKind::Long => {
                self.value_strat = previous_value_strat - self.nominal_position * price * (1.0 + self.fees_rate);
            }
        }
    }

    fn update_close_value(&mut self, previous_value_strat: f64, kind: &PositionKind, price: f64) {
        match kind {
            PositionKind::Short => {
                self.value_strat = previous_value_strat - self.nominal_position * price;
            }
            PositionKind::Long => {
                self.value_strat = previous_value_strat + self.nominal_position * price * (1.0 - self.fees_rate);
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn open(&mut self, pos: Position) -> Result<Operation> {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.traded_price = pos.price;
        self.previous_value_strat = self.value_strat;
        self.update_nominal_position(&position_kind);
        self.update_open_value(self.value_strat, &position_kind, pos.price);
        let mut op = Operation::new(
            pos,
            OperationKind::Open,
            self.nominal_position,
            self.dry_mode,
            self.order_mode,
            self.order_asset_type,
        );
        self.stage_operation(&mut op).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn close(&mut self, pos: Position) -> Result<Operation> {
        self.previous_value_strat = self.value_strat;
        self.update_close_value(self.value_strat, &pos.kind, pos.price);
        let mut op = Operation::new(
            pos,
            OperationKind::Close,
            self.nominal_position,
            self.dry_mode,
            self.order_mode,
            self.order_asset_type,
        );
        self.stage_operation(&mut op).await
    }

    fn clear_position(&mut self) {
        self.last_open_order = None;
        self.position = None;
        self.position_return = 0.0;
        self.nominal_position = 0.0;
    }

    fn save_operation(&mut self, op: &Operation) {
        let save = op.clone();
        if let Err(e) = self.db.put(OPERATIONS_KEY, &op.id, save) {
            error!("Error saving operation: {:?}", e);
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stage_operation(&mut self, op: &mut Operation) -> Result<Operation> {
        self.save_operation(op);
        self.set_ongoing_op(Some(op.clone()));
        self.ts
            .stage_trade(&op.trade)
            .await
            .map(|order| {
                op.order_detail = Some(order);
                self.save_operation(op);
                self.set_ongoing_op(Some(op.clone()));
                op.log();
                self.save();
                self.log_indicators();
                op.clone()
            })
            .map_err(|e| {
                self.cancel_ongoing_op();
                e
            })
    }

    fn save(&mut self) {
        let ts: TransientState = self.into();
        if let Err(e) = self.db.put(STATE_KEY, &self.key, ts) {
            error!("Error saving state: {:?}", e);
        }
    }

    pub fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.value_strat = v,
            MutableField::NominalPosition => self.nominal_position = v,
            MutableField::Pnl => self.pnl = v,
            MutableField::PreviousValueStrat => self.previous_value_strat = v,
        }
        self.save();
        Ok(())
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        self.db
            .get_all::<Operation>(OPERATIONS_KEY)
            .map(|v| v.into_iter().map(|kv| kv.1).collect())
            .unwrap_or_else(|_| Vec::new())
    }

    pub(crate) fn is_trading(&self) -> bool { self.is_trading }

    #[allow(dead_code)]
    fn get_operation(&self, uuid: &str) -> Option<Operation> { self.db.get(OPERATIONS_KEY, uuid).ok() }

    fn log_indicators(&mut self) {
        if log_enabled!(Debug) {
            let s: TransientState = self.into();
            debug!("{}", serde_json::to_string(&s).unwrap());
        }
    }

    fn new_price(&self, bp: &BookPosition, operation_kind: &OperationKind) -> Result<f64> {
        match (&self.position, operation_kind) {
            (Some(PositionKind::Short), OperationKind::Open) | (Some(PositionKind::Long), OperationKind::Close) => {
                Ok(bp.bid)
            }
            (Some(PositionKind::Short), OperationKind::Close) | (Some(PositionKind::Long), OperationKind::Open) => {
                Ok(bp.ask)
            }
            _ => {
                // Return early as there is nothing to be done, this should never happen
                Err(Error::InvalidPosition)
            }
        }
    }

    async fn get_interest_rate(&self, exchange: Exchange, asset: String) -> Option<InterestRate> {
        self.mirp
            .send(GetInterestRate { asset, exchange })
            .await
            .ok()
            .and_then(|o| o)
    }
}
