use std::panic;

use actix::Addr;
use anyhow::Result;
use chrono::{DateTime, Utc};
use log::Level::Debug;
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

use db::{Storage, StorageExt};

use crate::mean_reverting::options::Options;
use crate::order_manager::{OrderManager, TransactionService};
use crate::order_types::{StagedOrder, Transaction, TransactionStatus};
use crate::query::MutableField;
use crate::types::{BookPosition, OperationEvent, OrderMode, StratEvent, TradeEvent, TradeOperation};
use crate::types::{OperationKind, PositionKind, TradeKind};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct Position {
    pub kind: PositionKind,
    pub price: f64,
    pub time: DateTime<Utc>,
    pub pair: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Operation {
    pub id: String,
    pub kind: OperationKind,
    pub pos: Position,
    pub transaction: Option<Transaction>,
    pub trade: TradeOperation,
}

impl Operation {
    fn new(pos: Position, op_kind: OperationKind, qty: f64, dry_mode: bool) -> Self {
        let trade_kind = match (&pos.kind, &op_kind) {
            (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeKind::Sell,
            (PositionKind::Long, OperationKind::Open) | (PositionKind::Short, OperationKind::Close) => TradeKind::Buy,
        };
        Operation {
            id: Uuid::new_v4().to_string(),
            pos: pos.clone(),
            kind: op_kind,
            transaction: None,
            trade: TradeOperation {
                price: pos.price,
                qty,
                pair: pos.pair,
                kind: trade_kind,
                dry_mode,
            },
        }
    }
}

#[juniper::graphql_object]
impl Operation {
    #[graphql(description = "the kind of operation")]
    pub fn kind(&self) -> &OperationKind { &self.kind }

    #[graphql(description = "the position this operation is based on")]
    pub fn pos(&self) -> &Position { &self.pos }

    #[graphql(description = "the operation of the crypto pair")]
    pub fn trade(&self) -> &TradeOperation { &self.trade }

    #[graphql(description = "value")]
    pub fn value(&self) -> f64 { self.trade.qty * self.trade.price }
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
            op: self.kind.clone(),
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
    apo: f64,
    nominal_position: f64,
    traded_price: f64,
    short_position_return: f64,
    long_position_return: f64,
    units_to_buy: f64,
    units_to_sell: f64,
    pnl: f64,
    threshold_short: f64,
    threshold_long: f64,
    #[serde(skip_serializing)]
    db: Arc<Box<dyn Storage>>,
    key: String,
    #[serde(skip_serializing)]
    ts: TransactionService,
    ongoing_op: Option<Operation>,
    /// Remote operations are ran dry, meaning no actual action will be performed when possible
    dry_mode: bool,
    order_mode: OrderMode,
    is_trading: bool,
    fees_rate: f64,
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
    apo: f64,
}

impl MeanRevertingState {
    pub fn new(
        options: &Options,
        fees_rate: f64,
        db: Arc<Box<dyn Storage>>,
        om: Addr<OrderManager>,
    ) -> MeanRevertingState {
        db.ensure_table(STATE_KEY).unwrap();
        db.ensure_table(OPERATIONS_KEY).unwrap();
        let mut state = MeanRevertingState {
            position: None,
            value_strat: options.initial_cap,
            apo: 0.0,
            nominal_position: 0.0,
            traded_price: 0.0,
            short_position_return: 0.0,
            long_position_return: 0.0,
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
            order_mode: options.order_mode,
            is_trading: true,
            fees_rate,
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let mut ops: Vec<Operation> = self.get_operations();
        ops.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
        if let Some(o) = ops.last() {
            if OperationKind::Open == o.kind {
                self.set_position(o.pos.kind.clone());
            }
        }
        let previous_state: Option<TransientState> = self.db.get(STATE_KEY, &self.key).ok();
        if let Some(ps) = previous_state {
            self.set_units_to_sell(ps.units_to_sell);
            self.set_units_to_buy(ps.units_to_buy);
            self.set_threshold_short(ps.threshold_short);
            self.set_threshold_long(ps.threshold_long);
            self.value_strat = ps.value_strat;
            self.pnl = ps.pnl;
            self.traded_price = ps.traded_price;
            self.apo = ps.apo;
            if let Some(np) = ps.nominal_position {
                self.nominal_position = np;
            }
            if let Some(op_key) = ps.ongoing_op {
                let op: Option<Operation> = self.get_operation(&op_key);
                self.set_ongoing_op(op);
            }
        }
    }

    pub(super) fn no_position_taken(&self) -> bool { self.position.is_none() }

    pub(super) fn is_long(&self) -> bool { self.position.eq(&Some(PositionKind::Long)) }

    pub(super) fn is_short(&self) -> bool { self.position.eq(&Some(PositionKind::Short)) }

    pub(super) fn set_position(&mut self, k: PositionKind) { self.position = Some(k); }

    fn set_ongoing_op(&mut self, op: Option<Operation>) { self.ongoing_op = op; }

    pub fn ongoing_op(&self) -> &Option<Operation> { &self.ongoing_op }

    pub fn cancel_ongoing_op(&mut self) -> bool {
        match self.ongoing_op {
            None => false,
            _ => {
                self.ongoing_op = None;
                self.save();
                true
            }
        }
    }

    pub(super) fn traded_price(&self) -> f64 { self.traded_price }

    pub(super) fn set_pnl(&mut self) { self.pnl = self.value_strat; }

    pub(super) fn pnl(&self) -> f64 { self.pnl }

    pub(super) fn value_strat(&self) -> f64 { self.value_strat }

    pub(super) fn set_apo(&mut self, apo: f64) { self.apo = apo; }

    pub(super) fn apo(&self) -> f64 { self.apo }

    #[allow(dead_code)]
    pub(super) fn nominal_position(&self) -> f64 { self.nominal_position }

    pub(super) fn set_long_position_return(&mut self, current_price: f64) {
        self.long_position_return = (self.nominal_position
            * (current_price * (1.0 - self.fees_rate) - self.traded_price))
            / (self.nominal_position * self.traded_price);
    }

    pub(super) fn set_short_position_return(&mut self, current_price: f64) {
        self.short_position_return = self.nominal_position
            * (self.traded_price - current_price * (1.0 + self.fees_rate))
            / (self.nominal_position * self.traded_price);
    }

    pub(super) fn short_position_return(&self) -> f64 { self.short_position_return }

    pub(super) fn long_position_return(&self) -> f64 { self.long_position_return }

    pub(super) fn update_units(&mut self, bp: &BookPosition) {
        self.units_to_buy = self.value_strat / bp.ask * (1.0 + self.fees_rate);
        self.units_to_sell = self.value_strat / bp.bid * (1.0 - self.fees_rate);
    }

    fn set_units_to_buy(&mut self, v: f64) { self.units_to_buy = v; }

    fn set_units_to_sell(&mut self, v: f64) { self.units_to_sell = v; }

    fn set_nominal_position(&mut self, v: f64) { self.nominal_position = v; }

    pub fn set_threshold_short(&mut self, v: f64) { self.threshold_short = v; }

    pub fn set_threshold_long(&mut self, v: f64) { self.threshold_long = v; }

    pub fn threshold_short(&self) -> f64 { self.threshold_short }

    pub fn threshold_long(&self) -> f64 { self.threshold_long }

    fn clear_ongoing_operation(&mut self, last_price: f64, cummulative_qty: f64) {
        match self.ongoing_op.clone() {
            Some(Operation {
                kind: OperationKind::Close,
                pos,
                ..
            }) => {
                self.update_close_value(&pos.kind, last_price);
                self.set_pnl();
                self.clear_position();
            }
            Some(Operation {
                kind: OperationKind::Open,
                pos,
                ..
            }) => {
                self.set_nominal_position(cummulative_qty);
                self.update_open_value(&pos.kind, last_price);
            }
            _ => {}
        }
        self.set_ongoing_op(None);
        self.save();
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn resolve_pending_operations(&mut self, current_bp: &BookPosition) -> Result<()> {
        match self.ongoing_op.as_ref() {
            // There is an ongoing operation
            Some(o) => {
                match &o.transaction {
                    Some(olt) => {
                        let pending_transaction = self.ts.latest_transaction_change(olt).await?;
                        // One of the operations has changed, update the ongoing operation
                        let mut new_op = o.clone();
                        let transaction = &pending_transaction.1;
                        // Transaction changed
                        if pending_transaction.0 {
                            new_op.transaction = Some(transaction.clone());
                        }
                        // Operation filled, clear position
                        let result = if transaction.is_filled() {
                            debug!("Transaction is {} for operation {}", transaction.status, &o.id);
                            if let TransactionStatus::Filled(update) = &transaction.status {
                                self.clear_ongoing_operation(update.last_executed_price, update.cummulative_filled_qty);
                            }
                            Ok(())
                        } else if transaction.is_bad_request() {
                            self.is_trading = false;
                            Ok(())
                        } else {
                            // Need to resolve the operation, potentially with a new price
                            let current_price = self.new_price(current_bp, &o.kind)?;
                            let new_trade = o.trade.with_new_price(current_price);
                            let staged_order = StagedOrder {
                                request: new_trade.to_request(&self.order_mode),
                            };
                            if let Err(e) = self
                                .ts
                                .maybe_retry_trade(transaction.clone(), staged_order)
                                .await
                                .map(|tr| new_op.transaction = Some(tr))
                            {
                                error!("Failed to retry trade {:?}, {:?} : {}", &transaction, &new_trade, e);
                            }
                            self.set_ongoing_op(Some(new_op.clone()));
                            Err(anyhow!("Some operations have not been filled or had to be restaged"))
                        };
                        self.save_operation(&new_op);
                        result
                    }
                    _ => Err(anyhow!("No transaction to resolve in operation")),
                }
            }
            None => Ok(()),
        }
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

    fn update_open_value(&mut self, kind: &PositionKind, price: f64) {
        match kind {
            PositionKind::Short => {
                self.value_strat += self.nominal_position * price;
            }
            PositionKind::Long => {
                self.value_strat -= self.nominal_position * price * (1.0 + self.fees_rate);
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn open(&mut self, pos: Position) -> Result<Operation> {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.traded_price = pos.price;
        self.update_nominal_position(&position_kind);
        self.update_open_value(&position_kind, pos.price);
        let mut op = Operation::new(pos, OperationKind::Open, self.nominal_position, self.dry_mode);
        self.stage_operation(&mut op).await?;
        Ok(op)
    }

    fn update_close_value(&mut self, kind: &PositionKind, price: f64) {
        match kind {
            PositionKind::Short => {
                self.value_strat -= self.nominal_position * price;
            }
            PositionKind::Long => {
                self.value_strat += self.nominal_position * price * (1.0 - self.fees_rate);
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub(super) async fn close(&mut self, pos: Position) -> Result<Operation> {
        self.update_close_value(&pos.kind, pos.price);
        let mut op = Operation::new(pos, OperationKind::Close, self.nominal_position, self.dry_mode);
        self.stage_operation(&mut op).await?;
        Ok(op)
    }

    fn clear_position(&mut self) {
        self.position = None;
        self.long_position_return = 0.0;
        self.short_position_return = 0.0;
        self.nominal_position = 0.0;
    }

    fn save_operation(&mut self, op: &Operation) {
        let save = op.clone();
        if let Err(e) = self.db.put(OPERATIONS_KEY, &op.id, save) {
            error!("Error saving operation: {:?}", e);
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stage_operation(&mut self, op: &mut Operation) -> Result<()> {
        self.save_operation(op);
        let staged_order = StagedOrder {
            request: op.trade.to_request(&self.order_mode),
        };
        let reqs = self.ts.stage_order(staged_order).await;
        op.transaction = reqs.ok();
        self.save_operation(op);
        let transaction_result = match &op.transaction {
            None => Err(anyhow!("Failed transaction")),
            _ => Ok(()),
        };

        self.set_ongoing_op(Some(op.clone()));
        op.log();
        self.save();
        self.log_indicators(&op.pos.kind);
        transaction_result
    }

    fn save(&mut self) {
        if let Err(e) = self.db.put(STATE_KEY, &self.key, TransientState {
            value_strat: self.value_strat,
            pnl: self.pnl,
            nominal_position: Some(self.nominal_position),
            ongoing_op: self.ongoing_op.as_ref().map(|o| o.id.clone()),
            units_to_buy: self.units_to_buy,
            traded_price: self.traded_price,
            units_to_sell: self.units_to_sell,
            threshold_short: self.threshold_short,
            threshold_long: self.threshold_long,
            apo: self.apo,
        }) {
            error!("Error saving state: {:?}", e);
        }
    }

    pub fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.value_strat = v,
            MutableField::NominalPosition => self.nominal_position = v,
            MutableField::Pnl => self.pnl = v,
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

    pub fn dump_db(&self) -> String {
        let mut dump: HashMap<String, serde_json::Value> = HashMap::new();
        if let Ok(operations) = self.db.get_all::<Vec<Operation>>(OPERATIONS_KEY) {
            dump.insert("operations".to_string(), serde_json::to_value(&operations).unwrap());
        }
        if let Ok(state) = self.db.get_all::<TransientState>(STATE_KEY) {
            dump.insert("state".to_string(), serde_json::to_value(&state).unwrap());
        }
        serde_json::to_string(&dump).unwrap()
    }

    fn get_operation(&self, uuid: &str) -> Option<Operation> { self.db.get(OPERATIONS_KEY, uuid).ok() }

    fn log_indicators(&self, pos: &PositionKind) {
        if log_enabled!(Debug) {
            let indicator = MeanRevertingStateIndicator {
                units_to_buy: self.units_to_buy,
                units_to_sell: self.units_to_sell,
                apo: self.apo(),
                value_strat: self.value_strat,
                pos_return: match pos {
                    PositionKind::Short => self.short_position_return,
                    PositionKind::Long => self.long_position_return,
                },
                pnl: self.pnl(),
                nominal_position: self.nominal_position,
            };
            debug!("{}", serde_json::to_string(&indicator).unwrap());
        }
    }
    fn new_price(&self, bp: &BookPosition, operation_kind: &OperationKind) -> Result<f64> {
        match (&self.position, operation_kind) {
            (Some(PositionKind::Short), OperationKind::Open) | (Some(PositionKind::Long), OperationKind::Close) => {
                Ok(bp.ask)
            }
            (Some(PositionKind::Short), OperationKind::Close) | (Some(PositionKind::Long), OperationKind::Open) => {
                Ok(bp.bid)
            }
            _ => {
                // Return early as there is nothing to be done, this should never happen
                Err(anyhow!(
                    "Tried to determine new price for transaction when no position is taken"
                ))
            }
        }
    }
}

fn round_serialize<S>(x: &f64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{:.2}", x))
}

#[derive(Serialize)]
struct MeanRevertingStateIndicator {
    #[serde(serialize_with = "round_serialize")]
    units_to_buy: f64,
    #[serde(serialize_with = "round_serialize")]
    units_to_sell: f64,
    #[serde(serialize_with = "round_serialize")]
    apo: f64,
    value_strat: f64,
    pos_return: f64,
    pnl: f64,
    nominal_position: f64,
}
