use crate::mean_reverting::options::Options;
use crate::model;
use crate::model::BookPosition;
use crate::model::{OperationKind, PositionKind, TradeKind};
use crate::order_manager::{OrderId, OrderManager, StagedOrder, Transaction};
use crate::query::MutableField;
use actix::Addr;
use anyhow::Result;
use chrono::{DateTime, Utc};
use db::Db;
use log::Level::Info;
use serde::{Deserialize, Serialize};
use std::panic;
use uuid::Uuid;

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

#[derive(Clone, Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct TradeOperation {
    kind: TradeKind,
    pair: String,
    qty: f64,
    price: f64,
}

impl TradeOperation {
    pub fn with_new_price(&self, new_price: f64) -> TradeOperation {
        TradeOperation {
            price: new_price,
            ..self.clone()
        }
    }
}

#[juniper::graphql_object]
impl Operation {
    #[graphql(description = "the kind of operation")]
    pub fn kind(&self) -> &OperationKind {
        &self.kind
    }

    #[graphql(description = "the position this operation is based on")]
    pub fn pos(&self) -> &Position {
        &self.pos
    }

    #[graphql(description = "the operation of the crypto pair")]
    pub fn trade(&self) -> &TradeOperation {
        &self.trade
    }

    #[graphql(description = "value")]
    pub fn value(&self) -> f64 {
        self.trade.qty * self.trade.price
    }
}

impl Operation {
    pub fn value(&self) -> f64 {
        self.trade.qty * self.trade.price
    }

    pub fn is_resolved(&self) -> bool {
        match &self.transaction {
            Some(trr) => trr.is_filled(),
            _ => false,
        }
    }

    fn log(&self) {
        model::log_pos(&self.kind, &self.pos.kind, self.pos.time);
        model::log_trade(
            &self.trade.kind,
            self.trade.qty,
            &self.pos.pair,
            self.pos.price,
            self.value(),
        );
    }
}

pub(crate) static OPERATIONS_KEY: &str = "orders";

pub(crate) static STATE_KEY: &str = "state";

#[derive(Clone, Debug, Serialize)]
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
    db: Db,
    #[serde(skip_serializing)]
    om: Addr<OrderManager>,
    ongoing_op: Option<Operation>,
    /// Remote operations are ran dry, meaning no actual action will be performed when possible
    dry_mode: bool,
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
    pub fn new(options: &Options, db: Db, om: Addr<OrderManager>) -> MeanRevertingState {
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
            om,
            ongoing_op: None,
            dry_mode: options.dry_mode(),
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let mut ops: Vec<Operation> = self.get_operations();
        ops.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
        if let Some(o) = ops.last() {
            if OperationKind::OPEN == o.kind {
                self.set_position(o.pos.kind.clone());
            }
        }
        let previous_state: Option<TransientState> = self.db.read_json(STATE_KEY);
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
                let op: Option<Operation> = self.db.read_json(&op_key);
                self.set_ongoing_op(op);
            }
        }
    }

    pub(super) fn no_position_taken(&self) -> bool {
        self.position.is_none()
    }

    pub(super) fn is_long(&self) -> bool {
        self.position.eq(&Some(PositionKind::LONG))
    }

    pub(super) fn is_short(&self) -> bool {
        self.position.eq(&Some(PositionKind::SHORT))
    }

    pub(super) fn set_position(&mut self, k: PositionKind) {
        self.position = Some(k);
    }

    pub(super) fn unset_position(&mut self) {
        self.position = None;
    }

    fn set_ongoing_op(&mut self, op: Option<Operation>) {
        self.ongoing_op = op;
    }

    pub fn ongoing_op(&self) -> &Option<Operation> {
        &self.ongoing_op
    }

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

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn traded_price(&self) -> f64 {
        self.traded_price
    }

    pub(super) fn set_pnl(&mut self) {
        self.pnl = self.value_strat;
    }

    pub(super) fn pnl(&self) -> f64 {
        self.pnl
    }

    #[allow(dead_code)]
    pub(super) fn value_strat(&self) -> f64 {
        self.value_strat
    }

    pub(super) fn set_apo(&mut self, apo: f64) {
        self.apo = apo;
    }

    pub(super) fn apo(&self) -> f64 {
        self.apo
    }

    #[allow(dead_code)]
    pub(super) fn nominal_position(&self) -> f64 {
        self.nominal_position
    }

    pub(super) fn set_long_position_return(&mut self, fees_rate: f64, current_price: f64) {
        self.long_position_return = (self.nominal_position
            * (current_price * (1.0 - fees_rate) - self.traded_price))
            / (self.nominal_position * self.traded_price);
    }

    pub(super) fn set_short_position_return(&mut self, fees_rate: f64, current_price: f64) {
        self.short_position_return = self.nominal_position
            * (self.traded_price - current_price * (1.0 + fees_rate))
            / (self.nominal_position * self.traded_price);
    }

    pub(super) fn short_position_return(&self) -> f64 {
        self.short_position_return
    }

    pub(super) fn long_position_return(&self) -> f64 {
        self.long_position_return
    }

    pub(super) fn update_units(&mut self, bp: &BookPosition, fees_rate: f64) {
        self.units_to_buy = self.value_strat / bp.ask * (1.0 + fees_rate);
        self.units_to_sell = self.value_strat / bp.bid * (1.0 - fees_rate);
    }

    fn set_units_to_buy(&mut self, v: f64) {
        self.units_to_buy = v;
    }

    fn set_units_to_sell(&mut self, v: f64) {
        self.units_to_sell = v;
    }

    pub fn set_threshold_short(&mut self, v: f64) {
        self.threshold_short = v;
    }

    pub fn set_threshold_long(&mut self, v: f64) {
        self.threshold_long = v;
    }

    pub fn threshold_short(&self) -> f64 {
        self.threshold_short
    }

    pub fn threshold_long(&self) -> f64 {
        self.threshold_long
    }

    fn make_operation(&self, pos: Position, op_kind: OperationKind, qty: f64) -> Operation {
        let trade_kind = match (&pos.kind, &op_kind) {
            (PositionKind::SHORT, OperationKind::OPEN)
            | (PositionKind::LONG, OperationKind::CLOSE) => TradeKind::SELL,
            (PositionKind::LONG, OperationKind::OPEN)
            | (PositionKind::SHORT, OperationKind::CLOSE) => TradeKind::BUY,
        };
        Operation {
            id: format!("{}:{}", OPERATIONS_KEY, Uuid::new_v4()),
            pos: pos.clone(),
            kind: op_kind,
            transaction: None,
            trade: TradeOperation {
                price: pos.price,
                qty,
                pair: pos.pair,
                kind: trade_kind,
            },
        }
    }

    /// Fetches the latest version of this transaction for the order id
    /// Returns whether it changed, and the latest transaction retrieved
    async fn latest_transaction_change(
        &self,
        tr: &Transaction,
    ) -> anyhow::Result<(bool, Transaction)> {
        let new_tr = self.om.send(OrderId(tr.id.clone())).await??;
        Ok((!new_tr.variant_eq(&tr), new_tr))
    }

    async fn maybe_retry_trade(
        &self,
        tr: Transaction,
        trade: &TradeOperation,
    ) -> anyhow::Result<Transaction> {
        if tr.is_rejected() {
            // Changed and rejected, retry transaction
            // TODO need to handle rejections in a finer grained way
            // TODO introduce a backoff
            self.stage_order(trade.clone()).await
        } else {
            // TODO: Timeout can be managed here
            Err(anyhow!("Nor rejected nor filled"))
        }
    }

    fn clear_ongoing_operation(&mut self) {
        if let Some(Operation {
            kind: OperationKind::CLOSE,
            ..
        }) = self.ongoing_op
        {
            self.set_pnl();
            self.clear_position();
            info!("--------------------------------");
            info!("portfolio value {}", self.value_strat);
            info!("--------------------------------");
        }
        self.set_ongoing_op(None);
        self.save();
    }

    pub(super) async fn resolve_pending_operations(&mut self, bp: &BookPosition) -> Result<()> {
        match self.ongoing_op.as_ref() {
            // There is an ongoing operation
            Some(o) => {
                match &o.transaction {
                    Some(olt) => {
                        let pending_tr = self.latest_transaction_change(olt).await?;
                        // One of the operations has changed, update the ongoing operation
                        let transaction = &pending_tr.1;
                        let mut new_op = o.clone();
                        // Transaction changed
                        if pending_tr.0 {
                            new_op.transaction = Some(transaction.clone());
                        }
                        // Operation filled, clear position
                        let result = if transaction.is_filled() {
                            info!("Transactions filled for {}", &o.id);
                            self.clear_ongoing_operation();
                            Ok(())
                        } else if transaction.is_bad_request() {
                            // In this case, our bot did something wrong and repeating the operation
                            // will always fail, so cancel the operation
                            let oid = &o.id.clone();
                            self.ongoing_op = None;
                            info!("Both transactions rejected with bad request for {}", &oid);
                            self.clear_ongoing_operation();
                            Ok(())
                        } else {
                            // Need to resolve the operation, potentially with a new price
                            let current_price = match (&self.position, &o.kind) {
                                (Some(PositionKind::SHORT), OperationKind::OPEN)
                                | (Some(PositionKind::LONG), OperationKind::CLOSE) => bp.ask,
                                (Some(PositionKind::SHORT), OperationKind::CLOSE)
                                | (Some(PositionKind::LONG), OperationKind::OPEN) => bp.bid,
                                _ => {
                                    error!("Tried to determine new price for transaction when no position is taken");
                                    0.0
                                }
                            };
                            let new_trade = o.trade.with_new_price(current_price);
                            if let Err(e) = self
                                .maybe_retry_trade(transaction.clone(), &new_trade)
                                .await
                                .map(|tr| new_op.transaction = Some(tr))
                            {
                                error!(
                                    "Failed to retry trade {:?}, {:?} : {}",
                                    &transaction, &new_trade, e
                                );
                            }
                            self.set_ongoing_op(Some(new_op.clone()));
                            Err(anyhow!(
                                "Some operations have not been filled or had to be restaged"
                            ))
                        };
                        self.save_operation(&new_op);
                        result
                    }
                    _ => Err(anyhow!("One of the transactions didn't go through")),
                }
            }
            None => Ok(()),
        }
    }

    pub(super) async fn open(&mut self, pos: Position, fees: f64) -> Operation {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.traded_price = pos.price;
        match position_kind {
            PositionKind::SHORT => {
                self.nominal_position = self.units_to_sell;
                self.value_strat += self.units_to_sell * pos.price;
            }
            PositionKind::LONG => {
                self.nominal_position = self.units_to_buy;
                self.value_strat = self.value_strat - self.units_to_buy * pos.price * (1.0 + fees);
            }
        };
        let mut op = self.make_operation(pos, OperationKind::OPEN, self.nominal_position);
        op.log();
        self.stage_operation(&mut op).await;
        self.set_ongoing_op(Some(op.clone()));
        self.save();
        self.log_indicators(&position_kind);
        op
    }

    pub(super) async fn close(&mut self, pos: Position, fees: f64) -> Operation {
        let kind: PositionKind = pos.kind.clone();
        match kind {
            PositionKind::SHORT => {
                self.value_strat = self.value_strat - self.nominal_position * pos.price;
            }
            PositionKind::LONG => {
                self.value_strat =
                    self.value_strat + self.nominal_position * pos.price * (1.0 - fees);
            }
        };
        let mut op = self.make_operation(pos, OperationKind::CLOSE, self.nominal_position);
        self.stage_operation(&mut op).await;
        self.set_ongoing_op(Some(op.clone()));
        op.log();
        self.save();
        self.log_indicators(&kind);
        op
    }

    fn clear_position(&mut self) {
        self.unset_position();
        self.long_position_return = 0.0;
        self.short_position_return = 0.0;
        self.nominal_position = 0.0;
    }

    async fn stage_order(&self, trade_op: TradeOperation) -> Result<Transaction> {
        self.om
            .send(StagedOrder {
                op_kind: trade_op.kind,
                pair: trade_op.pair.into(),
                qty: trade_op.qty,
                price: trade_op.price,
                dry_run: self.dry_mode,
            })
            .await?
            .map_err(|e| anyhow!("mailbox error {}", e))
    }

    fn save_operation(&mut self, op: &Operation) {
        if let Err(e) = self.db.put_json(&op.id, op.clone()) {
            error!("Error saving operation: {:?}", e);
        }
    }
    async fn stage_operation(&mut self, op: &mut Operation) {
        self.save_operation(op);
        let reqs = self.stage_order(op.trade.clone()).await;

        op.transaction = reqs.ok();
        self.save_operation(&op);
        match &op.transaction {
            None => error!("Failed transaction"),
            _ => trace!("Transaction ok"),
        }
    }

    fn save(&self) {
        if let Err(e) = self.db.put_json(
            STATE_KEY,
            TransientState {
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
            },
        ) {
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
        self.db.read_json_vec(OPERATIONS_KEY)
    }

    pub fn dump_db(&self) -> Vec<String> {
        self.db.all()
    }

    #[allow(dead_code)]
    fn get_operation(&self, uuid: &str) -> Option<Operation> {
        self.db.read_json(&format!("{}:{}", OPERATIONS_KEY, uuid))
    }

    fn log_indicators(&self, pos: &PositionKind) {
        if log_enabled!(Info) {
            info!(
                "Additional info : units to buy {:.2} units to sell {:.2} apo {:.2} value strat {}, return {}, pnl {}, nominal_position {}",
                self.units_to_buy,
                self.units_to_sell,
                self.apo(),
                self.value_strat,
                match pos {
                    PositionKind::SHORT => self.short_position_return,
                    PositionKind::LONG => self.long_position_return,
                },
                self.pnl(),
                self.nominal_position,
            );
            info!("--------------------------------")
        }
    }
}
