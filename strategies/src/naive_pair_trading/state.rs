use crate::naive_pair_trading::data_table::BookPosition;
use crate::order_manager::{OrderId, OrderManager, StagedOrder, Transaction, TransactionStatus};
use crate::query::MutableField;
use actix::{Addr, MailboxError};
use anyhow::Result;
use chrono::{DateTime, Utc};
use coinnect_rt::types::TradeType;
use db::Db;
use log::Level::Info;
use serde::{Deserialize, Serialize};
use std::panic;
use strum_macros::{AsRefStr, EnumString};
use uuid::Uuid;

const TS_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Clone, Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct Position {
    pub kind: PositionKind,
    pub right_price: f64,
    pub left_price: f64,
    pub time: DateTime<Utc>,
    pub right_pair: String,
    pub left_pair: String,
}

#[derive(
    Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum PositionKind {
    #[strum(serialize = "short")]
    SHORT,
    #[strum(serialize = "long")]
    LONG,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Operation {
    pub id: String,
    pub kind: OperationKind,
    pub pos: Position,
    pub left_coef: f64,
    pub right_coef: f64,
    pub left_transaction: Option<Transaction>,
    pub right_transaction: Option<Transaction>,
    pub left_trade: TradeOperation,
    pub right_trade: TradeOperation,
}

#[derive(Clone, Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct TradeOperation {
    kind: TradeKind,
    pair: String,
    qty: f64,
    price: f64,
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

    #[graphql(description = "the operation of the 'left' crypto pair")]
    pub fn left_trade(&self) -> &TradeOperation {
        &self.left_trade
    }

    #[graphql(description = "the operation of the 'right' crypto pair")]
    pub fn right_trade(&self) -> &TradeOperation {
        &self.right_trade
    }

    #[graphql(description = "left quantity")]
    pub fn left_value(&self) -> f64 {
        self.left_trade.qty * self.pos.left_price * self.left_coef.abs()
    }

    #[graphql(description = "right quantity")]
    pub fn right_value(&self) -> f64 {
        self.right_trade.qty * self.pos.right_price * self.right_coef.abs()
    }
}

impl Operation {
    pub fn left_value(&self) -> f64 {
        self.left_trade.qty * self.pos.left_price * self.left_coef.abs()
    }

    pub fn right_value(&self) -> f64 {
        self.right_trade.qty * self.pos.right_price * self.right_coef.abs()
    }

    pub fn is_resolved(&self) -> bool {
        match (&self.right_transaction, &self.left_transaction) {
            (Some(trr), Some(trl)) => trr.is_filled() && trl.is_filled(),
            _ => false,
        }
    }

    fn log(&self) {
        self.log_pos(&self.kind, &self.pos.kind, self.pos.time);
        self.log_trade(
            &self.right_trade.kind,
            self.right_trade.qty,
            &self.pos.right_pair,
            self.pos.right_price,
            self.right_value(),
        );
        self.log_trade(
            &self.left_trade.kind,
            self.left_trade.qty,
            &self.pos.left_pair,
            self.pos.left_price,
            self.left_value(),
        );
    }

    fn log_pos(&self, op: &OperationKind, pos: &PositionKind, time: DateTime<Utc>) {
        if log_enabled!(Info) {
            info!(
                "{} {} position at {}",
                op.as_ref(),
                match pos {
                    PositionKind::SHORT => "short",
                    PositionKind::LONG => "long",
                },
                time.format(TS_FORMAT)
            );
        }
    }

    fn log_trade(&self, op: &TradeKind, qty: f64, pair: &str, price: f64, value: f64) {
        if log_enabled!(Info) {
            info!(
                "{} {:.2} {} at {} for {:.2}",
                op.as_ref(),
                qty,
                pair,
                price,
                value
            );
        }
    }
}

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum OperationKind {
    #[strum(serialize = "open")]
    OPEN,
    #[strum(serialize = "close")]
    CLOSE,
}

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    BUY,
    #[strum(serialize = "sell")]
    SELL,
}

impl Into<TradeType> for TradeKind {
    fn into(self) -> TradeType {
        match self {
            TradeKind::BUY => TradeType::Buy,
            TradeKind::SELL => TradeType::Sell,
        }
    }
}

pub(crate) static OPERATIONS_KEY: &str = "orders";

pub(crate) static STATE_KEY: &str = "state";

#[derive(Debug, Serialize)]
pub(super) struct MovingState {
    position: Option<PositionKind>,
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    beta_val: f64,
    alpha_val: f64,
    beta_lr: f64,
    predicted_right: f64,
    res: f64,
    nominal_position: f64,
    traded_price_right: f64,
    traded_price_left: f64,
    short_position_return: f64,
    long_position_return: f64,
    pnl: f64,
    #[serde(skip_serializing)]
    db: Db,
    #[serde(skip_serializing)]
    om: Addr<OrderManager>,
    ongoing_op: Option<Operation>,
}

#[derive(Serialize, Deserialize)]
struct TransientState {
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    pnl: f64,
    traded_price_left: f64,
    traded_price_right: f64,
    nominal_position: Option<f64>,
    ongoing_op: Option<String>,
}

impl MovingState {
    pub fn new(initial_value: f64, db: Db, om: Addr<OrderManager>) -> MovingState {
        let mut state = MovingState {
            position: None,
            value_strat: initial_value,
            units_to_buy_long_spread: 0.0,
            units_to_buy_short_spread: 0.0,
            beta_val: 0.0,
            alpha_val: 0.0,
            beta_lr: 0.0,
            predicted_right: 0.0,
            res: 0.0,
            nominal_position: 0.0,
            traded_price_right: 0.0,
            traded_price_left: 0.0,
            short_position_return: 0.0,
            long_position_return: 0.0,
            pnl: initial_value,
            db,
            om,
            ongoing_op: None,
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
            self.set_units_to_buy_long_spread(ps.units_to_buy_long_spread);
            self.set_units_to_buy_short_spread(ps.units_to_buy_short_spread);
            self.value_strat = ps.value_strat;
            self.pnl = ps.pnl;
            self.traded_price_left = ps.traded_price_left;
            self.traded_price_right = ps.traded_price_right;
            if let Some(np) = ps.nominal_position {
                self.nominal_position = np;
            }
            if let Some(op_key) = ps.ongoing_op {
                let op: Option<Operation> = self.db.read_json(&op_key);
                self.ongoing_op = op;
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

    pub(super) fn set_predicted_right(&mut self, predicted_right: f64) {
        self.predicted_right = predicted_right;
    }

    pub(super) fn predicted_right(&self) -> f64 {
        self.predicted_right
    }

    #[cfg(test)]
    pub(super) fn traded_price_right(&self) -> f64 {
        self.traded_price_right
    }

    #[cfg(test)]
    pub(super) fn traded_price_left(&self) -> f64 {
        self.traded_price_left
    }

    pub(super) fn set_pnl(&mut self) {
        self.pnl = self.value_strat;
    }

    pub(super) fn pnl(&self) -> f64 {
        self.pnl
    }

    pub(super) fn set_beta(&mut self, beta: f64) {
        self.beta_val = beta;
    }

    pub(super) fn beta(&self) -> f64 {
        self.beta_val
    }

    pub(super) fn set_res(&mut self, res: f64) {
        self.res = res;
    }

    pub(super) fn res(&self) -> f64 {
        self.res
    }

    pub(super) fn set_alpha(&mut self, alpha: f64) {
        self.alpha_val = alpha;
    }

    pub(super) fn alpha(&self) -> f64 {
        self.alpha_val
    }

    pub(super) fn value_strat(&self) -> f64 {
        self.value_strat
    }

    pub(super) fn set_beta_lr(&mut self) {
        self.beta_lr = self.beta_val;
    }

    pub(super) fn beta_lr(&self) -> f64 {
        self.beta_lr
    }

    pub(super) fn nominal_position(&self) -> f64 {
        self.nominal_position
    }

    pub(super) fn set_units_to_buy_long_spread(&mut self, units_to_buy_long_spread: f64) {
        self.units_to_buy_long_spread = units_to_buy_long_spread;
    }

    pub(super) fn set_units_to_buy_short_spread(&mut self, units_to_buy_short_spread: f64) {
        self.units_to_buy_short_spread = units_to_buy_short_spread;
    }

    pub(super) fn set_long_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) {
        self.long_position_return = (self.units_to_buy_long_spread
            * ((self.traded_price_left * self.beta_val * (1.0 - fees_rate))
                - (self.traded_price_right * (1.0 + fees_rate))
                + (current_price_right * (1.0 - fees_rate))
                - (current_price_left * self.beta_val * (1.0 + fees_rate))))
            / self.pnl;
    }

    pub(super) fn set_short_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) {
        self.short_position_return = (self.units_to_buy_short_spread
            * ((self.traded_price_right * (1.0 - fees_rate))
                - (self.traded_price_left * self.beta_val * (1.0 + fees_rate))
                + (current_price_left * self.beta_val * (1.0 - fees_rate))
                - (current_price_right * (1.0 + fees_rate))))
            / self.pnl;
    }

    pub(super) fn short_position_return(&self) -> f64 {
        self.short_position_return
    }

    pub(super) fn long_position_return(&self) -> f64 {
        self.long_position_return
    }

    fn make_operation(
        &self,
        pos: Position,
        op_kind: OperationKind,
        spread: f64,
        right_coef: f64,
        left_coef: f64,
    ) -> Operation {
        let (left_op, right_op) = match (&pos.kind, &op_kind) {
            (PositionKind::SHORT, OperationKind::OPEN) => (TradeKind::BUY, TradeKind::SELL),
            (PositionKind::LONG, OperationKind::OPEN) => (TradeKind::SELL, TradeKind::BUY),
            (PositionKind::SHORT, OperationKind::CLOSE) => (TradeKind::SELL, TradeKind::BUY),
            (PositionKind::LONG, OperationKind::CLOSE) => (TradeKind::BUY, TradeKind::SELL),
            _ => unimplemented!(),
        };
        Operation {
            id: format!("{}:{}", OPERATIONS_KEY, Uuid::new_v4()),
            pos: pos.clone(),
            kind: op_kind,
            left_coef,
            right_coef,
            left_transaction: None,
            right_transaction: None,
            left_trade: TradeOperation {
                price: pos.left_price.clone(),
                qty: spread * self.beta_val,
                pair: pos.left_pair.clone().into(),
                kind: left_op,
            },
            right_trade: TradeOperation {
                price: pos.right_price.clone(),
                qty: spread,
                pair: pos.right_pair.clone().into(),
                kind: right_op,
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
        match tr {
            // Changed and rejected, retry transaction
            // TODO need to handle rejections in a finer grained way
            // TODO introduce a backoff
            Transaction {
                status: TransactionStatus::Rejected(_),
                ..
            } => self.stage_order(trade.clone()).await,
            _ => {
                // TODO: Timeout can be managed here
                Err(anyhow!("Nor rejected nor filled"))
            }
        }
    }

    pub(super) async fn resolve_pending_operations(
        &mut self,
        left_bp: &BookPosition,
        right_bp: &BookPosition,
    ) -> Result<()> {
        match self.ongoing_op.as_ref() {
            // There is an ongoing operation
            Some(o) => {
                match (&o.left_transaction, &o.right_transaction) {
                    (Some(olt), Some(ort)) => {
                        let pending_trs: (
                            Result<(bool, Transaction)>,
                            Result<(bool, Transaction)>,
                        ) = futures::future::join(
                            self.latest_transaction_change(olt),
                            self.latest_transaction_change(ort),
                        )
                        .await;

                        let olr = pending_trs.0?;
                        let orr = pending_trs.1?;

                        // One of the operations has changed, update the ongoing operation
                        let lts = &olr.1;
                        let rts = &orr.1;
                        let mut new_op = o.clone();
                        // Left or Right transaction changed
                        if olr.0 || orr.0 {
                            new_op.left_transaction = Some(lts.clone());
                            new_op.right_transaction = Some(rts.clone());
                        }
                        // Both operations filled, clear position
                        let result = if let (
                            Transaction {
                                status: TransactionStatus::Filled(_),
                                ..
                            },
                            Transaction {
                                status: TransactionStatus::Filled(_),
                                ..
                            },
                        ) = (lts, rts)
                        {
                            self.ongoing_op = None;
                            self.set_pnl();
                            self.clear_position();
                            self.save();
                            Ok(())
                        } else {
                            let (current_price_left, current_price_right) = match (
                                &self.position,
                                &o.kind,
                            ) {
                                (Some(PositionKind::SHORT), OperationKind::OPEN) => {
                                    (left_bp.ask, right_bp.bid)
                                }
                                (Some(PositionKind::SHORT), OperationKind::CLOSE) => {
                                    (left_bp.bid, right_bp.ask)
                                }
                                (Some(PositionKind::LONG), OperationKind::OPEN) => {
                                    (left_bp.bid, right_bp.ask)
                                }
                                (Some(PositionKind::LONG), OperationKind::CLOSE) => {
                                    (left_bp.ask, right_bp.bid)
                                }
                                _ => {
                                    error!("Tried to determine new price for transactions when no position is taken");
                                    (0.0, 0.0)
                                }
                            };
                            let new_left_trade = TradeOperation {
                                price: current_price_left,
                                ..o.left_trade.clone()
                            };
                            if let Err(e) = self
                                .maybe_retry_trade(lts.clone(), &new_left_trade)
                                .await
                                .map(|tr| new_op.left_transaction = Some(tr))
                            {
                                error!(
                                    "Failed to retry trade {:?}, {:?} : {}",
                                    &lts, &new_left_trade, e
                                );
                            }
                            let new_right_trade = TradeOperation {
                                price: current_price_right,
                                ..o.left_trade.clone()
                            };
                            if let Err(e) = self
                                .maybe_retry_trade(rts.clone(), &new_right_trade)
                                .await
                                .map(|tr| new_op.right_transaction = Some(tr))
                            {
                                error!(
                                    "Failed to retry right trade {:?}, {:?} : {}",
                                    &rts, &new_right_trade, e
                                );
                            }
                            self.ongoing_op = Some(new_op.clone());
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
            None => Err(anyhow!("No pending operation")),
        }
    }

    pub(super) async fn open(&mut self, pos: Position, fees: f64) -> Operation {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.nominal_position = self.beta_val;
        self.traded_price_right = pos.right_price;
        self.traded_price_left = pos.left_price;
        let (spread, right_coef, left_coef) = match position_kind {
            PositionKind::SHORT => {
                let (spread, right_coef, left_coef) =
                    (self.units_to_buy_short_spread, 1.0 - fees, 1.0 + fees);
                self.value_strat += spread
                    * (pos.right_price * right_coef - pos.left_price * self.beta_val * left_coef);
                (spread, right_coef, left_coef)
            }
            PositionKind::LONG => {
                let (spread, right_coef, left_coef) =
                    (self.units_to_buy_long_spread, 1.0 + fees, 1.0 - fees);
                self.value_strat += spread
                    * (pos.left_price * self.beta_val * left_coef - pos.right_price * right_coef);
                (spread, right_coef, left_coef)
            }
        };
        let mut op = self.make_operation(pos, OperationKind::OPEN, spread, right_coef, left_coef);
        op.log();
        self.stage_operation(&mut op).await;
        self.save();
        self.log_info(&position_kind);
        op
    }

    pub(super) async fn close(&mut self, pos: Position, fees: f64) -> Operation {
        let kind: PositionKind = pos.kind.clone();
        self.unset_position();
        let (spread, right_coef, left_coef) = match kind {
            PositionKind::SHORT => {
                let (spread, right_coef, left_coef) =
                    (self.units_to_buy_short_spread, 1.0 + fees, 1.0 - fees);
                self.value_strat += spread
                    * (pos.left_price * self.beta_val * left_coef - pos.right_price * right_coef);
                (spread, right_coef, left_coef)
            }
            PositionKind::LONG => {
                let (spread, right_coef, left_coef) =
                    (self.units_to_buy_long_spread, 1.0 - fees, 1.0 + fees);
                self.value_strat += spread
                    * (pos.right_price * right_coef - pos.left_price * self.beta_val * left_coef);
                (spread, right_coef, left_coef)
            }
        };
        let mut op = self.make_operation(pos, OperationKind::CLOSE, spread, right_coef, left_coef);
        self.stage_operation(&mut op).await;
        op.log();
        self.save();
        self.log_info(&kind);
        op
    }

    fn clear_position(&mut self) {
        self.long_position_return = 0.0;
        self.short_position_return = 0.0;
        self.traded_price_left = 0.0;
        self.traded_price_right = 0.0;
    }

    async fn stage_order(&self, trade_op: TradeOperation) -> Result<Transaction> {
        self.om
            .send(StagedOrder {
                op_kind: trade_op.kind,
                pair: trade_op.pair.into(),
                qty: trade_op.qty,
                price: trade_op.price,
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
        let reqs: (Result<Transaction>, Result<Transaction>) = futures::future::join(
            self.stage_order(op.left_trade.clone()),
            self.stage_order(op.right_trade.clone()),
        )
        .await;

        op.left_transaction = reqs.0.ok();
        op.right_transaction = reqs.1.ok();
        self.save_operation(&op);
        match (&op.left_transaction, &op.right_transaction) {
            (None, _) | (_, None) => error!("Failed transaction"),
            _ => info!("Transaction ok"),
        }
    }

    fn extract_transaction(
        res: std::result::Result<&Result<Transaction>, &MailboxError>,
    ) -> Option<Transaction> {
        if let Ok(Ok(tr)) = res {
            Some(tr.clone())
        } else {
            None
        }
    }

    fn save(&self) {
        if let Err(e) = self.db.put_json(
            STATE_KEY,
            TransientState {
                units_to_buy_short_spread: self.units_to_buy_short_spread,
                units_to_buy_long_spread: self.units_to_buy_long_spread,
                value_strat: self.value_strat,
                pnl: self.pnl,
                traded_price_left: self.traded_price_left,
                traded_price_right: self.traded_price_right,
                nominal_position: Some(self.nominal_position),
                ongoing_op: self.ongoing_op.as_ref().map(|o| o.id.clone()),
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
        self.db.with_db(|env, store| {
            let reader = env.read().unwrap();
            let mut strings: Vec<String> = Vec::new();
            for r in store.iter_start(&reader).unwrap() {
                strings.push(format!("{:?}", r))
            }
            strings
        })
    }

    #[allow(dead_code)]
    fn get_operation(&self, uuid: &str) -> Option<Operation> {
        self.db.read_json(&format!("{}:{}", OPERATIONS_KEY, uuid))
    }

    fn log_info(&self, pos: &PositionKind) {
        if log_enabled!(Info) {
            info!(
                "Additional info : units {:.2} beta val {:.2} value strat {}",
                match pos {
                    PositionKind::SHORT => self.units_to_buy_short_spread,
                    PositionKind::LONG => self.units_to_buy_long_spread,
                },
                self.beta_val,
                self.value_strat
            );
            info!("--------------------------------")
        }
    }
}
