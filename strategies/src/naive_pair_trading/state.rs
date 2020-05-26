use crate::naive_pair_trading::order_manager::OrderManager;
use crate::query::MutableField;
use anyhow::Result;
use chrono::{DateTime, Utc};
use coinnect_rt::exchange::ExchangeApi;
use coinnect_rt::types::{
    AddOrderRequest, OrderInfo, OrderQuery, OrderStatus, OrderType, Pair, TradeType,
};
use db::Db;
use futures::join;
use futures::lock::{Mutex, MutexGuard};
use futures::stream::FuturesUnordered;
use log::Level::Info;
use serde::{Deserialize, Serialize};
use std::panic;
use std::sync::Arc;
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
    pub kind: OperationKind,
    pub pos: Position,
    pub left_op: OperationKind,
    pub right_op: OperationKind,
    pub left_spread: f64,
    pub right_spread: f64,
    pub left_coef: f64,
    pub right_coef: f64,
    pub left_transaction: Transaction,
    pub right_transaction: Transaction,
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

    #[graphql(description = "the buy/sell operation for the 'left' crypto pair")]
    pub fn left_op(&self) -> &OperationKind {
        &self.left_op
    }

    #[graphql(description = "the buy/sell operation for the 'right' crypto pair")]
    pub fn right_op(&self) -> &OperationKind {
        &self.right_op
    }

    #[graphql(description = "the spread of the 'left' crypto pair")]
    pub fn left_spread(&self) -> f64 {
        self.left_spread
    }

    #[graphql(description = "the spread of the 'right' crypto pair")]
    pub fn right_spread(&self) -> f64 {
        self.right_spread
    }

    #[graphql(description = "left quantity")]
    pub fn left_qty(&self) -> f64 {
        self.left_spread * self.pos.left_price * self.left_coef.abs()
    }

    #[graphql(description = "right quantity")]
    pub fn right_qty(&self) -> f64 {
        self.right_spread * self.pos.right_price * self.right_coef.abs()
    }
}

impl Operation {
    pub fn left_qty(&self) -> f64 {
        self.left_spread * self.pos.left_price * self.left_coef.abs()
    }

    pub fn right_qty(&self) -> f64 {
        self.right_spread * self.pos.right_price * self.right_coef.abs()
    }

    fn log(&self) {
        self.log_pos(&self.kind, &self.pos.kind, self.pos.time);
        self.log_trade(
            &self.right_op,
            self.right_spread,
            &self.pos.right_pair,
            self.pos.right_price,
            self.right_qty(),
        );
        self.log_trade(
            &self.left_op,
            self.left_spread,
            &self.pos.left_pair,
            self.pos.left_price,
            self.left_qty(),
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

    fn log_trade(&self, op: &OperationKind, spread: f64, pair: &str, value: f64, qty: f64) {
        if log_enabled!(Info) {
            info!(
                "{} {:.2} {} at {} for {:.2}",
                op.as_ref(),
                spread,
                pair,
                value,
                qty
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
    #[strum(serialize = "buy")]
    BUY,
    #[strum(serialize = "sell")]
    SELL,
}

impl Into<TradeType> for OperationKind {
    fn into(self) -> TradeType {
        match self {
            OperationKind::BUY => TradeType::Buy,
            OperationKind::SELL => TradeType::Sell,
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Rejection {
    BadRequest,
    InsufficientFunds,
    Timeout,
    Cancelled,
    OtherFailure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Transaction {
    Staged,
    New(OrderInfo),
    Filled,
    Rejected(Rejection),
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
    order_manager: OrderManager,
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
}

impl MovingState {
    pub fn new(initial_value: f64, db: Db, api: Arc<Mutex<Box<dyn ExchangeApi>>>) -> MovingState {
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
            order_manager: OrderManager::new(api.clone()),
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let mut ops: Vec<Operation> = self.db.read_json_vec(OPERATIONS_KEY);
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
            (PositionKind::SHORT, OperationKind::OPEN) => (OperationKind::BUY, OperationKind::SELL),
            (PositionKind::LONG, OperationKind::OPEN) => (OperationKind::SELL, OperationKind::BUY),
            (PositionKind::SHORT, OperationKind::CLOSE) => {
                (OperationKind::SELL, OperationKind::BUY)
            }
            (PositionKind::LONG, OperationKind::CLOSE) => (OperationKind::BUY, OperationKind::SELL),
            _ => unimplemented!(),
        };
        Operation {
            pos,
            kind: op_kind,
            left_spread: spread * self.beta_val,
            left_coef,
            left_op,
            right_spread: spread,
            right_coef,
            right_op,
            left_transaction: Transaction::Staged,
            right_transaction: Transaction::Staged,
        }
    }

    pub(super) fn open(&mut self, pos: Position, fees: f64) -> Operation {
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
        self.save_operation(&mut op);
        self.save();
        self.log_info(&position_kind);
        op
    }

    pub(super) fn close(&mut self, pos: Position, fees: f64) -> Operation {
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
        self.save_operation(&mut op);
        op.log();
        self.set_pnl();
        self.clear_position();
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

    async fn save_operation(&mut self, op: &mut Operation) {
        let op_key = format!("{}:{}", OPERATIONS_KEY, Uuid::new_v4());
        self.db.put_json(&op_key, op.clone());
        let left_req = self.order_manager.stage_order(
            op.left_op.clone().into(),
            op.pos.left_pair.clone().into(),
            op.left_spread,
            op.pos.left_price,
        );
        let right_req = self.order_manager.stage_order(
            op.right_op.clone().into(),
            op.pos.right_pair.clone().into(),
            op.right_spread,
            op.pos.right_price,
        );
        let (left_info, right_info) = join!(left_req, right_req);
        op.left_transaction = Self::into_transaction(left_info);
        op.right_transaction = Self::into_transaction(right_info);
        self.db.put_json(&op_key, op);
    }

    fn into_transaction(res: anyhow::Result<OrderInfo>) -> Transaction {
        match res {
            Ok(o) => Transaction::New(o),
            Err(_e) => Transaction::Rejected(Rejection::BadRequest),
        }
    }

    fn save(&self) {
        self.db.put_json(
            STATE_KEY,
            TransientState {
                units_to_buy_short_spread: self.units_to_buy_short_spread,
                units_to_buy_long_spread: self.units_to_buy_long_spread,
                value_strat: self.value_strat,
                pnl: self.pnl,
                traded_price_left: self.traded_price_left,
                traded_price_right: self.traded_price_right,
                nominal_position: Some(self.nominal_position),
            },
        );
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
