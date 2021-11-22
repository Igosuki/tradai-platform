use std::panic;
use std::sync::Arc;

use actix::Addr;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use db::{Storage, StorageExt};

use crate::coinnect_types::AssetType;
use crate::error::*;
use crate::naive_pair_trading::covar_model::DualBookPosition;
use crate::naive_pair_trading::options::Options;
use crate::query::MutableField;
use crate::types::{OperationEvent, StratEvent, TradeEvent};
use trading::order_manager::types::{OrderDetail, Transaction};
use trading::order_manager::{OrderExecutor, OrderManager, OrderResolution};
use trading::position::{OperationKind, PositionKind};
use trading::types::{OrderMode, TradeKind, TradeOperation};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, juniper::GraphQLObject)]
pub struct Position {
    pub kind: PositionKind,
    pub right_price: f64,
    pub left_price: f64,
    pub time: DateTime<Utc>,
    pub right_pair: String,
    pub left_pair: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Operation {
    pub id: String,
    pub kind: OperationKind,
    pub pos: Position,
    pub left_coef: f64,
    pub right_coef: f64,
    pub left_transaction: Option<Transaction>,
    pub right_transaction: Option<Transaction>,
    pub left_order: Option<OrderDetail>,
    pub right_order: Option<OrderDetail>,
    pub left_trade: TradeOperation,
    pub right_trade: TradeOperation,
}

impl Operation {
    pub fn left_value(&self) -> f64 { self.left_trade.qty * self.pos.left_price * self.left_coef.abs() }

    pub fn right_value(&self) -> f64 { self.right_trade.qty * self.pos.right_price * self.right_coef.abs() }

    pub fn is_resolved(&self) -> bool {
        match (&self.right_transaction, &self.left_transaction) {
            (Some(trr), Some(trl)) => trr.is_filled() && trl.is_filled(),
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

    pub fn trade_events(&self) -> [TradeEvent; 2] {
        [
            TradeEvent {
                op: self.right_trade.kind.clone(),
                qty: self.right_trade.qty,
                pair: self.pos.right_pair.clone(),
                price: self.pos.right_price,
                strat_value: self.right_value(),
                at: self.pos.time,
                borrowed: None,
                interest: None,
            },
            TradeEvent {
                op: self.left_trade.kind.clone(),
                qty: self.left_trade.qty,
                pair: self.pos.left_pair.clone(),
                price: self.pos.left_price,
                strat_value: self.left_value(),
                at: self.pos.time,
                borrowed: None,
                interest: None,
            },
        ]
    }

    fn log(&self) {
        StratEvent::Operation(self.operation_event()).log();
        for trade_event in self.trade_events() {
            StratEvent::Trade(trade_event).log();
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
    db: Arc<dyn Storage>,
    ongoing_op: Option<Operation>,
    /// Remote operations are ran dry, meaning no actual action will be performed when possible
    dry_mode: bool,
    order_mode: OrderMode,
    #[serde(skip_serializing)]
    ts: OrderExecutor,
    is_trading: bool,
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
    pub fn new(n: &Options, db: Arc<dyn Storage>, om: Addr<OrderManager>) -> MovingState {
        db.ensure_table(STATE_KEY).unwrap();
        db.ensure_table(OPERATIONS_KEY).unwrap();
        let mut state = MovingState {
            position: None,
            value_strat: n.initial_cap,
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
            pnl: n.initial_cap,
            db,
            ongoing_op: None,
            dry_mode: n.dry_mode(),
            order_mode: n.order_mode,
            ts: OrderExecutor::new(om),
            is_trading: true,
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
        let previous_state: Option<TransientState> = self.db.get(STATE_KEY, STATE_KEY).ok();
        if let Some(ps) = previous_state {
            self.units_to_buy_long_spread = ps.units_to_buy_long_spread;
            self.units_to_buy_short_spread = ps.units_to_buy_short_spread;
            self.value_strat = ps.value_strat;
            self.pnl = ps.pnl;
            self.traded_price_left = ps.traded_price_left;
            self.traded_price_right = ps.traded_price_right;
            if let Some(np) = ps.nominal_position {
                self.nominal_position = np;
            }
            if let Some(op_key) = ps.ongoing_op {
                let op: Option<Operation> = self.db.get(OPERATIONS_KEY, &op_key).ok();
                self.set_ongoing_op(op);
            }
        }
    }

    pub(super) fn no_position_taken(&self) -> bool { self.position.is_none() }

    pub(super) fn is_long(&self) -> bool { self.position.eq(&Some(PositionKind::Long)) }

    pub(super) fn is_short(&self) -> bool { self.position.eq(&Some(PositionKind::Short)) }

    pub(super) fn set_position(&mut self, k: PositionKind) { self.position = Some(k); }

    pub(super) fn unset_position(&mut self) { self.position = None; }

    fn set_ongoing_op(&mut self, op: Option<Operation>) { self.ongoing_op = op; }

    pub fn ongoing_op(&self) -> Option<&Operation> { self.ongoing_op.as_ref() }

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

    pub(super) fn set_predicted_right(&mut self, predicted_right: f64) { self.predicted_right = predicted_right; }

    pub(super) fn predicted_right(&self) -> f64 { self.predicted_right }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn traded_price_right(&self) -> f64 { self.traded_price_right }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn traded_price_left(&self) -> f64 { self.traded_price_left }

    pub(super) fn set_pnl(&mut self) { self.pnl = self.value_strat; }

    pub(super) fn pnl(&self) -> f64 { self.pnl }

    pub(super) fn set_beta(&mut self, beta: f64) { self.beta_val = beta; }

    pub(super) fn beta(&self) -> f64 { self.beta_val }

    pub(super) fn set_res(&mut self, res: f64) { self.res = res; }

    pub(super) fn res(&self) -> f64 { self.res }

    pub(super) fn set_alpha(&mut self, alpha: f64) { self.alpha_val = alpha; }

    pub(super) fn alpha(&self) -> f64 { self.alpha_val }

    #[cfg(test)]
    pub(super) fn value_strat(&self) -> f64 { self.value_strat }

    pub(super) fn set_beta_lr(&mut self) { self.beta_lr = self.beta_val; }

    pub(super) fn beta_lr(&self) -> f64 { self.beta_lr }

    pub(super) fn nominal_position(&self) -> f64 { self.nominal_position }

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

    pub(super) fn short_position_return(&self) -> f64 { self.short_position_return }

    pub(super) fn long_position_return(&self) -> f64 { self.long_position_return }

    fn make_operation(
        &self,
        pos: Position,
        op_kind: OperationKind,
        spread: f64,
        right_coef: f64,
        left_coef: f64,
    ) -> Operation {
        let (left_op, right_op) = match (&pos.kind, &op_kind) {
            (PositionKind::Short, OperationKind::Open) => (TradeKind::Buy, TradeKind::Sell),
            (PositionKind::Long, OperationKind::Open) => (TradeKind::Sell, TradeKind::Buy),
            (PositionKind::Short, OperationKind::Close) => (TradeKind::Sell, TradeKind::Buy),
            (PositionKind::Long, OperationKind::Close) => (TradeKind::Buy, TradeKind::Sell),
        };
        Operation {
            id: format!("{}:{}", OPERATIONS_KEY, Uuid::new_v4()),
            pos: pos.clone(),
            kind: op_kind,
            left_coef,
            right_coef,
            left_transaction: None,
            right_transaction: None,
            left_order: None,
            right_order: None,
            left_trade: TradeOperation {
                id: Some(TradeOperation::new_id()),
                price: pos.left_price,
                qty: spread * self.beta_val,
                pair: pos.left_pair,
                kind: left_op,
                dry_mode: self.dry_mode,
                mode: OrderMode::Limit,
                asset_type: AssetType::Spot,
                side_effect: None,
            },
            right_trade: TradeOperation {
                id: Some(TradeOperation::new_id()),
                price: pos.right_price,
                qty: spread,
                pair: pos.right_pair,
                kind: right_op,
                dry_mode: self.dry_mode,
                mode: OrderMode::Limit,
                asset_type: AssetType::Spot,
                side_effect: None,
            },
        }
    }

    fn clear_ongoing_operation(&mut self) {
        if let Some(Operation {
            kind: OperationKind::Close,
            ..
        }) = self.ongoing_op
        {
            self.set_pnl();
            self.clear_position();
        }
        self.set_ongoing_op(None);
        self.save();
    }

    pub(super) async fn resolve_pending_operations(&mut self, ongoing_op: &Operation) -> Result<OrderResolution> {
        if ongoing_op.left_order.is_none() || ongoing_op.right_order.is_none() {
            return Err(Error::NoTransactionInOperation);
        }
        let left_order = ongoing_op.left_order.as_ref().unwrap();
        let right_order = ongoing_op.right_order.as_ref().unwrap();
        let (olr, orr) = futures::future::join(
            self.ts.resolve_pending_order(left_order),
            self.ts.resolve_pending_order(right_order),
        )
        .await;
        let olr = olr?;
        let orr = orr?;
        if olr.2 == OrderResolution::Filled && orr.2 == OrderResolution::Filled {
            info!("Both transactions filled for {}", &ongoing_op.id);
            self.clear_ongoing_operation();
            return Ok(OrderResolution::Filled);
        }
        if olr.2 == OrderResolution::BadRequest
            || olr.2 == OrderResolution::Rejected
            || orr.2 == OrderResolution::BadRequest
            || orr.2 == OrderResolution::Rejected
        {
            let mut new_op = ongoing_op.clone();
            new_op.left_order = Some(olr.0);
            new_op.left_transaction = olr.1;
            new_op.right_order = Some(orr.0);
            new_op.right_transaction = orr.1;
            self.set_ongoing_op(Some(new_op.clone()));
            self.save_operation(&new_op);
            self.stop_trading();
            Ok(OrderResolution::Rejected)
        } else if olr.2 == OrderResolution::Retryable || orr.2 == OrderResolution::Retryable {
            self.cancel_ongoing_op();
            Ok(OrderResolution::Retryable)
        } else {
            self.stop_trading();
            Ok(OrderResolution::Rejected)
        }
    }

    pub(super) async fn open(&mut self, pos: Position, fees: f64) -> Operation {
        let position_kind = pos.kind.clone();
        self.set_position(position_kind.clone());
        self.nominal_position = self.beta_val;
        self.traded_price_right = pos.right_price;
        self.traded_price_left = pos.left_price;
        let (spread, right_coef, left_coef) = match position_kind {
            PositionKind::Short => {
                let (spread, right_coef, left_coef) = (self.units_to_buy_short_spread, 1.0 - fees, 1.0 + fees);
                self.value_strat +=
                    spread * (pos.right_price * right_coef - pos.left_price * self.beta_val * left_coef);
                (spread, right_coef, left_coef)
            }
            PositionKind::Long => {
                let (spread, right_coef, left_coef) = (self.units_to_buy_long_spread, 1.0 + fees, 1.0 - fees);
                self.value_strat +=
                    spread * (pos.left_price * self.beta_val * left_coef - pos.right_price * right_coef);
                (spread, right_coef, left_coef)
            }
        };
        let mut op = self.make_operation(pos, OperationKind::Open, spread, right_coef, left_coef);
        op.log();
        self.stage_operation(&mut op).await;
        self.set_ongoing_op(Some(op.clone()));
        self.save();
        self.log_indicators(&position_kind);
        op
    }

    pub(super) async fn close(&mut self, pos: Position, fees: f64) -> Operation {
        let kind: PositionKind = pos.kind.clone();
        let (spread, right_coef, left_coef) = match kind {
            PositionKind::Short => {
                let (spread, right_coef, left_coef) = (self.units_to_buy_short_spread, 1.0 + fees, 1.0 - fees);
                self.value_strat +=
                    spread * (pos.left_price * self.beta_val * left_coef - pos.right_price * right_coef);
                (spread, right_coef, left_coef)
            }
            PositionKind::Long => {
                let (spread, right_coef, left_coef) = (self.units_to_buy_long_spread, 1.0 - fees, 1.0 + fees);
                self.value_strat +=
                    spread * (pos.right_price * right_coef - pos.left_price * self.beta_val * left_coef);
                (spread, right_coef, left_coef)
            }
        };
        let mut op = self.make_operation(pos, OperationKind::Close, spread, right_coef, left_coef);
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
        self.traded_price_left = 0.0;
        self.traded_price_right = 0.0;
    }

    fn save_operation(&mut self, op: &Operation) {
        if let Err(e) = self.db.put(OPERATIONS_KEY, &op.id, op.clone()) {
            error!("Error saving operation: {:?}", e);
        }
    }
    async fn stage_operation(&mut self, op: &mut Operation) {
        self.save_operation(op);
        let reqs: (Result<OrderDetail>, Result<OrderDetail>) = futures::future::join(
            self.ts.stage_trade(&op.left_trade).err_into(),
            self.ts.stage_trade(&op.right_trade).err_into(),
        )
        .await;

        op.left_order = reqs.0.ok();
        op.right_order = reqs.1.ok();
        self.save_operation(op);
        match (&op.left_order, &op.right_order) {
            (None, _) | (_, None) => error!("Failed transaction"),
            _ => trace!("Transaction ok"),
        }
    }

    pub(super) fn update_spread(&mut self, row: &DualBookPosition) {
        self.units_to_buy_long_spread = self.value_strat / row.right.ask;
        self.units_to_buy_short_spread = self.value_strat / (row.left.ask * self.beta());
    }

    fn save(&mut self) {
        if let Err(e) = self.db.put(STATE_KEY, STATE_KEY, TransientState {
            units_to_buy_short_spread: self.units_to_buy_short_spread,
            units_to_buy_long_spread: self.units_to_buy_long_spread,
            value_strat: self.value_strat,
            pnl: self.pnl,
            traded_price_left: self.traded_price_left,
            traded_price_right: self.traded_price_right,
            nominal_position: Some(self.nominal_position),
            ongoing_op: self.ongoing_op.as_ref().map(|o| o.id.clone()),
        }) {
            error!("Error saving state: {:?}", e);
        }
    }

    pub fn change_state(&mut self, field: MutableField, v: f64) -> Result<()> {
        match field {
            MutableField::ValueStrat => self.value_strat = v,
            MutableField::Pnl => self.pnl = v,
        }
        self.save();
        Ok(())
    }

    pub fn get_operations(&self) -> Vec<Operation> {
        self.db
            .get_ranged(OPERATIONS_KEY, OPERATIONS_KEY)
            .unwrap_or_else(|_| Vec::new())
    }

    #[allow(dead_code)]
    fn get_operation(&self, uuid: &str) -> Option<Operation> {
        self.db
            .get(OPERATIONS_KEY, &format!("{}:{}", OPERATIONS_KEY, uuid))
            .ok()
    }

    fn log_indicators(&self, pos: &PositionKind) {
        info!(
            "Additional info : units {:.2} beta val {:.2} value strat {}, return {}, res {}, pnl {}",
            match pos {
                PositionKind::Short => self.units_to_buy_short_spread,
                PositionKind::Long => self.units_to_buy_long_spread,
            },
            self.beta_val,
            self.value_strat,
            match pos {
                PositionKind::Short => self.short_position_return,
                PositionKind::Long => self.long_position_return,
            },
            self.res(),
            self.pnl(),
        );
    }

    pub fn stop_trading(&mut self) { self.is_trading = false; }

    pub fn resume_trading(&mut self) { self.is_trading = true; }

    pub fn is_trading(&mut self) -> bool { self.is_trading }
}
