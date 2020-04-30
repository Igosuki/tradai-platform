use chrono::{DateTime, Utc};
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

#[derive(Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
pub enum PositionKind {
    #[strum(serialize = "short")]
    SHORT,
    #[strum(serialize = "long")]
    LONG,
}

#[derive(Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct Operation {
    pub kind: OperationKind,
    pub pos: Position,
    pub left_op: OperationKind,
    pub right_op: OperationKind,
    pub left_spread: f64,
    pub right_spread: f64,
    pub left_coef: f64,
    pub right_coef: f64,
}

impl Operation {
    fn left_qty(&self) -> f64 {
        self.left_spread * self.pos.left_price * self.left_coef.abs()
    }

    fn right_qty(&self) -> f64 {
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

#[derive(Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
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

pub(crate) const OPERATIONS_KEY: &'static str = "orders";

pub(crate) const STATE_KEY: &'static str = "state";

#[derive(Clone, Debug, Serialize)]
pub(super) struct MovingState {
    position_short: i8,
    position_long: i8,
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
}

#[derive(Serialize, Deserialize)]
struct ValueStrat {
    value_strat: f64,
    units_to_buy_long_spread: f64,
    units_to_buy_short_spread: f64,
    pnl: f64,
}

impl MovingState {
    pub fn new(initial_value: f64, db: Db) -> MovingState {
        let mut state = MovingState {
            position_short: 0,
            position_long: 0,
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
            pnl: 0.0,
            db,
        };
        state.reload_state();
        state
    }

    fn reload_state(&mut self) {
        let mut ops: Vec<Operation> = self.db.read_json_vec(OPERATIONS_KEY);
        ops.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
        ops.last().map(|o| match o.kind {
            OperationKind::OPEN => match o.pos.kind {
                PositionKind::SHORT => self.set_position_short(),
                PositionKind::LONG => self.set_position_long(),
            },
            _ => {}
        });
        let previous_state: Option<ValueStrat> = self.db.read_json(STATE_KEY);
        if let Some(ps) = previous_state {
            self.set_units_to_buy_long_spread(ps.units_to_buy_long_spread);
            self.set_units_to_buy_short_spread(ps.units_to_buy_short_spread);
            self.value_strat = ps.value_strat;
            self.pnl = ps.pnl;
        }
    }

    pub(super) fn no_position_taken(&self) -> bool {
        self.position_short == 0 && self.position_long == 0
    }

    pub(super) fn is_long(&self) -> bool {
        self.position_long == 1
    }

    pub(super) fn is_short(&self) -> bool {
        self.position_short == 1
    }

    pub(super) fn set_position_short(&mut self) {
        self.position_short = 1;
    }

    pub(super) fn unset_position_short(&mut self) {
        self.position_short = 0;
    }

    pub(super) fn set_position_long(&mut self) {
        self.position_long = 1;
    }

    pub(super) fn unset_position_long(&mut self) {
        self.position_long = 0;
    }

    pub(super) fn set_predicted_right(&mut self, predicted_right: f64) {
        self.predicted_right = predicted_right;
    }

    pub(super) fn predicted_right(&self) -> f64 {
        self.predicted_right
    }

    pub(super) fn traded_price_right(&self) -> f64 {
        self.traded_price_right
    }

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

    fn get_long_position_value(
        &self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        return self.units_to_buy_long_spread
            * (self.traded_price_left * self.beta_val * (1.0 - fees_rate)
                - self.traded_price_right * (1.0 + fees_rate)
                + current_price_right * (1.0 - fees_rate)
                - current_price_left * self.beta_val * (1.0 + fees_rate));
    }

    pub(super) fn set_long_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) {
        self.long_position_return =
            self.get_long_position_value(fees_rate, current_price_right, current_price_left)
                / self.pnl;
    }

    pub fn get_short_position_value(
        &self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) -> f64 {
        return self.units_to_buy_short_spread
            * (self.traded_price_right * (1.0 - fees_rate)
                - self.traded_price_left * self.beta_val * (1.0 + fees_rate)
                + current_price_left * self.beta_val * (1.0 - fees_rate)
                - current_price_right * (1.0 + fees_rate));
    }

    pub(super) fn set_short_position_return(
        &mut self,
        fees_rate: f64,
        current_price_right: f64,
        current_price_left: f64,
    ) {
        self.short_position_return =
            self.get_short_position_value(fees_rate, current_price_right, current_price_left)
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
        }
    }

    pub(super) fn open(&mut self, pos: Position, fees: f64) -> Operation {
        let position_kind = pos.kind.clone();
        match position_kind {
            PositionKind::SHORT => self.set_position_short(),
            PositionKind::LONG => self.set_position_long(),
        };
        self.nominal_position = self.beta_val;
        self.traded_price_right = pos.right_price;
        self.traded_price_left = pos.left_price;
        let (spread, right_coef, left_coef) = match position_kind {
            PositionKind::SHORT => (
                self.units_to_buy_short_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
            PositionKind::LONG => (
                self.units_to_buy_long_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
        };
        self.value_strat += spread * (pos.right_price * right_coef + pos.left_price * left_coef);
        let op = self.make_operation(
            pos.clone(),
            OperationKind::OPEN,
            spread,
            right_coef,
            left_coef,
        );
        op.log();
        self.save_operation(&op);
        self.save();
        self.log_info(&position_kind);
        op
    }

    pub(super) fn close(&mut self, pos: Position, fees: f64) -> Operation {
        let kind: PositionKind = pos.kind.clone();
        match kind {
            PositionKind::SHORT => self.unset_position_short(),
            PositionKind::LONG => self.unset_position_long(),
        };
        let (spread, right_coef, left_coef) = match kind {
            PositionKind::SHORT => (
                self.units_to_buy_short_spread,
                -(1.0 + fees),
                self.beta_val * (1.0 - fees),
            ),
            PositionKind::LONG => (
                self.units_to_buy_long_spread,
                1.0 - fees,
                -(self.beta_val * (1.0 + fees)),
            ),
        };
        self.value_strat += spread * (pos.right_price * right_coef + pos.left_price * left_coef);
        let op = self.make_operation(pos, OperationKind::CLOSE, spread, right_coef, left_coef);
        op.log();
        self.save_operation(&op);
        self.save();
        self.log_info(&kind);
        op
    }

    fn save_operation(&self, op: &Operation) {
        self.db
            .put_json(&format!("{}:{}", OPERATIONS_KEY, Uuid::new_v4()), op);
    }

    fn save(&self) {
        self.db.put_json(
            STATE_KEY,
            ValueStrat {
                units_to_buy_short_spread: self.units_to_buy_short_spread,
                units_to_buy_long_spread: self.units_to_buy_long_spread,
                value_strat: self.value_strat,
                pnl: self.pnl,
            },
        );
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
