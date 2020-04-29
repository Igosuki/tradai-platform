use actix::Message;
use chrono::{DateTime, Utc};
use log::Level::Debug;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};

const TS_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Debug, Deserialize, Serialize, juniper::GraphQLObject)]
pub struct Position {
    pub kind: PositionKind,
    pub right_price: f64,
    pub left_price: f64,
    pub time: DateTime<Utc>,
    pub right_pair: String,
    pub left_pair: String,
}

#[derive(Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
pub enum PositionKind {
    #[strum(serialize = "short")]
    SHORT,
    #[strum(serialize = "long")]
    LONG,
}

#[derive(Debug, Deserialize, Serialize, EnumString, AsRefStr)]
pub(super) enum Operation {
    #[strum(serialize = "open")]
    OPEN,
    #[strum(serialize = "close")]
    CLOSE,
    #[strum(serialize = "buy")]
    BUY,
    #[strum(serialize = "sell")]
    SELL,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    open_position: f64,
    nominal_position: f64,
    traded_price_right: f64,
    traded_price_left: f64,
    short_position_return: f64,
    long_position_return: f64,
    close_position: f64,
    pnl: f64,
}

impl MovingState {
    pub fn new(initial_value: f64) -> MovingState {
        MovingState {
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
            open_position: 0.0,
            nominal_position: 0.0,
            traded_price_right: 0.0,
            traded_price_left: 0.0,
            short_position_return: 0.0,
            long_position_return: 0.0,
            close_position: 0.0,
            pnl: 0.0,
        }
    }

    fn log_pos(&self, op: &Operation, pos: &PositionKind, time: DateTime<Utc>) {
        if log_enabled!(Debug) {
            debug!(
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

    fn log_trade(&self, op: Operation, spread: f64, pair: &str, value: f64, qty: f64) {
        if log_enabled!(Debug) {
            debug!(
                "{} {:.2} {} at {} for {:.2}",
                op.as_ref(),
                spread,
                pair,
                value,
                qty
            );
        }
    }

    fn log_info(&self, pos: &PositionKind) {
        if log_enabled!(Debug) {
            debug!(
                "Additional info : units {:.2} beta val {:.2} value strat {}",
                match pos {
                    PositionKind::SHORT => self.units_to_buy_short_spread,
                    PositionKind::LONG => self.units_to_buy_long_spread,
                },
                self.beta_val,
                self.value_strat
            );
            debug!("--------------------------------")
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

    fn log_position(
        &self,
        pos: &Position,
        op: Operation,
        spread: f64,
        right_coef: f64,
        left_coef: f64,
    ) {
        self.log_pos(&op, &pos.kind, pos.time);
        let (left_op, right_op) = match (&pos.kind, &op) {
            (PositionKind::SHORT, Operation::OPEN) => (Operation::BUY, Operation::SELL),
            (PositionKind::LONG, Operation::OPEN) => (Operation::SELL, Operation::BUY),
            (PositionKind::SHORT, Operation::CLOSE) => (Operation::SELL, Operation::BUY),
            (PositionKind::LONG, Operation::CLOSE) => (Operation::BUY, Operation::SELL),
            _ => unimplemented!(),
        };
        self.log_trade(
            right_op,
            spread,
            &pos.right_pair,
            pos.right_price,
            spread * pos.right_price * right_coef.abs(),
        );
        self.log_trade(
            left_op,
            spread * self.beta_val,
            &pos.left_pair,
            pos.left_price,
            spread * pos.left_price * left_coef.abs(),
        );
        self.log_info(&pos.kind);
    }

    pub(super) fn open(&mut self, pos: Position, fees: f64) {
        match pos.kind {
            PositionKind::SHORT => self.set_position_short(),
            PositionKind::LONG => self.set_position_long(),
        };
        self.open_position = 1e5;
        self.nominal_position = self.beta_val;
        self.traded_price_right = pos.right_price;
        self.traded_price_left = pos.left_price;
        let (spread, right_coef, left_coef) = match pos.kind {
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
        self.log_position(&pos, Operation::OPEN, spread, right_coef, left_coef);
    }

    pub(super) fn close(&mut self, pos: Position, fees: f64) {
        match pos.kind {
            PositionKind::SHORT => self.unset_position_short(),
            PositionKind::LONG => self.unset_position_long(),
        };
        self.close_position = 1e5;
        let (spread, right_coef, left_coef) = match pos.kind {
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
        self.log_position(&pos, Operation::CLOSE, spread, right_coef, left_coef);
    }
}
