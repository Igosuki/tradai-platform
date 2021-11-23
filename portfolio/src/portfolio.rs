use std::collections::BTreeMap;

use coinnect_rt::types::{OrderQuery, Pair};
use trading::order_manager::types::OrderDetail;
use trading::position::Position;
use trading::signal::TradeSignal;

use crate::error::*;

/// Determines how to handle multiple positions
pub enum MarketLockRule {
    /// Portfolio is considered to have no position when all positions are closed
    Coupled,
    /// Portfolio can open a position on market A even if market B still has an open position
    Decoupled,
}

/// A [`Portfolio`] has real time access to accounts, and keeps track of PnL,
/// value, and positions.
/// [`TradeSignal`]s typically go through the [`Portfolio`] to determine whether or not they
/// can be converted into an order after assessing allocation and risk.
#[derive(Debug)]
pub struct Portfolio {
    value: f64,
    pnl: f64,
    positions: BTreeMap<Pair, Position>,
}

/// Workflow :
/// Receive a TradeSignal
/// If decide to do an order -> lock the position
/// When fill happens -> update the position
/// When close, update the position, update the indicators and unlock
impl Portfolio {
    /// If no position taken for market and risk and provisioning pass, emit and order query and
    /// set a lock for the position
    fn maybe_convert(&self, signal: TradeSignal) -> Result<Option<OrderQuery>> { Ok(None) }

    /// Check position for the market, update accordingly
    fn update_position(&self, order: OrderDetail) {}
}
