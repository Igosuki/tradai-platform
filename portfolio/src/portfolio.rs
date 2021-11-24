use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use coinnect_rt::types::{OrderQuery, Pair};
use db::Storage;
use trading::order_manager::types::OrderDetail;
use trading::position::Position;
use trading::signal::TradeSignal;

use crate::error::*;
use crate::risk::RiskEvaluator;

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
    key: String,
    repo: Arc<dyn PortfolioRepo>,
    risk: Arc<dyn RiskEvaluator>,
}

/// Workflow :
/// Receive a TradeSignal
/// If decide to do an order -> lock the position
/// When fill happens -> update the position
/// When close, update the position, update the indicators and unlock
impl Portfolio {
    fn try_new(initial_value: f64, key: String, repo: Arc<dyn PortfolioRepo>, risk: Arc<dyn RiskEvaluator>) -> Self {
        Self {
            value: initial_value,
            pnl: initial_value,
            key,
            repo,
            positions: Default::default(),
            risk,
        }
    }

    /// If no position taken for market and risk and provisioning pass, emit and order query and
    /// set a lock for the position
    fn maybe_convert(&self, signal: TradeSignal) -> Result<Option<OrderQuery>> { Ok(None) }

    /// Check position for the market, update accordingly
    fn update_position(&self, order: OrderDetail) {}
}

pub trait PortfolioRepo: Debug + Send + Sync {
    fn put_position(&self, pos: Position) -> Result<()>;
    fn get_position(&self, pos: Position) -> Result<Option<Position>>;
    fn delete_position(&self, pos: Position) -> Result<Option<Position>>;
    fn update_vars(&self, _: &Portfolio) -> Result<()>;
    fn load(&self, _: &Portfolio) -> Result<Option<Portfolio>>;
}

#[derive(Debug)]
pub struct PersistentPortfolio {
    db: Arc<dyn Storage>,
}

impl PortfolioRepo for PersistentPortfolio {
    fn put_position(&self, pos: Position) -> Result<()> { todo!() }

    fn get_position(&self, pos: Position) -> Result<Option<Position>> { todo!() }

    fn delete_position(&self, pos: Position) -> Result<Option<Position>> { todo!() }

    fn update_vars(&self, _: &Portfolio) -> Result<()> { todo!() }

    fn load(&self, _: &Portfolio) -> Result<Option<Portfolio>> { todo!() }
}
