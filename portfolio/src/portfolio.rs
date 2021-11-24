use coinnect_rt::prelude::TradeType;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use coinnect_rt::types::AddOrderRequest;
use db::Storage;
use trading::order_manager::types::OrderDetail;
use trading::position::{Position, PositionKind};
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
    positions: BTreeMap<(String, String), Position>,
    key: String,
    repo: Arc<dyn PortfolioRepo>,
    risk: Arc<dyn RiskEvaluator>,
    risk_threshold: f64,
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
            risk_threshold: 0.5,
        }
    }

    /// If no position taken for market and risk and provisioning pass, emit and order query and
    /// set a lock for the position
    fn maybe_convert(&mut self, signal: TradeSignal) -> Result<Option<AddOrderRequest>> {
        // Determine whether position can be opened or closed
        let request: AddOrderRequest = if let Some(p) = self.positions.get(&signal.xch_and_pair()) {
            if signal.operation_kind.is_open() && p.is_closed() || signal.operation_kind.is_close() && p.is_opened() {
                signal.into()
            } else {
                return Ok(None);
            }
        } else if signal.operation_kind.is_open() {
            let open_pos = Position { ..Position::default() };
            self.positions.insert(signal.xch_and_pair(), open_pos);
            signal.into()
        } else {
            return Ok(None);
        };
        // TODO: Check that cash can be provisionned for pair, this should be compatible with margin trading multiplers
        if self.risk.evaluate(self, &request) > self.risk_threshold {
            return Ok(None);
        }
        Ok(Some(request))
    }

    /// Check position for the market, update accordingly
    fn update_position(&mut self, order: OrderDetail) {
        if let Some(pos) = self.positions.get_mut(&(order.exchange.clone(), order.pair.clone())) {
            match (pos.kind, order.side) {
                // Open
                (PositionKind::Short, TradeType::Sell) | (PositionKind::Long, TradeType::Buy) => {
                    if order.is_filled() {
                        match pos.kind {
                            PositionKind::Short => self.value += order.realized_quote_value(),
                            PositionKind::Long => {
                                self.value -= order.quote_value();
                            }
                        }
                    }
                    pos.open_order = Some(order);
                }
                // Close
                (PositionKind::Short, TradeType::Buy) | (PositionKind::Long, TradeType::Sell) => {
                    if order.is_filled() {
                        match pos.kind {
                            PositionKind::Short => self.value -= order.quote_value(),
                            PositionKind::Long => self.value += order.realized_quote_value(),
                        }
                        self.pnl = self.value;
                    }
                    pos.close_order = Some(order);
                }
            }
            self.repo.put_position(pos);
            self.repo.update_vars(self);
        }
    }
}

pub trait PortfolioRepo: Debug + Send + Sync {
    fn put_position(&self, pos: &Position) -> Result<()>;
    fn get_position(&self, pos_id: String) -> Result<Option<Position>>;
    fn delete_position(&self, pos_id: String) -> Result<Option<Position>>;
    fn update_vars(&self, _: &Portfolio) -> Result<()>;
    fn load(&self, _: &Portfolio) -> Result<Option<Portfolio>>;
}

#[derive(Debug)]
pub struct PersistentPortfolio {
    db: Arc<dyn Storage>,
}

impl PortfolioRepo for PersistentPortfolio {
    fn put_position(&self, _pos: &Position) -> Result<()> { todo!() }

    fn get_position(&self, _pos_id: String) -> Result<Option<Position>> { todo!() }

    fn delete_position(&self, _pos_id: String) -> Result<Option<Position>> { todo!() }

    fn update_vars(&self, _: &Portfolio) -> Result<()> { todo!() }

    fn load(&self, _: &Portfolio) -> Result<Option<Portfolio>> { todo!() }
}
