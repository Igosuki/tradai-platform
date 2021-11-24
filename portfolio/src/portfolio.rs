use chrono::{DateTime, Utc};
use coinnect_rt::prelude::TradeType;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

use coinnect_rt::types::{AddOrderRequest, MarketEventEnvelope};
use db::{Storage, StorageExt};
use ext::ResultExt;
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

pub type PositionKey = (String, String);

#[derive(Debug, Serialize, Deserialize)]
pub struct PositionLock {
    at: DateTime<Utc>,
    order_id: String,
}

/// A [`Portfolio`] has real time access to accounts, and keeps track of PnL,
/// value, and positions.
/// [`TradeSignal`]s typically go through the [`Portfolio`] to determine whether or not they
/// can be converted into an order after assessing allocation and risk.
#[derive(Debug)]
pub struct Portfolio {
    value: f64,
    pnl: f64,
    positions: BTreeMap<PositionKey, Position>,
    locks: BTreeMap<PositionKey, PositionLock>,
    key: String,
    repo: Arc<dyn PortfolioRepo>,
    risk: Arc<dyn RiskEvaluator>,
    risk_threshold: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct PortfolioVars {
    value: f64,
    pnl: f64,
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
            locks: Default::default(),
        }
    }

    fn vars(&self) -> PortfolioVars {
        PortfolioVars {
            value: self.value,
            pnl: self.pnl,
        }
    }

    /// Convert the signal to a request if it passes checks and no locks exist for the target market
    /// Sets a lock for the market, which can be removed by filling the order
    pub fn maybe_convert(&mut self, signal: TradeSignal) -> Result<Option<AddOrderRequest>> {
        // Determine whether position can be opened or closed
        let pos_key = signal.xch_and_pair();
        if self.locks.get(&pos_key).is_some() {
            return Ok(None);
        }
        let request: AddOrderRequest = if let Some(p) = self.positions.get(&pos_key) {
            if signal.operation_kind.is_close() && p.is_opened() {
                signal.into()
            } else {
                return Ok(None);
            }
        } else if signal.operation_kind.is_open() {
            signal.into()
        } else {
            return Ok(None);
        };
        // TODO: Check that cash can be provisionned for pair, this should be compatible with margin trading multiplers
        if self.risk.evaluate(self, &request) > self.risk_threshold {
            return Ok(None);
        }
        let lock = PositionLock {
            at: Utc::now(),
            order_id: request.order_id.clone(),
        };
        self.repo.set_lock(&pos_key, &lock);
        self.locks.insert(pos_key, lock);
        Ok(Some(request))
    }

    /// Update the position from an order, closing or opening with the wrong side and kind will
    /// result in error. The lock will be released if the order is filled
    pub fn update_position(&mut self, order: OrderDetail) -> Result<()> {
        let pos_key: PositionKey = (order.exchange.clone(), order.pair.clone());
        {
            if let Some(pos) = self.positions.get_mut(&pos_key) {
                // Close
                if matches!(
                    (pos.kind, order.side),
                    (PositionKind::Short, TradeType::Buy) | (PositionKind::Long, TradeType::Sell)
                ) {
                    if order.is_filled() {
                        match pos.kind {
                            PositionKind::Short => self.value -= order.quote_value(),
                            PositionKind::Long => self.value += order.realized_quote_value(),
                        }
                        self.pnl = self.value;
                    }
                    pos.close(self.value, &order);
                } else {
                    return Ok(());
                }
            } else {
                // Open
                if order.is_filled() {
                    let pos = Position::open(&order);
                    let kind = pos.kind;
                    self.positions.insert(pos_key.clone(), pos);
                    if matches!(
                        (kind, order.side),
                        (PositionKind::Short, TradeType::Sell) | (PositionKind::Long, TradeType::Buy)
                    ) {
                        match kind {
                            PositionKind::Short => self.value += order.realized_quote_value(),
                            PositionKind::Long => self.value -= order.quote_value(),
                        }
                    } else {
                        return Ok(());
                    }
                }
            }
        }
        match self.positions.entry(pos_key.clone()) {
            Entry::Vacant(_) => {}
            Entry::Occupied(pos_entry) => {
                let pos = pos_entry.get();
                // Archive position, release lock and update vars
                self.repo.put_position(pos)?;
                self.repo.release_lock(&pos_key)?;
                if pos.is_closed() {
                    pos_entry.remove();
                }
                self.repo.update_vars(self)?;
            }
        }
        Ok(())
    }

    pub fn update_from_market(&mut self, event: MarketEventEnvelope) {
        if let Some(p) = self.positions.get_mut(&(event.xch.to_string(), event.pair.to_string())) {
            p.update(event);
        }
    }
}

/// Repository to handle portoflio persistence
pub trait PortfolioRepo: Debug + Send + Sync {
    /// Save a position
    fn put_position(&self, pos: &Position) -> Result<()>;
    /// Get a position
    fn get_position(&self, pos_id: Uuid) -> Result<Option<Position>>;
    /// Delet ea position
    fn delete_position(&self, pos_id: Uuid) -> Result<()>;
    /// Set a lock on a position
    fn set_lock(&self, key: &PositionKey, locK: &PositionLock) -> Result<()>;
    /// Release a position lock
    fn release_lock(&self, key: &PositionKey) -> Result<()>;
    /// Update portfolio variables
    fn update_vars(&self, _: &Portfolio) -> Result<()>;
    /// Load a portfolio from storage
    fn load(&self, _: &mut Portfolio) -> Result<Option<Portfolio>>;
}

static POSITIONS_TABLE: &str = "positions";
static OPEN_POSITIONS_TABLE: &str = "open_positions";
static POSITION_LOCKS_TABLE: &str = "locks";
static PORTFOLIO_VARS: &str = "vars";

#[derive(Debug)]
pub struct PersistentPortfolio {
    db: Arc<dyn Storage>,
}

impl PersistentPortfolio {
    fn new(db: Arc<dyn Storage>) -> Self {
        for table in &[POSITION_LOCKS_TABLE, POSITIONS_TABLE, PORTFOLIO_VARS] {
            db.ensure_table(table).unwrap();
        }
        Self { db }
    }

    fn key_string(key: &PositionKey) -> String { format!("{}_{}", key.0, key.1) }
}

impl PortfolioRepo for PersistentPortfolio {
    fn put_position(&self, pos: &Position) -> Result<()> {
        self.db.put(POSITIONS_TABLE, pos.id.as_bytes(), pos).err_into()
    }

    fn get_position(&self, pos_id: Uuid) -> Result<Option<Position>> {
        self.db.get(POSITIONS_TABLE, pos_id.as_bytes()).err_into()
    }

    fn delete_position(&self, pos_id: Uuid) -> Result<()> {
        self.db.delete(POSITIONS_TABLE, pos_id.as_bytes()).err_into()
    }

    fn set_lock(&self, key: &PositionKey, lock: &PositionLock) -> Result<()> {
        self.db
            .put(POSITION_LOCKS_TABLE, Self::key_string(key), lock)
            .err_into()
    }

    fn release_lock(&self, key: &PositionKey) -> Result<()> {
        self.db.delete(POSITION_LOCKS_TABLE, Self::key_string(key)).err_into()
    }

    fn update_vars(&self, p: &Portfolio) -> Result<()> {
        let vars = p.vars();
        self.db.put(PORTFOLIO_VARS, &p.key, vars).err_into()
    }

    fn load(&self, _: &mut Portfolio) -> Result<Option<Portfolio>> { todo!() }
}
