use chrono::{DateTime, Utc};
use coinnect_rt::prelude::{Exchange, TradeType};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use coinnect_rt::types::{AddOrderRequest, MarketEventEnvelope, Pair};
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

pub type PositionKey = (Exchange, Pair);

#[derive(Debug, Serialize, Deserialize)]
pub struct PositionLock {
    at: DateTime<Utc>,
    order_id: String,
}

/// A [`Portfolio`] has real time access to accounts, and keeps track of PnL,
/// value, and positions.
/// [`TradeSignal`]s typically go through the [`Portfolio`] to determine whether or not they
/// can be converted into an order after assessing allocation and risk.
/// A typical workflow is the following :
/// - Receive a TradeSignal
/// - Maybe emit an order and lock the position
/// - When the order is filled, unlock the position and set it to open or closed accordingly
/// - When closing, update the position and indicators
/// - When receiving a market event, update the position temporary values such as the return
/// The portfolio can also be queries for already open positions so as to not open the same position twice
#[derive(Debug)]
pub struct Portfolio {
    value: f64,
    pnl: f64,
    open_positions: BTreeMap<PositionKey, Position>,
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

impl Portfolio {
    fn try_new(
        initial_value: f64,
        key: String,
        repo: Arc<dyn PortfolioRepo>,
        risk: Arc<dyn RiskEvaluator>,
    ) -> Result<Self> {
        let mut p = Self {
            value: initial_value,
            pnl: initial_value,
            key,
            repo,
            open_positions: Default::default(),
            risk,
            risk_threshold: 0.5,
            locks: Default::default(),
        };
        {
            let arc = p.repo.clone();
            arc.load(&mut p)?;
        }
        Ok(p)
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
        if self.is_locked(&pos_key) {
            return Ok(None);
        }
        let request: AddOrderRequest = if let Some(p) = self.open_positions.get(&pos_key) {
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
        self.repo.set_lock(&pos_key, &lock)?;
        self.locks.insert(pos_key, lock);
        Ok(Some(request))
    }

    /// Update the position from an order, closing or opening with the wrong side and kind will
    /// result in error. The lock will be released if the order is filled
    pub fn update_position(&mut self, order: OrderDetail) -> Result<()> {
        let pos_key: PositionKey = (Exchange::from_str(&order.exchange).unwrap(), order.pair.clone().into());
        {
            if let Some(pos) = self.open_positions.get_mut(&pos_key) {
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
                    return Err(Error::BadSideForPosition("close", pos.kind, order.side));
                }
            } else {
                // Open
                if order.is_filled() {
                    let pos = Position::open(&order);
                    let kind = pos.kind;
                    self.open_positions.insert(pos_key.clone(), pos);
                    if matches!(
                        (kind, order.side),
                        (PositionKind::Short, TradeType::Sell) | (PositionKind::Long, TradeType::Buy)
                    ) {
                        match kind {
                            PositionKind::Short => self.value += order.realized_quote_value(),
                            PositionKind::Long => self.value -= order.quote_value(),
                        }
                    } else {
                        return Err(Error::BadSideForPosition("open", kind, order.side));
                    }
                }
            }
        }
        if let Entry::Occupied(pos_entry) = self.open_positions.entry(pos_key.clone()) {
            let pos = pos_entry.get();
            // Archive position, release lock and update vars
            self.repo.put_position(pos)?;
            if pos.is_closed() {
                pos_entry.remove();
            }
            self.remove_lock(&pos_key)?;
            self.repo.update_vars(self)?;
        }
        Ok(())
    }

    /// Update the corresponding position with the latest event (typically the price)
    pub fn update_from_market(&mut self, event: MarketEventEnvelope) {
        if let Some(p) = self.open_positions.get_mut(&(event.xch, event.pair.clone())) {
            p.update(event);
        }
    }

    /// True if there is an open position
    pub fn has_open_position(&self, xch: Exchange, pair: Pair) -> bool {
        self.open_positions.get(&(xch, pair)).is_some()
    }

    /// True if there are any open positions
    pub fn has_any_open_position(&self) -> bool { !self.open_positions.is_empty() }

    /// Unlock a previously locked position
    pub fn unlock_position(&mut self, xch: Exchange, pair: Pair) -> Result<()> {
        let position_key = (xch, pair);
        match self.locks.get(&position_key) {
            None => Ok(()),
            Some(_) => {
                self.remove_lock(&position_key)?;
                if let Some(pos) = self.open_positions.get(&position_key) {
                    if pos.is_failed_open() {
                        self.repo.close_position(pos)?;
                        self.open_positions.remove(&position_key);
                    }
                }
                Ok(())
            }
        }
    }

    /// Force close a currently open position
    pub fn force_close(&mut self, xch: Exchange, pair: Pair) -> Result<()> {
        let position_key = (xch, pair);
        match self.open_positions.get(&position_key) {
            Some(pos) if !self.is_locked(&position_key) && pos.is_opened() && !pos.is_closed() => {
                unimplemented!();
            }
            None => Err(Error::NoPositionFound),
            _ => Err(Error::PositionLocked),
        }
    }

    fn is_locked(&self, key: &PositionKey) -> bool { self.locks.get(key).is_some() }

    fn remove_lock(&mut self, key: &PositionKey) -> Result<()> {
        self.locks.remove(key);
        self.repo.release_lock(key)
    }
}

/// Repository to handle portoflio persistence
pub trait PortfolioRepo: Debug + Send + Sync {
    /// Open a position
    fn open_position(&self, pos: &Position) -> Result<()>;
    /// Close a position
    fn close_position(&self, pos: &Position) -> Result<()>;
    /// Is this position open
    fn is_open(&self, pos_id: &Uuid) -> Result<bool>;
    /// Save a position
    fn put_position(&self, pos: &Position) -> Result<()>;
    /// Get a position
    fn get_position(&self, pos_id: Uuid) -> Result<Option<Position>>;
    /// Get all positions
    fn all_positions(&self) -> Result<Vec<Position>>;
    /// Delete a position
    fn delete_position(&self, pos_id: Uuid) -> Result<()>;
    /// Set a lock on a position
    fn set_lock(&self, key: &PositionKey, lock: &PositionLock) -> Result<()>;
    /// Release a position lock
    fn release_lock(&self, key: &PositionKey) -> Result<()>;
    /// Update portfolio variables
    fn update_vars(&self, _: &Portfolio) -> Result<()>;
    /// Load a portfolio from storage
    fn load(&self, _: &mut Portfolio) -> Result<()>;
}

static POSITIONS_ARCHIVE_TABLE: &str = "positions";
static OPEN_POSITIONS_TABLE: &str = "open_positions";
static POSITION_LOCKS_TABLE: &str = "locks";
static PORTFOLIO_VARS: &str = "vars";

#[derive(Debug)]
pub struct PersistentPortfolio {
    db: Arc<dyn Storage>,
}

impl PersistentPortfolio {
    fn new(db: Arc<dyn Storage>) -> Self {
        for table in &[POSITION_LOCKS_TABLE, POSITIONS_ARCHIVE_TABLE, PORTFOLIO_VARS] {
            db.ensure_table(table).unwrap();
        }
        Self { db }
    }

    fn key_string(key: &PositionKey) -> String { format!("{}_{}", key.0, key.1) }

    fn parse_key_string(key: &str) -> PositionKey {
        let (xch, pair) = key.split_once('_').unwrap();
        (Exchange::from_str(xch).unwrap(), Pair::from(pair))
    }
}

impl PortfolioRepo for PersistentPortfolio {
    fn open_position(&self, pos: &Position) -> Result<()> {
        self.put_position(pos)?;
        self.db.put(OPEN_POSITIONS_TABLE, pos.id.as_bytes(), true).err_into()
    }

    fn close_position(&self, pos: &Position) -> Result<()> {
        self.put_position(pos)?;
        self.db.delete(OPEN_POSITIONS_TABLE, pos.id.as_bytes()).err_into()
    }

    fn is_open(&self, pos_id: &Uuid) -> Result<bool> {
        let is_open: Option<bool> = self.db.get(OPEN_POSITIONS_TABLE, pos_id.as_bytes())?;
        Ok(is_open.is_some())
    }

    fn put_position(&self, pos: &Position) -> Result<()> {
        self.db.put(POSITIONS_ARCHIVE_TABLE, pos.id.as_bytes(), pos).err_into()
    }

    fn get_position(&self, pos_id: Uuid) -> Result<Option<Position>> {
        self.db.get(POSITIONS_ARCHIVE_TABLE, pos_id.as_bytes()).err_into()
    }

    fn all_positions(&self) -> Result<Vec<Position>> {
        Ok(self
            .db
            .get_all::<Position>(POSITIONS_ARCHIVE_TABLE)?
            .into_iter()
            .map(|(_, pos)| pos)
            .collect())
    }

    fn delete_position(&self, pos_id: Uuid) -> Result<()> {
        self.db.delete(POSITIONS_ARCHIVE_TABLE, pos_id.as_bytes()).err_into()
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

    fn load(&self, p: &mut Portfolio) -> Result<()> {
        let maybe_vars: Option<PortfolioVars> = self.db.get(PORTFOLIO_VARS, &p.key)?;
        if let Some(vars) = maybe_vars {
            p.pnl = vars.pnl;
            p.value = vars.value;
        }
        for (pos_id, _) in self.db.get_all::<bool>(OPEN_POSITIONS_TABLE)? {
            let pos_id = Uuid::from_slice(pos_id.as_bytes())?;
            if let Some(pos) = self.get_position(pos_id)? {
                error!("failed to retrieve open position {} from storage", pos_id);
                p.open_positions.insert((pos.exchange, pos.symbol.clone()), pos);
            }
        }
        p.locks.extend(
            self.db
                .get_all(POSITION_LOCKS_TABLE)?
                .into_iter()
                .map(|(k, v)| (Self::parse_key_string(k.as_str()), v)),
        );
        Ok(())
    }
}
