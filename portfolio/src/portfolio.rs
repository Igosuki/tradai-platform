use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use coinnect_rt::prelude::{Exchange, TradeType};
use coinnect_rt::types::{AddOrderRequest, MarketEventEnvelope, Pair};
use db::{Storage, StorageExt};
use ext::ResultExt;
use trading::interest::InterestRateProvider;
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

fn pos_key_from_order(order: &OrderDetail) -> Result<PositionKey> {
    Ok((Exchange::from_str(&order.exchange)?, order.pair.clone().into()))
}

fn pos_key_from_position(pos: &Position) -> PositionKey { (pos.exchange, pos.symbol.clone()) }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PositionLock {
    pub at: DateTime<Utc>,
    pub order_id: String,
}

/// A [`Portfolio`] has real time access to accounts, and keeps track of `PnL`,
/// value, and positions.
/// [`TradeSignal`]s typically go through the [`Portfolio`] to determine whether or not they
/// can be converted into an order after assessing allocation and risk.
/// A typical workflow is the following :
/// - Receive a [`TradeSignal`]
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
    interest_rates: Arc<dyn InterestRateProvider>,
    fees_rate: f64,
    risk_threshold: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PortfolioVars {
    value: f64,
    pnl: f64,
}

impl Portfolio {
    /// # Errors
    ///
    /// The portfolio repo fails to load existing data
    pub fn try_new(
        initial_value: f64,
        fees_rate: f64,
        key: String,
        repo: Arc<dyn PortfolioRepo>,
        risk: Arc<dyn RiskEvaluator>,
        interest_rates: Arc<dyn InterestRateProvider>,
    ) -> Result<Self> {
        let mut p = Self {
            value: initial_value,
            pnl: initial_value,
            key,
            repo,
            open_positions: BTreeMap::default(),
            risk,
            risk_threshold: 0.5,
            locks: BTreeMap::default(),
            interest_rates,
            fees_rate,
        };
        {
            let arc = p.repo.clone();
            arc.load(&mut p)?;
        }
        Ok(p)
    }

    pub fn vars(&self) -> PortfolioVars {
        PortfolioVars {
            value: self.value,
            pnl: self.pnl,
        }
    }

    /// Convert the signal to a request if it passes checks and no locks exist for the target market
    /// Sets a lock for the market, which can be removed by filling the order
    ///
    /// # Errors
    ///
    /// If the position is already locked, or the signal is incompatible with the existing state
    #[allow(clippy::missing_panics_doc)]
    pub async fn maybe_convert(&mut self, signal: &TradeSignal) -> Result<Option<AddOrderRequest>> {
        // Determine whether position can be opened or closed
        let pos_key = signal.xch_and_pair();
        if self.is_locked(&pos_key) {
            return Err(Error::PositionLocked);
        }
        // TODO: replace with allocator
        if self.pnl <= 0.0 {
            return Ok(None);
        }
        let mut request: AddOrderRequest = if let Some(p) = self.open_positions.get(&pos_key) {
            if signal.op_kind.is_close() {
                if p.is_opened() {
                    let interests = self.interest_fees_since_open(p.open_order.as_ref()).await?;
                    AddOrderRequest {
                        quantity: p.close_qty(self.fees_rate, interests),
                        ..signal.into()
                    }
                } else {
                    return Err(bad_signal(p, signal));
                }
            } else {
                return Err(bad_signal(p, signal));
            }
        } else if signal.op_kind.is_open() {
            signal.into()
        } else {
            return Err(Error::BadCloseSignal(signal.pos_kind));
        };
        // Default quantity allocation is portfolio value / price
        if request.quantity.is_none() {
            request.quantity = Some(self.value / signal.price);
        }
        if request.quantity.unwrap() <= 0.0 {
            return Err(Error::ZeroOrNegativeOrderQty);
        }
        // TODO: Check that cash can be provisionned for pair, this should be compatible with margin trading multiplers
        if self.risk.evaluate(self, &request) > self.risk_threshold {
            return Ok(None);
        }
        let lock = PositionLock {
            at: Utc::now(),
            order_id: request.order_id.clone(),
        };
        self.lock_position(pos_key, lock)?;
        Ok(Some(request))
    }

    /// Update the position from an order, closing or opening with the wrong side and kind will
    /// result in error. The lock will be released if the order is filled
    ///
    /// # Errors
    ///
    /// If a lock did not exist or is incompatible for a position corresponding to the order
    pub fn update_position(&mut self, order: &OrderDetail) -> Result<Option<Position>> {
        let pos_key: PositionKey = pos_key_from_order(order)?;
        // TODO: Using SQL could get rid of this, if performance allows
        if let Some(PositionLock { order_id, .. }) = self.locks.get(&pos_key) {
            if order_id != &order.id {
                return Err(Error::NoLockForOrder);
            }
        }
        if let Some(pos) = self.open_positions.get_mut(&pos_key) {
            // Close
            if matches!(
                (pos.kind, order.side),
                (PositionKind::Short, TradeType::Buy) | (PositionKind::Long, TradeType::Sell)
            ) && pos.is_opened()
            {
                let value_strat_before = self.value;
                pos.close(self.value, order);
                if order.is_filled() {
                    match pos.kind {
                        PositionKind::Short => self.value -= order.quote_value(),
                        PositionKind::Long => self.value += order.realized_quote_value(),
                    }
                }
                Self::log_position(order, value_strat_before, self.value, pos.kind, pos.quantity);
            } else {
                return Err(Error::BadSideForPosition("close", pos.kind, order.side));
            }
        } else {
            // Open
            if order.is_filled() {
                let pos = Position::open(order);
                let qty = pos.quantity;
                let kind = pos.kind;
                self.open_positions.insert(pos_key.clone(), pos);
                if matches!(
                    (kind, order.side),
                    (PositionKind::Short, TradeType::Sell) | (PositionKind::Long, TradeType::Buy)
                ) {
                    let value_strat_before = self.value;
                    match kind {
                        PositionKind::Short => self.value += order.realized_quote_value(),
                        PositionKind::Long => self.value -= order.quote_value(),
                    }
                    Self::log_position(order, value_strat_before, self.value, kind, qty);
                } else {
                    return Err(Error::BadSideForPosition("open", kind, order.side));
                }
            }
        }

        let mut resp = Ok(None);
        if let Entry::Occupied(pos_entry) = self.open_positions.entry(pos_key.clone()) {
            let pos = pos_entry.get();
            if order.is_filled() {
                resp = Ok(Some(pos.clone()));
                if pos.is_closed() {
                    self.repo.close_position(pos)?;
                    pos_entry.remove();
                    // TODO: this isn't the right way to manage multiple positions, as the pnl should be the sum of all gains and losses
                    if self.open_positions.is_empty() {
                        self.pnl = self.value;
                    }
                } else if pos.is_opened() {
                    self.repo.open_position(pos)?;
                }
                self.repo.update_vars(self)?;
            }
            if order.is_resolved() {
                self.remove_lock(&pos_key)?;
            }
        }
        resp
    }

    fn log_position(
        order: &OrderDetail,
        value_strat_before: f64,
        value_strat_after: f64,
        kind: PositionKind,
        qty: f64,
    ) {
        debug!(
            pos_knd = %kind,
            pair = %order.pair,
            fees = format!("{:.6}", order.quote_fees()).as_str(),
            realized_quote_value = format!("{:.2}", order.realized_quote_value()).as_str(),
            quote_value = format!("{:.2}", order.quote_value()).as_str(),
            pos_qty = format!("{:.6}", qty).as_str(),
            open_price = format!("{:.6}", order.price.unwrap_or(0.0)).as_str(),
            value = format!("{:.6}", qty * order.price.unwrap_or(0.0)).as_str(),
            value_strat_before = format!("{:.2}", value_strat_before).as_str(),
            value_strat_after = format!("{:.2}", value_strat_after).as_str(),
        );
    }

    /// Update the corresponding position with the latest event (typically the price)
    ///
    /// # Errors
    ///
    /// Interest rates could not be fetched
    pub async fn update_from_market(&mut self, event: &MarketEventEnvelope) -> Result<()> {
        // This ugly bit of code is because of the mutable borrow, it should be refactored away
        let interests = if let Some(p) = self.open_positions.get(&(event.xch, event.pair.clone())) {
            let option = p.open_order.as_ref();
            self.interest_fees_since_open(option).await
        } else {
            return Ok(());
        }?;
        if let Some(p) = self.open_positions.get_mut(&(event.xch, event.pair.clone())) {
            p.update(event, self.fees_rate, interests);
        }
        Ok(())
    }

    // TODO : it seems heavy to query an actor for something that changes once a day
    async fn interest_fees_since_open(&self, order: Option<&OrderDetail>) -> Result<f64> {
        match order {
            None => Ok(0.0),
            Some(o) => self
                .interest_rates
                .interest_fees_since(Exchange::from_str(&o.exchange).unwrap(), o)
                .await
                .err_into(),
        }
    }

    /// True if there is an open position
    pub fn has_open_position(&self, xch: Exchange, pair: Pair) -> bool {
        self.open_positions.get(&(xch, pair)).is_some()
    }

    /// True if there are any open positions
    pub fn has_any_open_position(&self) -> bool { !self.open_positions.is_empty() }

    /// True if there are any positions with a failed order
    pub fn has_any_failed_position(&self) -> bool {
        self.open_positions
            .iter()
            .any(|(_, p)| p.is_failed_open() || p.is_failed_close())
    }

    /// Unlock a previously locked position
    ///
    /// # Errors
    ///
    /// The position could not be unlocked or closed (if it was only opened)
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
    ///
    /// # Panics
    ///
    /// if the position can be closed, unimplemented
    pub fn force_close(&mut self, xch: Exchange, pair: Pair) -> Result<()> {
        let position_key = (xch, pair);
        match self.open_positions.get(&position_key) {
            Some(pos) if !self.is_locked(&position_key) && pos.is_opened() && !pos.is_closed() => {
                unimplemented!();
                // if let Some(pos) = self.open_positions.get(&position_key) {
                //     if pos.is_failed_open() {
                //         self.repo.close_position(pos)?;
                //         self.open_positions.remove(&position_key);
                //     }
                // }
            }
            None => Err(Error::NoPositionFound),
            _ => Err(Error::PositionLocked),
        }
    }

    /// If a lock exists for a [`PositionKey`]
    pub fn is_locked(&self, key: &PositionKey) -> bool { self.locks.get(key).is_some() }

    /// Current position locks
    pub fn locks(&self) -> &BTreeMap<PositionKey, PositionLock> { &self.locks }

    fn remove_lock(&mut self, key: &PositionKey) -> Result<()> {
        self.locks.remove(key);
        self.repo.release_lock(key)
    }

    fn lock_position(&mut self, pos_key: PositionKey, lock: PositionLock) -> Result<()> {
        self.repo.set_lock(&pos_key, &lock)?;
        self.locks.insert(pos_key, lock);
        Ok(())
    }

    pub fn value(&self) -> f64 { self.value }

    pub fn pnl(&self) -> f64 { self.pnl }

    pub fn set_value(&mut self, value: f64) -> Result<()> {
        self.value = value;
        self.repo.update_vars(self)
    }

    pub fn set_pnl(&mut self, pnl: f64) -> Result<()> {
        self.pnl = pnl;
        self.repo.update_vars(self)
    }

    pub fn open_position(&self, xch: Exchange, pair: Pair) -> Option<&Position> {
        self.open_positions.get(&(xch, pair))
    }

    pub fn open_positions(&self) -> &BTreeMap<PositionKey, Position> { &self.open_positions }

    pub fn current_return(&self) -> f64 {
        if self.open_positions.is_empty() {
            0.0
        } else {
            self.open_positions
                .iter()
                .map(|(_, pos)| pos.unreal_profit_loss)
                .sum::<f64>()
                / self.pnl
        }
    }

    pub fn positions_history(&self) -> Result<Vec<Position>> { self.repo.all_positions() }
}

fn bad_signal(pos: &Position, signal: &TradeSignal) -> Error {
    Error::BadSignal(
        pos.open_order.as_ref().map_or(false, OrderDetail::is_filled),
        pos.close_order.as_ref().map_or(false, OrderDetail::is_filled),
        pos.kind,
        signal.op_kind,
    )
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
    fn put_position(&self, pos: &Position, is_open: bool) -> Result<()>;
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

static POSITIONS_TABLE: &str = "positions";
static OPEN_POSITIONS_INDEX: &str = "open_pos_idx";
static POSITION_LOCKS_TABLE: &str = "locks";
static PORTFOLIO_VARS: &str = "vars";

/// K/V Store based implementation of the portfolio repository
#[derive(Debug)]
pub struct PortfolioRepoImpl {
    db: Arc<dyn Storage>,
}

impl PortfolioRepoImpl {
    /// # Panics
    ///
    /// if tables cannot be ensured
    pub fn new(db: Arc<dyn Storage>) -> Self {
        for table in &[
            POSITION_LOCKS_TABLE,
            POSITIONS_TABLE,
            PORTFOLIO_VARS,
            OPEN_POSITIONS_INDEX,
        ] {
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

impl PortfolioRepo for PortfolioRepoImpl {
    fn open_position(&self, pos: &Position) -> Result<()> { self.put_position(pos, true) }

    fn close_position(&self, pos: &Position) -> Result<()> { self.put_position(pos, false) }

    fn is_open(&self, pos_id: &Uuid) -> Result<bool> {
        match self.db.get(OPEN_POSITIONS_INDEX, pos_id.as_bytes()) {
            Err(db::Error::NotFound(_)) => Ok(false),
            r => r.err_into(),
        }
    }

    fn put_position(&self, pos: &Position, is_open: bool) -> Result<()> {
        let pos_id = pos.id.as_bytes();
        self.db
            .batch(&[
                (POSITIONS_TABLE, pos_id, Some(Box::new(pos.clone()))),
                (
                    OPEN_POSITIONS_INDEX,
                    pos_id,
                    if is_open { Some(Box::new(is_open)) } else { None },
                ),
            ])
            .err_into()
    }

    fn get_position(&self, pos_id: Uuid) -> Result<Option<Position>> {
        match self.db.get(POSITIONS_TABLE, pos_id.as_bytes()) {
            Err(db::Error::NotFound(_)) => Ok(None),
            r => r.err_into(),
        }
    }

    fn all_positions(&self) -> Result<Vec<Position>> {
        Ok(self
            .db
            .get_all::<Position>(POSITIONS_TABLE)?
            .into_iter()
            .map(|(_, pos)| pos)
            .collect())
    }

    fn delete_position(&self, pos_id: Uuid) -> Result<()> {
        let pos_id = pos_id.as_bytes();
        self.db
            .batch(&[(POSITIONS_TABLE, pos_id, None), (OPEN_POSITIONS_INDEX, pos_id, None)])
            .err_into()
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
        let maybe_vars: Option<PortfolioVars> = match self.db.get(PORTFOLIO_VARS, &p.key) {
            Err(db::Error::NotFound(_)) => Ok(None),
            r => r,
        }?;
        if let Some(vars) = maybe_vars {
            p.pnl = vars.pnl;
            p.value = vars.value;
        }
        for (pos_id, _) in self.db.get_all::<bool>(OPEN_POSITIONS_INDEX)? {
            let pos_id = Uuid::from_slice(&*pos_id)?;
            if let Some(pos) = self.get_position(pos_id)? {
                error!("failed to retrieve open position {} from storage", pos_id);
                p.open_positions.insert(pos_key_from_position(&pos), pos);
            }
        }
        p.locks.extend(
            self.db
                .get_all::<PositionLock>(POSITION_LOCKS_TABLE)?
                .into_iter()
                .map(|(k, v)| (Self::parse_key_string(std::str::from_utf8(&*k).unwrap()), v)),
        );
        Ok(())
    }
}

#[cfg(test)]
mod repository_test {
    use std::assert_matches::assert_matches;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::Utc;
    use test_log::test;

    use coinnect_rt::types::AddOrderRequest;
    use trading::interest::FlatInterestRateProvider;
    use trading::order_manager::types::OrderDetail;
    use trading::position::Position;

    use crate::portfolio::{pos_key_from_position, Portfolio, PortfolioRepo, PortfolioRepoImpl, PositionLock};
    use crate::risk::DefaultMarketRiskEvaluator;
    use crate::test_util::test_db;

    fn make_test_repo() -> PortfolioRepoImpl {
        let db = test_db();
        PortfolioRepoImpl::new(db.clone())
    }

    #[test(tokio::test)]
    async fn open_position_should_not_be_open_after_close() {
        let repo = make_test_repo();
        let mut pos = Position::default();
        let open = repo.open_position(&pos);
        assert_matches!(open, Ok(_));
        let get_pos = repo.get_position(pos.id);
        assert_matches!(get_pos, Ok(Some(_)));
        assert_eq!(get_pos.unwrap().unwrap(), pos);
        let is_open = repo.is_open(&pos.id);
        assert_matches!(is_open, Ok(true));
        pos.close_order = Some(OrderDetail::from_query(AddOrderRequest {
            pair: pos.symbol.clone(),
            ..AddOrderRequest::default()
        }));
        let close_pos = repo.close_position(&pos);
        assert_matches!(close_pos, Ok(_));
        let get_pos = repo.get_position(pos.id);
        assert_matches!(get_pos, Ok(Some(_)));
        assert_eq!(get_pos.unwrap().unwrap(), pos);
        let is_open = repo.is_open(&pos.id);
        assert_matches!(is_open, Ok(false));
    }

    #[test(tokio::test)]
    async fn set_and_release_lock() {
        let repo = make_test_repo();
        let pos = Position::default();
        let pos_key = pos_key_from_position(&pos);
        let lock = PositionLock {
            at: Utc::now(),
            order_id: "id".to_string(),
        };
        let locked = repo.set_lock(&pos_key, &lock);
        assert_matches!(locked, Ok(_));
        let unlocked = repo.release_lock(&pos_key);
        assert_matches!(unlocked, Ok(_));
    }

    #[test(tokio::test)]
    async fn put_and_delete_position() {
        let repo = make_test_repo();
        let pos = Position::default();
        let put = repo.put_position(&pos, true);
        assert_matches!(put, Ok(_));
        let pos_id = pos.id;
        let get_pos = repo.get_position(pos_id);
        assert_matches!(get_pos, Ok(Some(_)));
        assert_eq!(get_pos.unwrap().unwrap(), pos);
        let all_pos = repo.all_positions();
        assert_matches!(all_pos, Ok(_));
        assert_eq!(all_pos.unwrap(), vec![pos]);
        let delete = repo.delete_position(pos_id);
        assert_matches!(delete, Ok(_));
        let get_pos = repo.get_position(pos_id);
        assert_matches!(get_pos, Ok(None));
        let all_pos = repo.all_positions();
        assert_matches!(all_pos, Ok(_));
        assert_eq!(all_pos.unwrap(), vec![]);
    }

    #[test(tokio::test)]
    async fn load_portfolio() {
        let repo = make_test_repo();
        let risk = DefaultMarketRiskEvaluator::default();
        let mut portfolio = Portfolio::try_new(
            100.0,
            0.001,
            "key".to_string(),
            Arc::new(repo),
            Arc::new(risk),
            Arc::new(FlatInterestRateProvider::new(0.002)),
        )
        .unwrap();
        let arc = portfolio.repo.clone();
        let load = arc.load(&mut portfolio);
        assert_matches!(load, Ok(_));
        assert!(approx_eq!(f64, portfolio.pnl, 100.0));
        assert!(approx_eq!(f64, portfolio.value, 100.0));
        assert!(portfolio.open_positions.is_empty());
        assert!(portfolio.locks.is_empty());
        let pos = Position::default();
        let pos_key = pos_key_from_position(&pos);
        portfolio.pnl = 200.0;
        portfolio.value = 400.0;
        let put = arc.update_vars(&portfolio);
        assert_matches!(put, Ok(_));
        let put = arc.open_position(&pos);
        assert_matches!(put, Ok(_));
        let lock = PositionLock {
            at: Utc::now(),
            order_id: "id".to_string(),
        };
        let locked = arc.set_lock(&pos_key, &lock);
        assert_matches!(locked, Ok(_));
        let load = arc.load(&mut portfolio);
        assert_matches!(load, Ok(_));
        assert!(approx_eq!(f64, portfolio.pnl, 200.0));
        assert!(approx_eq!(f64, portfolio.value, 400.0));
        let mut expected = BTreeMap::new();
        expected.insert(pos_key.clone(), pos);
        assert_eq!(portfolio.open_positions, expected);
        let mut expected = BTreeMap::new();
        expected.insert(pos_key, lock);
        assert_eq!(portfolio.locks, expected);
    }
}

#[cfg(test)]
mod portfolio_test {
    use std::sync::Arc;

    use test_log::test;

    use trading::interest::FlatInterestRateProvider;
    use trading::signal::TradeSignal;

    use crate::portfolio::{Portfolio, PortfolioRepoImpl};
    use crate::risk::DefaultMarketRiskEvaluator;
    use crate::test_util::test_db;

    fn make_test_portfolio() -> Portfolio {
        let db = test_db();
        let repo = PortfolioRepoImpl::new(db.clone());
        let risk = DefaultMarketRiskEvaluator::default();
        Portfolio::try_new(
            100.0,
            0.001,
            "portfolio_key".to_string(),
            Arc::new(repo),
            Arc::new(risk),
            Arc::new(FlatInterestRateProvider::new(0.002)),
        )
        .unwrap()
    }

    #[test(tokio::test)]
    async fn convert_open_signal() {
        let _portfolio = make_test_portfolio();
        let _signal = TradeSignal {
            ..TradeSignal::default()
        };
    }
}
