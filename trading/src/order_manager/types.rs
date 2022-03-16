use std::ops::Sub;

use actix::Message;
use chrono::{DateTime, TimeZone, Utc};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use brokers::exchange::Exchange;
use brokers::pair::symbol_to_pair;
use brokers::types::{AddOrderRequest, AssetType, InterestRate, MarginSideEffect, OrderEnforcement, OrderQuery,
                     OrderStatus as BrokerOrderStatus, OrderSubmission, OrderType, OrderUpdate, Pair, TradeType};
use util::time::now;

use super::error::*;
use super::wal::WalCmp;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "reject_type", content = "__field0")]
pub enum Rejection {
    BadRequest(String),
    InsufficientFunds,
    Timeout,
    Cancelled(Option<String>),
    Other(String),
    Unknown(String),
    InvalidPrice,
}

impl Rejection {
    pub(crate) fn from_status(os: &BrokerOrderStatus, reason: Option<String>) -> Self {
        match os {
            BrokerOrderStatus::Rejected => match reason {
                Some(reason) => Rejection::Other(reason),
                None => Rejection::Other("".to_string()),
            },
            BrokerOrderStatus::Expired => Rejection::Timeout,
            BrokerOrderStatus::Canceled | BrokerOrderStatus::PendingCancel => Rejection::Cancelled(reason),
            _ => Rejection::Unknown("".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq)]
#[serde(tag = "type")]
pub enum TransactionStatus {
    #[display(fmt = "staged")]
    Staged(OrderQuery),
    #[display(fmt = "new")]
    New(OrderSubmission),
    #[display(fmt = "filled")]
    Filled(OrderUpdate),
    #[display(fmt = "partially_filled")]
    PartiallyFilled(OrderUpdate),
    #[display(fmt = "rejected")]
    Rejected(Rejection),
}

impl TransactionStatus {
    pub(crate) fn is_incomplete(&self) -> bool {
        matches!(self, Self::PartiallyFilled(_) | Self::Staged(_) | Self::New(_))
    }

    pub(crate) fn get_pair(&self, xchg: Exchange) -> Result<Pair> {
        match self {
            TransactionStatus::PartiallyFilled(ou) | TransactionStatus::Filled(ou) => {
                Ok(symbol_to_pair(&xchg, &ou.symbol.clone().into())?)
            }
            TransactionStatus::New(os) => Ok(os.pair.clone()),
            TransactionStatus::Staged(OrderQuery::AddOrder(ao)) => Ok(ao.pair.clone()),
            _ => Err(brokers::error::Error::PairUnsupported.into()),
        }
    }
}

impl WalCmp for TransactionStatus {
    fn is_before(&self, v: &Self) -> bool {
        if std::mem::discriminant(self) == std::mem::discriminant(v) {
            return false;
        }
        match self {
            Self::Staged(_) => matches!(
                v,
                Self::New(_) | Self::PartiallyFilled(_) | Self::Rejected(_) | Self::Filled(_)
            ),
            Self::New(_) => matches!(v, Self::PartiallyFilled(_) | Self::Rejected(_) | Self::Filled(_)),
            Self::PartiallyFilled(_) => matches!(v, Self::Rejected(_) | Self::Filled(_)),
            Self::Filled(_) => matches!(v, Self::Rejected(_)),
            Self::Rejected(_) => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Transaction {
    pub id: String,
    pub status: TransactionStatus,
    pub ts: Option<i64>,
}

impl Transaction {
    pub fn is_filled(&self) -> bool {
        matches!(
            self.status,
            TransactionStatus::Filled(_)
                | TransactionStatus::New(OrderSubmission {
                    status: BrokerOrderStatus::Filled,
                    ..
                })
        )
    }

    pub fn is_bad_request(&self) -> bool {
        matches!(self.status, TransactionStatus::Rejected(Rejection::BadRequest(_)))
    }

    pub fn is_rejected(&self) -> bool { matches!(self.status, TransactionStatus::Rejected(_)) }

    pub fn is_cancelled(&self) -> bool { matches!(self.status, TransactionStatus::Rejected(Rejection::Cancelled(_))) }

    pub fn variant_eq(&self, b: &Transaction) -> bool {
        std::mem::discriminant(&self.status) == std::mem::discriminant(&b.status)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "Result<OrderDetail>")]
pub struct StagedOrder {
    pub request: AddOrderRequest,
}

#[derive(Message, Debug)]
#[rtype(result = "Result<()>")]
pub struct PassOrder {
    pub id: String,
    pub query: OrderQuery,
}

#[derive(Message)]
#[rtype(result = "(Result<OrderDetail>, Result<Transaction>)")]
pub struct OrderId(pub String);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Staged,
    Created,
    Filled,
    PartiallyFilled,
    Rejected,
    Canceled,
}

impl From<BrokerOrderStatus> for OrderStatus {
    fn from(o: BrokerOrderStatus) -> Self {
        match o {
            BrokerOrderStatus::New => Self::Created,
            BrokerOrderStatus::PartiallyFilled => Self::PartiallyFilled,
            BrokerOrderStatus::Filled | BrokerOrderStatus::Traded => Self::Filled,
            BrokerOrderStatus::Canceled | BrokerOrderStatus::PendingCancel => Self::Canceled,
            BrokerOrderStatus::Rejected | BrokerOrderStatus::Expired => Self::Rejected,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct OrderDetail {
    /// Order id
    pub id: String,
    /// Optional transaction id, if this order was part of a larger transaction
    pub transaction_id: Option<String>,
    /// Optional emitter id, for the entity that emitted the order
    pub emitter_id: Option<String>,
    /// Identifer with the remote platform
    pub remote_id: Option<String>,
    pub status: OrderStatus,
    pub exchange: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub side: TradeType,
    pub order_type: OrderType,
    pub enforcement: Option<OrderEnforcement>,
    pub base_qty: Option<f64>,
    pub quote_qty: Option<f64>,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub iceberg_qty: Option<f64>,
    pub is_test: bool,
    pub asset_type: AssetType,
    pub executed_qty: Option<f64>,
    pub cummulative_quote_qty: Option<f64>,
    pub margin_side_effect: Option<MarginSideEffect>,
    pub borrowed_amount: Option<f64>,
    pub borrowed_asset: Option<String>,
    pub fills: Vec<OrderFill>,
    /// Weighted price updated from fills
    pub weighted_price: f64,
    pub total_executed_qty: f64,
    pub rejection_reason: Option<Rejection>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub open_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct OrderFill {
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_asset: Option<String>,
    pub ts: DateTime<Utc>,
}

impl OrderDetail {
    pub fn is_same_status(&self, os: &OrderStatus) -> bool {
        std::mem::discriminant(&self.status) == std::mem::discriminant(os)
    }

    pub fn is_filled(&self) -> bool { matches!(self.status, OrderStatus::Filled) }

    pub fn is_bad_request(&self) -> bool {
        self.is_rejected() && matches!(self.rejection_reason, Some(Rejection::BadRequest(_)))
    }

    pub fn is_rejected(&self) -> bool { matches!(self.status, OrderStatus::Rejected) }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.rejection_reason,
            Some(Rejection::Cancelled(_) | Rejection::Timeout)
        )
    }

    pub fn is_cancelled(&self) -> bool {
        self.is_rejected() && matches!(self.rejection_reason, Some(Rejection::Cancelled(_)))
    }

    pub fn is_resolved(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Canceled
        )
    }

    pub fn from_query(add_order: AddOrderRequest) -> Self {
        let pair_string = add_order.pair.to_string();
        let (base_asset, quote_asset) = pair_string.split_once('_').expect("pair string should be BASE_QUOTE");
        let base_asset = base_asset.to_string();
        let quote_asset = quote_asset.to_string();
        Self {
            id: add_order.order_id,
            transaction_id: add_order.transaction_id,
            emitter_id: add_order.emitter_id,
            remote_id: None,
            status: OrderStatus::Staged,
            exchange: add_order.xch.to_string(),
            symbol: pair_string,
            base_asset,
            quote_asset,
            side: add_order.side,
            order_type: add_order.order_type,
            enforcement: add_order.enforcement,
            base_qty: add_order.quantity,
            quote_qty: add_order.quote_order_qty,
            price: add_order.price,
            stop_price: add_order.stop_price,
            iceberg_qty: add_order.iceberg_qty,
            is_test: add_order.dry_run,
            asset_type: add_order.asset_type.unwrap_or_default(),
            executed_qty: None,
            cummulative_quote_qty: None,
            margin_side_effect: add_order.side_effect_type,
            borrowed_amount: None,
            borrowed_asset: None,
            fills: vec![],
            weighted_price: 0.0,
            total_executed_qty: 0.0,
            rejection_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
            open_at: None,
        }
    }

    pub fn from_submission(&mut self, submission: OrderSubmission) {
        self.executed_qty = Some(submission.executed_qty);
        self.cummulative_quote_qty = Some(submission.cummulative_quote_qty);
        self.borrowed_asset = submission.borrow_asset;
        self.borrowed_amount = submission.borrowed_amount;
        self.remote_id = Some(submission.id);
        self.enforcement = Some(submission.enforcement);
        self.status = submission.status.into();
        let fills = submission
            .trades
            .into_iter()
            .map(|trade| OrderFill {
                price: trade.price,
                qty: trade.qty,
                fee: trade.fee,
                fee_asset: Some(trade.fee_asset.to_string()),
                ts: Utc.timestamp_millis(submission.timestamp as i64),
            })
            .collect();
        self.fills = fills;
        self.update_weighted_price();
        self.updated_at = Utc::now();
        self.total_executed_qty = self.fills.iter().map(|f| f.qty).sum();
        self.open_at = Some(Utc.timestamp_millis(submission.timestamp));
        if self.status == OrderStatus::Filled {
            self.closed_at = Some(now());
        }
    }

    #[allow(clippy::cast_possible_wrap)]
    pub fn from_fill_update(&mut self, update: OrderUpdate) {
        if self.status == OrderStatus::Filled {
            return;
        }
        let time = Utc.timestamp_millis(update.timestamp as i64);
        let fill = OrderFill {
            price: update.last_executed_price,
            qty: update.last_executed_qty,
            fee: update.commission,
            fee_asset: update.commission_asset,
            ts: time,
        };
        self.fills.push(fill);
        self.cummulative_quote_qty = Some(update.cummulative_quote_asset_transacted_qty);
        self.total_executed_qty = update.cummulative_filled_qty;
        self.status = update.new_status.into();
        if self.status == OrderStatus::Filled {
            self.closed_at = Some(time);
        }
        self.updated_at = Utc::now();
        self.update_weighted_price();
    }

    pub fn from_rejected(&mut self, rejection: Rejection) {
        self.rejection_reason = Some(rejection);
        self.status = OrderStatus::Rejected;
        self.updated_at = Utc::now();
    }

    pub fn update_weighted_price(&mut self) {
        let total_qty = self.fills.iter().map(|fill| fill.qty).sum::<f64>();
        if self.fills.is_empty() || total_qty == 0.0 {
            self.weighted_price = 0.0;
        } else {
            self.weighted_price = self.fills.iter().map(|fill| fill.price * fill.qty).sum::<f64>() / total_qty;
        }
    }

    /// Calculate total interest owed, calculated in borrowed asset
    ///
    /// # Panics
    ///
    /// if `closed_at` is None
    pub fn total_interest(&self, interest_rate: &InterestRate) -> f64 {
        let hours_elapsed = now().sub(self.closed_at.unwrap());
        let borrowed_amount = self.borrowed_amount.unwrap();
        // The first hour is owed at t0
        interest_rate.resolve(borrowed_amount, hours_elapsed.num_hours() + 1)
    }

    /// Calculate total interest owed, calculated in quote asset
    ///
    /// # Panics
    ///
    /// if `borrowed_asset` is None
    pub fn total_quote_interest(&self, interest_rate: &InterestRate) -> f64 {
        let interests = self.total_interest(interest_rate);
        let borrowed_asset = self.borrowed_asset.as_ref().unwrap();
        if borrowed_asset == &self.base_asset {
            interests * self.weighted_price
        } else {
            interests
        }
    }

    pub fn from_status(&mut self, status: TransactionStatus) {
        match status {
            TransactionStatus::New(submission) => self.from_submission(submission),
            TransactionStatus::Filled(update) | TransactionStatus::PartiallyFilled(update) => {
                self.from_fill_update(update);
            }
            TransactionStatus::Rejected(rejection) => self.from_rejected(rejection),
            TransactionStatus::Staged(_) => {}
        }
    }

    pub fn quote_fees(&self) -> f64 {
        self.fills
            .iter()
            .map(|f| match f.fee_asset.as_ref() {
                Some(a) if a == &self.base_asset => f.fee * f.price,
                Some(a) if a == &self.quote_asset => f.fee,
                _ => f.fee,
            })
            .sum()
    }

    pub fn base_fees(&self) -> f64 {
        self.fills
            .iter()
            .map(|f| match f.fee_asset.as_ref() {
                Some(a) if a == &self.base_asset => f.fee,
                Some(a) if a == &self.quote_asset => f.fee / f.price,
                _ => f.fee,
            })
            .sum()
    }

    pub fn quote_value(&self) -> f64 { self.total_executed_qty * self.weighted_price }

    pub fn realized_quote_value(&self) -> f64 { self.quote_value() - self.quote_fees() }
}

pub enum SessionEvent {
    Tick,
    SessionStart,
    SessionEnd,
    PeriodEnd,
    BeforeTradingStart,
}

pub struct EODCancelPolicy;

impl CancelPolicy for EODCancelPolicy {
    fn should_cancel(&mut self, e: SessionEvent) -> bool { matches!(e, SessionEvent::SessionEnd) }
}

pub struct NeverCancelPolicy;

impl CancelPolicy for NeverCancelPolicy {
    fn should_cancel(&mut self, _e: SessionEvent) -> bool { false }
}

trait CancelPolicy {
    fn should_cancel(&mut self, e: SessionEvent) -> bool;
}

#[cfg(test)]
mod test {
    use std::ops::Sub;

    use chrono::{Duration, Utc};

    use brokers::types::{AddOrderRequest, InterestRate, InterestRatePeriod, OrderFill, OrderQuery,
                         OrderStatus as CoinOrderStatus, OrderSubmission, OrderUpdate};

    use super::{OrderDetail, OrderStatus, Rejection, Transaction, TransactionStatus};

    #[test]
    fn test_variant_eq() {
        let order_id = "1".to_string();
        let statuses = vec![
            TransactionStatus::New(OrderSubmission {
                timestamp: 0,
                id: order_id.clone(),
                ..OrderSubmission::default()
            }),
            TransactionStatus::Staged(OrderQuery::AddOrder(AddOrderRequest::default())),
            TransactionStatus::Filled(OrderUpdate::default()),
            TransactionStatus::Rejected(Rejection::Other("".to_string())),
        ];
        let tr0 = Transaction {
            id: order_id.clone(),
            status: statuses[0].clone(),
            ts: None,
        };
        let tr1 = Transaction {
            id: order_id,
            status: statuses[1].clone(),
            ts: None,
        };
        assert!(tr0.variant_eq(&tr0));
        assert!(!tr0.variant_eq(&tr1));
    }

    #[test]
    #[should_panic]
    fn test_order_detail_bad_pair() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "btcusdt".into(),
            ..AddOrderRequest::default()
        };
        OrderDetail::from_query(request);
    }

    fn trades() -> Vec<OrderFill> {
        vec![
            OrderFill {
                id: None,
                price: 1.0,
                qty: 1.0,
                fee: 0.001,
                fee_asset: "BTC".into(),
            },
            OrderFill {
                id: None,
                price: 1.1,
                qty: 1.2,
                fee: 0.001,
                fee_asset: "BTC".into(),
            },
        ]
    }

    #[test]
    fn test_order_detail_new_filled() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let mut order = OrderDetail::from_query(request.clone());
        let transact_time = Utc::now();
        let i = transact_time.timestamp_millis();
        let submission = OrderSubmission {
            status: CoinOrderStatus::Filled,
            timestamp: i,
            trades: trades(),
            ..request.into()
        };
        assert_eq!(order.status, OrderStatus::Staged);
        order.from_submission(submission);
        assert_eq!(order.status, OrderStatus::Filled);
        // Compare only millis as nanos is a random value
        assert_eq!(
            order.closed_at.map(|d| d.timestamp_millis()),
            Some(transact_time.timestamp_millis()),
            "closed_at"
        );
        assert_eq!(
            order.open_at.map(|d| d.timestamp_millis()),
            Some(transact_time.timestamp_millis()),
            "open_at"
        );
        assert_eq!(format!("{:.6}", order.weighted_price), "1.054545");
        assert_eq!(format!("{:.6}", order.total_executed_qty), "2.200000");
    }

    #[test]
    fn test_order_detail_fill_after_already_filled() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let mut order = OrderDetail::from_query(request.clone());
        let transact_time = Utc::now();
        let i = transact_time.timestamp_millis();
        let trades = trades();
        let submission = OrderSubmission {
            status: CoinOrderStatus::Filled,
            timestamp: i,
            trades: trades.clone(),
            ..request.into()
        };
        assert_eq!(order.status, OrderStatus::Staged);
        order.from_submission(submission);
        assert_eq!(order.status, OrderStatus::Filled);
        let first_trade = trades.first().unwrap();
        let update = OrderUpdate {
            last_executed_price: first_trade.price,
            last_executed_qty: first_trade.qty,
            new_status: CoinOrderStatus::Filled,
            ..OrderUpdate::default()
        };
        order.from_fill_update(update);
        assert_eq!(order.fills.len(), trades.len());
    }

    #[test]
    fn test_order_detail_successive_fills() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let mut order = OrderDetail::from_query(request.clone());
        let transact_time = Utc::now();
        let i = transact_time.timestamp_millis();
        let trades = trades();
        let submission = OrderSubmission {
            status: CoinOrderStatus::New,
            timestamp: i,
            trades: trades.clone(),
            ..request.into()
        };
        assert_eq!(order.status, OrderStatus::Staged);
        order.from_submission(submission);
        assert_eq!(order.status, OrderStatus::Created);
        let first_trade = trades.first().unwrap();
        let partial_update = OrderUpdate {
            last_executed_price: first_trade.price,
            last_executed_qty: first_trade.qty,
            new_status: CoinOrderStatus::PartiallyFilled,
            ..OrderUpdate::default()
        };
        order.from_fill_update(partial_update);
        assert_eq!(order.fills.len(), trades.len() + 1);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        let fill_update = OrderUpdate {
            last_executed_price: first_trade.price,
            last_executed_qty: first_trade.qty,
            new_status: CoinOrderStatus::Filled,
            ..OrderUpdate::default()
        };
        order.from_fill_update(fill_update);
        assert_eq!(order.fills.len(), trades.len() + 2);
        assert_eq!(order.status, OrderStatus::Filled);
    }

    #[test]
    fn test_order_detail_rejected() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let mut order = OrderDetail::from_query(request);
        assert_eq!(order.status, OrderStatus::Staged);
        let rejection = Rejection::Cancelled(None);
        order.from_rejected(rejection.clone());
        assert_eq!(order.status, OrderStatus::Rejected);
        assert_eq!(order.rejection_reason, Some(rejection));
    }

    #[test]
    fn test_order_detail_margin_interest_rate() {
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let mut order = OrderDetail::from_query(request.clone());
        let transact_time = Utc::now().sub(Duration::days(1)).sub(Duration::hours(2));
        let i = transact_time.timestamp_millis();
        assert_eq!(order.status, OrderStatus::Staged);
        let submission = OrderSubmission {
            status: CoinOrderStatus::Filled,
            timestamp: i,
            trades: trades(),
            borrowed_amount: Some(0.01),
            borrow_asset: Some("USDT".to_string()),
            ..request.into()
        };
        order.from_submission(submission);
        assert_eq!(order.status, OrderStatus::Filled);
        let interest_rate = InterestRate {
            symbol: "BTC".to_string(),
            ts: 0,
            rate: 0.002,
            period: InterestRatePeriod::Hourly,
        };
        let total_interest = order.total_interest(&interest_rate);
        assert_eq!(format!("{:.5}", total_interest), "0.00002");
    }

    #[test]
    fn test_order_detail_same_status() {
        let transact_time = Utc::now();
        let i = transact_time.timestamp_millis();
        let request = AddOrderRequest {
            order_id: "id".to_string(),
            pair: "BTC_USDT".into(),
            ..AddOrderRequest::default()
        };
        let order = OrderDetail::from_query(request.clone());
        let mut next_order = order.clone();
        let submission = OrderSubmission {
            status: CoinOrderStatus::Filled,
            timestamp: i,
            trades: trades(),
            borrowed_amount: Some(0.01),
            borrow_asset: Some("USDT".to_string()),
            ..request.into()
        };
        next_order.from_submission(submission);

        assert!(!order.is_same_status(&next_order.status));
    }
}
