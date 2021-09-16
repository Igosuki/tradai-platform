use actix::Message;
use chrono::{DateTime, TimeZone, Utc};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use coinnect_rt::exchange::Exchange;
use coinnect_rt::pair::symbol_to_pair;
use coinnect_rt::types::{AddOrderRequest, AssetType, MarginSideEffect, OrderQuery, OrderStatus as ExchangeOrderStatus,
                         OrderSubmission, OrderType, OrderUpdate, Pair, TradeType};

use crate::coinnect_types::OrderEnforcement;
use crate::error::*;
use crate::wal::WalCmp;

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
    pub(crate) fn from_status(os: ExchangeOrderStatus, reason: Option<String>) -> Self {
        match os {
            ExchangeOrderStatus::Rejected => match reason {
                Some(reason) => Rejection::Other(reason),
                None => Rejection::Other("".to_string()),
            },
            ExchangeOrderStatus::Expired => Rejection::Timeout,
            ExchangeOrderStatus::Canceled | ExchangeOrderStatus::PendingCancel => Rejection::Cancelled(reason),
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
            _ => Err(coinnect_rt::error::Error::PairUnsupported.into()),
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
}

impl Transaction {
    pub fn is_filled(&self) -> bool {
        matches!(
            self.status,
            TransactionStatus::Filled(_)
                | TransactionStatus::New(OrderSubmission {
                    status: ExchangeOrderStatus::Filled,
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
#[rtype(result = "Result<Transaction>")]
pub(crate) struct StagedOrder {
    pub request: AddOrderRequest,
}

#[derive(Message, Debug)]
#[rtype(result = "Result<()>")]
pub struct PassOrder {
    pub id: String,
    pub query: OrderQuery,
}

#[derive(Message)]
#[rtype(result = "Result<Transaction>")]
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

impl From<ExchangeOrderStatus> for OrderStatus {
    fn from(o: ExchangeOrderStatus) -> Self {
        match o {
            ExchangeOrderStatus::New => Self::Created,
            ExchangeOrderStatus::PartialyFilled => Self::PartiallyFilled,
            ExchangeOrderStatus::Filled => Self::Filled,
            ExchangeOrderStatus::Canceled => Self::Canceled,
            ExchangeOrderStatus::PendingCancel => Self::Canceled,
            ExchangeOrderStatus::Rejected => Self::Rejected,
            ExchangeOrderStatus::Expired => Self::Rejected,
            ExchangeOrderStatus::Traded => Self::Filled,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderDetail {
    /// Order id
    id: String,
    /// Optional transaction id, if this order was part of a larger transaction
    transaction_id: Option<String>,
    /// Identifer with the remote platform
    remote_id: Option<String>,
    status: OrderStatus,
    exchange: String,
    pair: String,
    base_asset: String,
    quote_asset: String,
    side: TradeType,
    order_type: OrderType,
    enforcement: Option<OrderEnforcement>,
    base_qty: Option<f64>,
    quote_qty: Option<f64>,
    price: Option<f64>,
    stop_price: Option<f64>,
    iceberg_qty: Option<f64>,
    is_test: bool,
    asset_type: AssetType,
    executed_qty: Option<f64>,
    cummulative_quote_qty: Option<f64>,
    margin_side_effect: Option<MarginSideEffect>,
    borrowed_amount: Option<f64>,
    borrowed_asset: Option<String>,
    fills: Vec<OrderFill>,
    /// Weighted price updated from fills
    weighted_price: f64,
    total_executed_qty: f64,
    rejection_reason: Option<Rejection>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderFill {
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_asset: Option<String>,
    pub ts: DateTime<Utc>,
}

impl OrderDetail {
    #[allow(dead_code)]
    pub fn from_query(exchange: Exchange, transaction_id: Option<String>, add_order: AddOrderRequest) -> Self {
        let pair_string = add_order.pair.to_string();
        let (base_asset, quote_asset) = pair_string.split_once('_').expect("pair string should be BASE_QUOTE");
        let base_asset = base_asset.to_string();
        let quote_asset = quote_asset.to_string();
        Self {
            id: add_order.order_id.expect("order id is required"),
            transaction_id,
            remote_id: None,
            status: OrderStatus::Staged,
            exchange: exchange.to_string(),
            pair: pair_string,
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
            margin_side_effect: None,
            borrowed_amount: None,
            borrowed_asset: None,
            fills: vec![],
            weighted_price: 0.0,
            total_executed_qty: 0.0,
            rejection_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
        }
    }

    #[allow(dead_code)]
    pub fn from_submission(&mut self, submission: OrderSubmission) {
        self.executed_qty = Some(submission.executed_qty);
        self.cummulative_quote_qty = Some(submission.cummulative_quote_qty);
        self.borrowed_asset = submission.borrow_asset;
        self.borrowed_amount = submission.borrowed_amount;
        self.remote_id = Some(submission.id);
        self.status = OrderStatus::Created;
        self.updated_at = Utc::now();
    }

    #[allow(dead_code)]
    pub fn from_fill_update(&mut self, update: OrderUpdate) {
        let fill = OrderFill {
            price: update.last_executed_price,
            qty: update.last_executed_qty,
            fee: update.commission,
            fee_asset: update.commission_asset,
            ts: Utc.timestamp_millis(update.timestamp as i64),
        };
        self.fills.push(fill);
        self.cummulative_quote_qty = Some(update.cummulative_quote_asset_transacted_qty);
        self.total_executed_qty = update.cummulative_filled_qty;
        self.status = update.new_status.into();
        if self.status == OrderStatus::Filled {
            self.closed_at = Some(Utc::now());
        }
        self.updated_at = Utc::now();
        self.weighted_price = self.fills.iter().map(|fill| fill.price * fill.qty).sum::<f64>()
            / self.fills.iter().map(|fill| fill.qty).sum::<f64>();
    }

    #[allow(dead_code)]
    pub fn from_rejected(&mut self, rejection: Rejection) {
        self.rejection_reason = Some(rejection);
        self.status = OrderStatus::Rejected;
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod test {
    use coinnect_rt::types::{AddOrderRequest, OrderQuery, OrderSubmission, OrderUpdate};

    use crate::order_types::{Rejection, Transaction, TransactionStatus};

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
        };
        let tr1 = Transaction {
            id: order_id,
            status: statuses[1].clone(),
        };
        assert!(tr0.variant_eq(&tr0));
        assert!(!tr0.variant_eq(&tr1));
    }
}
