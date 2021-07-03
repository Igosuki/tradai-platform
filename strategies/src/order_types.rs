use actix::Message;
use anyhow::Result;
use coinnect_rt::types::{OrderInfo, OrderQuery, OrderStatus, OrderUpdate, Pair};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::types::TradeKind;
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
    pub(crate) fn from_status(os: OrderStatus, reason: Option<String>) -> Self {
        match os {
            OrderStatus::Rejected => match reason {
                Some(reason) => Rejection::Other(reason),
                None => Rejection::Other("".to_string()),
            },
            OrderStatus::Expired => Rejection::Timeout,
            OrderStatus::Canceled | OrderStatus::PendingCancel => Rejection::Cancelled(reason),
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
    New(OrderInfo),
    #[display(fmt = "filled")]
    Filled(OrderUpdate),
    #[display(fmt = "partially_filled")]
    PartiallyFilled(OrderUpdate),
    #[display(fmt = "rejected")]
    Rejected(Rejection),
}

impl WalCmp for TransactionStatus {
    fn is_before(&self, v: &Self) -> bool {
        if std::mem::discriminant(self) == std::mem::discriminant(v) {
            return false;
        }
        match self {
            Self::Staged(_) => match v {
                Self::New(_) | Self::PartiallyFilled(_) | Self::Rejected(_) | Self::Filled(_) => true,
                _ => false,
            },
            Self::New(_) => match v {
                Self::PartiallyFilled(_) | Self::Rejected(_) | Self::Filled(_) => true,
                _ => false,
            },
            Self::PartiallyFilled(_) => match v {
                Self::Rejected(_) | Self::Filled(_) => true,
                _ => false,
            },
            Self::Filled(_) => match v {
                Self::Rejected(_) => true,
                _ => false,
            },
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
        match self.status {
            TransactionStatus::Filled(_) => true,
            _ => false,
        }
    }

    pub fn is_bad_request(&self) -> bool {
        match self.status {
            TransactionStatus::Rejected(Rejection::BadRequest(_)) => true,
            _ => false,
        }
    }

    pub fn is_rejected(&self) -> bool {
        match self.status {
            TransactionStatus::Rejected(_) => true,
            _ => false,
        }
    }

    pub fn variant_eq(&self, b: &Transaction) -> bool {
        std::mem::discriminant(&self.status) == std::mem::discriminant(&b.status)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "Result<Transaction>")]
pub(crate) struct StagedOrder {
    pub op_kind: TradeKind,
    pub pair: Pair,
    pub qty: f64,
    pub price: f64,
    pub dry_run: bool,
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

#[cfg(test)]
mod test {
    use crate::order_types::{Rejection, Transaction, TransactionStatus};
    use coinnect_rt::types::{AddOrderRequest, OrderInfo, OrderQuery, OrderUpdate};

    #[test]
    fn test_variant_eq() {
        let order_id = "1".to_string();
        let statuses = vec![
            TransactionStatus::New(OrderInfo {
                timestamp: 0,
                id: order_id.clone(),
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
            id: order_id.clone(),
            status: statuses[1].clone(),
        };
        assert!(tr0.variant_eq(&tr0));
        assert!(!tr0.variant_eq(&tr1));
    }
}
