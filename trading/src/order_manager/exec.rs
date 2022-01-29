use super::error::*;
use crate::order_manager::types::{OrderDetail, OrderId, StagedOrder, Transaction};
use crate::order_manager::OrderManager;
use crate::types::TradeOperation;
use actix::Addr;
use std::fmt::Debug;

#[derive(Debug, AsRefStr, PartialEq)]
pub enum OrderResolution {
    #[strum(serialize = "filled")]
    Filled,
    #[strum(serialize = "cancelled")]
    Cancelled,
    #[strum(serialize = "no_change")]
    NoChange,
    #[strum(serialize = "bad_request")]
    BadRequest,
    #[strum(serialize = "rejected")]
    Rejected,
    #[strum(serialize = "retryable")]
    Retryable,
}

/// Handles and queries orders
#[async_trait]
pub trait OrderExecutor: Send + Sync + Debug {
    /// Stage an order
    async fn stage_order(&self, staged_order: StagedOrder) -> Result<OrderDetail>;
    /// Retry staging an order if it was rejected
    async fn stage_trade(&self, trade: &TradeOperation) -> Result<OrderDetail>;
    /// Resolves the order against the latest known order detail and transaction,
    /// the order resolution is indicative of what can be done in relation
    /// to the two orders
    async fn resolve_pending_order(
        &self,
        order: &OrderDetail,
    ) -> Result<(OrderDetail, Option<Transaction>, OrderResolution)>;
    /// Returns the latest known detail and transaction for this order id
    async fn get_order(&self, order_id: &str) -> Result<(OrderDetail, Option<Transaction>)>;
}

#[derive(Debug, Clone)]
pub struct OrderManagerClient {
    om: Addr<OrderManager>,
}

impl OrderManagerClient {
    pub fn new(om: Addr<OrderManager>) -> Self { Self { om } }
}

#[async_trait]
impl OrderExecutor for OrderManagerClient {
    async fn stage_order(&self, staged_order: StagedOrder) -> Result<OrderDetail> {
        self.om
            .send(staged_order)
            .await
            .map_err(|_| super::Error::OrderManagerMailboxError)?
    }

    async fn stage_trade(&self, trade: &TradeOperation) -> Result<OrderDetail> {
        let staged_order = StagedOrder {
            request: trade.clone().into(),
        };
        self.stage_order(staged_order).await.map_err(|e| {
            error!("Failed to retry trade {:?} : {}", trade, e);
            e
        })
    }

    async fn get_order(&self, order_id: &str) -> Result<(OrderDetail, Option<Transaction>)> {
        self.om
            .send(OrderId(order_id.to_string()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)
            .and_then(|(or, t)| or.map(|o| (o, t.ok())))
    }

    async fn resolve_pending_order(
        &self,
        order: &OrderDetail,
    ) -> Result<(OrderDetail, Option<Transaction>, OrderResolution)> {
        let (stored_order, resolved_transaction) = self.get_order(order.id.as_str()).await?;
        let result = if order.is_same_status(&stored_order.status) {
            OrderResolution::NoChange
        } else if stored_order.is_filled() {
            OrderResolution::Filled
        } else if stored_order.is_bad_request() {
            OrderResolution::BadRequest
        } else if stored_order.is_rejected() {
            if stored_order.is_retryable() {
                OrderResolution::Retryable
            } else {
                OrderResolution::Rejected
            }
        } else {
            OrderResolution::NoChange
        };
        Ok((stored_order, resolved_transaction, result))
    }
}
