use crate::naive_pair_trading::state::OperationKind;
use anyhow::Result;
use chrono::Utc;
use coinnect_rt::exchange::ExchangeApi;
use coinnect_rt::types::{
    AddOrderRequest, OrderEnforcement, OrderInfo, OrderQuery, OrderType, Pair, TradeType,
};
use db::{Db, WriteResult};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Rejection {
    BadRequest,
    InsufficientFunds,
    Timeout,
    Cancelled,
    OtherFailure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionStatus {
    Staged(OrderQuery),
    New(OrderInfo),
    Filled,
    Rejected(Rejection),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    id: String,
    status: TransactionStatus,
}

fn into_transaction(res: anyhow::Result<OrderInfo>) -> TransactionStatus {
    match res {
        Ok(o) => TransactionStatus::New(o),
        Err(_e) => TransactionStatus::Rejected(Rejection::BadRequest),
    }
}

pub(crate) struct StagedOrder {
    pub op_kind: OperationKind,
    pub pair: Pair,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug)]
pub(crate) struct OrderManager {
    api: Arc<Box<dyn ExchangeApi>>,
    orders: HashMap<String, TransactionStatus>,
    transactions_wal: Db,
}

impl OrderManager {
    pub(crate) fn new(api: Arc<Box<dyn ExchangeApi>>, db_path: &Path) -> Self {
        let wal_db = Db::new(
            &format!("{}/transactions", db_path.to_str().unwrap()),
            "transactions_wal".to_string(),
        );
        OrderManager {
            api,
            orders: HashMap::new(),
            transactions_wal: wal_db,
        }
    }

    pub(crate) async fn stage_order(&self, staged_order: StagedOrder) -> Result<Transaction> {
        let side = match staged_order.op_kind {
            OperationKind::BUY => TradeType::Buy,
            OperationKind::SELL => TradeType::Sell,
            _ => unimplemented!(),
        };
        let order_id = Uuid::new_v4().to_string();
        let add_order = OrderQuery::AddOrder(AddOrderRequest {
            pair: staged_order.pair,
            side,
            order_id: Some(order_id.clone()),
            quantity: Some(staged_order.qty),
            price: Some(staged_order.price),
            order_type: OrderType::Limit,
            enforcement: Some(OrderEnforcement::FOK),
            ..AddOrderRequest::default()
        });
        let staged_transaction = TransactionStatus::Staged(add_order.clone());
        self.append_wal(staged_transaction)?;
        let order_info: Result<OrderInfo> = self
            .api
            .order(add_order)
            .await
            .map_err(|e| anyhow!("Coinnect error {0}", e));
        let written_transaction = into_transaction(order_info);
        self.append_wal(written_transaction.clone())?;
        // self.orders
        //     .insert(order_id.clone(), written_transaction.clone());
        Ok(Transaction {
            id: order_id.clone(),
            status: written_transaction.clone(),
        })
    }

    pub(crate) fn cancel_order(&mut self, order_id: String) {
        self.orders.remove(&order_id);
    }

    pub(crate) fn get_order(&self, order_id: String) {
        self.orders.get(&order_id);
    }

    fn append_wal(&self, t: TransactionStatus) -> WriteResult {
        self.transactions_wal
            .put_json(&Utc::now().timestamp_millis().to_string(), t)
    }

    pub(crate) async fn stage_orders(&self, orders: Vec<StagedOrder>) -> Vec<Result<Transaction>> {
        let order_queries = orders.into_iter().map(|o| self.stage_order(o));
        futures::future::join_all(order_queries).await
    }
}
