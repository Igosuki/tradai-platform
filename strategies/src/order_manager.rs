use crate::naive_pair_trading::state::OperationKind;
use crate::wal::Wal;
use actix::{Actor, Context, Handler, Message, ResponseActFuture, Running, WrapFuture};
use anyhow::Result;
use async_std::sync::RwLock;
use coinnect_rt::exchange::ExchangeApi;
use coinnect_rt::types::{
    AccountEvent, AccountEventEnveloppe, AddOrderRequest, OrderEnforcement, OrderInfo, OrderQuery,
    OrderType, OrderUpdate, Pair, TradeType,
};
use db::Db;
use serde::Serialize;
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
    Filled(OrderUpdate),
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

#[derive(Message)]
#[rtype(result = "Result<Transaction>")]
pub(crate) struct StagedOrder {
    pub op_kind: OperationKind,
    pub pair: Pair,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub struct OrderManager {
    api: Arc<Box<dyn ExchangeApi>>,
    orders: Arc<RwLock<HashMap<String, TransactionStatus>>>,
    transactions_wal: Wal,
}

impl OrderManager {
    pub fn new(api: Arc<Box<dyn ExchangeApi>>, db_path: &Path) -> Self {
        let wal_db = Db::new(
            &format!("{}/transactions", db_path.to_str().unwrap()),
            "transactions_wal".to_string(),
        );
        let wal = Wal::new(wal_db);
        let orders = Arc::new(RwLock::new(wal.read_all()));
        OrderManager {
            api,
            orders,
            transactions_wal: wal,
        }
    }

    pub(crate) fn update_order(&self, order: OrderUpdate) {}

    pub(crate) async fn stage_order(&mut self, staged_order: StagedOrder) -> Result<Transaction> {
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
        self.transactions_wal
            .append(order_id.clone(), staged_transaction)?;
        let order_info: Result<OrderInfo> = self
            .api
            .order(add_order)
            .await
            .map_err(|e| anyhow!("Coinnect error {0}", e));
        let written_transaction = into_transaction(order_info);
        self.transactions_wal
            .append(order_id.clone(), written_transaction.clone())?;
        let mut writer = self.orders.write().await;
        writer.insert(order_id.clone(), written_transaction.clone());
        Ok(Transaction {
            id: order_id.clone(),
            status: written_transaction.clone(),
        })
    }

    pub(crate) async fn cancel_order(&mut self, order_id: String) {
        let mut writer = self.orders.write().await;
        writer.remove(&order_id);
    }

    pub(crate) async fn get_order(&self, order_id: String) -> Option<TransactionStatus> {
        let reader = self.orders.read().await;
        reader.get(&order_id).map(|o| o.clone())
    }
}

impl Actor for OrderManager {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("Starting Order Manager");
    }
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("Stopping Order Manager");
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Order Manager actor stopped...");
    }
}

impl Handler<AccountEventEnveloppe> for OrderManager {
    type Result = ();

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        match msg.1 {
            AccountEvent::OrderUpdate(update) => self.update_order(update),
            // Ignore anything besides order updates
            _ => {}
        }
    }
}

impl Handler<StagedOrder> for OrderManager {
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: StagedOrder, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::new(async move { zis.stage_order(order).await }.into_actor(self))
    }
}
