use crate::model::TradeKind;
use crate::wal::Wal;
use actix::{
    Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Running, WrapFuture,
};
use anyhow::Result;
use async_std::sync::RwLock;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::exchange_bot::Ping;
use coinnect_rt::types::{
    AccountEvent, AccountEventEnveloppe, AddOrderRequest, OrderEnforcement, OrderInfo, OrderQuery,
    OrderStatus, OrderType, OrderUpdate, Pair, TradeType,
};
use db::Db;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Rejection {
    BadRequest(String),
    InsufficientFunds,
    Timeout,
    Cancelled(Option<String>),
    Other(String),
    Unknown(String),
}

impl Rejection {
    fn from_status(os: OrderStatus, reason: Option<String>) -> Self {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionStatus {
    Staged(OrderQuery),
    New(OrderInfo),
    Filled(OrderUpdate),
    PartiallyFilled(OrderUpdate),
    Rejected(Rejection),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Message)]
#[rtype(result = "Result<Transaction>")]
pub(crate) struct StagedOrder {
    pub op_kind: TradeKind,
    pub pair: Pair,
    pub qty: f64,
    pub price: f64,
    pub dry_run: bool,
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

    pub(crate) async fn update_order(&mut self, order: OrderUpdate) -> Result<()> {
        let order_id = order.orig_order_id.clone();
        let tr = if order.new_status.is_rejection() {
            Some(TransactionStatus::Rejected(Rejection::from_status(
                order.new_status,
                order.rejection_reason,
            )))
        } else if order.new_status == OrderStatus::PartialyFilled {
            Some(TransactionStatus::PartiallyFilled(order))
        } else if order.new_status == OrderStatus::Filled {
            Some(TransactionStatus::Filled(order))
        } else {
            None
        };
        if let Some(transaction) = tr {
            self.register(order_id, transaction).await
        } else {
            Err(anyhow!("Unknown order update"))
        }
    }

    pub(crate) async fn stage_order(&mut self, staged_order: StagedOrder) -> Result<Transaction> {
        let side = match staged_order.op_kind {
            TradeKind::BUY => TradeType::Buy,
            TradeKind::SELL => TradeType::Sell,
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
            dry_run: staged_order.dry_run,
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
        let written_transaction = match order_info {
            Ok(o) => TransactionStatus::New(o),
            Err(e) => TransactionStatus::Rejected(Rejection::BadRequest(format!("{}", e))),
        };
        self.register(order_id.clone(), written_transaction.clone())
            .await?;
        // Dry mode simulates transactions as filled
        // TODO: do this with a mocked account api instead
        if staged_order.dry_run {
            let filled_transaction = TransactionStatus::Filled(OrderUpdate::default());
            self.register(order_id.clone(), filled_transaction.clone())
                .await?;
        }
        Ok(Transaction {
            id: order_id.clone(),
            status: written_transaction,
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn cancel_order(&mut self, order_id: String) -> Result<()> {
        self.register(
            order_id,
            TransactionStatus::Rejected(Rejection::Cancelled(Some(
                "Order canceled directly".to_string(),
            ))),
        )
        .await
    }

    pub(crate) async fn get_order(&self, order_id: String) -> Option<TransactionStatus> {
        let reader = self.orders.read().await;
        reader.get(&order_id).cloned()
    }

    pub(crate) async fn register(&mut self, order_id: String, tr: TransactionStatus) -> Result<()> {
        self.transactions_wal.append(order_id.clone(), tr.clone())?;
        let mut writer = self.orders.write().await;
        writer.insert(order_id.clone(), tr.clone());
        Ok(())
    }
}

#[allow(clippy::unnested_or_patterns)]
fn equivalent_status(trs: &TransactionStatus, os: &OrderStatus) -> bool {
    match (trs, os) {
        (TransactionStatus::Filled(_), OrderStatus::Filled)
        | (TransactionStatus::Rejected(_), OrderStatus::Rejected)
        | (TransactionStatus::Rejected(_), OrderStatus::PendingCancel)
        | (TransactionStatus::Rejected(_), OrderStatus::Canceled)
        | (TransactionStatus::Rejected(_), OrderStatus::Expired)
        | (TransactionStatus::Staged(_), OrderStatus::New)
        | (TransactionStatus::PartiallyFilled(_), OrderStatus::PartialyFilled) => true,
        _ => false,
    }
}

impl Actor for OrderManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Starting Order Manager");
        ctx.run_later(Duration::from_millis(0), |act, ctx| {
            async {
                let orders_read_lock = act.orders.read().await;
                // Fetch all latest orders
                let latest_orders = futures::future::join_all(
                    orders_read_lock
                        .iter()
                        .filter(|(_k, v)| match v {
                            TransactionStatus::PartiallyFilled(_)
                            | TransactionStatus::Staged(_) => true,
                            _ => false,
                        })
                        .map(|(tr_id, _tr_status)| act.api.get_order(tr_id.clone())),
                )
                .await;
                for lo in latest_orders {
                    if let Ok(order) = lo {
                        let order_id = order.orig_order_id.clone();
                        if let Some(tr_status) = orders_read_lock.get(&order_id) {
                            if !equivalent_status(&tr_status, &order.status) {
                                ctx.notify(AccountEventEnveloppe(
                                    Exchange::Binance,
                                    AccountEvent::OrderUpdate(order.into()),
                                ));
                            }
                        } else {
                            ctx.notify(AccountEventEnveloppe(
                                Exchange::Binance,
                                AccountEvent::OrderUpdate(order.into()),
                            ));
                        }
                    }
                }
            }
            .into_actor(act);
        });
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
    type Result = ResponseActFuture<Self, Result<()>>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(
            async move {
                match msg.1 {
                    AccountEvent::OrderUpdate(update) => zis.update_order(update).await,
                    // Ignore anything besides order updates
                    _ => Ok(()),
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<StagedOrder> for OrderManager {
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: StagedOrder, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(async move { zis.stage_order(order).await }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<Transaction>")]
pub struct OrderId(pub String);

impl Handler<OrderId> for OrderManager {
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: OrderId, _ctx: &mut Self::Context) -> Self::Result {
        let zis = self.clone();
        Box::pin(
            async move {
                let order_id = order.0.clone();
                zis.get_order(order_id.clone())
                    .await
                    .map(move |status| Transaction {
                        id: order_id,
                        status,
                    })
                    .ok_or_else(|| anyhow!("No order found"))
            }
            .into_actor(self),
        )
    }
}

impl Handler<Ping> for OrderManager {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}

#[cfg(test)]
pub mod test_util {
    use crate::order_manager::OrderManager;
    use actix::{Actor, Addr};
    use coinnect_rt::exchange::ExchangeApi;
    use coinnect_rt::exchange::MockApi;
    use std::path::Path;
    use std::sync::Arc;

    pub fn mock_manager(path: &str) -> Addr<OrderManager> {
        let capi: Box<dyn ExchangeApi> = Box::new(MockApi);
        let api = Arc::new(capi);
        let order_manager = OrderManager::new(api, Path::new(path));
        OrderManager::start(order_manager)
    }
}
