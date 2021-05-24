use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, ResponseActFuture, Running, WrapFuture};
use anyhow::Result;
use async_std::sync::RwLock;
use uuid::Uuid;

use coinnect_rt::error::Error as CoinnectError;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::exchange_bot::Ping;
use coinnect_rt::types::{AccountEvent, AccountEventEnveloppe, AddOrderRequest, OrderEnforcement, OrderQuery,
                         OrderStatus, OrderType, OrderUpdate, TradeType};
use db::Db;

use crate::order_types::{OrderId, Rejection, StagedOrder, Transaction, TransactionStatus};
use crate::types::TradeKind;
use crate::wal::Wal;

#[derive(Debug, Clone)]
pub(crate) struct TransactionService {
    om: Addr<OrderManager>,
}

impl TransactionService {
    pub fn new(om: Addr<OrderManager>) -> Self { Self { om } }

    pub async fn stage_order(&self, staged_order: StagedOrder) -> Result<Transaction> {
        self.om
            .send(staged_order)
            .await?
            .map_err(|e| anyhow!("mailbox error {}", e))
    }

    /// Fetches the latest version of this transaction for the order id
    /// Returns whether it changed, and the latest transaction retrieved
    pub async fn latest_transaction_change(&self, tr: &Transaction) -> anyhow::Result<(bool, Transaction)> {
        let new_tr = self.om.send(OrderId(tr.id.clone())).await??;
        Ok((!new_tr.variant_eq(&tr), new_tr))
    }

    pub async fn maybe_retry_trade(&self, tr: Transaction, order: StagedOrder) -> anyhow::Result<Transaction> {
        if tr.is_rejected() {
            // Changed and rejected, retry transaction
            // TODO need to handle rejections in a finer grained way
            // TODO introduce a backoff
            self.stage_order(order).await
        } else {
            // TODO: Timeout can be managed here
            Err(anyhow!("Nor rejected nor filled"))
        }
    }
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
        self.transactions_wal.append(order_id.clone(), staged_transaction)?;
        let order_info = self.api.order(add_order).await;
        let written_transaction = match order_info {
            Ok(o) => TransactionStatus::New(o),
            Err(e) => TransactionStatus::Rejected(match e {
                CoinnectError::InvalidPrice => Rejection::InvalidPrice,
                _ => Rejection::BadRequest(format!("{}", e)),
            }),
        };
        self.register(order_id.clone(), written_transaction.clone()).await?;
        // Dry mode simulates transactions as filled
        if staged_order.dry_run {
            let filled_transaction = TransactionStatus::Filled(OrderUpdate::default());
            self.register(order_id.clone(), filled_transaction.clone()).await?;
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
            TransactionStatus::Rejected(Rejection::Cancelled(Some("Order canceled directly".to_string()))),
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
                            TransactionStatus::PartiallyFilled(_) | TransactionStatus::Staged(_) => true,
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

impl Handler<OrderId> for OrderManager {
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: OrderId, _ctx: &mut Self::Context) -> Self::Result {
        let zis = self.clone();
        Box::pin(
            async move {
                let order_id = order.0.clone();
                zis.get_order(order_id.clone())
                    .await
                    .map(move |status| Transaction { id: order_id, status })
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
    use std::path::Path;
    use std::sync::Arc;

    use actix::{Actor, Addr};

    use coinnect_rt::exchange::ExchangeApi;
    use coinnect_rt::exchange::MockApi;

    use crate::order_manager::OrderManager;

    pub fn mock_manager(path: &str) -> Addr<OrderManager> {
        let capi: Box<dyn ExchangeApi> = Box::new(MockApi);
        let api = Arc::new(capi);
        let order_manager = OrderManager::new(api, Path::new(path));
        OrderManager::start(order_manager)
    }

    pub fn local_manager(path: &str, api: Box<dyn ExchangeApi>) -> Addr<OrderManager> {
        let order_manager = OrderManager::new(Arc::new(api), Path::new(path));
        OrderManager::start(order_manager)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::sync::Arc;

    use coinnect_rt::coinnect;
    use coinnect_rt::exchange::Exchange::Binance;
    use coinnect_rt::exchange::MockApi;
    use coinnect_rt::exchange::{Exchange, ExchangeApi};

    use crate::order_manager::OrderManager;
    use crate::order_types::{Rejection, StagedOrder, TransactionStatus};
    use crate::test_util::test_dir;
    use crate::types::TradeKind;

    #[actix_rt::test]
    async fn test_append_rejected() {
        let path = "default";
        let capi: Box<dyn ExchangeApi> = Box::new(MockApi);
        let api = Arc::new(capi);
        let mut order_manager = OrderManager::new(api, Path::new(path));
        let registered = order_manager
            .register(
                "id".to_string(),
                TransactionStatus::Rejected(Rejection::BadRequest("bad request".to_string())),
            )
            .await;
        assert!(registered.is_ok(), "{:?}", registered);
    }

    fn test_keys() -> String { "../config/keys_real_test.json".to_string() }

    fn test_pair() -> String { "BTC_USDT".to_string() }

    async fn it_order_manager(exchange: Exchange) -> OrderManager {
        let api = coinnect::build_exchange_api(test_keys().into(), &exchange, true)
            .await
            .unwrap();
        let om_path = format!("{}/om_{}", test_dir(), exchange);
        OrderManager::new(Arc::new(api), Path::new(&om_path))
    }

    #[actix_rt::test]
    async fn test_binance_stage_order_invalid() {
        let mut order_manager = it_order_manager(Binance).await;
        let registered = order_manager
            .stage_order(StagedOrder {
                op_kind: TradeKind::BUY,
                pair: test_pair().into(),
                qty: 0.0,
                price: 0.0,
                dry_run: false,
            })
            .await;
        println!("{:?}", registered);
        assert!(registered.is_ok(), "{:?}", registered);
    }
}
