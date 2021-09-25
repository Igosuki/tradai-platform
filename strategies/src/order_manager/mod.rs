use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture};
use actix_derive::{Message, MessageResponse};
use futures::FutureExt;
use tokio::sync::RwLock;
use uuid::Uuid;

use coinnect_rt::bot::Ping;
use coinnect_rt::error::Error as CoinnectError;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{AccountEvent, AccountEventEnveloppe, AddOrderRequest, AssetType, Order, OrderQuery,
                         OrderStatus, OrderUpdate, Pair};
use db::{get_or_create, DbOptions, Storage, StorageExt};
use ext::ResultExt;

use crate::coinnect_types::AccountType;
use crate::error::Error;
use crate::error::Error::OrderNotFound;
use crate::error::Result;
use crate::types::TradeOperation;
use crate::wal::{Wal, WalCmp};

use self::types::{OrderDetail, OrderId, PassOrder, Rejection, StagedOrder, Transaction, TransactionStatus};

pub mod test_util;
#[cfg(test)]
mod tests;
pub mod types;

static TRANSACTIONS_TABLE: &str = "transactions_wal";
static ORDERS_TABLE: &str = "orders";

pub enum OrderResolution {
    OperationCancelled,
    NoTransactionChange,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderRepository {
    db: Arc<dyn Storage>,
}

impl OrderRepository {
    pub(crate) fn new(db: Arc<dyn Storage>) -> Self { Self { db } }

    pub(crate) fn get(&self, id: &str) -> Result<OrderDetail> { self.db.get(ORDERS_TABLE, id).err_into() }

    pub(crate) fn put(&self, order: OrderDetail) -> Result<()> {
        self.db.put(ORDERS_TABLE, &order.id.clone(), order).err_into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionService {
    om: Addr<OrderManager>,
}

impl TransactionService {
    pub fn new(om: Addr<OrderManager>) -> Self { Self { om } }

    /// Stage an order
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn stage_order(&self, staged_order: StagedOrder) -> Result<OrderDetail> {
        self.om
            .send(staged_order)
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?
    }

    /// Retry staging an order if it was rejected
    pub async fn stage_trade(&self, trade: &TradeOperation) -> Result<OrderDetail> {
        let staged_order = StagedOrder {
            request: trade.clone().into(),
        };
        self.stage_order(staged_order).await.map_err(|e| {
            error!("Failed to retry trade {:?} : {}", trade, e);
            e
        })
    }

    /// TODO: handle finer grained rejections such as timeouts
    /// TODO: introduce a optional backoff for restaging orders
    pub async fn resolve_pending_order(
        &self,
        order: &OrderDetail,
    ) -> Result<(OrderDetail, Option<Transaction>, Result<()>)> {
        if order.is_cancelled() {
            return Err(Error::OperationCancelled);
        }
        let (resolved_order, resolved_transaction) = self
            .om
            .send(OrderId(order.id.clone()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?;
        let stored_order = resolved_order?;
        let result = if !order.is_same_status(&stored_order.status) {
            return Err(Error::NoTransactionChange);
        } else if stored_order.is_filled() {
            Ok(())
        } else if order.is_bad_request() {
            Err(Error::OperationBadRequest)
        } else if order.is_rejected() {
            Err(Error::OperationRestaged)
        } else {
            Err(Error::NoTransactionChange)
        };
        Ok((stored_order, resolved_transaction.ok(), result))
    }
}

// TODO: Use GraphQLUnion to refactor this ugly bit of code
#[derive(Debug, Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    Transactions(Vec<Transaction>),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Result<Option<DataResult>>")]
pub enum DataQuery {
    /// All transactions history
    Transactions,
}

#[derive(Debug, Clone)]
pub struct OrderManager {
    xchg: Exchange,
    api: Arc<dyn ExchangeApi>,
    orders: Arc<RwLock<HashMap<String, TransactionStatus>>>,
    transactions_wal: Arc<Wal>,
    repo: OrderRepository,
}

// TODO: notify listeners every time a transaction is updated
impl OrderManager {
    pub fn new<S: AsRef<Path>, S2: AsRef<Path>>(
        api: Arc<dyn ExchangeApi>,
        db_options: &DbOptions<S>,
        db_path: S2,
    ) -> Self {
        let manager_db = get_or_create(db_options, db_path, vec![
            TRANSACTIONS_TABLE.to_string(),
            ORDERS_TABLE.to_string(),
        ]);
        let wal = Arc::new(Wal::new(manager_db.clone(), TRANSACTIONS_TABLE.to_string()));
        let orders = Arc::new(RwLock::new(HashMap::new()));
        OrderManager {
            xchg: api.exchange(),
            api,
            orders,
            transactions_wal: wal,
            repo: OrderRepository::new(manager_db),
        }
    }

    /// Updates an already registered order
    pub(crate) async fn update_order(&mut self, order: OrderUpdate) -> Result<()> {
        let order_id = order.orig_order_id.clone();
        let tr = if order.new_status.is_rejection() {
            TransactionStatus::Rejected(Rejection::from_status(order.new_status, order.rejection_reason))
        } else if order.new_status == OrderStatus::PartialyFilled {
            TransactionStatus::PartiallyFilled(order)
        } else if order.new_status == OrderStatus::Filled {
            TransactionStatus::Filled(order)
        } else {
            return Ok(());
        };
        self.register(order_id, tr).await
    }

    /// Registers an order, and passes it to be later processed
    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) async fn stage_order(&mut self, staged_order: StagedOrder) -> Result<(AddOrderRequest, OrderDetail)> {
        let mut request = staged_order.request;
        if request.order_id.is_none() {
            let order_id = Uuid::new_v4().to_string();
            request.order_id = Some(order_id);
        }
        let add_order = OrderQuery::AddOrder(request.clone());
        let staged_transaction = TransactionStatus::Staged(add_order.clone());
        let order_id = request.order_id.as_ref().unwrap().clone();
        self.register(order_id.clone(), staged_transaction.clone()).await?;
        Ok((request, self.repo.get(&order_id)?))
    }

    /// Directly passes an order query
    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) async fn pass_order(&mut self, order: PassOrder) -> Result<()> {
        // Dry mode simulates transactions as filled
        let written_transaction = if let PassOrder {
            query:
                OrderQuery::AddOrder(AddOrderRequest {
                    dry_run: true,
                    quantity: Some(qty),
                    price: Some(price),
                    side,
                    ..
                }),
            ..
        } = order
        {
            let update = OrderUpdate {
                cummulative_filled_qty: qty,
                last_executed_price: price,
                side,
                ..OrderUpdate::default()
            };
            TransactionStatus::Filled(update)
        } else {
            // Here the order is truncated according to the exchange configuration
            let pair_conf = coinnect_rt::pair::pair_conf(&self.xchg, &order.query.pair())?;
            let query = order.query.truncate(pair_conf);
            let order_info = self.api.order(query).await;
            match order_info {
                Ok(o) => TransactionStatus::New(o),
                Err(e) => TransactionStatus::Rejected(match e {
                    CoinnectError::InvalidPrice => Rejection::InvalidPrice,
                    _ => Rejection::BadRequest(format!("{}", e)),
                }),
            }
        };
        self.register(order.id.clone(), written_transaction.clone()).await?;
        Ok(())
    }

    /// Cancel an order
    #[allow(dead_code)]
    pub(crate) async fn cancel_order(&mut self, order_id: String) -> Result<()> {
        self.register(
            order_id,
            TransactionStatus::Rejected(Rejection::Cancelled(Some("Order canceled directly".to_string()))),
        )
        .await
    }

    /// Get the latest status for this order id
    pub(crate) async fn get_order(&self, order_id: String) -> Option<TransactionStatus> {
        let reader = self.orders.read().await;
        reader.get(&order_id).cloned()
    }

    /// Get the order from storage
    pub(crate) fn get_order_from_storage(&self, order_id: String) -> Option<OrderDetail> {
        self.repo.get(&order_id).ok()
    }

    /// Get the latest remote status for this order id
    pub(crate) async fn fetch_order(&self, order_id: String, pair: Pair, asset_type: AssetType) -> Result<Order> {
        Ok(self.api.get_order(order_id, pair, asset_type).await?)
    }

    /// Registers a transaction
    pub(crate) async fn register(&mut self, order_id: String, tr: TransactionStatus) -> Result<()> {
        self.transactions_wal.append(order_id.clone(), tr.clone())?;
        let should_write = {
            let orders = self.orders.read().await;
            orders
                .get(&order_id)
                .map(|status| status.is_before(&tr))
                .unwrap_or(true)
        };
        let order = self.repo.get(&order_id);
        let result = match (tr.clone(), order) {
            (TransactionStatus::Staged(OrderQuery::AddOrder(add_order)), _) => {
                self.repo.put(OrderDetail::from_query(self.xchg, None, add_order))
            }
            (TransactionStatus::New(submission), Ok(mut order)) => {
                order.from_submission(submission);
                self.repo.put(order)
            }
            (TransactionStatus::Filled(update) | TransactionStatus::PartiallyFilled(update), Ok(mut order)) => {
                order.from_fill_update(update);
                self.repo.put(order)
            }
            (TransactionStatus::Rejected(rejection), Ok(mut order)) => {
                order.from_rejected(rejection);
                self.repo.put(order)
            }
            _ => Err(OrderNotFound(order_id.clone())),
        };
        if let Err(e) = result {
            tracing::error!(order_id = %order_id, error = %e, "Failed to update order in order table")
        }
        if should_write {
            let mut writer = self.orders.write().await;
            writer.insert(order_id.clone(), tr.clone());
        }
        Ok(())
    }

    /// Returns all history of transactions
    pub(crate) fn transactions(&self) -> Result<Vec<Transaction>> {
        self.transactions_wal.get_all().map(|r| {
            r.into_iter()
                .map(|(_ts, (id, status))| Transaction { status, id })
                .collect()
        })
    }
}

#[allow(clippy::unnested_or_patterns)]
fn equivalent_status(trs: &TransactionStatus, os: &OrderStatus) -> bool {
    matches!(
        (trs, os),
        (TransactionStatus::Filled(_), OrderStatus::Filled)
            | (TransactionStatus::Rejected(_), OrderStatus::Rejected)
            | (TransactionStatus::Rejected(_), OrderStatus::PendingCancel)
            | (TransactionStatus::Rejected(_), OrderStatus::Canceled)
            | (TransactionStatus::Rejected(_), OrderStatus::Expired)
            | (TransactionStatus::Staged(_), OrderStatus::New)
            | (TransactionStatus::PartiallyFilled(_), OrderStatus::PartialyFilled)
    )
}

impl Actor for OrderManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(xchg = ?self.xchg, "starting order manager");
        let refresh_orders = async {}
            .into_actor(self)
            .then(|_, acty: &mut OrderManager, _ctx| {
                let act = acty.clone();
                async move {
                    {
                        let mut writer = act.orders.write().await;
                        if let Ok(wal_transactions) = act.transactions_wal.get_all_compacted() {
                            writer.extend(wal_transactions);
                        }
                    }
                    let orders_read_lock = act.orders.read().await;
                    // Fetch all latest orders
                    info!(xchg = ?act.xchg, "fetching remote orders for all unfilled transactions");
                    let non_filled_order_futs =
                        futures::future::join_all(orders_read_lock.iter().filter(|(_k, v)| v.is_incomplete()).map(
                            |(tr_id, tr_status)| {
                                let pair = tr_status.get_pair(act.xchg);
                                info!(order_id = ?tr_id.clone(), pair = ?pair, "fetching remote for unresolved order");
                                let order = act.repo.get(tr_id);
                                order
                                    .and_then(|o| {
                                        pair.map(|pair| act.fetch_order(tr_id.clone(), pair, o.asset_type).boxed())
                                    })
                                    .unwrap_or_else(|e| {
                                        debug!(error = ?e, "failed to fetch order");
                                        Box::pin(futures::future::err(e))
                                    })
                            },
                        ))
                        .await;
                    let mut notifications = vec![];
                    for order in non_filled_order_futs.into_iter() {
                        match order {
                            Ok(order) => {
                                if let Some(tr_status) = orders_read_lock.get(&order.orig_order_id) {
                                    if !equivalent_status(tr_status, &order.status) {
                                        notifications.push(AccountEventEnveloppe {
                                            xchg: act.xchg,
                                            event: AccountEvent::OrderUpdate(order.into()),
                                            account_type: AccountType::Spot,
                                        });
                                    }
                                } else {
                                    notifications.push(AccountEventEnveloppe {
                                        xchg: act.xchg,
                                        event: AccountEvent::OrderUpdate(order.into()),
                                        account_type: AccountType::Spot,
                                    });
                                }
                            }
                            Err(e) => error!(error = ?e, "Failed to resolve remote order"),
                        }
                    }
                    notifications
                }
                .into_actor(acty)
            })
            .map(|notifications, _, ctx| {
                for notification in notifications {
                    ctx.notify(notification)
                }
            });
        ctx.spawn(Box::pin(refresh_orders));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("order manager actor stopped...");
    }
}

impl Handler<AccountEventEnveloppe> for OrderManager {
    type Result = ResponseActFuture<Self, anyhow::Result<()>>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(
            async move {
                match msg.event {
                    AccountEvent::OrderUpdate(update) => zis.update_order(update).await.map_err(|e| anyhow!(e)),
                    // Ignore anything besides order updates
                    _ => Ok(()),
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<StagedOrder> for OrderManager {
    type Result = ResponseActFuture<Self, Result<OrderDetail>>;

    fn handle(&mut self, order: StagedOrder, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(
            async move { zis.stage_order(order).await }
                .into_actor(self)
                .map(|tr, _act, ctx| {
                    if let Ok((request, order_detail)) = &tr {
                        ctx.notify(PassOrder {
                            id: order_detail.id.clone(),
                            query: OrderQuery::AddOrder(request.clone()),
                        });
                    }
                    tr.map(|r| r.1)
                }),
        )
    }
}

impl Handler<PassOrder> for OrderManager {
    type Result = ResponseActFuture<Self, Result<()>>;

    fn handle(&mut self, msg: PassOrder, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(async move { zis.pass_order(msg).await }.into_actor(self))
    }
}

impl Handler<OrderId> for OrderManager {
    type Result = ResponseActFuture<Self, (Result<OrderDetail>, Result<Transaction>)>;

    fn handle(&mut self, order: OrderId, _ctx: &mut Self::Context) -> Self::Result {
        let zis = self.clone();
        Box::pin(
            async move {
                let order_id = order.0.clone();
                (
                    zis.get_order_from_storage(order_id.clone())
                        .ok_or_else(|| Error::OrderNotFound(order_id.clone())),
                    zis.get_order(order_id.clone())
                        .await
                        .ok_or_else(|| Error::OrderNotFound(order_id.clone()))
                        .map(move |status| Transaction { id: order_id, status }),
                )
            }
            .into_actor(self),
        )
    }
}

impl Handler<DataQuery> for OrderManager {
    type Result = <DataQuery as actix::Message>::Result;

    fn handle(&mut self, query: DataQuery, _ctx: &mut Self::Context) -> Self::Result {
        match query {
            DataQuery::Transactions => self.transactions().map(|r| Some(DataResult::Transactions(r))),
        }
    }
}

impl Handler<Ping> for OrderManager {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}
