use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture};
use actix_derive::{Message, MessageResponse};
use futures::FutureExt;
use itertools::Itertools;
use strum_macros::AsRefStr;
use tokio::sync::RwLock;
use uuid::Uuid;

use coinnect_rt::bot::Ping;
use coinnect_rt::error::Error as CoinnectError;
use coinnect_rt::prelude::*;
use coinnect_rt::types::{Order, OrderStatus, OrderUpdate};
use db::{get_or_create, DbOptions, Storage, StorageExt};
use ext::ResultExt;

use self::error::{Error, Result};
use crate::types::TradeOperation;
use wal::{Wal, WalCmp};

use self::types::{OrderDetail, OrderId, PassOrder, Rejection, StagedOrder, Transaction, TransactionStatus};

pub mod error;
#[cfg(any(
    test,
    feature = "test_util",
    feature = "live_e2e_tests",
    feature = "manual_e2e_tests"
))]
pub mod test_util;
#[cfg(test)]
mod tests;
pub mod types;
mod wal;

static TRANSACTIONS_TABLE: &str = "transactions_wal";
static ORDERS_TABLE: &str = "orders";

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

#[derive(Debug, Clone)]
pub struct OrderRepository {
    db: Arc<dyn Storage>,
}

impl OrderRepository {
    pub(crate) fn new(db: Arc<dyn Storage>) -> Self { Self { db } }

    pub(crate) fn get(&self, id: &str) -> Result<OrderDetail> { self.db.get(ORDERS_TABLE, id).err_into() }

    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) fn put(&self, order: OrderDetail) -> Result<()> {
        self.db.put(ORDERS_TABLE, &order.id.clone(), order).err_into()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionService {
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

    pub async fn resolve_pending_order(
        &self,
        order: &OrderDetail,
    ) -> Result<(OrderDetail, Option<Transaction>, OrderResolution)> {
        let (resolved_order, resolved_transaction) = self
            .om
            .send(OrderId(order.id.clone()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?;
        let stored_order = resolved_order?;
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
    AllTransactions,
    OrderTransactions(String),
}

#[derive(Debug, Clone)]
pub struct OrderManager {
    xchg: Exchange,
    api: Arc<dyn ExchangeApi>,
    orders: Arc<RwLock<HashMap<String, TransactionStatus>>>,
    pub transactions_wal: Arc<Wal>,
    pub repo: OrderRepository,
}

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
        } else if order.new_status == OrderStatus::PartiallyFilled {
            TransactionStatus::PartiallyFilled(order)
        } else if order.new_status == OrderStatus::Filled {
            TransactionStatus::Filled(order)
        } else {
            return Ok(());
        };
        self.register(order_id, tr).await
    }

    /// Registers an order, and passes it to be later processed
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
    pub(crate) async fn pass_order(&mut self, order: PassOrder) -> Result<()> {
        // Dry mode simulates transactions as filled
        let written_transaction = if let PassOrder {
            query: OrderQuery::AddOrder(request @ AddOrderRequest { dry_run: true, .. }),
            ..
        } = order
        {
            TransactionStatus::New(request.into())
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
    pub(crate) fn get_order_from_storage(&self, order_id: &str) -> Result<OrderDetail> {
        self.repo.get(order_id).err_into()
    }

    /// Get the latest remote status for this order id
    pub(crate) async fn fetch_order(&self, order_id: String, pair: Pair, asset_type: AssetType) -> Result<Order> {
        Ok(self.api.get_order(order_id, pair, asset_type).await?)
    }

    /// Registers a transaction
    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) async fn register(&mut self, order_id: String, tr: TransactionStatus) -> Result<()> {
        self.transactions_wal.append(order_id.clone(), tr.clone())?;
        let should_write = {
            let orders = self.orders.read().await;
            orders
                .get(&order_id)
                .map(|status| status.is_before(&tr))
                .unwrap_or(true)
        };
        let order = self.get_order_from_storage(&order_id);
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
            _ => Err(Error::OrderNotFound(order_id.clone())),
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

    ///
    ///
    /// # Arguments
    ///
    /// * `order_id`: if None, return all transactions, if set, only the transactions of this order
    ///
    /// returns: Result<Vec<Transaction, Global>, Error>
    ///
    pub(crate) fn transactions(&self, order_id: Option<String>) -> Result<Vec<Transaction>> {
        match order_id {
            Some(order_id) => self.transactions_wal.get_all_k(&order_id).map(|tr| {
                tr.into_iter()
                    .map(|(ts, tr)| Transaction {
                        status: tr,
                        id: String::new(),
                        ts: Some(ts),
                    })
                    .collect()
            }),
            None => self.transactions_wal.get_all().map(|r| {
                r.into_iter()
                    .map(|(ts, (id, status))| Transaction {
                        status,
                        id,
                        ts: Some(ts),
                    })
                    .collect()
            }),
        }
    }

    pub fn transactions_wal(&self) -> Arc<Wal> { self.transactions_wal.clone() }

    /// Checks that any transactions have corresponding order detail,
    /// and refresh any unfinished order from remote
    pub async fn repair_orders(&self) -> Vec<AccountEventEnveloppe> {
        {
            let mut writer = self.orders.write().await;
            if let Ok(wal_transactions) = self.transactions_wal.get_all_compacted() {
                writer.extend(wal_transactions);
            }
        }
        let orders_read_lock = self.orders.read().await;
        // Fetch all latest orders
        info!(xchg = ?self.xchg, "fetching remote orders for all unfilled transactions");
        let non_filled_order_futs =
            futures::future::join_all(orders_read_lock.iter().filter(|(_k, v)| v.is_incomplete()).map(
                |(tr_id, tr_status)| {
                    let pair = tr_status.get_pair(self.xchg);
                    info!(order_id = ?tr_id.clone(), pair = ?pair, "fetching remote for unresolved order");
                    let order = self.repo.get(tr_id).or_else(|_| {
                        // If not found, try to rebuild the order detail from the transactions
                        let transactions: Vec<(i64, TransactionStatus)> = self.transactions_wal.get_all_k(tr_id)?;
                        let (mut iter, iter2) = transactions.into_iter().map(|t| t.1).tee();
                        let staged_order_predicate =
                            |ts: &TransactionStatus| matches!(ts, TransactionStatus::Staged(_));
                        let staged_tr = iter.find(staged_order_predicate);
                        let other_trs = iter2.filter(|ts| !staged_order_predicate(ts));
                        if let Some(TransactionStatus::Staged(OrderQuery::AddOrder(request))) = staged_tr {
                            let mut od = OrderDetail::from_query(self.xchg, None, request);
                            for tr in other_trs {
                                od.from_status(tr);
                            }
                            self.repo.put(od.clone())?;
                            Ok(od)
                        } else {
                            Err(Error::StagedOrderRequired)
                        }
                    });
                    order
                        .and_then(|o| pair.map(|pair| self.fetch_order(tr_id.clone(), pair, o.asset_type).boxed()))
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
                    let account_type = if order.asset_type.is_margin() {
                        AccountType::Margin
                    } else {
                        AccountType::Spot
                    };
                    if let Some(tr_status) = orders_read_lock.get(&order.orig_order_id) {
                        if !equivalent_status(tr_status, &order.status) {
                            notifications.push(AccountEventEnveloppe {
                                xchg: self.xchg,
                                event: AccountEvent::OrderUpdate(order.into()),
                                account_type,
                            });
                        }
                    } else {
                        notifications.push(AccountEventEnveloppe {
                            xchg: self.xchg,
                            event: AccountEvent::OrderUpdate(order.into()),
                            account_type,
                        });
                    }
                }
                Err(e) => error!(error = ?e, "Failed to resolve remote order"),
            }
        }
        notifications
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
            | (TransactionStatus::PartiallyFilled(_), OrderStatus::PartiallyFilled)
    )
}

impl Actor for OrderManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(xchg = ?self.xchg, "starting order manager");
        let manager = self.clone();
        let refresh_orders =
            async move { manager.repair_orders().await }
                .into_actor(self)
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
                    zis.get_order_from_storage(&order_id),
                    zis.get_order(order_id.clone())
                        .await
                        .ok_or_else(|| Error::OrderNotFound(order_id.clone()))
                        .map(move |status| Transaction {
                            id: order_id,
                            status,
                            ts: None,
                        }),
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
            DataQuery::AllTransactions => self.transactions(None),
            DataQuery::OrderTransactions(id) => self.transactions(Some(id)),
        }
        .map(|r| Some(DataResult::Transactions(r)))
    }
}

impl Handler<Ping> for OrderManager {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}
