use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture};
use actix_derive::{Message, MessageResponse};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use futures::FutureExt;
use itertools::Itertools;
use std::time::Duration;
use tokio::sync::RwLock;

use brokers::bot::Ping;
use brokers::error::Error as CoinnectError;
use brokers::manager::{BrokerageManager, BrokerageManagerRef};
use brokers::prelude::*;
use brokers::types::{Order, OrderQuery, OrderStatus, OrderUpdate};
use db::{get_or_create, DbOptions, Storage};
use ext::ResultExt;
use wal::{Wal, WalCmp};

use crate::order_manager::repo::OrderRepository;

use self::error::{Error, Result};
use self::types::{OrderDetail, OrderId, PassOrder, Rejection, StagedOrder, Transaction, TransactionStatus};

pub mod error;
mod exec;
pub use exec::*;
mod repo;
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

#[derive(Serialize, Deserialize)]
pub struct BackoffConfig {
    #[serde(deserialize_with = "util::ser::string_duration_opt")]
    initial_interval: Option<Duration>,
    randomization_factor: Option<f64>,
    multiplier: Option<f64>,
    #[serde(deserialize_with = "util::ser::string_duration_opt")]
    max_interval: Option<Duration>,
    #[serde(deserialize_with = "util::ser::string_duration_opt")]
    max_elapsed_time: Option<Duration>,
}

impl BackoffConfig {
    fn exponential(&self) -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(self.max_elapsed_time)
            .with_max_interval(
                self.max_interval
                    .unwrap_or(Duration::from_millis(backoff::default::MAX_INTERVAL_MILLIS)),
            )
            .with_multiplier(self.multiplier.unwrap_or(backoff::default::MULTIPLIER))
            .with_randomization_factor(
                self.randomization_factor
                    .unwrap_or(backoff::default::RANDOMIZATION_FACTOR),
            )
            .with_initial_interval(
                self.initial_interval
                    .unwrap_or(Duration::from_millis(backoff::default::INITIAL_INTERVAL_MILLIS)),
            )
            .build()
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct OrderManagerConfig {
    order_retry_backoff: Option<BackoffConfig>,
}

impl OrderManagerConfig {
    fn backoff(&self) -> Option<ExponentialBackoff> { self.order_retry_backoff.as_ref().map(|bc| bc.exponential()) }
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
    xchg_manager: BrokerageManagerRef,
    orders: Arc<RwLock<HashMap<String, TransactionStatus>>>,
    pub transactions_wal: Arc<Wal>,
    pub repo: OrderRepository,
    pub order_retry_backoff: Option<ExponentialBackoff>,
}

impl OrderManager {
    const TRANSACTIONS_TABLE: &'static str = "transactions_wal";

    pub fn new(apis: BrokerageManagerRef, storage: Arc<dyn Storage>) -> Self {
        Self::new_with_options(apis, storage, OrderManagerConfig::default())
    }

    pub async fn actor(db: &DbOptions<String>, exchange_manager: Arc<BrokerageManager>) -> Addr<Self> {
        let storage = get_or_create(db, "order_manager", vec![]);
        let order_manager = Self::new_with_options(exchange_manager, storage, OrderManagerConfig::default());
        Self::start(order_manager)
    }

    pub fn new_with_options(
        exchange_manager: BrokerageManagerRef,
        storage: Arc<dyn Storage>,
        config: OrderManagerConfig,
    ) -> Self {
        let wal = Arc::new(Wal::new(storage.clone(), Self::TRANSACTIONS_TABLE.to_string()));
        let orders = Arc::new(RwLock::new(HashMap::new()));
        OrderManager {
            xchg_manager: exchange_manager,
            orders,
            transactions_wal: wal,
            repo: OrderRepository::new(storage),
            order_retry_backoff: config.backoff(),
        }
    }

    /// Updates an already registered order
    pub(crate) async fn update_order(&mut self, order: OrderUpdate) -> Result<()> {
        let order_id = order.orig_order_id.clone();
        let tr = if order.new_status.is_rejection() {
            TransactionStatus::Rejected(Rejection::from_status(&order.new_status, order.rejection_reason))
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
        let request = staged_order.request;
        let add_order = OrderQuery::AddOrder(request.clone());
        let staged_transaction = TransactionStatus::Staged(add_order);
        let order_id = request.order_id.clone();
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
            let pair_conf = brokers::pair::pair_conf(&order.query.xch(), &order.query.pair())?;
            let query = order.query.truncate(&pair_conf);
            let order_info = self.xchg_manager.expect_api(query.xch()).order(query).await;
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
    pub(crate) async fn fetch_order(
        &self,
        order_id: String,
        xch: Exchange,
        pair: Pair,
        asset_type: AssetType,
    ) -> Result<Order> {
        Ok(self
            .xchg_manager
            .expect_api(xch)
            .get_order(order_id, pair, asset_type)
            .await?)
    }

    /// Registers a transaction
    #[tracing::instrument(skip(self), level = "info")]
    pub(crate) async fn register(&mut self, order_id: String, tr: TransactionStatus) -> Result<()> {
        self.transactions_wal.append(order_id.as_str(), tr.clone())?;
        let should_write = {
            let orders = self.orders.read().await;
            orders.get(&order_id).map_or(true, |status| status.is_before(&tr))
        };
        let order = self.get_order_from_storage(&order_id);
        let result = match (tr.clone(), order) {
            (TransactionStatus::Staged(OrderQuery::AddOrder(add_order)), _) => {
                self.repo.put(OrderDetail::from_query(add_order))
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
            tracing::error!(order_id = %order_id, error = %e, "Failed to update order in order table");
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
    ///
    pub async fn repair_orders(&self) -> Vec<AccountEventEnveloppe> {
        {
            let mut writer = self.orders.write().await;
            if let Ok(wal_transactions) = self.transactions_wal.get_all_compacted() {
                writer.extend(wal_transactions);
            }
        }
        let orders_read_lock = self.orders.read().await;
        // Fetch all latest orders
        info!("fetching remote orders for all unfilled transactions");
        // TODO : replace with sql or simply the orders table to stop using the log
        let non_filled_order_futs =
            futures::future::join_all(orders_read_lock.iter().filter(|(_k, v)| v.is_incomplete()).map(
                |(tr_id, tr_status)| {
                    let pair = tr_status.get_pair(Exchange::Binance);
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
                            let mut od = OrderDetail::from_query(request);
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
                        .and_then(|o| {
                            pair.and_then(|pair| {
                                Ok(self
                                    .fetch_order(tr_id.clone(), Exchange::from_str(&o.exchange)?, pair, o.asset_type)
                                    .boxed())
                            })
                        })
                        .unwrap_or_else(|e| {
                            debug!(error = ?e, "failed to fetch order");
                            Box::pin(futures::future::err(e))
                        })
                },
            ))
            .await;
        let mut notifications = vec![];
        for order in non_filled_order_futs {
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
                                xchg: order.xch,
                                event: AccountEvent::OrderUpdate(order.into()),
                                account_type,
                            });
                        }
                    } else {
                        notifications.push(AccountEventEnveloppe {
                            xchg: order.xch,
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
        info!("starting order manager");
        let manager = self.clone();
        let refresh_orders =
            async move { manager.repair_orders().await }
                .into_actor(self)
                .map(|notifications, _, ctx| {
                    for notification in notifications {
                        ctx.notify(notification);
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
        Box::pin(
            async move {
                match zis.pass_order(msg).await {
                    Err(_e) => {
                        // TODO: for now, never retry, as we don't know if the order has passed or not at that point
                        //backoff::Error::Permanent(e)
                        // if let Some(backoff) = zis.order_retry_backoff {
                        //     let mut backoff = backoff.clone();
                        //     if let Some(_) = backoff.next_backoff() {
                        //         ctx.notify(msg.clone());
                        //     }
                        // }
                        Ok(())
                    }
                    Ok(()) => Ok(()),
                }
            }
            .into_actor(self),
        )
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
