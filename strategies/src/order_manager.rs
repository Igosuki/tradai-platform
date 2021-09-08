use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture};
use actix_derive::{Message, MessageResponse};
use async_std::sync::RwLock;
use futures::FutureExt;
use uuid::Uuid;

use coinnect_rt::error::Error as CoinnectError;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::exchange_bot::Ping;
use coinnect_rt::types::{AccountEvent, AccountEventEnveloppe, AddOrderRequest, AssetType, Order, OrderQuery,
                         OrderStatus, OrderUpdate, Pair};
use db::{get_or_create, DbOptions};

use crate::error::Error;
use crate::error::Result;
use crate::order_types::{OrderId, PassOrder, Rejection, StagedOrder, Transaction, TransactionStatus};
use crate::wal::{Wal, WalCmp};

#[derive(Debug, Clone)]
pub(crate) struct TransactionService {
    om: Addr<OrderManager>,
}

impl TransactionService {
    pub fn new(om: Addr<OrderManager>) -> Self { Self { om } }

    /// Stage an order
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn stage_order(&self, staged_order: StagedOrder) -> Result<Transaction> {
        self.om
            .send(staged_order)
            .await
            .map_err(|_| Error::OrderManagerMailboxError)?
    }

    /// Fetches the latest version of this transaction for the order id
    /// Returns whether it changed, and the latest transaction retrieved
    pub async fn latest_transaction_change(&self, tr: &Transaction) -> crate::error::Result<(bool, Transaction)> {
        let new_tr = self
            .om
            .send(OrderId(tr.id.clone()))
            .await
            .map_err(|_| Error::OrderManagerMailboxError)??;
        Ok((!new_tr.variant_eq(tr), new_tr))
    }

    /// Retry staging an order if it was rejected
    pub async fn maybe_retry_trade(&self, tr: Transaction, order: StagedOrder) -> Result<Transaction> {
        if tr.is_rejected() {
            // Changed and rejected, retry transaction
            // TODO need to handle rejections in a finer grained way
            // TODO introduce a backoff
            self.stage_order(order).await
        } else {
            // TODO: Timeout can be managed here
            Ok(tr)
        }
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
}

static TRANSACTIONS_TABLE: &str = "transactions_wal";

// TODO: notify listeners every time a transaction is updated
impl OrderManager {
    pub fn new<S: AsRef<Path>, S2: AsRef<Path>>(
        api: Arc<dyn ExchangeApi>,
        db_options: &DbOptions<S>,
        db_path: S2,
    ) -> Self {
        let wal_db = get_or_create(db_options, db_path, vec![TRANSACTIONS_TABLE.to_string()]);
        let wal = Arc::new(Wal::new(wal_db, TRANSACTIONS_TABLE.to_string()));
        let orders = Arc::new(RwLock::new(HashMap::new()));
        OrderManager {
            xchg: api.exchange(),
            api,
            orders,
            transactions_wal: wal,
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
    pub(crate) async fn stage_order(&mut self, staged_order: StagedOrder) -> Result<Transaction> {
        let mut request = staged_order.request;
        let order_id = Uuid::new_v4().to_string();
        request.order_id = Some(order_id.clone());
        let add_order = OrderQuery::AddOrder(request);
        let staged_transaction = TransactionStatus::Staged(add_order.clone());
        self.register(order_id.clone(), staged_transaction.clone()).await?;
        Ok(Transaction {
            id: order_id.clone(),
            status: staged_transaction,
        })
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
                                pair.map(|pair| act.fetch_order(tr_id.clone(), pair, AssetType::Spot).boxed())
                                    .unwrap_or_else(|e| Box::pin(futures::future::err(e)))
                            },
                        ))
                        .await;
                    let mut notifications = vec![];
                    for order in non_filled_order_futs.into_iter().flatten() {
                        if let Some(tr_status) = orders_read_lock.get(&order.orig_order_id) {
                            if !equivalent_status(tr_status, &order.status) {
                                notifications
                                    .push(AccountEventEnveloppe(act.xchg, AccountEvent::OrderUpdate(order.into())));
                            }
                        } else {
                            notifications
                                .push(AccountEventEnveloppe(act.xchg, AccountEvent::OrderUpdate(order.into())));
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
                match msg.1 {
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
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: StagedOrder, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(
            async move { zis.stage_order(order).await }
                .into_actor(self)
                .map(|tr, _act, ctx| {
                    if let Ok(Transaction {
                        id,
                        status: TransactionStatus::Staged(query),
                    }) = &tr
                    {
                        ctx.notify(PassOrder {
                            id: id.clone(),
                            query: query.clone(),
                        });
                    }
                    tr
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
    type Result = ResponseActFuture<Self, Result<Transaction>>;

    fn handle(&mut self, order: OrderId, _ctx: &mut Self::Context) -> Self::Result {
        let zis = self.clone();
        Box::pin(
            async move {
                let order_id = order.0.clone();
                zis.get_order(order_id.clone())
                    .await
                    .ok_or_else(|| Error::OrderNotFound(order_id.clone()))
                    .map(move |status| Transaction { id: order_id, status })
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

pub mod test_util {
    use std::path::Path;
    use std::sync::Arc;

    use actix::{Actor, Addr};

    use coinnect_rt::coinnect::Coinnect;
    use coinnect_rt::exchange::MockApi;
    use coinnect_rt::exchange::{Exchange, ExchangeApi};
    use db::DbOptions;

    use crate::order_manager::OrderManager;

    pub async fn it_order_manager<S: AsRef<Path>, S2: AsRef<Path>>(
        keys_file: S2,
        dir: S,
        exchange: Exchange,
    ) -> OrderManager {
        let api = Coinnect::build_exchange_api(keys_file.as_ref().to_path_buf(), &exchange, true)
            .await
            .unwrap();
        let om_path = format!("om_{}", exchange);
        OrderManager::new(api, &DbOptions::new(dir), om_path)
    }

    pub fn new_mock_manager<S: AsRef<Path>>(path: S) -> OrderManager {
        let api: Arc<dyn ExchangeApi> = Arc::new(MockApi);
        OrderManager::new(api, &DbOptions::new(path), "")
    }

    pub fn mock_manager<S: AsRef<Path>>(path: S) -> Addr<OrderManager> {
        let order_manager = new_mock_manager(path);
        let act = OrderManager::start(order_manager);
        loop {
            if act.connected() {
                break;
            }
        }
        act
    }

    pub fn local_manager<S: AsRef<Path>>(path: S, api: Arc<dyn ExchangeApi>) -> Addr<OrderManager> {
        let order_manager = OrderManager::new(api, &DbOptions::new(path), "");
        OrderManager::start(order_manager)
    }
}

#[cfg(test)]
mod test {
    use coinnect_rt::exchange::Exchange::Binance;
    use coinnect_rt::types::{AddOrderRequest, OrderQuery, OrderSubmission, OrderUpdate, TradeType};
    use util::test::test_dir;

    use crate::order_manager::test_util::{it_order_manager, new_mock_manager};
    use crate::order_types::{Rejection, StagedOrder, Transaction, TransactionStatus};

    #[actix::test]
    async fn test_append_rejected() {
        let test_dir = test_dir();
        let mut order_manager = new_mock_manager(test_dir);
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

    #[actix::test]
    async fn test_binance_stage_order_invalid() {
        let test_dir = util::test::test_dir();
        let mut order_manager = it_order_manager(test_keys(), test_dir, Binance).await;
        let registered = order_manager
            .stage_order(StagedOrder {
                request: AddOrderRequest {
                    pair: test_pair().into(),
                    price: Some(0.0),
                    dry_run: true,
                    quantity: Some(0.0),
                    side: TradeType::Buy,
                    ..AddOrderRequest::default()
                },
            })
            .await;
        assert!(registered.is_ok(), "{:?}", registered);
    }

    #[actix::test]
    async fn test_register_transactions() {
        let test_dir = util::test::test_dir();
        let mut order_manager = it_order_manager(test_keys(), test_dir, Binance).await;
        let order_id = "1".to_string();
        let statuses = vec![
            TransactionStatus::New(OrderSubmission::default()),
            TransactionStatus::Staged(OrderQuery::AddOrder(AddOrderRequest::default())),
            TransactionStatus::Filled(OrderUpdate::default()),
            TransactionStatus::Rejected(Rejection::Other("".to_string())),
        ];
        // Register each status in order
        for status in &statuses {
            let reg = order_manager.register(order_id.clone(), status.clone()).await;
            assert!(reg.is_ok(), "{:?}", reg);
        }
        // Get the transactions log
        let transactions = order_manager.transactions();
        assert!(transactions.is_ok(), "{:?}", transactions);
        assert_eq!(
            transactions.unwrap(),
            statuses
                .clone()
                .into_iter()
                .map(|status| {
                    Transaction {
                        status,
                        id: order_id.clone(),
                    }
                })
                .collect::<Vec<Transaction>>()
        );
        // The last status for this id should be the last registered status
        let order = order_manager.get_order(order_id.clone()).await;
        assert_eq!(
            &order.unwrap(),
            statuses.last().unwrap(),
            "latest order should the last in statuses"
        );
        // Insert a new order status
        let reg = order_manager
            .register(
                order_id.clone(),
                TransactionStatus::New(OrderSubmission {
                    timestamp: 0,
                    id: order_id.clone(),
                    ..OrderSubmission::default()
                }),
            )
            .await;
        assert!(reg.is_ok());
        let order = order_manager.get_order(order_id.clone()).await;
        // The order registry should remain unchanged
        assert_eq!(
            &order.unwrap(),
            statuses.last().unwrap(),
            "latest order should the last in statuses after registering a new order"
        );
        let compacted = order_manager.transactions_wal.get_all_compacted();
        assert!(compacted.is_ok());
        assert_eq!(
            compacted.unwrap().get(&order_id.clone()),
            statuses.last(),
            "Compacted record should be the highest inserted status"
        )
    }
}
