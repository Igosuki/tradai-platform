use actix::MailboxError;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{AddOrderRequest, OrderEnforcement, OrderQuery, OrderType, TradeType};
use futures::lock::Mutex;
use futures::Stream;
use juniper::{FieldError, FieldResult, RootNode};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use strategies::naive_pair_trading::state::Operation;
use strategies::query::{DataQuery, DataResult, FieldMutation};
use strategies::{Strategy, StrategyKey};
use uuid::Uuid;

pub struct Context {
    pub strats: Arc<HashMap<StrategyKey, Strategy>>,
    pub exchanges: Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>,
}

impl juniper::Context for Context {}

impl Context {
    async fn with_strat<T, F>(&self, tk: TypeAndKeyInput, q: DataQuery, f: F) -> FieldResult<T>
    where
        F: Fn(DataResult) -> FieldResult<T>,
    {
        let strat = StrategyKey::from(&tk.t, &tk.id).ok_or_else(|| {
            FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            )
        })?;
        match self.strats.get(&strat) {
            None => Err(FieldError::new(
                "Strategy not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(strat) => {
                let res = strat.1.send(q).await;
                match res {
                    Ok(Ok(Some(dr))) => f(dr),
                    Err(_) => Err(FieldError::new(
                        "Strategy mailbox was full",
                        graphql_value!({ "unavailable": "strategy mailbox full" }),
                    )),
                    r => {
                        error!("{:?}", r);
                        Err(FieldError::new(
                            "Unexpected error",
                            graphql_value!({ "unavailable": "unexpected error" }),
                        ))
                    }
                }
            }
        }
    }
}

pub struct QueryRoot;

#[derive(juniper::GraphQLObject)]
pub struct TypeAndKey {
    #[graphql(name = "type")]
    t: String,
    id: String,
}

#[derive(juniper::GraphQLInputObject)]
pub struct TypeAndKeyInput {
    #[graphql(name = "type")]
    t: String,
    id: String,
}

#[derive(juniper::GraphQLEnum)]
pub enum TradeTypeInput {
    Sell,
    Buy,
}

impl Into<TradeType> for TradeTypeInput {
    fn into(self) -> TradeType {
        match self {
            TradeTypeInput::Sell => TradeType::Sell,
            TradeTypeInput::Buy => TradeType::Buy,
        }
    }
}

#[derive(juniper::GraphQLEnum)]
pub enum OrderTypeInput {
    Limit,
    Market,
}

impl Into<OrderType> for OrderTypeInput {
    fn into(self) -> OrderType {
        match self {
            OrderTypeInput::Limit => OrderType::Limit,
            OrderTypeInput::Market => OrderType::Market,
        }
    }
}

#[derive(juniper::GraphQLInputObject)]
pub struct AddOrderInput {
    exchg: String,
    order_type: OrderTypeInput,
    side: TradeTypeInput,
    pair: String,
    quantity: f64,
    price: f64,
    #[graphql(description = "Set this to true to pass a real order")]
    dry_run: bool,
}

#[derive(juniper::GraphQLObject)]
pub struct OrderResult {
    pub identifier: String,
}

fn unhandled_data_result<T>() -> FieldResult<T> {
    Err(FieldError::new(
        "Unhandled result",
        graphql_value!({ "unavailable": "wrong result for query" }),
    ))
}

#[juniper::graphql_object(Context = Context)]
impl QueryRoot {
    #[graphql(description = "List of all strats")]
    fn strats(context: &Context) -> FieldResult<Vec<TypeAndKey>> {
        let keys: Vec<&StrategyKey> = context.strats.keys().collect();
        Ok(keys
            .iter()
            .map(|&sk| TypeAndKey {
                t: sk.0.to_string(),
                id: sk.1.clone(),
            })
            .collect())
    }

    #[graphql(description = "Get all positions for this strat")]
    async fn dump_strat_db(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<String>> {
        context
            .with_strat(tk, DataQuery::Dump, |dr| {
                if let DataResult::Dump(pos_vec) = dr {
                    Ok(pos_vec)
                } else {
                    unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Get all positions for this strat")]
    async fn operations(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Operation>> {
        context
            .with_strat(tk, DataQuery::Operations, |dr| {
                match dr {
                    DataResult::NaiveOperations(pos_vec) | DataResult::MeanRevertingOperations(pos_vec) {
                        Ok(pos_vec)
                    }
                    _ => unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Get the current operation")]
    async fn current_operation(
        context: &Context,
        tk: TypeAndKeyInput,
    ) -> FieldResult<Option<Operation>> {
        context
            .with_strat(tk, DataQuery::CurrentOperation, |dr| {
                match dr {
                    DataResult::NaiveOperation(op) | DataResult::MeanRevertingOperation(op) {
                        Ok(op)
                    }
                    _ => unhandled_data_result()
                }
            })
            .await
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(Context = Context)]
impl MutationRoot {
    #[graphql(description = "Get all positions for this strat")]
    fn state(context: &Context, tk: TypeAndKeyInput, fm: FieldMutation) -> FieldResult<bool> {
        StrategyKey::from(&tk.t, &tk.id)
            .ok_or_else(|| {
                FieldError::new(
                    "Strategy type not found",
                    graphql_value!({ "not_found": "strategy type not found" }),
                )
            })
            .and_then(|strat| match context.strats.get(&strat) {
                None => Err(FieldError::new(
                    "Strategy not found",
                    graphql_value!({ "not_found": "strategy not found" }),
                )),
                Some(strat) => {
                    let f = strat.1.send(fm);
                    match futures::executor::block_on(f) {
                        Ok(_) => Ok(true),
                        Err(MailboxError::Closed | MailboxError::Timeout) => Err(FieldError::new(
                            "Strategy mailbox was full",
                            graphql_value!({ "unavailable": "strategy mailbox full" }),
                        )),
                    }
                }
            })
    }

    #[graphql(description = "Add an order (for testing)")]
    async fn add_order(context: &Context, input: AddOrderInput) -> FieldResult<OrderResult> {
        let exchg: Exchange = input.exchg.clone().into();
        let mut api_lock = context.exchanges.lock().await;
        let api = api_lock.get_mut(&exchg).ok_or_else(|| {
            FieldError::new(
                "Exchange type not found",
                graphql_value!({ "not_found": "exchange type not found" }),
            )
        })?;

        let request = AddOrderRequest {
            order_type: input.order_type.into(),
            side: input.side.into(),
            quantity: Some(input.quantity),
            pair: input.pair.into(),
            price: Some(input.price),
            enforcement: Some(OrderEnforcement::FOK),
            dry_run: input.dry_run,
            order_id: Some(Uuid::new_v4().to_string()),
            ..AddOrderRequest::default()
        };
        api.order(OrderQuery::AddOrder(request))
            .await
            .map_err(|e| {
                let error_str = format!("{:?}", e);
                FieldError::new("Coinnect error", graphql_value!({ "error": error_str }))
            })
            .map(|oi| OrderResult { identifier: oi.id })
    }
}

pub struct Subscription;

type StringStream = Pin<Box<dyn Stream<Item = Result<String, FieldError>> + Send>>;

#[juniper::graphql_subscription(Context = Context)]
impl Subscription {
    async fn hello_world() -> StringStream {
        let stream =
            tokio::stream::iter(vec![Ok(String::from("Hello")), Ok(String::from("World!"))]);
        Box::pin(stream)
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, Subscription>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot, MutationRoot, Subscription)
}
