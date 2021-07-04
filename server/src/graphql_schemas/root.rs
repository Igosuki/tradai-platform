use crate::graphql_schemas::types::*;
use actix::{Addr, MailboxError};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::OrderQuery;
use futures::lock::Mutex;
use futures::Stream;
use juniper::{FieldError, FieldResult, RootNode};
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use strategies::mean_reverting::state::Operation as MeanOperation;
use strategies::naive_pair_trading::state::Operation;
use strategies::order_manager::OrderManager;
use strategies::order_types::PassOrder;
use strategies::query::{DataQuery, DataResult, FieldMutation};
use strategies::{order_manager, Strategy, StrategyKey};

pub struct Context {
    pub strats: Arc<HashMap<StrategyKey, Strategy>>,
    pub exchanges: Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>,
    pub order_managers: Arc<HashMap<Exchange, Addr<OrderManager>>>,
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

    async fn with_order_manager<T, F, M: 'static>(&self, exchange: &str, q: M, f: F) -> FieldResult<T>
    where
        F: Fn(M::Result) -> FieldResult<T>,
        M: actix::Message + Send,
        M::Result: Send + Debug,
        OrderManager: actix::Handler<M>,
    {
        let xchg = Exchange::from_str(exchange).ok().ok_or_else(|| {
            FieldError::new(
                "Exchange not found",
                graphql_value!({ "not_found": "exchange not found" }),
            )
        })?;
        match self.order_managers.get(&xchg) {
            None => Err(FieldError::new(
                "Exchange not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(om) => {
                let res = om.send(q).await;
                match res {
                    Ok(dr) => f(dr),
                    Err(_) => Err(FieldError::new(
                        "OrderManager mailbox was full",
                        graphql_value!({ "unavailable": "order manager mailbox full" }),
                    )),
                }
            }
        }
    }
}

pub struct QueryRoot;

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

    #[graphql(description = "Dump the stored state for a strategy")]
    async fn dump_strat_db(context: &Context, tk: TypeAndKeyInput) -> FieldResult<String> {
        context
            .with_strat(tk, DataQuery::Dump, |dr| {
                if let DataResult::Dump(dump) = dr {
                    Ok(dump)
                } else {
                    unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Dump the current in memory state for a strategy")]
    async fn strat_state(context: &Context, tk: TypeAndKeyInput) -> FieldResult<String> {
        context
            .with_strat(tk, DataQuery::State, |dr| {
                if let DataResult::State(state_str) = dr {
                    Ok(state_str)
                } else {
                    unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Get all positions for this naive strat")]
    async fn naive_operations(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Operation>> {
        context
            .with_strat(tk, DataQuery::Operations, |dr| match dr {
                DataResult::NaiveOperations(pos_vec) => Ok(pos_vec),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get all positions for this mean strat")]
    async fn mean_operations(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<MeanOperation>> {
        context
            .with_strat(tk, DataQuery::Operations, |dr| match dr {
                DataResult::MeanRevertingOperations(pos_vec) => Ok(pos_vec),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get the current naive strat operation")]
    async fn current_naive_operation(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Option<Operation>> {
        context
            .with_strat(tk, DataQuery::CurrentOperation, |dr| match dr {
                DataResult::NaiveOperation(op) => Ok(op),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get the current naive strat operation")]
    async fn current_mean_operation(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Option<MeanOperation>> {
        context
            .with_strat(tk, DataQuery::CurrentOperation, |dr| match dr {
                DataResult::MeanRevertingOperation(op) => Ok(op),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get all transactions history for an order manager")]
    async fn transactions(context: &Context, exchange: String) -> FieldResult<Vec<String>> {
        context
            .with_order_manager(&exchange, order_manager::DataQuery::Transactions, |dr| match dr {
                Ok(Some(order_manager::DataResult::Transactions(transaction))) => Ok(transaction
                    .into_iter()
                    .map(|t| serde_json::to_string(&t).unwrap())
                    .collect()),
                _ => unhandled_data_result(),
            })
            .await
    }
}

pub struct MutationRoot;

#[juniper::graphql_object(Context = Context)]
impl MutationRoot {
    #[graphql(description = "Get all positions for this strat")]
    async fn state(context: &Context, tk: TypeAndKeyInput, fm: FieldMutation) -> FieldResult<bool> {
        let strat = StrategyKey::from(&tk.t, &tk.id).ok_or_else(|| {
            FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            )
        })?;
        match context.strats.get(&strat) {
            None => Err(FieldError::new(
                "Strategy not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(strat) => match strat.1.send(fm).await {
                Ok(_) => Ok(true),
                Err(MailboxError::Closed | MailboxError::Timeout) => Err(FieldError::new(
                    "Strategy mailbox was full",
                    graphql_value!({ "unavailable": "strategy mailbox full" }),
                )),
            },
        }
    }

    #[graphql(description = "Cancel the ongoing operation")]
    async fn cancel_ongoing_op(context: &Context, tk: TypeAndKeyInput) -> FieldResult<bool> {
        context
            .with_strat(tk, DataQuery::CancelOngoingOp, |dr| match dr {
                DataResult::OngongOperationCancelation(was_canceled) => Ok(was_canceled),
                _ => unhandled_data_result(),
            })
            .await
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

        api.order(input.into())
            .await
            .map_err(|e| {
                let error_str = format!("{:?}", e);
                FieldError::new("Coinnect error", graphql_value!({ "error": error_str }))
            })
            .map(|oi| OrderResult { identifier: oi.id })
    }

    #[graphql(description = "Pass an order with an order manager")]
    async fn _pass_order(context: &Context, exchange: String, input: AddOrderInput) -> FieldResult<String> {
        let query: OrderQuery = input.into();
        let id = query.id();
        match id {
            Some(id) => {
                context
                    .with_order_manager(&exchange, PassOrder { id, query }, |dr| match dr {
                        Ok(_) => Ok("passed".to_string()),
                        Err(e) => {
                            let error_str = format!("{}", e);
                            Err(FieldError::new("order error", graphql_value!({ "error": error_str })))
                        }
                    })
                    .await
            }
            None => unhandled_data_result(),
        }
    }
}

pub struct Subscription;

type StringStream = Pin<Box<dyn Stream<Item = Result<String, FieldError>> + Send>>;

#[juniper::graphql_subscription(Context = Context)]
impl Subscription {
    async fn hello_world() -> StringStream {
        let stream = tokio_stream::iter(vec![Ok(String::from("Hello")), Ok(String::from("World!"))]);
        Box::pin(stream)
    }
}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot, Subscription>;

pub fn create_schema() -> Schema { Schema::new(QueryRoot, MutationRoot, Subscription) }
