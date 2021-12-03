use std::pin::Pin;

use futures::Stream;
use itertools::Itertools;
use juniper::{FieldError, FieldResult, RootNode};

use coinnect_rt::prelude::*;
use strategy::query::{DataQuery, DataResult, ModelReset, PortfolioSnapshot, StateFieldMutation};
use strategy::{StrategyKey, StrategyLifecycleCmd, StrategyStatus};
use trading::order_manager;
use trading::order_manager::types::PassOrder;
use trading::position::Position;

use crate::graphql_schemas::unhandled_data_result;

use super::context::Context;
use super::types::*;

pub(crate) struct QueryRoot;

#[juniper::graphql_object(Context = Context)]
impl QueryRoot {
    #[graphql(description = "List of all strats")]
    fn strats(context: &Context) -> FieldResult<Vec<StrategyState>> {
        let keys: Vec<&StrategyKey> = context.strats.keys().collect();
        Ok(keys
            .iter()
            .map(|&sk| StrategyState {
                t: sk.0.clone(),
                id: sk.1.clone(),
            })
            .collect())
    }

    #[graphql(description = "Dump the current portfolio indicators for a strategy")]
    async fn strat_indicators(context: &Context, tk: TypeAndKeyInput) -> FieldResult<PortfolioSnapshot> {
        context
            .with_strat(tk, DataQuery::Indicators, |dr| {
                if let DataResult::Indicators(indicators) = dr {
                    Ok(indicators)
                } else {
                    unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Current strategy status")]
    async fn strat_status(context: &Context, tk: TypeAndKeyInput) -> FieldResult<String> {
        context
            .with_strat(tk, DataQuery::Status, |dr| {
                if let DataResult::Status(status) = dr {
                    Ok(serde_json::to_string(&status).unwrap())
                } else {
                    unhandled_data_result()
                }
            })
            .await
    }

    #[graphql(description = "Get all positions for this strat")]
    async fn positions(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Position>> {
        context
            .with_strat(tk, DataQuery::PositionHistory, |dr| {
                match dr {
                    DataResult::PositionHistory(pos_vec) => Ok(pos_vec),
                    _ => unhandled_data_result(),
                }
                .map(|positions: Vec<Position>| {
                    positions
                        .into_iter()
                        .sorted_by(|a, b| a.meta.open_at.cmp(&b.meta.open_at))
                        .rev()
                        .collect()
                })
            })
            .await
    }

    #[graphql(description = "Get the ongoing operation for the strat")]
    async fn open_positions(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Position>> {
        context
            .with_strat(tk, DataQuery::OpenPositions, |dr| match dr {
                DataResult::OpenPositions(o) => Ok(o),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get all transactions history for an order manager")]
    async fn transactions(context: &Context, exchange: String) -> FieldResult<Vec<String>> {
        context
            .with_order_manager(&exchange, order_manager::DataQuery::AllTransactions, |dr| match dr? {
                Some(order_manager::DataResult::Transactions(transaction)) => Ok(transaction
                    .into_iter()
                    .map(|t| serde_json::to_string(&t).unwrap())
                    .collect()),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Get all transactions of a single order")]
    async fn order_transactions(context: &Context, exchange: String, order_id: String) -> FieldResult<Vec<String>> {
        context
            .with_order_manager(
                &exchange,
                order_manager::DataQuery::OrderTransactions(order_id),
                |dr| match dr? {
                    Some(order_manager::DataResult::Transactions(transaction)) => Ok(transaction
                        .into_iter()
                        .map(|t| serde_json::to_string(&t).unwrap())
                        .collect()),
                    _ => unhandled_data_result(),
                },
            )
            .await
    }

    #[graphql(description = "Get the latest model values")]
    async fn models(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Model>> {
        context
            .with_strat(tk, DataQuery::Models, |dr| match dr {
                DataResult::Models(o) => {
                    let response = o
                        .iter()
                        .map(|(k, v)| Model {
                            id: k.to_string(),
                            json: serde_json::to_string(v).unwrap(),
                        })
                        .collect();
                    Ok(response)
                }
                _ => unhandled_data_result(),
            })
            .await
    }
}

pub(crate) struct MutationRoot;

#[juniper::graphql_object(Context = Context)]
impl MutationRoot {
    #[graphql(description = "Get all positions for this strat")]
    async fn state(context: &Context, tk: TypeAndKeyInput, fm: StateFieldMutation) -> FieldResult<bool> {
        context.with_strat_mut(tk, fm).await.map(|r| r.is_ok())
    }

    #[graphql(description = "Cancel the ongoing operation")]
    async fn cancel_ongoing_op(context: &Context, tk: TypeAndKeyInput) -> FieldResult<bool> {
        context
            .with_strat(tk, DataQuery::CancelOngoingOp, |dr| match dr {
                DataResult::Success(was_canceled) => Ok(was_canceled),
                _ => unhandled_data_result(),
            })
            .await
    }

    #[graphql(description = "Reset the specified model")]
    async fn reset_model(context: &Context, tk: TypeAndKeyInput, mr: ModelReset) -> FieldResult<StrategyStatus> {
        context.with_strat_mut(tk, mr).await.and_then(|r| {
            r.map_err(|e| {
                FieldError::new(
                    format!("{}", e),
                    graphql_value!({"strategy error" : "failed to reset model"}),
                )
            })
        })
    }

    #[graphql(description = "Send a lifecycle command to the strategy")]
    async fn lifecycle_cmd(
        context: &Context,
        tk: TypeAndKeyInput,
        slc: StrategyLifecycleCmd,
    ) -> FieldResult<StrategyStatus> {
        context.with_strat_mut(tk, slc).await.and_then(|r| {
            r.map_err(|e| {
                FieldError::new(
                    format!("{}", e),
                    graphql_value!({"strategy error" : "failed to send lifecycle command"}),
                )
            })
        })
    }

    #[graphql(description = "Add an order (for testing)")]
    async fn add_order(context: &Context, input: AddOrderInput) -> FieldResult<OrderResult> {
        let exchg: Exchange = input.exchg.clone().into();
        let api = context.exchanges.get(&exchg).ok_or_else(|| {
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
}

pub(crate) struct Subscription;

type StringStream = Pin<Box<dyn Stream<Item = Result<String, FieldError>> + Send>>;

#[juniper::graphql_subscription(Context = Context)]
impl Subscription {
    async fn hello_world() -> StringStream {
        let stream = tokio_stream::iter(vec![Ok(String::from("Hello")), Ok(String::from("World!"))]);
        Box::pin(stream)
    }
}

pub(crate) type Schema = RootNode<'static, QueryRoot, MutationRoot, Subscription>;

pub(crate) fn create_schema() -> Schema { Schema::new(QueryRoot, MutationRoot, Subscription) }
