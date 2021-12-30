#![allow(unused_braces)]

use chrono::{DateTime, Utc};
use juniper::FieldResult;

use coinnect_rt::prelude::*;
use strategy::query::{DataQuery, DataResult, PortfolioSnapshot};
use trading::position::{OperationKind, PositionKind};
use trading::types::TradeOperation;

use crate::graphql_schemas::context::Context;
use crate::graphql_schemas::unhandled_data_result;

pub(crate) struct StrategyState {
    pub t: String,
    pub id: String,
}

impl StrategyState {
    fn as_input(&self) -> TypeAndKeyInput {
        TypeAndKeyInput {
            t: self.t.clone(),
            id: self.id.clone(),
        }
    }
}

#[juniper::graphql_object(Context = Context)]
impl StrategyState {
    #[graphql(name = "type")]
    pub fn t(&self) -> &str { &self.t }
    pub fn id(&self) -> &str { &self.id }
    pub async fn indicators(&self, context: &Context) -> FieldResult<PortfolioSnapshot> {
        context
            .with_strat(self.as_input(), DataQuery::Indicators, |dr| match dr {
                DataResult::Indicators(indicators) => Ok(indicators),
                _ => unhandled_data_result(),
            })
            .await
    }
    pub async fn status(&self, context: &Context) -> FieldResult<String> {
        context
            .with_strat(self.as_input(), DataQuery::Status, |dr| match dr {
                DataResult::Status(status) => Ok(status.as_ref().to_string()),
                _ => unhandled_data_result(),
            })
            .await
    }
}

#[derive(juniper::GraphQLInputObject)]
pub struct TypeAndKeyInput {
    #[graphql(name = "type")]
    pub t: String,
    pub id: String,
}

#[derive(juniper::GraphQLEnum)]
pub enum TradeTypeInput {
    Sell,
    Buy,
}

impl From<TradeTypeInput> for TradeType {
    fn from(ti: TradeTypeInput) -> TradeType {
        match ti {
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

impl From<OrderTypeInput> for OrderType {
    fn from(oti: OrderTypeInput) -> OrderType {
        match oti {
            OrderTypeInput::Limit => OrderType::Limit,
            OrderTypeInput::Market => OrderType::Market,
        }
    }
}

#[derive(juniper::GraphQLInputObject)]
pub struct AddOrderInput {
    pub exchg: String,
    pub order_type: OrderTypeInput,
    pub side: TradeTypeInput,
    pub pair: String,
    pub quantity: f64,
    pub price: f64,
    #[graphql(description = "Set this to true to pass a real order")]
    pub dry_run: bool,
}

impl From<AddOrderInput> for OrderQuery {
    fn from(aoi: AddOrderInput) -> OrderQuery {
        OrderQuery::AddOrder(AddOrderRequest {
            order_type: aoi.order_type.into(),
            side: aoi.side.into(),
            quantity: Some(aoi.quantity),
            pair: aoi.pair.into(),
            price: Some(aoi.price),
            enforcement: Some(OrderEnforcement::FOK),
            dry_run: aoi.dry_run,
            order_id: AddOrderRequest::new_id(),
            ..AddOrderRequest::default()
        })
    }
}

#[derive(juniper::GraphQLObject)]
pub struct OrderResult {
    pub identifier: String,
}

#[derive(juniper::GraphQLObject)]
pub struct OperationHistory {
    id: String,
    kind: OperationKind,
    transactions: Vec<TransactionHistory>,
    pub ts: DateTime<Utc>,
}

#[derive(juniper::GraphQLObject)]
pub struct TransactionHistory {
    value: f64,
    pair: String,
    time: DateTime<Utc>,
    pos: PositionKind,
    price: f64,
    last_transaction: Option<String>,
    trade: TradeOperation,
    last_order: Option<String>,
    qty: f64,
}

#[derive(juniper::GraphQLObject)]
pub struct Model {
    pub id: String,
    pub json: String,
}
