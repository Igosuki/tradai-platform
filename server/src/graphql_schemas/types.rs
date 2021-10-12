#![allow(unused_braces)]

use chrono::{DateTime, Utc};
use juniper::FieldResult;
use uuid::Uuid;

use coinnect_rt::types::{AddOrderRequest, OrderEnforcement, OrderQuery, OrderType, TradeType};
use strategies::query::DataQuery;
use strategies::types::{OperationKind, PositionKind, TradeOperation};

use crate::graphql_schemas::context::Context;

pub(crate) struct StrategyState {
    pub t: String,
    pub id: String,
}

impl StrategyState {
    fn as_input(&self) -> TypeAndKeyInput {
        TypeAndKeyInput {
            t: self.t.to_owned(),
            id: self.id.to_owned(),
        }
    }
}

#[juniper::graphql_object(Context = Context)]
impl StrategyState {
    #[graphql(name = "type")]
    pub fn t(&self) -> &str { &self.t }
    pub fn id(&self) -> &str { &self.id }
    pub async fn state(&self, context: &Context) -> FieldResult<String> {
        context.data_query_as_string(self.as_input(), DataQuery::State).await
    }
    pub async fn status(&self, context: &Context) -> FieldResult<String> {
        context.data_query_as_string(self.as_input(), DataQuery::Status).await
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
            order_id: Some(Uuid::new_v4().to_string()),
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

impl From<strategies::naive_pair_trading::state::Operation> for OperationHistory {
    fn from(o: strategies::naive_pair_trading::state::Operation) -> Self {
        Self {
            id: o.id,
            kind: o.kind,
            ts: o.pos.time,
            transactions: vec![
                TransactionHistory {
                    value: o.left_trade.qty * o.pos.left_price,
                    pair: o.pos.left_pair,
                    time: o.pos.time,
                    pos: o.pos.kind.clone(),
                    price: o.pos.left_price,
                    last_transaction: o.left_transaction.and_then(|tr| serde_json::to_string(&tr).ok()),
                    qty: o.left_trade.qty,
                    trade: o.left_trade,
                    last_order: o
                        .left_order
                        .as_ref()
                        .and_then(|order| serde_json::to_string(order).ok()),
                },
                TransactionHistory {
                    value: o.right_trade.qty * o.pos.left_price,
                    pair: o.pos.right_pair,
                    time: o.pos.time,
                    pos: o.pos.kind,
                    price: o.pos.left_price,
                    last_transaction: o.right_transaction.and_then(|tr| serde_json::to_string(&tr).ok()),
                    last_order: o
                        .right_order
                        .as_ref()
                        .and_then(|order| serde_json::to_string(order).ok()),
                    qty: o.right_trade.qty,
                    trade: o.right_trade,
                },
            ],
        }
    }
}

impl From<strategies::mean_reverting::state::Operation> for OperationHistory {
    fn from(o: strategies::mean_reverting::state::Operation) -> Self {
        let value = o.value();
        Self {
            id: o.id,
            kind: o.kind,
            ts: o.pos.time,
            transactions: vec![TransactionHistory {
                value,
                pair: o.pos.pair,
                time: o.pos.time,
                pos: o.pos.kind.clone(),
                price: o.pos.price,
                last_transaction: o.transaction.and_then(|tr| serde_json::to_string(&tr).ok()),
                qty: o.trade.qty,
                trade: o.trade,
                last_order: o
                    .order_detail
                    .as_ref()
                    .and_then(|order| serde_json::to_string(order).ok()),
            }],
        }
    }
}

#[derive(juniper::GraphQLObject)]
pub struct Model {
    pub id: String,
    pub json: String,
}
