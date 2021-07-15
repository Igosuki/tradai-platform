use coinnect_rt::types::{AddOrderRequest, OrderEnforcement, OrderQuery, OrderType, TradeType};
use uuid::Uuid;

#[derive(juniper::GraphQLObject)]
pub struct TypeAndKey {
    #[graphql(name = "type")]
    pub t: String,
    pub id: String,
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
