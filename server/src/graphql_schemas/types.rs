use coinnect_rt::types::{OrderType, TradeType};

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
    pub exchg: String,
    pub order_type: OrderTypeInput,
    pub side: TradeTypeInput,
    pub pair: String,
    pub quantity: f64,
    pub price: f64,
    #[graphql(description = "Set this to true to pass a real order")]
    pub dry_run: bool,
}

#[derive(juniper::GraphQLObject)]
pub struct OrderResult {
    pub identifier: String,
}
