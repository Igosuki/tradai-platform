use crate::naive_pair_trading::state::Operation;
use actix_derive::{Message, MessageResponse};
use juniper::ScalarValue;

#[derive(Debug, Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    Operations(Vec<Operation>),
    Dump(Vec<String>),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Option<DataResult>")]
pub enum DataQuery {
    Operations,
    Dump,
}

#[derive(Deserialize, Serialize, juniper::GraphQLEnum)]
pub enum MutableField {
    #[graphql(name = "value_strat")]
    ValueStrat,
    #[graphql(name = "pnl")]
    Pnl,
    #[graphql(name = "nominal_position")]
    NominalPosition,
}

#[derive(Deserialize, Serialize, Message, juniper::GraphQLInputObject)]
#[rtype(result = "std::io::Result<()>")]
pub struct FieldMutation {
    pub field: MutableField,
    pub value: f64,
}
