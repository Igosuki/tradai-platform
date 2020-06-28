use crate::naive_pair_trading::state::Operation;
use actix_derive::{Message, MessageResponse};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    Operations(Vec<Operation>),
    Dump(Vec<String>),
    Operation(Option<Operation>),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Result<Option<DataResult>, anyhow::Error>")]
pub enum DataQuery {
    Operations,
    Dump,
    CurrentOperation,
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
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct FieldMutation {
    pub field: MutableField,
    pub value: f64,
}
