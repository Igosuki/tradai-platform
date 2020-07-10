use crate::mean_reverting::state::Operation as MeanRevertingOperation;
use crate::naive_pair_trading::state::Operation as NaiveOperation;
use actix_derive::{Message, MessageResponse};
use serde::{Deserialize, Serialize};

// TODO: Refactor this ugly bit of code
#[derive(Debug, Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    NaiveOperations(Vec<NaiveOperation>),
    MeanRevertingOperations(Vec<MeanRevertingOperation>),
    Dump(Vec<String>),
    NaiveOperation(Option<NaiveOperation>),
    MeanRevertingOperation(Option<MeanRevertingOperation>),
    OngongOperationCancelation(bool),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Result<Option<DataResult>, anyhow::Error>")]
pub enum DataQuery {
    Operations,
    Dump,
    CurrentOperation,
    CancelOngoingOp,
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
