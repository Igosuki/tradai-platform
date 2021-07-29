use crate::error::*;
use crate::mean_reverting::state::Operation as MeanRevertingOperation;
use crate::naive_pair_trading::state::Operation as NaiveOperation;
use crate::StrategyStatus;
use actix_derive::{Message, MessageResponse};
use serde::{Deserialize, Serialize};

// TODO: Use GraphQLUnion to refactor this ugly bit of code
#[derive(Debug, Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    NaiveOperations(Vec<NaiveOperation>),
    MeanRevertingOperations(Vec<MeanRevertingOperation>),
    Dump(String),
    NaiveOperation(Box<Option<NaiveOperation>>),
    MeanRevertingOperation(Box<Option<MeanRevertingOperation>>),
    OngongOperationCancelation(bool),
    State(String),
    Status(StrategyStatus),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Result<Option<DataResult>>")]
pub enum DataQuery {
    /// All operations history
    Operations,
    /// Dumps operation and state tables into a json string
    Dump,
    /// Currently ongoing operation
    CurrentOperation,
    /// Cancel the ongoing operation
    CancelOngoingOp,
    /// Latest state
    State,
    /// Status
    Status,
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
#[rtype(result = "Result<()>")]
pub struct FieldMutation {
    pub field: MutableField,
    pub value: f64,
}
