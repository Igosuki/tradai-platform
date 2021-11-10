use actix::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::*;
use crate::mean_reverting::state::Operation as MeanRevertingOperation;
use crate::naive_pair_trading::state::Operation as NaiveOperation;
use crate::types::TradeOperation;
use crate::StrategyStatus;

// TODO: Use GraphQLUnion to refactor this ugly bit of code
#[derive(Debug, Deserialize, Serialize, PartialEq, actix_derive::MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    NaiveOperations(Vec<NaiveOperation>),
    MeanRevertingOperations(Vec<MeanRevertingOperation>),
    NaiveOperation(Box<Option<NaiveOperation>>),
    MeanRevertingOperation(Box<Option<MeanRevertingOperation>>),
    Success(bool),
    State(String),
    Models(Vec<(String, Option<Value>)>),
    Status(StrategyStatus),
    Operations(Vec<TradeOperation>),
    Indicators(StrategyIndicators),
}

#[derive(Deserialize, Serialize, actix::Message)]
#[rtype(result = "Result<Option<DataResult>>")]
pub enum DataQuery {
    /// All operations history
    OperationHistory,
    /// Currently ongoing operation
    OpenOperations,
    /// Cancel the ongoing operation
    CancelOngoingOp,
    /// Latest state
    State,
    /// Latest models
    Models,
    /// Status
    Status,
    /// Indicators
    Indicators,
}

#[derive(Deserialize, Serialize, juniper::GraphQLEnum)]
pub enum MutableField {
    #[graphql(name = "value_strat")]
    ValueStrat,
    #[graphql(name = "pnl")]
    Pnl,
}

#[derive(Deserialize, Serialize, Message, juniper::GraphQLInputObject)]
#[rtype(result = "Result<()>")]
pub struct StateFieldMutation {
    pub field: MutableField,
    pub value: f64,
}

pub enum Mutation {
    State(StateFieldMutation),
    Model(ModelReset),
}

#[derive(Default, Message, juniper::GraphQLInputObject)]
#[rtype(result = "Result<StrategyStatus>")]
pub struct ModelReset {
    #[graphql(description = "The model name, if unspecified all models are reset")]
    #[graphql(default)]
    pub name: Option<String>,
    #[graphql(description = "whether to stop trading during this operation")]
    #[graphql(default = true)]
    pub stop_trading: bool,
    #[graphql(description = "whether to restart afterwards")]
    #[graphql(default = true)]
    pub restart_after: bool,
}

/// Global performance indicators of a strategy
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, juniper::GraphQLObject)]
pub struct StrategyIndicators {
    pub pnl: f64,
    pub current_return: f64,
}
