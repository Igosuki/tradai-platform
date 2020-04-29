use super::naive_pair_trading::state::Position;
use super::naive_pair_trading::state::PositionKind;
use crate::naive_pair_trading::state::Operation;
use actix_derive::{Message, MessageResponse};

#[derive(Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    Operations(Vec<Operation>),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Option<DataResult>")]
pub enum DataQuery {
    Operations,
}
