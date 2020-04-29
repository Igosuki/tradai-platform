use super::naive_pair_trading::state::Position;
use super::naive_pair_trading::state::PositionKind;
use actix_derive::{Message, MessageResponse};

#[derive(Deserialize, Serialize, MessageResponse)]
#[serde(tag = "type")]
pub enum DataResult {
    Positions(Vec<Position>),
}

#[derive(Deserialize, Serialize, Message)]
#[rtype(result = "Option<DataResult>")]
pub enum DataQuery {
    Positions,
}
