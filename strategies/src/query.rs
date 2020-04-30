use crate::naive_pair_trading::state::Operation;
use actix_derive::{Message, MessageResponse};

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
