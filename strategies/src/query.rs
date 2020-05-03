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

#[derive(Deserialize, Serialize, Message, juniper::GraphQLInputObject)]
#[rtype(result = "std::io::Result<()>")]
pub struct FieldMutation {
    pub field: String,
    pub value: f64,
}
