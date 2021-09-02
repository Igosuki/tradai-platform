use juniper::{FieldError, FieldResult};

pub(crate) use context::Context;

mod context;
pub mod root;
mod types;

fn unhandled_data_result<T>() -> FieldResult<T> {
    Err(FieldError::new(
        "Unhandled result",
        graphql_value!({ "unavailable": "wrong result for query" }),
    ))
}
