use juniper::{FieldError, FieldResult, RootNode};

use std::collections::HashMap;
use std::sync::Arc;
use strategies::naive_pair_trading::state::Operation;
use strategies::query::{DataQuery, DataResult};
use strategies::{Strategy, StrategyKey};

pub struct Context {
    pub strats: Arc<HashMap<StrategyKey, Strategy>>,
}

impl juniper::Context for Context {}

pub struct QueryRoot;

#[derive(juniper::GraphQLObject)]
pub struct TypeAndKey {
    #[graphql(name = "type")]
    t: String,
    id: String,
}

#[derive(juniper::GraphQLInputObject)]
pub struct TypeAndKeyInput {
    #[graphql(name = "type")]
    t: String,
    id: String,
}

#[juniper::object(Context = Context)]
impl QueryRoot {
    #[graphql(description = "List of all strats")]
    fn strats(context: &Context) -> FieldResult<Vec<TypeAndKey>> {
        let keys: Vec<&StrategyKey> = context.strats.keys().collect();
        Ok(keys
            .iter()
            .map(|&sk| TypeAndKey {
                t: sk.0.to_string(),
                id: sk.1.clone(),
            })
            .collect())
    }

    #[graphql(description = "Get all positions for this strat")]
    fn dump_strat_db(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<String>> {
        StrategyKey::from(&tk.t, &tk.id)
            .ok_or(FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            ))
            .and_then(|strat| match context.strats.get(&strat) {
                None => Err(FieldError::new(
                    "Strategy not found",
                    graphql_value!({ "not_found": "strategy not found" }),
                )),
                Some(strat) => {
                    let f = strat.1.send(DataQuery::Dump);
                    match futures::executor::block_on(f) {
                        Ok(Some(DataResult::Dump(pos_vec))) => Ok(pos_vec),
                        Err(_) => Err(FieldError::new(
                            "Strategy mailbox was full",
                            graphql_value!({ "unavailable": "strategy mailbox full" }),
                        )),
                        e => {
                            error!("{:?}", e);
                            Err(FieldError::new(
                                "Unexpected error",
                                graphql_value!({ "unavailable": "unexpected error" }),
                            ))
                        }
                    }
                }
            })
    }

    #[graphql(description = "Get all positions for this strat")]
    fn operations(context: &Context, tk: TypeAndKeyInput) -> FieldResult<Vec<Operation>> {
        StrategyKey::from(&tk.t, &tk.id)
            .ok_or(FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            ))
            .and_then(|strat| match context.strats.get(&strat) {
                None => Err(FieldError::new(
                    "Strategy not found",
                    graphql_value!({ "not_found": "strategy not found" }),
                )),
                Some(strat) => {
                    let f = strat.1.send(DataQuery::Operations);
                    match futures::executor::block_on(f) {
                        Ok(Some(DataResult::Operations(pos_vec))) => Ok(pos_vec),
                        Err(_) => Err(FieldError::new(
                            "Strategy mailbox was full",
                            graphql_value!({ "unavailable": "strategy mailbox full" }),
                        )),
                        _ => Err(FieldError::new(
                            "Unexpected error",
                            graphql_value!({ "unavailable": "unexpected error" }),
                        )),
                    }
                }
            })
    }
}

pub struct MutationRoot;

#[juniper::object(Context = Context)]
impl MutationRoot {}

pub type Schema = RootNode<'static, QueryRoot, MutationRoot>;

pub fn create_schema() -> Schema {
    Schema::new(QueryRoot, MutationRoot)
}
