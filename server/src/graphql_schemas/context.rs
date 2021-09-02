use core::fmt::Debug;
use core::marker::Send;
use core::option::Option::{None, Some};
use core::result::Result::{Err, Ok};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use actix::{Addr, MailboxError};
use juniper::executor::{FieldError, FieldResult};

use coinnect_rt::exchange::{Exchange, ExchangeApi};
use strategies::order_manager::OrderManager;
use strategies::query::{DataQuery, DataResult};
use strategies::{Strategy, StrategyActor, StrategyKey};

use crate::graphql_schemas::unhandled_data_result;

use super::types::TypeAndKeyInput;

pub(crate) struct Context {
    pub strats: Arc<HashMap<StrategyKey, Strategy>>,
    pub exchanges: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    pub order_managers: Arc<HashMap<Exchange, Addr<OrderManager>>>,
}

impl juniper::Context for Context {}

impl Context {
    pub async fn with_strat<T, F>(&self, tk: TypeAndKeyInput, q: DataQuery, f: F) -> FieldResult<T>
    where
        F: Fn(DataResult) -> FieldResult<T>,
    {
        let strat = StrategyKey::from(&tk.t, &tk.id).ok_or_else(|| {
            FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            )
        })?;
        match self.strats.get(&strat) {
            None => Err(FieldError::new(
                "Strategy not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(strat) => {
                let res = strat.1.send(q).await;
                match res {
                    Ok(Ok(Some(dr))) => f(dr),
                    Err(_) => Err(FieldError::new(
                        "Strategy mailbox was full",
                        graphql_value!({ "unavailable": "strategy mailbox full" }),
                    )),
                    r => {
                        error!("{:?}", r);
                        Err(FieldError::new(
                            "Unexpected error",
                            graphql_value!({ "unavailable": "unexpected error" }),
                        ))
                    }
                }
            }
        }
    }

    pub async fn with_order_manager<T, F, M: 'static>(&self, exchange: &str, q: M, f: F) -> FieldResult<T>
    where
        F: Fn(M::Result) -> FieldResult<T>,
        M: actix::Message + Send,
        M::Result: Send + Debug,
        OrderManager: actix::Handler<M>,
    {
        let xchg = Exchange::from_str(exchange).ok().ok_or_else(|| {
            FieldError::new(
                "Exchange not found",
                graphql_value!({ "not_found": "exchange not found" }),
            )
        })?;
        match self.order_managers.get(&xchg) {
            None => Err(FieldError::new(
                "Exchange not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(om) => {
                let res = om.send(q).await;
                match res {
                    Ok(dr) => f(dr),
                    Err(_) => Err(FieldError::new(
                        "OrderManager mailbox was full",
                        graphql_value!({ "unavailable": "order manager mailbox full" }),
                    )),
                }
            }
        }
    }

    pub async fn with_strat_mut<M: 'static>(&self, tk: TypeAndKeyInput, m: M) -> FieldResult<M::Result>
    where
        M: actix::Message + Send,
        M::Result: Send + Debug,
        StrategyActor: actix::Handler<M>,
    {
        let strat = StrategyKey::from(&tk.t, &tk.id).ok_or_else(|| {
            FieldError::new(
                "Strategy type not found",
                graphql_value!({ "not_found": "strategy type not found" }),
            )
        })?;
        match self.strats.get(&strat) {
            None => Err(FieldError::new(
                "Strategy not found",
                graphql_value!({ "not_found": "strategy not found" }),
            )),
            Some(strat) => match strat.1.send(m).await {
                Ok(response) => Ok(response),
                Err(MailboxError::Closed | MailboxError::Timeout) => Err(FieldError::new(
                    "Strategy mailbox was full",
                    graphql_value!({ "unavailable": "strategy mailbox full" }),
                )),
            },
        }
    }

    pub async fn data_query_as_string(&self, tk: TypeAndKeyInput, query: DataQuery) -> FieldResult<String> {
        self.with_strat(tk, query, |dr| match dr {
            DataResult::Status(status) => Ok(status.as_ref().to_string()),
            DataResult::State(state) => Ok(state),
            _ => unhandled_data_result(),
        })
        .await
    }
}
