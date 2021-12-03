use std::collections::HashSet;

use serde_json::Value;

use coinnect_rt::types::MarketEventEnvelope;
use portfolio::portfolio::Portfolio;
use trading::signal::TradeSignal;

use crate::error::*;
use crate::query::{DataQuery, DataResult, Mutation};
use crate::{error, Channel};

#[async_trait]
pub trait StrategyDriver {
    /// A unique key or id for the strategy
    async fn key(&self) -> String;

    /// Receive a data event from live streams
    async fn add_event(&mut self, le: &MarketEventEnvelope) -> error::Result<()>;

    /// Handle a `DataQuery`
    /// this is used to inspect the internal state of strategies
    async fn data(&mut self, q: DataQuery) -> crate::error::Result<DataResult>;

    /// Handle a `Mutation`
    /// this is used to correct strategies manually
    fn mutate(&mut self, m: Mutation) -> error::Result<()>;

    /// The channels this strategy plugs into
    fn channels(&self) -> Vec<Channel>;

    /// Stop emitting trading signals
    fn stop_trading(&mut self);

    /// Resume trading signals
    fn resume_trading(&mut self);

    /// When called upon, resolve previously emitted trading signals
    async fn resolve_orders(&mut self);
}

pub type SerializedModel = Vec<(String, Option<Value>)>;

#[async_trait]
pub(crate) trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    fn key(&self) -> String;

    fn init(&mut self) -> Result<()>;

    async fn eval(&mut self, e: &MarketEventEnvelope, ctx: &DefaultStrategyContext)
        -> Result<Option<Vec<TradeSignal>>>;

    async fn update_model(&mut self, e: &MarketEventEnvelope) -> Result<()>;

    fn model(&self) -> SerializedModel;

    fn channels(&self) -> HashSet<Channel>;
}

pub(crate) struct DefaultStrategyContext<'a> {
    pub portfolio: &'a Portfolio,
}
