use std::collections::HashSet;

use coinnect_rt::types::MarketEventEnvelope;
use portfolio::portfolio::Portfolio;
use trading::signal::TradeSignal;

use crate::error::*;
use crate::models::io::SerializedModel;
use crate::query::{DataQuery, DataResult, Mutation};
use crate::{error, Channel};

#[async_trait]
pub trait StrategyDriver: Send + Sync {
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
    fn channels(&self) -> HashSet<Channel>;

    /// Stop emitting trading signals
    fn stop_trading(&mut self);

    /// Resume trading signals
    fn resume_trading(&mut self);

    /// When called upon, resolve previously emitted trading signals
    async fn resolve_orders(&mut self);

    /// Check if there are any pending locks
    async fn is_locked(&self) -> bool;
}

#[async_trait]
pub trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    /// A unique key for this strategy
    fn key(&self) -> String;

    /// A chance to initialize the strategy
    fn init(&mut self) -> Result<()>;

    /// Evaluate this market event
    async fn eval(&mut self, e: &MarketEventEnvelope, ctx: &DefaultStrategyContext)
        -> Result<Option<Vec<TradeSignal>>>;

    /// Exports a serialized view of the model
    fn model(&self) -> SerializedModel;

    /// Channels the strategy subscribes to
    fn channels(&self) -> HashSet<Channel>;
}

pub struct DefaultStrategyContext<'a> {
    pub portfolio: &'a Portfolio,
}
