use smallvec::SmallVec;
use std::collections::HashSet;
use std::sync::Arc;

use brokers::types::MarketEventEnvelope;
use db::Storage;
use portfolio::portfolio::Portfolio;
use trading::engine::TradingEngine;
use trading::signal::TradeSignal;

use crate::error::*;
use crate::models::io::SerializedModel;
use crate::query::{DataQuery, DataResult, Mutation};
use crate::{error, MarketChannel};

#[async_trait]
pub trait StrategyDriver: Send + Sync {
    /// Initialize the driver
    async fn init(&mut self) -> Result<()>;

    /// A unique key or id for the strategy
    async fn key(&self) -> String;

    /// Receive a data event from live streams
    async fn on_market_event(&mut self, le: &MarketEventEnvelope) -> error::Result<()>;

    /// Handle a `DataQuery`
    /// this is used to inspect the internal state of strategies
    async fn query(&mut self, q: DataQuery) -> crate::error::Result<DataResult>;

    /// Handle a `Mutation`
    /// this is used to correct strategies manually
    fn mutate(&mut self, m: Mutation) -> error::Result<()>;

    /// The channels this strategy plugs into
    fn channels(&self) -> HashSet<MarketChannel>;

    /// Stop emitting trading signals
    fn stop_trading(&mut self) -> Result<()>;

    /// Resume trading signals
    fn resume_trading(&mut self) -> Result<()>;

    /// When called upon, resolve previously emitted trading signals
    async fn resolve_orders(&mut self);

    /// Check if there are any pending locks
    async fn is_locked(&self) -> bool;
}

pub type TradeSignals = SmallVec<[TradeSignal; 10]>;

#[async_trait]
pub trait Strategy: Sync + Send {
    //async fn try_new(&self, conf: serde_json::Value) -> Self;

    /// A unique key for this strategy
    fn key(&self) -> String;

    /// A chance to initialize the strategy
    fn init(&mut self) -> Result<()>;

    /// Evaluate this market event
    async fn eval(&mut self, e: &MarketEventEnvelope, ctx: &DefaultStrategyContext) -> Result<Option<TradeSignals>>;

    /// Warmup
    fn warmup(&mut self, e: Vec<MarketEventEnvelope>) { todo!() }

    /// Exports a serialized view of the model
    fn model(&self) -> SerializedModel;

    /// Exports a serialized view of model constants for performance purposes
    fn constants(&self) -> SerializedModel { vec![] }

    /// Channels the strategy subscribes to
    fn channels(&self) -> HashSet<MarketChannel>;
}

pub struct DefaultStrategyContext<'a> {
    pub portfolio: &'a Portfolio,
}

pub struct StrategyInitContext {
    pub engine: Arc<TradingEngine>,
    pub db: Arc<dyn Storage>,
}

pub type StratProvider<'a> = dyn Fn(StrategyInitContext) -> Box<dyn Strategy> + 'a;
pub type StratProviderRef = Arc<dyn Fn(StrategyInitContext) -> Box<dyn Strategy> + Send + Sync>;
