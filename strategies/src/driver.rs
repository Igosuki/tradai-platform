use coinnect_rt::types::LiveEventEnvelope;

use crate::query::{DataQuery, DataResult, Mutation};
use crate::{error, Channel};

#[async_trait]
pub trait StrategyDriver {
    /// Receive a data event from live streams
    async fn add_event(&mut self, le: &LiveEventEnvelope) -> error::Result<()>;

    /// Handle a `DataQuery`
    /// this is used to inspect the internal state of strategies
    fn data(&mut self, q: DataQuery) -> crate::error::Result<DataResult>;

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
