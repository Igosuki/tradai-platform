use coinnect_rt::types::LiveEventEnvelope;

use crate::query::{DataQuery, DataResult, Mutation};
use crate::{error, Channel};

#[async_trait]
pub trait StrategyDriver {
    async fn add_event(&mut self, le: &LiveEventEnvelope) -> error::Result<()>;

    fn data(&mut self, q: DataQuery) -> Option<DataResult>;

    fn mutate(&mut self, m: Mutation) -> error::Result<()>;

    fn channels(&self) -> Vec<Channel>;

    fn stop_trading(&mut self);

    fn resume_trading(&mut self);
}
