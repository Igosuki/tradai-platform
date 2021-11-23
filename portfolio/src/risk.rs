use coinnect_rt::types::OrderQuery;

use crate::error::*;
use crate::portfolio::Portfolio;

/// Trait to assess risk level associated to an order
#[async_trait]
pub trait RiskEvaluator {
    /// Evaluate the risk level of an order within the given portfolio
    /// Returns a value between 0 (low) and 1 (high)
    async fn evaluate(&self, portfolio: Portfolio, order: OrderQuery) -> Result<f64>;
}

#[derive(Debug)]
pub struct DefaultMarketRiskEvaluator {}

#[async_trait]
impl RiskEvaluator for DefaultMarketRiskEvaluator {
    async fn evaluate(&self, portfolio: Portfolio, order: OrderQuery) -> Result<f64> { todo!() }
}
