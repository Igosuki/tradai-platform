use coinnect_rt::types::AddOrderRequest;
use std::fmt::Debug;

use crate::portfolio::Portfolio;

/// Trait to assess risk level associated to an order
#[async_trait]
pub trait RiskEvaluator: Debug + Send + Sync {
    /// Evaluate the risk level of an order within the given portfolio
    /// Returns a value between 0 (low) and 1 (high)
    fn evaluate(&self, portfolio: &Portfolio, order: &AddOrderRequest) -> f64;
}

#[derive(Debug, Default)]
pub struct DefaultMarketRiskEvaluator {}

#[async_trait]
impl RiskEvaluator for DefaultMarketRiskEvaluator {
    fn evaluate(&self, _portfolio: &Portfolio, _order: &AddOrderRequest) -> f64 { 0.0 }
}
