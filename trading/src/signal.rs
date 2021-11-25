#[cfg(feature = "python")]
use pyo3::prelude::*;
use uuid::Uuid;

use coinnect_rt::prelude::*;
use coinnect_rt::types::MarginSideEffect;

use crate::position::{OperationKind, PositionKind};
use crate::types::TradeKind;

#[allow(dead_code)]
#[cfg_attr(feature = "python", pyclass)]
pub struct TradeSignal {
    pub trace_id: Uuid,
    pub position_kind: PositionKind,
    pub operation_kind: OperationKind,
    pub trade_kind: TradeKind,
    pub price: f64,
    pub qty: f64,
    pub pair: Pair,
    pub exchange: Exchange,
    pub instructions: Option<ExecutionInstruction>,
    pub dry_mode: bool,
    pub order_type: OrderType,
    pub enforcement: Option<OrderEnforcement>,
    pub asset_type: Option<AssetType>,
    pub side_effect: Option<MarginSideEffect>,
}

impl TradeSignal {
    pub fn xch_and_pair(&self) -> (Exchange, Pair) { (self.exchange, self.pair.clone()) }
}

impl From<TradeSignal> for AddOrderRequest {
    fn from(t: TradeSignal) -> Self {
        let side = match (t.position_kind, t.operation_kind) {
            (PositionKind::Short, OperationKind::Open) | (PositionKind::Long, OperationKind::Close) => TradeType::Sell,
            (PositionKind::Short, OperationKind::Close) | (PositionKind::Long, OperationKind::Open) => TradeType::Buy,
        };
        Self {
            pair: t.pair.clone(),
            side,
            order_type: t.order_type,
            enforcement: t.enforcement,
            quantity: Some(t.qty),
            price: Some(t.price),
            order_id: Uuid::new_v4().to_string(),
            dry_run: t.dry_mode,
            asset_type: t.asset_type,
            side_effect_type: t.side_effect,
            ..AddOrderRequest::default()
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, EnumString)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionInstruction {
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
    LastPrice,
}
