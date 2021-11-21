use crate::position::{OperationKind, PositionKind};
use crate::types::TradeKind;
use coinnect_rt::prelude::*;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[allow(dead_code)]
#[cfg_attr(feature = "python", pyclass)]
pub struct TradeSignal {
    pub position_kind: PositionKind,
    pub operation_kind: OperationKind,
    pub trade_kind: TradeKind,
    pub price: f64,
    pub pair: Pair,
    pub exchange: Exchange,
    pub instructions: Option<ExecutionInstruction>,
    pub dry_mode: bool,
    pub asset_type: AssetType,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, EnumString)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionInstruction {
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
    LastPrice,
}
