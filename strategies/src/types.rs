use chrono::{DateTime, Utc};

use trading::position::{OperationKind, PositionKind};
use trading::stop::StopEvent;
use trading::types::TradeKind;

// ------------ Behavioral Types ---------

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperationEvent {
    pub(crate) op: OperationKind,
    pub(crate) pos: PositionKind,
    pub(crate) at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradeEvent {
    pub(crate) op: TradeKind,
    pub(crate) qty: f64,
    pub(crate) pair: String,
    pub(crate) price: f64,
    pub(crate) strat_value: f64,
    pub(crate) at: DateTime<Utc>,
    pub(crate) borrowed: Option<f64>,
    pub(crate) interest: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "event")]
pub enum StratEvent {
    Stop { stop: StopEvent },
    Operation(OperationEvent),
    Trade(TradeEvent),
}

impl StratEvent {
    pub fn log(&self) {
        debug!(strat_event = ?self);
    }
}

impl From<StopEvent> for StratEvent {
    fn from(stop: StopEvent) -> Self { Self::Stop { stop } }
}
