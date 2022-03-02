use chrono::{DateTime, Utc};

use trading::position::{OperationKind, Position, PositionKind};
use trading::stop::StopEvent;
use trading::types::TradeKind;

// ------------ Behavioral Types ---------

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperationEvent {
    pub op: OperationKind,
    pub pos: PositionKind,
    pub at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradeEvent {
    pub side: TradeKind,
    pub qty: f64,
    pub pair: String,
    pub price: f64,
    pub strat_value: f64,
    pub at: DateTime<Utc>,
    pub borrowed: Option<f64>,
    pub interest: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PositionSummary {
    #[serde(flatten)]
    pub op: OperationEvent,
    #[serde(flatten)]
    pub trade: TradeEvent,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "event")]
pub enum StratEvent {
    Stop(StopEvent),
    OpenPosition(Position),
    ClosePosition(Position),
    PositionSummary(PositionSummary),
}

impl StratEvent {
    pub fn log(&self) {
        debug!(strat_event = ?self);
    }
}

impl From<StopEvent> for StratEvent {
    fn from(stop: StopEvent) -> Self { Self::Stop(stop) }
}

impl TryFrom<Position> for StratEvent {
    type Error = crate::error::Error;

    fn try_from(pos: Position) -> Result<Self, Self::Error> {
        if pos.is_closed() {
            Ok(Self::ClosePosition(pos))
        } else if pos.is_opened() {
            Ok(Self::OpenPosition(pos))
        } else {
            Err(Self::Error::InvalidPosition)
        }
    }
}

impl From<PositionSummary> for StratEvent {
    fn from(e: PositionSummary) -> Self { Self::PositionSummary(e) }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum TradingStatus {
    StartOfDay,
    PreOpen,
    PreOpenNoCancel,
    PreOpenFreeze,
    Open,
    FastMarket,
    Halt,
    CloseNotFinal,
    PreClose,
    PreCloseNoCancel,
    PreCloseFreeze,
    Close,
    PostClose,
    EndOfDay,
}
