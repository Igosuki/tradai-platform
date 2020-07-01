use coinnect_rt::types::TradeType;
use strum_macros::{AsRefStr, EnumString};

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    BUY,
    #[strum(serialize = "sell")]
    SELL,
}

#[derive(
    Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum PositionKind {
    #[strum(serialize = "short")]
    SHORT,
    #[strum(serialize = "long")]
    LONG,
}

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum OperationKind {
    #[strum(serialize = "open")]
    OPEN,
    #[strum(serialize = "close")]
    CLOSE,
}

impl Into<TradeType> for TradeKind {
    fn into(self) -> TradeType {
        match self {
            TradeKind::BUY => TradeType::Buy,
            TradeKind::SELL => TradeType::Sell,
        }
    }
}
