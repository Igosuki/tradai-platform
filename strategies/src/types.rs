use std::convert::TryFrom;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::Level::Info;
use strum_macros::{AsRefStr, EnumString};
use thiserror::Error;

use coinnect_rt::types::{Orderbook, TradeType};

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    BUY,
    #[strum(serialize = "sell")]
    SELL,
}

impl Into<TradeType> for TradeKind {
    fn into(self) -> TradeType {
        match self {
            TradeKind::BUY => TradeType::Buy,
            TradeKind::SELL => TradeType::Sell,
        }
    }
}

impl Into<i32> for TradeKind {
    fn into(self) -> i32 {
        match self {
            TradeKind::BUY => 0,
            TradeKind::SELL => 1
        }
    }
}

#[derive(
    Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum,
)]
#[serde(rename_all = "lowercase")]
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

#[derive(
    Clone, PartialEq, Eq, Debug, Deserialize, Serialize
)]
pub enum StopEvent {
    GAIN,
    LOSS,
    NA
}

#[derive(
    Clone, Debug, Deserialize, Serialize
)]
pub struct OperationEvent {
    pub(crate) op: OperationKind,
    pub(crate) pos: PositionKind,
    pub(crate) at: DateTime<Utc>
}

#[derive(
Clone, Debug, Deserialize, Serialize
)]
pub struct TradeEvent {
    pub(crate) op: TradeKind,
    pub(crate) qty: f64,
    pub(crate) pair: String,
    pub(crate) price: f64,
    pub(crate) strat_value: f64,
    pub(crate) at: DateTime<Utc>
}

#[derive(
    Clone, Debug, Deserialize, Serialize
)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "event")]
pub enum StratEvent {
    Stop { stop: StopEvent },
    Operation(OperationEvent),
    Trade(TradeEvent)
}

impl StratEvent {
    pub fn log(&self) {
        if log_enabled!(Info) {
            let s = serde_json::to_string(self).unwrap();
            info!("{}", s);
        }
    }
}

#[derive(Error, Debug)]
pub enum DataTableError {
    #[error("at least one bid expected")]
    MissingBids,
    #[error("at least one ask expected")]
    MissingAsks,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    pub ask: f64,
    // crypto_a
    pub ask_q: f64,
    // crypto_a_q
    pub bid: f64,
    // crypto_b
    pub bid_q: f64, // crypto_b_q
}

impl BookPosition {
    pub fn new(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> Self {
        let first_ask = asks[0];
        let first_bid = bids[0];
        Self {
            ask: first_ask.0,
            ask_q: first_ask.1,
            bid: first_bid.0,
            bid_q: first_bid.1,
            mid: Self::mid(asks, bids),
        }
    }

    fn mid(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> f64 {
        let (asks_iter, asks_iter2) = asks.iter().tee();
        let (bids_iter, bids_iter2) = bids.iter().tee();
        (asks_iter.map(|a| a.0 * a.1).sum::<f64>() + bids_iter.map(|b| b.0 * b.1).sum::<f64>())
            / asks_iter2.interleave(bids_iter2).map(|t| t.1).sum::<f64>()
    }
}

impl TryFrom<Orderbook> for BookPosition {
    type Error = DataTableError;

    fn try_from(t: Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(DataTableError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(DataTableError::MissingBids);
        }
        Ok(BookPosition::new(&t.asks, &t.bids))
    }
}

#[cfg(test)]
mod test {
    use quickcheck::{Arbitrary, Gen};

    use crate::types::BookPosition;

    impl Arbitrary for BookPosition {
        fn arbitrary(g: &mut Gen) -> BookPosition {
            BookPosition {
                ask: f64::arbitrary(g),
                ask_q: f64::arbitrary(g),
                bid: f64::arbitrary(g),
                bid_q: f64::arbitrary(g),
                mid: f64::arbitrary(g),
            }
        }
    }
}
