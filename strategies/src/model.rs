use chrono::{DateTime, Utc};
use coinnect_rt::types::{Orderbook, TradeType};
use itertools::Itertools;
use log::Level::Info;
use std::convert::TryFrom;
use strum_macros::{AsRefStr, EnumString};
use thiserror::Error;

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

const TS_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub fn log_pos(op: &OperationKind, pos: &PositionKind, time: DateTime<Utc>) {
    if log_enabled!(Info) {
        info!(
            "{} {} position at {}",
            op.as_ref(),
            match pos {
                PositionKind::SHORT => "short",
                PositionKind::LONG => "long",
            },
            time.format(TS_FORMAT)
        );
    }
}

pub fn log_trade(op: &TradeKind, qty: f64, pair: &str, price: f64, value: f64) {
    if log_enabled!(Info) {
        info!(
            "{} {:.2} {} at {} for {:.2}",
            op.as_ref(),
            qty,
            pair,
            price,
            value
        );
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
    use crate::model::BookPosition;
    use quickcheck::{Arbitrary, Gen};

    impl Arbitrary for BookPosition {
        fn arbitrary<G: Gen>(g: &mut G) -> BookPosition {
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
