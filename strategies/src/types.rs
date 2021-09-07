use std::convert::TryFrom;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use log::Level::Debug;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use strum_macros::{AsRefStr, EnumString};
use thiserror::Error;

use coinnect_rt::types::{AddOrderRequest, OrderEnforcement, OrderType, Orderbook, TradeType};

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    Buy,
    #[strum(serialize = "sell")]
    Sell,
}

impl From<TradeKind> for TradeType {
    fn from(tk: TradeKind) -> TradeType {
        match tk {
            TradeKind::Buy => TradeType::Buy,
            TradeKind::Sell => TradeType::Sell,
        }
    }
}

impl From<TradeKind> for i32 {
    fn from(tk: TradeKind) -> i32 {
        match tk {
            TradeKind::Buy => 0,
            TradeKind::Sell => 1,
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "lowercase")]
pub enum PositionKind {
    #[strum(serialize = "short")]
    Short,
    #[strum(serialize = "long")]
    Long,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationKind {
    #[strum(serialize = "open")]
    Open,
    #[strum(serialize = "close")]
    Close,
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StopEvent {
    Gain,
    Loss,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderMode {
    Market,
    Limit,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, juniper::GraphQLObject)]
pub struct TradeOperation {
    pub kind: TradeKind,
    pub pair: String,
    pub qty: f64,
    pub price: f64,
    pub dry_mode: bool,
}

impl TradeOperation {
    pub fn with_new_price(&self, new_price: f64) -> TradeOperation {
        TradeOperation {
            price: new_price,
            ..self.clone()
        }
    }

    pub fn to_request(&self, mode: &OrderMode) -> AddOrderRequest {
        let mut request: AddOrderRequest = self.clone().into();
        match mode {
            OrderMode::Limit => {
                request.order_type = OrderType::Limit;
                request.enforcement = Some(OrderEnforcement::FOK);
            }
            OrderMode::Market => {
                request.order_type = OrderType::Market;
                request.price = None;
            }
        }
        request
    }
}

impl From<TradeOperation> for AddOrderRequest {
    fn from(to: TradeOperation) -> Self {
        AddOrderRequest {
            pair: to.pair.into(),
            side: to.kind.into(),
            quantity: Some(to.qty),
            price: Some(to.price),
            dry_run: to.dry_mode,
            ..AddOrderRequest::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, EnumString)]
pub enum ExecutionInstruction {
    ParticipateDoNotInitiate,
    CancelIfNotBest,
    DoNotIncrease,
    DoNotReduce,
}

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
        if log_enabled!(Debug) {
            let s = serde_json::to_string(self).unwrap();
            debug!("{}", s);
        }
    }
}

impl From<StopEvent> for StratEvent {
    fn from(stop: StopEvent) -> Self { Self::Stop { stop } }
}

#[derive(Error, Debug)]
pub enum DataTableError {
    #[error("at least one bid expected")]
    MissingBids,
    #[error("at least one ask expected")]
    MissingAsks,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "python", pyclass)]
pub struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    pub ask: f64,
    // crypto_a
    pub ask_q: f64,
    // crypto_a_q
    pub bid: f64,
    // crypto_b
    pub bid_q: f64,
    // crypto_b_q
    pub event_time: DateTime<Utc>,
}

impl BookPosition {
    pub fn new(event_time: DateTime<Utc>, asks: &[(f64, f64)], bids: &[(f64, f64)]) -> Self {
        let first_ask = asks[0];
        let first_bid = bids[0];
        Self {
            ask: first_ask.0,
            ask_q: first_ask.1,
            bid: first_bid.0,
            bid_q: first_bid.1,
            mid: Self::mid(asks, bids),
            event_time,
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
        let event_time = Utc.timestamp_millis(t.timestamp);
        Ok(BookPosition::new(event_time, &t.asks, &t.bids))
    }
}

impl<'a> TryFrom<&'a Orderbook> for BookPosition {
    type Error = DataTableError;

    fn try_from(t: &'a Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(DataTableError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(DataTableError::MissingBids);
        }
        let event_time = Utc.timestamp_millis(t.timestamp);
        Ok(BookPosition::new(event_time, &t.asks, &t.bids))
    }
}

#[cfg(test)]
mod test {
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};

    use crate::types::BookPosition;

    lazy_static! {
        static ref MAX_ALLOWED_F64: f64 = 1.0_f64.powf(10.0);
    }

    fn zero_if_nan(x: f64) -> f64 {
        if x.is_nan() || x.is_infinite() {
            0.0
        } else {
            x
        }
    }

    impl Arbitrary for BookPosition {
        fn arbitrary(g: &mut Gen) -> BookPosition {
            BookPosition {
                ask: zero_if_nan(f64::arbitrary(g)),
                ask_q: zero_if_nan(f64::arbitrary(g)),
                bid: zero_if_nan(f64::arbitrary(g)),
                bid_q: zero_if_nan(f64::arbitrary(g)),
                mid: zero_if_nan(f64::arbitrary(g)),
                event_time: fake::faker::chrono::en::DateTime().fake(),
            }
        }
    }
}
