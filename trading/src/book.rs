use chrono::{DateTime, TimeZone, Utc};
use coinnect_rt::types::Orderbook;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use uuid::Uuid;

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
    // trade_id
    pub trace_id: Uuid,
}

impl BookPosition {
    pub fn new(trace_id: Uuid, event_time: DateTime<Utc>, asks: &[(f64, f64)], bids: &[(f64, f64)]) -> Self {
        let first_ask = asks[0];
        let first_bid = bids[0];
        Self {
            ask: first_ask.0,
            ask_q: first_ask.1,
            bid: first_bid.0,
            bid_q: first_bid.1,
            mid: Self::mid(asks, bids),
            event_time,
            trace_id,
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
    type Error = BookError;

    fn try_from(t: Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(BookError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(BookError::MissingBids);
        }
        let event_time = Utc.timestamp_millis(t.timestamp);
        // TODO: trace_id should come from event envelope
        Ok(BookPosition::new(Uuid::new_v4(), event_time, &t.asks, &t.bids))
    }
}

impl<'a> TryFrom<&'a Orderbook> for BookPosition {
    type Error = BookError;

    fn try_from(t: &'a Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(BookError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(BookError::MissingBids);
        }
        let event_time = Utc.timestamp_millis(t.timestamp);
        // TODO: trace_id should come from event envelope
        Ok(BookPosition::new(Uuid::new_v4(), event_time, &t.asks, &t.bids))
    }
}

#[cfg(any(test, feature = "test_util"))]
mod test {
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};
    use uuid::Uuid;

    use super::BookPosition;

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
                trace_id: Uuid::new_v4(),
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BookError {
    #[error("at least one bid expected")]
    MissingBids,
    #[error("at least one ask expected")]
    MissingAsks,
}
