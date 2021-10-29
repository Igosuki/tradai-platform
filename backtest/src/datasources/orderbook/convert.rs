use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;

use strategies::{Exchange, LiveEventEnvelope, Pair};

use crate::datafusion_util::get_col_as;
use crate::datasources::orderbook::live_order_book;

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// event_ms : TimestampMillisecond
pub fn events_from_orderbooks(xchg: Exchange, pair: Pair, records: Vec<RecordBatch>) -> Vec<LiveEventEnvelope> {
    let mut live_events = vec![];
    for record_batch in records {
        let sa: StructArray = record_batch.into();
        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
        for i in 0..sa.len() {
            let mut bids = vec![];
            for bid in bids_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = bid
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap()
                    .values();
                bids.push((vals[0], vals[1]));
            }
            let mut asks = vec![];
            for ask in asks_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = ask
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap()
                    .values();
                asks.push((vals[0], vals[1]));
            }
            let ts = event_ms_col.value(i);
            live_events.push(live_order_book(xchg, pair.clone(), ts, asks, bids));
        }
    }
    live_events
}
