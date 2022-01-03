use datafusion::arrow::array::{Array, DictionaryArray, Int64Array, ListArray, PrimitiveArray, StructArray, Utf8Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
use std::str::FromStr;
use strategy::coinnect::prelude::{Exchange, MarketEventEnvelope, Pair};

use crate::datafusion_util::get_col_as;

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
pub fn events_from_orderbooks(record_batch: RecordBatch) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sa: StructArray = record_batch.into();

    stream! {
        for (i, column) in sa.fields().iter().enumerate() {
            trace!("sa[{}] = {:?}", i, column.data_type());
        }

        let asks_col = get_col_as::<ListArray<i32>>(&sa, "asks");
        let bids_col = get_col_as::<ListArray<i32>>(&sa, "bids");
        let event_ms_col = get_col_as::<Int64Array>(&sa, "event_ts");
        let pair_col = get_col_as::<Utf8Array<i32>>(&sa, "pr");
        let xch_col = get_col_as::<DictionaryArray<u8>>(&sa, "xch");
        let xch_values = xch_col.values().as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        for i in 0..sa.len() {
            let mut bids = vec![];
            for bid in bids_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray<i32>>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = bid
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap()
                    .values();
                bids.push((vals[0], vals[1]));
            }
            let mut asks = vec![];
            for ask in asks_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray<i32>>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = ask
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap()
                    .values();
                asks.push((vals[0], vals[1]));
            }
            let ts = event_ms_col.value(i);
            let pair = pair_col.value(i);

            let k = xch_col.keys().value(i);
            let xch = xch_values.value(k as usize);
            let xchg = Exchange::from_str(xch).unwrap_or_else(|_| panic!("wrong xchg {}", xch));

            yield MarketEventEnvelope::order_book_event(
                xchg,
                Pair::from(pair),
                ts,
                asks,
                bids,
            );
        }
    }
}
