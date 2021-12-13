use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StringArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;

use strategy::coinnect::prelude::{Exchange, MarketEventEnvelope, Pair};

use crate::datafusion_util::{get_col_as, to_struct_array};

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// event_ms : TimestampMillisecond
/// pr : String
pub fn events_from_orderbooks(
    xchg: Exchange,
    record_batch: RecordBatch,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    stream! {
        let sa: StructArray = to_struct_array(&record_batch);
        for (i, column) in sa.columns().iter().enumerate() {
            trace!("sa[{}] = {:?}", i, column.data_type());
        }
        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ts");
        let pair_col = get_col_as::<StringArray>(&sa, "pr");
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
            let pair = pair_col.value(i);
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
