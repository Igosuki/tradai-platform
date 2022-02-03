use crate::datafusion_util::{get_col_as, tables_as_stream};
use arrow2::array::*;
use datafusion::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;
use strategy::coinnect::prelude::*;

/// Read partitions as trades
pub fn trades_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    tables_as_stream(table_paths, format, Some("trades".to_string()), format!("select xch, to_timestamp_millis(event_ms) as event_ts, pr, asset, price, qty, quote_qty, is_buyer_maker from (select * from {table}) as tds order by event_ms asc", table = "trades")).map(events_from_trades)
        .flatten()
}

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_trades(record_batch: RecordBatch) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sa: StructArray = record_batch.into();
    stream! {
        for (i, column) in sa.fields().iter().enumerate() {
            trace!("sa[{}] = {:?}", i, column.data_type());
        }

        let price_col = get_col_as::<Float64Array>(&sa, "price");
        let qty_col = get_col_as::<Float64Array>(&sa, "qty");
        let is_buyer_maker_col = get_col_as::<BooleanArray>(&sa, "is_buyer_maker");
        let event_ms_col = get_col_as::<Int64Array>(&sa, "event_ts");
        let pair_col = get_col_as::<Utf8Array<i32>>(&sa, "pr");
        let xch_col = get_col_as::<DictionaryArray<u8>>(&sa, "xch");
        let xch_values = xch_col.values().as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        for i in 0..sa.len() {
            let price = price_col.value(i);
            let qty = qty_col.value(i);
            let is_buyer_maker = is_buyer_maker_col.value(i);
            let pair = pair_col.value(i);
            let ts = event_ms_col.value(i);

            let k = xch_col.keys().value(i);
            let xch = xch_values.value(k as usize);
            let xchg = Exchange::from_str(xch).unwrap_or_else(|_| panic!("wrong xchg {}", xch));

            yield MarketEventEnvelope::trade_event(xchg, Pair::from(pair), ts, price, qty, is_buyer_maker.into());
        }
    }
}
