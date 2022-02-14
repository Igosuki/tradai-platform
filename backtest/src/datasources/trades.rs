use crate::datafusion_util::{get_col_as, tables_as_stream};
use brokers::pair::symbol_to_pair;
use brokers::prelude::*;
use brokers::types::Candle;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::*;
use datafusion::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use stats::kline::Resolution;
use stats::kline::TimeUnit::Minute;
use stats::Next;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

/// Read partitions as trades
pub fn candles_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> {
    trades_df(table_paths, format)
        .scan(
            stats::kline::Kline::new(Resolution::new(Minute, 1), 2_usize.pow(16)),
            |kl, msg: MarketEventEnvelope| {
                let candles = Next::<(f64, f64, DateTime<Utc>)>::next(kl, (msg.e.price(), msg.e.vol(), msg.e.time()));
                let candle = candles.first().unwrap();
                let mut msg = msg;
                msg.e = MarketEvent::CandleTick(Candle {
                    event_time: candle.event_time,
                    pair: msg.pair.clone(),
                    start_time: candle.start_time,
                    end_time: candle.end_time,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: candle.volume,
                    quote_volume: candle.quote_volume,
                    trade_count: candle.trade_count,
                    is_final: candle.is_final,
                });
                futures::future::ready(Some(msg))
            },
        )
        .filter(|msg: &MarketEventEnvelope| {
            let r = matches!(msg.e, MarketEvent::CandleTick(Candle { is_final: true, .. }));
            futures::future::ready(r)
        })
}

/// Read partitions as trades
pub fn trades_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    tables_as_stream(table_paths, format, Some("trades".to_string()), format!("select xch, to_timestamp_millis(event_ms) as event_ts, pr, asset, price, qty, quote_qty, is_buyer_maker from {table} order by event_ms asc", table = "trades")).map(events_from_trades)
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
            trace!("trades[{}] = {:?}", i, column.data_type());
        }

        let price_col = get_col_as::<Float64Array>(&sa, "price");
        let qty_col = get_col_as::<Float64Array>(&sa, "qty");
        let is_buyer_maker_col = get_col_as::<BooleanArray>(&sa, "is_buyer_maker");
        let event_ms_col = get_col_as::<Int64Array>(&sa, "event_ts");
        let pair_col = get_col_as::<DictionaryArray<u8>>(&sa, "pr");
        let pair_values = pair_col.values().as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        let xch_col = get_col_as::<DictionaryArray<u8>>(&sa, "xch");
        let xch_values = xch_col.values().as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        for i in 0..sa.len() {
            let price = price_col.value(i);
            let qty = qty_col.value(i);
            let is_buyer_maker = is_buyer_maker_col.value(i);
            let ts = event_ms_col.value(i);

            let p = pair_col.keys().value(i);
            let pair = pair_values.value(p as usize);

            let k = xch_col.keys().value(i);
            let xch = xch_values.value(k as usize);
            let xchg = Exchange::from_str(xch).unwrap_or_else(|_| panic!("wrong xchg {}", xch));

            yield MarketEventEnvelope::trade_event(xchg, symbol_to_pair(&xchg, &pair.into()).unwrap(), ts, price, qty, is_buyer_maker.into());
        }
    }
}
