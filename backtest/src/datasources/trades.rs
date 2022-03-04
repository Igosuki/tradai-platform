use crate::datafusion_util::{get_col_as, multitables_as_df, multitables_as_stream, print_struct_schema,
                             string_partition, TimestampMillisecondArray, UInt16DictionaryArray};
use crate::datasources::event_ms_where_clause;
use brokers::pair::symbol_to_pair;
use brokers::prelude::*;
use brokers::types::{Candle, SecurityType, Symbol};
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
use tracing::Level;

/// Read partitions as trades
pub fn candles_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
    resolution: Option<Resolution>,
) -> impl Stream<Item = MarketEventEnvelope> {
    let resolution = resolution.unwrap_or_else(|| Resolution::new(Minute, 15));
    trades_stream(table_paths, format, lower_dt, upper_dt).scan(
        stats::kline::Kline::new(resolution, 2_usize.pow(16)),
        |kl, msg: MarketEventEnvelope| {
            let candles = Next::<(f64, f64, DateTime<Utc>)>::next(kl, (msg.e.price(), msg.e.vol(), msg.e.time()));
            let candle = candles.first().unwrap();
            let mut msg = msg;
            msg.e = MarketEvent::CandleTick(Candle {
                event_time: candle.event_time,
                pair: msg.symbol.value.clone(),
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
    // .filter(|msg: &MarketEventEnvelope| {
    //     let r = matches!(msg.e, MarketEvent::CandleTick(Candle { is_final: true, .. }));
    //     futures::future::ready(r)
    // })
}

/// Read partitions as trades
pub fn trades_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    multitables_as_stream(table_paths, format, Some("trades".to_string()), format!("select xch, to_timestamp_millis(event_ms) as event_ts, sym, ast, price, qty, quote_qty, is_buyer_maker from {table} {where} order by event_ms asc", table = "trades", where = event_ms_where_clause("event_ms", upper_dt, lower_dt))).map(events_from_trades)
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
        print_struct_schema(&sa, "trades");
        let price_col = get_col_as::<Float64Array>(&sa, "price");
        let qty_col = get_col_as::<Float64Array>(&sa, "qty");
        let is_buyer_maker_col = get_col_as::<BooleanArray>(&sa, "is_buyer_maker");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ts");
        let sym_col = get_col_as::<UInt16DictionaryArray>(&sa, "sym");
        let xch_col = get_col_as::<UInt16DictionaryArray>(&sa, "xch");
        let ast_col = get_col_as::<UInt16DictionaryArray>(&sa, "ast");

        for i in 0..sa.len() {
            let price = price_col.value(i);
            let qty = qty_col.value(i);
            let is_buyer_maker = is_buyer_maker_col.value(i);
            let ts = event_ms_col.value(i);

            let sym_str = string_partition(sym_col, i).unwrap();

            let xch_str = string_partition(xch_col, i).unwrap();
            let xchg = Exchange::from_str(&xch_str).unwrap_or_else(|_| panic!("wrong xchg {}", xch_str));

            let ast_str = string_partition(ast_col, i).unwrap();
            let ast = SecurityType::from_str(&ast_str).unwrap_or_else(|_| panic!("wrong security type {}", ast_str));

            yield MarketEventEnvelope::trade_event(
                Symbol::new(
                    symbol_to_pair(&xchg, &sym_str.into()).unwrap(),
                    ast,
                    xchg,
                ),
                ts,
                price,
                qty,
                is_buyer_maker.into(),
            );

        }
    }
}

/// Read partitions as trades
pub async fn trades_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
) -> crate::error::Result<RecordBatch> {
    let batch = multitables_as_df(table_paths, format, Some("trades".to_string()), format!("select xch, to_timestamp_millis(event_ms) as event_ts, sym, ast, price, qty, quote_qty, is_buyer_maker from {table} {where} order by event_ms asc", table = "trades", where = event_ms_where_clause("event_ms", upper_dt, lower_dt))).await?;
    if tracing::enabled!(Level::TRACE) {
        trace!("trades = {:?}", datafusion::arrow_print::write(&[batch.clone()]));
    }
    Ok(batch)
}

/// Read trades partitions as candles
pub async fn candles_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
    resolution: Option<Resolution>,
) -> crate::error::Result<RecordBatch> {
    let resolution = resolution.unwrap_or_else(|| Resolution::new(Minute, 15));
    let resolution_millis = resolution.as_millis();
    let sql_query = format!(
        r#"
        SELECT t1.price AS open,
           m.high,
           m.low,
           t2.price as close,
           open_time
        FROM (SELECT MIN(event_ms) AS min_time,
                     MAX(event_ms) AS max_time,
                     MIN(price) as low,
                     MAX(price) as high,
                     FLOOR(event_ms / {resolution}) as open_time
              FROM trades {where}
              GROUP BY open_time) m
        JOIN trades t1 ON t1.event_ms = min_time
        JOIN trades t2 ON t2.event_ms = max_time
    "#,
        resolution = resolution_millis,
        where = event_ms_where_clause("event_ms", upper_dt, lower_dt)
    );
    let batch = multitables_as_df(table_paths, format, Some("trades".to_string()), sql_query.to_string()).await?;
    if tracing::enabled!(Level::TRACE) {
        trace!("candles = {:?}", datafusion::arrow_print::write(&[batch.clone()]));
    }
    Ok(batch)
}
