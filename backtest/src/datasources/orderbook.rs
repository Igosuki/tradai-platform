use brokers::prelude::Exchange;
use chrono::{DateTime, Duration, Utc};
use datafusion::arrow;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, ListArray, StringArray, StructArray,
                               TimestampMillisecondArray, UInt16DictionaryArray};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use brokers::prelude::MarketEventEnvelope;
use brokers::types::{SecurityType, Symbol};
use futures::StreamExt;
use tokio_stream::Stream;
use tracing::Level;

use crate::datafusion_util::{get_col_as, multitables_as_df, multitables_as_stream, print_struct_schema,
                             string_partition};
use crate::datasources::{event_ms_where_clause, in_clause, join_where_clause};

const ORDER_BOOK_TABLE_NAME: &str = "order_books";

/// Read partitions as flat order booksm where asks and bids are flattened in columns [a{i}, aq{i}, b{i}, bq{i}]
pub fn flat_orderbooks_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    levels: usize,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sql_query = format!(
        "select to_timestamp_millis(event_ms) as event_ts, * from {table} order by event_ms asc",
        table = ORDER_BOOK_TABLE_NAME
    );
    multitables_as_stream(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query)
        .map(move |rb| events_from_csv_orderbooks(rb, levels))
        .flatten()
}

/// Read partitions as raw order books
pub fn raw_orderbooks_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    sample_rate: Duration,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sql_query = format!(
        "select xch, pair, to_timestamp_millis(event_ms) as event_ms, asks, bids from
   (select xch, pair, asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select xch, pair, asks, bids, event_ms / {sample_rate} as sample_time, event_ms from {table}) as raw_books) as sampled_books where row_num = 1;",
        sample_rate = sample_rate.num_milliseconds(),
        table = ORDER_BOOK_TABLE_NAME
    );
    multitables_as_stream(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query)
        .map(events_from_raw_orderbooks)
        .flatten()
}

/// Read partitions as raw order books recordbatch
pub async fn raw_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    sample_rate: Duration,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
) -> crate::error::Result<RecordBatch> {
    let sql_query = format!(
        "select xch, pair, to_timestamp_millis(event_ms) as event_ms, asks, bids from
   (select xch, pair, asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select xch, pair, asks, bids, event_ms / {sample_rate} as sample_time, event_ms from {table} {where}) as raw_books) as raw_books where row_num = 1;",
        sample_rate = sample_rate.num_milliseconds(),
        table = ORDER_BOOK_TABLE_NAME, where = join_where_clause(event_ms_where_clause("event_ms", upper_dt, lower_dt))
    );
    let batch = multitables_as_df(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query).await?;
    if tracing::enabled!(Level::TRACE) {
        trace!(
            "raw_orderbooks = {:?}",
            arrow::util::pretty::print_batches(&[batch.clone()])
        );
    }
    Ok(batch)
}

/// Read partitions as a sampled order books stream
pub fn sampled_orderbooks_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
    pairs: Vec<String>,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let pairs_clause = in_clause("pr", pairs);
    let mut vec1 = event_ms_where_clause("event_ms", upper_dt, lower_dt);
    vec1.push(pairs_clause);
    let where_clause = join_where_clause(&vec1);
    multitables_as_stream(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!(
            "select xch, to_timestamp_millis(event_ms) as event_ts, pr, asks, asksq, bids, bidsq from {table} {where} order by event_ms asc",
            table = "order_books", where = where_clause
        ),
    )
        .map(events_from_orderbooks)
        .flatten()
}

/// Read partitions as a sampled order books stream
pub async fn sampled_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    lower_dt: Option<DateTime<Utc>>,
    upper_dt: Option<DateTime<Utc>>,
    pairs: Vec<String>,
) -> crate::error::Result<RecordBatch> {
    let pairs_clause = in_clause("pr", pairs);
    let mut vec1 = event_ms_where_clause("event_ms", upper_dt, lower_dt);
    vec1.push(pairs_clause);
    let where_clause = join_where_clause(&vec1);
    let batch = multitables_as_df(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!(
            "select xch, to_timestamp_millis(event_ms) as event_ts, pr, asks, bids from {table} {where} order by event_ms asc",
            table = "order_books", where = where_clause
        ),
    )
        .await?;
    Ok(batch)
}

/// Find all distinct pairs in the sampled orderbook partitions
#[allow(dead_code)]
pub async fn sampled_orderbooks_pairs_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = String> + 'static {
    multitables_as_stream(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!("select distinct pr from {table}", table = "order_books"),
    )
    .map(|record_batch| {
        let sa: StructArray = record_batch.into();
        stream! {
            let pr_col = get_col_as::<StringArray>(&sa, "pr");
            for pr in pr_col.into_iter().flatten() {
                yield pr.to_string()
            }
        }
    })
    .flatten()
}

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_orderbooks(record_batch: RecordBatch) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sa: StructArray = record_batch.into();
    stream! {
        print_struct_schema(&sa, "orderbook");

        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let asksq_col = get_col_as::<ListArray>(&sa, "asksq");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let bidsq_col = get_col_as::<ListArray>(&sa, "bidsq");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ts");
        let pair_col = get_col_as::<StringArray>(&sa, "pr");
        let xch_col = get_col_as::<UInt16DictionaryArray>(&sa, "xch");

        for i in 0..sa.len() {
            let bidsp = bids_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
            let bidsq = bidsq_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
            let bids: Vec<(f64, f64)> = bidsp.iter().copied().zip(bidsq.iter().copied()).collect();
            let asksp = asks_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
            let asksq = asksq_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
            let asks: Vec<(f64, f64)> = asksp.iter().copied().zip(asksq.iter().copied()).collect();

            let ts = event_ms_col.value(i);
            let pair = pair_col.value(i);

            let xch_str = string_partition(xch_col, i).unwrap();
            let xchg = Exchange::from_str(&xch_str).unwrap_or_else(|_| panic!("wrong xchg {}", xch_str));

            yield MarketEventEnvelope::order_book_event(
                Symbol::new(pair.into(), SecurityType::Crypto, xchg),
                ts,
                asks,
                bids,
            );
        }
    }
}

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_csv_orderbooks(records: RecordBatch, _levels: usize) -> impl Stream<Item = MarketEventEnvelope> {
    let sa: StructArray = records.into();

    stream! {
        let _asks_col = get_col_as::<Float64Array>(&sa, "a1");
        let event_ms_col = get_col_as::<Int64Array>(&sa, "event_ms");
        let pair_col = get_col_as::<StringArray>(&sa, "pr");
        let xch_col = get_col_as::<StringArray>(&sa, "xch");
        for i in 0..sa.len() {
            let bids = vec![];
            let asks = vec![];
            let ts = event_ms_col.value(i);
            yield MarketEventEnvelope::order_book_event(
                Symbol::new(pair_col.value(i).into(), SecurityType::Crypto, Exchange::from_str(xch_col.value(i)).unwrap()),
                ts,
                asks,
                bids,
            );
        }
    }
}

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_raw_orderbooks(record_batch: RecordBatch) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    let sa: StructArray = record_batch.into();
    stream! {
        print_struct_schema(&sa, "orderbook");

        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
        let pair_col = get_col_as::<StringArray>(&sa, "pair");
        let xch_col = get_col_as::<UInt16DictionaryArray>(&sa, "xch");

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
                    .downcast_ref::<Float64Array>()
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
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values();
                asks.push((vals[0], vals[1]));
            }

            let ts = event_ms_col.value(i);
            let pair = pair_col.value(i);

            let xch_str = string_partition(xch_col, i).unwrap();
            let xchg = Exchange::from_str(&xch_str).unwrap_or_else(|_| panic!("wrong xchg {}", xch_str));

            yield MarketEventEnvelope::order_book_event(
                Symbol::new(pair.into(), SecurityType::Crypto, xchg),
                ts,
                asks,
                bids,
            );
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::DataType;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::datafusion_util::new_context;
    use datafusion::datasource::file_format::avro::AvroFormat;
    use datafusion::datasource::listing::ListingOptions;

    // TODO: make some small sample files to test this out instead of production files
    #[tokio::test]
    #[ignore]
    async fn test_read_df() -> datafusion::error::Result<()> {
        let ctx = new_context();
        let partition =
            PathBuf::from(std::env::var("COINDATA_CACHE_DIR").unwrap_or_else(|_| "".to_string())).join("data");
        let listing_options = ListingOptions {
            file_extension: "avro".to_string(),
            format: Arc::new(AvroFormat::default()),
            table_partition_cols: vec![
                ("xch".to_string(), DataType::Utf8),
                ("chan".to_string(), DataType::Utf8),
                ("dt".to_string(), DataType::Utf8),
            ],
            collect_stat: true,
            target_partitions: 8,
            file_sort_order: None,
            infinite_source: false,
        };
        ctx.register_listing_table("order_books", &partition.to_str().unwrap(), listing_options, None, None)
            .await?;

        let df2 = ctx
            .clone()
            .sql("select xch, pr, to_timestamp_millis(event_ms) as event_ts, asks, bids from order_books where dt='20211129' and xch='Binance' and chan='1mn_order_books' order by event_ts asc LIMIT 100")
            .await?;

        df2.collect().await?;
        Ok(())
    }
}
