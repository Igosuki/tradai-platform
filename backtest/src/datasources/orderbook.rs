use brokers::prelude::{Exchange, Pair};
use chrono::Duration;
use datafusion::arrow::array::{Array, PrimitiveArray, StructArray};
use datafusion::record_batch::RecordBatch;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use brokers::prelude::MarketEventEnvelope;
use futures::StreamExt;
use tokio_stream::Stream;
use tracing::Level;

use crate::datafusion_util::{get_col_as, multitables_as_df, multitables_as_stream, print_struct_schema, Float64Type,
                             Int64Type, ListArray, StringArray, TimestampMillisecondArray, UInt16DictionaryArray};

const ORDER_BOOK_TABLE_NAME: &str = "order_books";

/// Read partitions as flat order booksm where asks and bids are flattened in columns [a{i}, aq{i}, b{i}, bq{i}]
pub fn flat_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
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
        "select to_timestamp_millis(event_ms) as event_ms, asks, bids from
   (select asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select asks, bids, event_ms / {sample_rate} as sample_time, event_ms from {table})) where row_num = 1;",
        sample_rate = sample_rate.num_milliseconds(),
        table = ORDER_BOOK_TABLE_NAME
    );
    multitables_as_stream(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query)
        .map(events_from_orderbooks)
        .flatten()
}

/// Read partitions as raw order books recordbatch
pub async fn raw_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    sample_rate: Duration,
    format: String,
) -> crate::error::Result<RecordBatch> {
    let sql_query = format!(
        "select to_timestamp_millis(event_ms) as event_ms, asks, bids from
   (select asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select asks, bids, event_ms / {sample_rate} as sample_time, event_ms from {table})) where row_num = 1;",
        sample_rate = sample_rate.num_milliseconds(),
        table = ORDER_BOOK_TABLE_NAME
    );
    let batch = multitables_as_df(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query).await?;
    if tracing::enabled!(Level::TRACE) {
        trace!(
            "raw_orderbooks = {:?}",
            datafusion::arrow_print::write(&[batch.clone()])
        );
    }
    Ok(batch)
}

/// Read partitions as a sampled order books stream
pub fn sampled_orderbooks_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    multitables_as_stream(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!(
            "select xch, to_timestamp_millis(event_ms) as event_ts, pr, asks, bids from {table} order by event_ms asc",
            table = "order_books"
        ),
    )
    .map(events_from_orderbooks)
    .flatten()
}

/// Read partitions as a sampled order books stream
pub async fn sampled_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> crate::error::Result<RecordBatch> {
    let batch = multitables_as_df(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!(
            "select xch, to_timestamp_millis(event_ms) as event_ts, pr, asks, bids from {table} order by event_ms asc",
            table = "order_books"
        ),
    )
    .await?;
    if tracing::enabled!(Level::TRACE) {
        trace!(
            "sampled_orderbooks = {:?}",
            datafusion::arrow_print::write(&[batch.clone()])
        );
    }
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
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ts");
        let pair_col = get_col_as::<StringArray>(&sa, "pr");
        let xch_col = get_col_as::<UInt16DictionaryArray>(&sa, "xch");
        let xch_values = xch_col.values().as_any().downcast_ref::<StringArray>().unwrap();

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

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_csv_orderbooks(records: RecordBatch, _levels: usize) -> impl Stream<Item = MarketEventEnvelope> {
    let sa: StructArray = records.into();

    stream! {
        let _asks_col = get_col_as::<PrimitiveArray<Float64Type>>(&sa, "a1");
        let event_ms_col = get_col_as::<PrimitiveArray<Int64Type>>(&sa, "event_ms");
        let pair_col = get_col_as::<StringArray>(&sa, "pr");
        let xch_col = get_col_as::<StringArray>(&sa, "xch");
        for i in 0..sa.len() {
            let bids = vec![];
            let asks = vec![];
            let ts = event_ms_col.value(i);
            yield MarketEventEnvelope::order_book_event(
                Exchange::from_str(xch_col.value(i)).unwrap(),
                pair_col.value(i).into(),
                ts,
                asks,
                bids,
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use datafusion::datasource::file_format::avro::AvroFormat;
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::prelude::ExecutionContext;

    // TODO: make some small sample files to test this out instead of production files
    #[tokio::test]
    #[ignore]
    async fn test_read_df() -> datafusion::error::Result<()> {
        let mut ctx = ExecutionContext::new();
        let partition = PathBuf::from(std::env::var("COINDATA_CACHE_DIR").unwrap_or("".to_string())).join("data");
        let listing_options = ListingOptions {
            file_extension: "avro".to_string(),
            format: Arc::new(AvroFormat::default()),
            table_partition_cols: vec!["xch".to_string(), "chan".to_string(), "dt".to_string()],
            collect_stat: true,
            target_partitions: 8,
        };
        ctx.register_listing_table(
            "order_books",
            &format!("file://{}", partition.to_str().unwrap()),
            listing_options,
            None,
        )
        .await?;

        let df2 = ctx
            .clone()
            .sql("select xch, pr, to_timestamp_millis(event_ms) as event_ts, asks, bids from order_books where dt='20211129' and xch='Binance' and chan='1mn_order_books' order by event_ts asc LIMIT 100")
            .await?;
        df2.collect().await?;
        Ok(())
    }
}
