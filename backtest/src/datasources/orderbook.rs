use arrow2::array::StructArray;
use brokers::prelude::{Exchange, Pair};
use chrono::Duration;
use datafusion::arrow::array::{Array, DictionaryArray, Int64Array, ListArray, PrimitiveArray};
use datafusion::record_batch::RecordBatch;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use brokers::prelude::MarketEventEnvelope;
use datafusion::arrow::array::Utf8Array;
use futures::StreamExt;
use tokio_stream::Stream;

use crate::datafusion_util::{get_col_as, tables_as_stream};

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
    tables_as_stream(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query)
        .map(move |rb| events_from_csv_orderbooks(rb, levels))
        .flatten()
}

/// Read partitions as raw order books
pub fn raw_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
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
    tables_as_stream(table_paths, format, Some(ORDER_BOOK_TABLE_NAME.to_string()), sql_query)
        .map(events_from_orderbooks)
        .flatten()
}

/// Read partitions as sampled order books
pub fn sampled_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = MarketEventEnvelope> + 'static {
    tables_as_stream(table_paths, format, Some("order_books".to_string()), format!("select xch, to_timestamp_millis(event_ms) as event_ts, pr, asks, bids from (select * from {table}) as obs order by event_ms asc", table = "order_books")).map(events_from_orderbooks)
        .flatten()
}

/// Find all distinct pairs in the sampled orderbook partitions
#[allow(dead_code)]
pub async fn sampled_orderbooks_pairs_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = String> + 'static {
    tables_as_stream(
        table_paths,
        format,
        Some("order_books".to_string()),
        format!("select distinct pr from {table}", table = "order_books"),
    )
    .map(|record_batch| {
        let sa: StructArray = record_batch.into();
        stream! {
            let pr_col = get_col_as::<Utf8Array<i32>>(&sa, "pr");
            for pr in pr_col.values_iter() {
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
        for (i, column) in sa.fields().iter().enumerate() {
            trace!("orderbook[{}] = {:?}", i, column.data_type());
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

/// Expects a record batch with the following schema :
/// asks : List(Tuple(f64))
/// bids : List(Tuple(f64))
/// `event_ts` : `TimestampMillisecond`
/// pr : String
/// xch : String
fn events_from_csv_orderbooks(records: RecordBatch, _levels: usize) -> impl Stream<Item = MarketEventEnvelope> {
    let sa: StructArray = records.into();
    // let mut asks_cols = vec![];
    // let mut asks_q_cols = vec![];
    // let mut bids_cols = vec![];
    // let mut bids_q_cols = vec![];
    // for col_num in (1..levels + 1) {}
    // for i in sa.len() {
    //     asks_col.value()
    // }

    stream! {
        let _asks_col = get_col_as::<PrimitiveArray<f64>>(&sa, "a1");
        let event_ms_col = get_col_as::<PrimitiveArray<i64>>(&sa, "event_ms");
        let pair_col = get_col_as::<Utf8Array<i32>>(&sa, "pr");
        let xch_col = get_col_as::<Utf8Array<i32>>(&sa, "xch");
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
        let partition = PathBuf::from(env!("COINDATA_CACHE_DIR")).join("data");
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
