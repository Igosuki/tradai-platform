use chrono::Duration;
use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use strategies::coinnect_types::{LiveEventEnvelope, Pair};
use strategies::Exchange;

use crate::error::*;

pub async fn raw_orderbooks_df(
    partitions: Vec<String>,
    sample_rate: Duration,
    order_book_split_cols: bool,
    format: &str,
) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    let order_book_selector = if order_book_split_cols {
        "asks[0][0] as a1, asks[0][1] as aq1, asks[1][0] as a2, asks[1][1] as aq2, asks[2][0] as a3, asks[2][1] as aq3, asks[3][0] as a4, asks[3][1] as aq4, asks[4][0] as a5, asks[4][1] as aq5, bids[0][0] as b1, bids[0][1] as bq1, bids[1][0] as b2, bids[1][1] as bq2, bids[2][0] as b3, bids[2][1] as bq3, bids[3][0] as b4, bids[3][1] as bq4, bids[4][0] as b5, bids[4][1] as bq5"
    } else {
        "asks, bids"
    };
    for partition in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{partition}';",
            partition = &partition,
            format = format
        ))?;
        //ctx.register_avro("order_books", &partition, AvroReadOptions::default())?;
        let sql_query = format!(
            "select to_timestamp_millis(event_ms) as event_ms, {order_book_selector} from
   (select asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select asks, bids, event_ms / {sample_rate} as sample_time, event_ms from order_books)) where row_num = 1;",
            sample_rate = sample_rate.num_milliseconds(),
            order_book_selector = order_book_selector
        );
        let df = ctx.sql(&sql_query)?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}
