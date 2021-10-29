use chrono::Duration;
use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use strategies::coinnect_types::{LiveEventEnvelope, Pair};
use strategies::Exchange;

use crate::error::*;

pub async fn sampled_orderbooks_df(partitions: Vec<String>, pair: String, format: &str) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    let order_book_selector = "asks, bids";
    for partition in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{partition}';",
            partition = &partition,
            format = format
        ))?;
        //ctx.register_avro("order_books", &partition, AvroReadOptions::default())?;
        let sql_query = format!(
            "select to_timestamp_millis(event_ms) as event_ms, {order_book_selector} from order_books where pr = '{pair}'",
            order_book_selector = order_book_selector,
            pair = pair
        );
        let df = ctx.sql(&sql_query)?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}
