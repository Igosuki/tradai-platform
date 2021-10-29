use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use crate::error::*;

pub async fn sampled_orderbooks_df(partitions: Vec<String>, pair: String, format: &str) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    for partition in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{partition}';",
            partition = &partition,
            format = format
        ))
        .await?;
        //ctx.register_avro("order_books", &partition, AvroReadOptions::default())?;
        let sql_query = format!(
            "select to_timestamp_millis(event_ms) as event_ms, asks, bids from order_books where pr = '{pair}'",
            pair = pair
        );
        let df = ctx.sql(&sql_query).await?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}
