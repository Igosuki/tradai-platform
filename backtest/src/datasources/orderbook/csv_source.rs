use std::collections::HashSet;

use datafusion::arrow::array::{Array, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use strategies::coinnect_types::{LiveEventEnvelope, Pair};
use strategies::Exchange;

use crate::datafusion_util::{get_col_as, to_struct_array};
use crate::datasources::orderbook::live_order_book;
use crate::error::*;

pub async fn csv_orderbooks_df(partitions: HashSet<String>) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    for partition in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS CSV LOCATION '{partition}';",
            partition = &partition
        ))
        .await?;
        let sql_query = "select to_timestamp_millis(event_ms) as event_ms, * from order_books";
        let df = ctx.sql(sql_query).await?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}

pub fn events_from_csv_orderbooks(xchg: Exchange, pair: Pair, records: &[RecordBatch]) -> Vec<LiveEventEnvelope> {
    let mut live_events = vec![];
    for record_batch in records {
        let sa: StructArray = to_struct_array(record_batch);
        let asks_col = get_col_as::<PrimitiveArray<Float64Type>>(&sa, "a1");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
        for i in 0..sa.len() {
            let bids = vec![];
            let asks = vec![];
            eprintln!("asks_col = {:?}", asks_col);
            let ts = event_ms_col.value(i);
            live_events.push(live_order_book(xchg, pair.clone(), ts, asks, bids));
        }
    }
    live_events
}
