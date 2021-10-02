use crate::datasources::{get_col_as, live_order_book};
use crate::error::*;
use chrono::Duration;
use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use strategies::coinnect_types::{LiveEventEnvelope, Pair};
use strategies::Exchange;

pub async fn avro_orderbooks_df(
    partitions: Vec<String>,
    sample_rate: Duration,
    order_book_split_cols: bool,
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
            "CREATE EXTERNAL TABLE order_books STORED AS AVRO LOCATION '{partition}';",
            partition = &partition
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

pub fn events_from_avro_orderbooks(xchg: Exchange, pair: Pair, records: Vec<RecordBatch>) -> Vec<LiveEventEnvelope> {
    let mut live_events = vec![];
    for record_batch in records {
        let sa: StructArray = record_batch.into();
        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
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
            live_events.push(live_order_book(xchg, pair.clone(), ts, asks, bids));
        }
    }
    live_events
}
