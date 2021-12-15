use datafusion::arrow::array::{Array, PrimitiveArray, StringArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use futures::Stream;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;

use strategy::coinnect::prelude::{Exchange, MarketEventEnvelope};

use crate::datafusion_util::{get_col_as, to_struct_array, where_clause};
use crate::error::*;

pub async fn csv_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    partitions: HashSet<(P, Vec<(&'static str, String)>)>,
) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    for (base_path, partition) in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS CSV LOCATION '{base_path}';",
            base_path = base_path.as_ref().to_str().unwrap()
        ))
        .await?;
        let where_clause = where_clause(&mut partition.iter().map(|p| format!("{}={}", p.0, p.1)));
        let sql_query = format!(
            "select to_timestamp_millis(event_ms) as event_ts, * from order_books {where_clause} order by event_ms asc",
            where_clause = &where_clause
        );
        let df = ctx.sql(&sql_query).await?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}

pub fn events_from_csv_orderbooks(records: RecordBatch) -> impl Stream<Item = MarketEventEnvelope> {
    stream! {
        let sa: StructArray = to_struct_array(&records);
        let _asks_col = get_col_as::<PrimitiveArray<Float64Type>>(&sa, "a1");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
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
