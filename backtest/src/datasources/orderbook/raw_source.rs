use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;

use crate::datafusion_util::where_clause;
use chrono::Duration;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use crate::error::*;

pub async fn raw_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    partitions: HashSet<(P, Vec<(&'static str, String)>)>,
    sample_rate: Duration,
    order_book_split_cols: bool,
    format: &str,
) -> Result<Vec<RecordBatch>> {
    dbg!(&partitions);
    let mut records = vec![];
    let order_book_selector = if order_book_split_cols {
        "asks[0][0] as a1, asks[0][1] as aq1, asks[1][0] as a2, asks[1][1] as aq2, asks[2][0] as a3, asks[2][1] as aq3, asks[3][0] as a4, asks[3][1] as aq4, asks[4][0] as a5, asks[4][1] as aq5, bids[0][0] as b1, bids[0][1] as bq1, bids[1][0] as b2, bids[1][1] as bq2, bids[2][0] as b3, bids[2][1] as bq3, bids[3][0] as b4, bids[3][1] as bq4, bids[4][0] as b5, bids[4][1] as bq5"
    } else {
        "asks, bids"
    };
    let tasks: Vec<Result<Vec<RecordBatch>>> =
        futures::future::join_all(partitions.iter().map(|(base_path, partition)| {
            let base_path = base_path.as_ref().to_str().unwrap();
            async move {
                let mut ctx = ExecutionContext::new();
                ctx.sql(&format!(
                    "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{base_path}';",
                    base_path = base_path,
                    format = format
                ))
                .await?;
                let where_clause = where_clause(&mut partition.iter().map(|p| format!("{}={}", p.0, p.1)));
                let sql_query = format!(
                    "select to_timestamp_millis(event_ms) as event_ms, {order_book_selector} from
   (select asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select asks, bids, event_ms / {sample_rate} as sample_time, event_ms from order_books {where_clause})) where row_num = 1;",
                    sample_rate = sample_rate.num_milliseconds(),
                    order_book_selector = order_book_selector,
                    where_clause = where_clause
                );
                let df = ctx.sql(&sql_query).await?;
                let results: Vec<RecordBatch> = df.collect().await?;
                Result::Ok(results)
            }
        }))
        .await;
    for result in tasks {
        records.extend_from_slice(result.unwrap().as_slice());
    }
    // let results = futures::future::join_all(tasks).await;
    // results.transpose();
    Ok(records)
}
