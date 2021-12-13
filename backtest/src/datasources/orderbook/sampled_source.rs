use std::collections::HashSet;
use std::time::Instant;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use tokio_stream::Stream;

use crate::error::*;

/// Read partitions as sampled order books
pub fn sampled_orderbooks_df(partitions: HashSet<String>, format: String) -> impl Stream<Item = RecordBatch> + 'static {
    dbg!(&partitions);
    stream! {
        for partition in partitions {
            let ctx = ExecutionContext::new();
            ctx.clone()
                .sql(&format!(
                    "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{partition}';",
                    partition = &partition,
                    format = format
                ))
                .await.unwrap();
            let now = Instant::now();
            let df = ctx
                .clone()
                .sql("select pr, to_timestamp_millis(event_ms) as event_ms, asks, bids from order_books")
                .await
                .unwrap();
            let collected = df.execute_stream().await.unwrap();
            let elapsed = now.elapsed();
            info!(
                "Read records for {} in {}.{}s",
                partition,
                elapsed.as_secs(),
                elapsed.subsec_millis()
            );
            for await batch in collected {
                yield batch.unwrap();
            }
        }
    }
}

/// Find all distinct pairs in the sampled orderbook partitions
#[allow(dead_code)]
pub async fn sampled_orderbooks_pairs(
    partitions: HashSet<String>,
    pair: Option<String>,
    format: &str,
) -> Result<Vec<String>> {
    dbg!(&partitions);
    let mut records = vec![];

    let tasks: Vec<Result<Vec<String>>> = futures::future::join_all(partitions.iter().map(|partition| {
        let pair = pair.clone();
        async move {
            let ctx = ExecutionContext::new();
            ctx.clone()
                .sql(&format!(
                    "CREATE EXTERNAL TABLE order_books STORED AS {format} LOCATION '{partition}';",
                    partition = &partition,
                    format = format
                ))
                .await?;
            let now = Instant::now();
            let pairs = match pair {
                None => {
                    let df2 = ctx.clone().sql("select distinct pr from order_books").await?;
                    let pairs = df2.collect().await?;
                    let pairs: Vec<String> = pairs
                        .iter()
                        .map(|rb| {
                            let pr = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                            pr
                        })
                        .flatten()
                        .filter_map(|s| s.map(|s| s.to_string()))
                        .collect();
                    eprintln!("pairs = {:?}", pairs);
                    pairs
                }
                Some(p) => vec![p],
            };
            let elapsed = now.elapsed();
            info!("Read pairs in {}.{}s", elapsed.as_secs(), elapsed.subsec_millis());
            Result::Ok(pairs)
        }
    }))
    .await;
    for result in tasks {
        records.extend_from_slice(result.unwrap().as_slice());
    }
    Ok(records)
}
