use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;

use crate::error::*;

pub async fn sampled_orderbooks_df(
    partitions: HashSet<String>,
    pair: Option<String>,
    format: &str,
) -> Result<HashMap<String, Vec<RecordBatch>>> {
    dbg!(&partitions);
    let mut records = HashMap::new();

    let tasks: Vec<Result<HashMap<String, Vec<RecordBatch>>>> = futures::future::join_all(partitions.iter().map(|partition| {
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
            let mut results: HashMap<String, Vec<RecordBatch>> = HashMap::new();
            let pairs = match pair {
                None => {
                    let df2 = ctx.clone().sql("select distinct pr from order_books").await?;
                    let pairs = df2.collect().await?;
                    eprintln!("pairs = {:?}", pairs);
                    let pairs: Vec<String> = pairs.iter().map(|rb| {
                        let pr = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                        pr
                    }).flatten().filter_map(|s| s.map(|s| s.to_string())).collect();
                    pairs
                }
                Some(p) => vec![p]
            };
            for pair in pairs.into_iter() {
                let sql_query = format!(
                    "select pr, to_timestamp_millis(event_ms) as event_ms, asks, bids from order_books where pr = '{pair}'",
                    pair = pair
                );
                let df = ctx.clone().sql(&sql_query).await?;
                let collected = df.collect().await?;
                results.insert(pair.clone(), collected);
            }
            Result::Ok(results)
        }
    }))
    .await;
    for result in tasks {
        for df_entry in result.unwrap() {
            match records.entry(df_entry.0) {
                Entry::Occupied(mut o) => {
                    let x: &mut Vec<RecordBatch> = o.get_mut();
                    x.extend_from_slice(df_entry.1.as_slice());
                }
                Entry::Vacant(e) => {
                    e.insert(df_entry.1);
                }
            }
        }
    }
    Ok(records)
}
