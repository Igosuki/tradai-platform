use crate::datafusion_util::{df_format, where_clause};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::context::ExecutionContext;
use futures::StreamExt;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::time::Instant;
use tokio_stream::Stream;

use crate::error::*;

/// Read partitions as sampled order books
pub fn sampled_orderbooks_df<P: 'static + AsRef<Path> + Debug>(
    partitions: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
) -> impl Stream<Item = RecordBatch> + 'static {
    dbg!(&partitions);
    let s = partitions.into_iter().map(move |(base_path, partition)| {
        let format = format.clone();
        let base_path = base_path.as_ref().to_str().unwrap_or("").to_string();

        stream! {
            let now = Instant::now();
            let mut ctx = ExecutionContext::new();
            let (ext, file_format) = df_format(format);
            let listing_options = ListingOptions {
                file_extension: ext.to_string(),
                format: file_format,
                table_partition_cols: partition.iter().map(|p| p.0.to_string()).collect(),
                collect_stat: true,
                target_partitions: 8,
            };
            ctx.register_listing_table(
                "order_books",
                &format!("file://{}", base_path),
                listing_options,
                None,
            )
            .await.unwrap();
            let where_clause = where_clause(&mut partition.iter());
            let df = ctx
                .clone()
                .sql(&format!("select xch, pr, to_timestamp_millis(event_ms) as event_ts, asks, bids from order_books {where_clause} order by event_ms asc", where_clause = &where_clause))
                .await
                .unwrap();
            let collected = df.execute_stream().await.unwrap();
            for await batch in collected {
                yield batch.unwrap();
            }
            let elapsed = now.elapsed();
            info!(
                "Read records in {} for {:?} in {}.{}s",
                base_path,
                partition,
                elapsed.as_secs(),
                elapsed.subsec_millis()
            );
        }
    });
    tokio_stream::iter(s).flatten()
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

#[cfg(test)]
mod test {
    use datafusion::datasource::file_format::avro::AvroFormat;
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::prelude::ExecutionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

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
        let batch = df2.collect().await?;
        dbg!(batch);
        Ok(())
    }
}
