use datafusion::arrow::array::StructArray;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::field_util::StructArrayExt;
use datafusion::logical_plan::Expr;
use datafusion::prelude::{col, lit};
use datafusion::record_batch::RecordBatch;
use ext::ResultExt;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

pub fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {}", name))
}

pub fn df_format(format: &str) -> (&'static str, Arc<dyn FileFormat>) {
    match format.to_lowercase().as_str() {
        "avro" => ("avro", Arc::new(AvroFormat::default())),
        "parquet" => ("parquet", Arc::new(ParquetFormat::default())),
        "csv" => ("csv", Arc::new(CsvFormat::default())),
        _ => unimplemented!(),
    }
}

#[allow(dead_code)]
pub fn where_clause<'a, K: 'a + std::fmt::Display, V: 'a + std::fmt::Display, I: Iterator<Item = &'a (K, V)>>(
    iter: &mut I,
) -> String
where
    I: std::iter::ExactSizeIterator,
{
    if iter.is_empty() {
        String::new()
    } else {
        "where ".to_string() + iter.map(|s| format!("{}='{}'", s.0, s.1)).join(" and ").as_str()
    }
}

pub fn partition_filter_clause<
    'a,
    K: 'a + std::fmt::Display + AsRef<str>,
    V: 'a + std::fmt::Display + datafusion::logical_plan::Literal,
    I: IntoIterator<Item = (K, V)>,
>(
    iter: I,
) -> Option<Expr> {
    iter.into_iter()
        .map(|s| col(s.0.as_ref()).eq(lit(s.1)))
        .reduce(|lhs, rhs| lhs.and(rhs))
}

#[cfg(all(feature = "remote_execution", not(feature = "standalone_execution")))]
pub fn new_context() -> ballista::context::BallistaContext {
    let config = BallistaConfig::builder()
        .set("ballista.shuffle.partitions", "32")
        .build()?;
    ballista::context::BallistaContext::remote("localhost", 50050, &config)
}

#[cfg(all(feature = "standalone_execution", not(feature = "remote_execution")))]
pub fn new_context() -> ballista::context::BallistaContext { ballista::context::BallistaContext::standalone() }

#[cfg(any(
    not(any(feature = "remote_execution", feature = "standalone_execution")),
    all(feature = "remote_execution", feature = "standalone_execution")
))]
pub fn new_context() -> datafusion::prelude::ExecutionContext { datafusion::prelude::ExecutionContext::new() }

pub fn listing_options(format: String, partition: Vec<(&str, String)>) -> ListingOptions {
    let (ext, file_format) = df_format(&format);
    ListingOptions {
        file_extension: ext.to_string(),
        format: file_format,
        table_partition_cols: partition.iter().map(|p| p.0.to_string()).collect(),
        collect_stat: true,
        target_partitions: 8,
    }
}

pub fn tables_as_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> impl Stream<Item = RecordBatch> + 'static {
    debug!("{:?}", &table_paths);
    let s = table_paths.into_iter().map(move |(base_path, partitions)| {
        table_as_stream(
            base_path,
            partitions,
            format.clone(),
            table_name.clone(),
            sql_query.clone(),
        )
    });
    tokio_stream::iter(s).flatten()
}

pub const DEFAULT_TABLE_NAME: &'static str = "listing_table";

pub fn table_as_stream<P: 'static + AsRef<Path> + Debug>(
    base_path: P,
    partitions: Vec<(&'static str, String)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> impl Stream<Item = RecordBatch> + 'static {
    stream! {
        let base_path = base_path.as_ref().to_str().unwrap_or("").to_string();
        let now = Instant::now();
        let collected = table_as_df(base_path.clone(), partitions.clone(), format, table_name, sql_query).await.unwrap();
        let elapsed = now.elapsed();
        info!(
            "Read records in {} for {:?} in {}.{}s",
            base_path,
            partitions,
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        for await batch in collected {
            yield batch.unwrap();
        }
        let elapsed = now.elapsed();
        info!(
            "Pushed record stream in {} for {:?} in {}.{}s",
            base_path,
            partitions,
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
    }
}

use datafusion::dataframe::DataFrame;

pub async fn table_as_df(
    base_path: String,
    partitions: Vec<(&'static str, String)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> crate::error::Result<impl Stream<Item = Result<RecordBatch, arrow2::error::ArrowError>> + 'static> {
    let table_name = table_name.unwrap_or(DEFAULT_TABLE_NAME.to_string());
    let mut ctx = crate::datafusion_util::new_context();
    let listing_options = listing_options(format, partitions.clone());
    ctx.register_listing_table("listing_table", &format!("file://{}", base_path), listing_options, None)
        .await
        .map_err(|err| {
            error!(
                "Failed to read from {base_path} : {err}",
                base_path = base_path,
                err = err
            );
            err
        })?;
    let mut table: Arc<dyn DataFrame> = ctx.clone().table("listing_table")?;
    if let Some(filter) = partition_filter_clause(partitions) {
        table = table.filter(filter)?;
    }
    let df_impl = Arc::new(DataFrameImpl::new(ctx.state.clone(), &table.to_logical_plan()));
    ctx.register_table(table_name.as_str(), df_impl.clone())?;
    let df = ctx.clone().sql(&sql_query).await?;
    df.execute_stream().await.err_into()
}
