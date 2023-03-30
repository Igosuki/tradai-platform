use datafusion::arrow::array::{Array, DictionaryArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::logical_expr::Literal;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::prelude::*;
use ext::ResultExt;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

/// Downcasts a column to an Array type
pub fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {}", name,))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| {
            panic!(
                "column {} with type {:?}",
                name,
                sa.column_by_name(name).unwrap().data_type()
            )
        })
}

/// Returns the actual FileFormat handle for a given extension
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
    I: ExactSizeIterator,
{
    if iter.is_empty() {
        String::new()
    } else {
        "where ".to_string() + iter.map(|s| format!("{}='{}'", s.0, s.1)).join(" and ").as_str()
    }
}

/// Combines an iterator of key value pairs into an and expression filter clause
/// Example : [(a, 1), (b, 2)] turns into 'a=1 and b=2'
pub fn partition_filter_clause<
    'a,
    K: 'a + std::fmt::Display + AsRef<str>,
    V: 'a + std::fmt::Display + Literal,
    I: IntoIterator<Item = (K, V)>,
>(
    iter: I,
) -> Option<Expr> {
    iter.into_iter()
        .map(|s| col(s.0.as_ref()).eq(lit(s.1)))
        .reduce(|lhs, rhs| lhs.and(rhs))
}

/// Gets the value at i for from the partition column
pub fn string_partition<K: ArrowPrimitiveType>(col: &DictionaryArray<K>, i: usize) -> Option<String> {
    col.key(i).and_then(|k| {
        col.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(k).to_string())
    })
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
pub fn new_context() -> SessionContext { SessionContext::new() }

/// Utility method to give default listing options from a format and partitions
pub fn listing_options(format: String, partition: Vec<(&str, String)>) -> ListingOptions {
    let (ext, file_format) = df_format(&format);
    ListingOptions {
        file_extension: ext.to_string(),
        format: file_format,
        table_partition_cols: partition.iter().map(|p| (p.0.to_string(), DataType::Utf8)).collect(),
        collect_stat: true,
        target_partitions: 8,
        file_sort_order: None,
        infinite_source: false,
    }
}

pub const DEFAULT_TABLE_NAME: &str = "listing_table";

/// Reads multiple table paths with a common format into a unified stream of batches
pub fn multitables_as_stream<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> impl Stream<Item = RecordBatch> + 'static {
    debug!("reading partitions {:?}", &table_paths);
    let s = table_paths.into_iter().map(move |(base_path, partitions)| {
        tables_as_stream(
            base_path,
            partitions,
            format.clone(),
            table_name.clone(),
            sql_query.clone(),
        )
    });
    tokio_stream::iter(s).flatten()
}

/// Reads a single table path with a common format into a unified stream of batches
pub fn tables_as_stream<P: 'static + AsRef<Path> + Debug>(
    base_path: P,
    partitions: Vec<(&'static str, String)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> impl Stream<Item = RecordBatch> + 'static {
    trace!("base_path = {:?}", base_path);
    trace!("partitions = {:?}", partitions);

    stream! {
        let base_path = base_path.as_ref().to_str().unwrap_or("").to_string();
        let now = Instant::now();
        let df = table_as_df(base_path.clone(), partitions.clone(), format, table_name, sql_query).await.expect(&format!("table path {}", base_path));
        let stream = df.execute_stream().await.unwrap();
        let elapsed = now.elapsed();
        debug!(
            "Read records in {} for {:?} in {}.{}s",
            base_path,
            partitions,
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
        for await batch in stream {
            yield batch.unwrap();
        }
        let elapsed = now.elapsed();
        debug!(
            "Pushed record stream in {} for {:?} in {}.{}s",
            base_path,
            partitions,
            elapsed.as_secs(),
            elapsed.subsec_millis()
        );
    }
}

/// Reads multiple table paths with a common format into a single record batch
pub async fn multitables_as_df<P: 'static + AsRef<Path> + Debug>(
    table_paths: HashSet<(P, Vec<(&'static str, String)>)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> crate::error::Result<RecordBatch> {
    debug!("reading partitions {:?}", &table_paths);
    let records: Vec<RecordBatch> =
        futures::future::try_join_all(table_paths.into_iter().map(move |(base_path, partitions)| {
            tables_as_df(
                base_path,
                partitions,
                format.clone(),
                table_name.clone(),
                sql_query.clone(),
            )
        }))
        .await?;
    // Filter batches without columns aka empty tables
    let records = records
        .into_iter()
        .filter(|rb| !rb.columns().is_empty())
        .collect::<Vec<RecordBatch>>();
    if records.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let rb = concat_batches(
        &records[0].schema().clone(),
        &records,
        records.iter().map(|rb| rb.num_rows()).sum(),
    )?;
    Ok(rb)
}

/// Reads a single table path with a common format into a single record batch
pub async fn tables_as_df<P: 'static + AsRef<Path> + Debug>(
    base_path: P,
    partitions: Vec<(&'static str, String)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> crate::error::Result<RecordBatch> {
    trace!("base_path = {:?}", base_path);
    trace!("partitions = {:?}", partitions);

    let base_path = base_path.as_ref().to_str().unwrap_or("").to_string();
    let now = Instant::now();
    let df = table_as_df(base_path.clone(), partitions.clone(), format, table_name, sql_query).await?;
    let records = df.collect().await?;
    if records.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let rb = concat_batches(
        &records[0].schema().clone(),
        &records,
        records.iter().map(|rb| rb.num_rows()).sum(),
    )?;
    let elapsed = now.elapsed();
    debug!(
        "Read records in {} for {:?} in {}.{}s",
        base_path,
        partitions,
        elapsed.as_secs(),
        elapsed.subsec_millis()
    );
    Ok(rb)
}

// TODO: see if this can be done without hopping through a listing table first
pub async fn table_as_df(
    base_path: String,
    partitions: Vec<(&'static str, String)>,
    format: String,
    table_name: Option<String>,
    sql_query: String,
) -> crate::error::Result<DataFrame> {
    let table_name = table_name.unwrap_or_else(|| DEFAULT_TABLE_NAME.to_string());
    let ctx = new_context();
    let listing_options = listing_options(format, partitions.clone());

    ctx.register_listing_table("listing_table", &base_path, listing_options, None, None)
        .await
        .map_err(|err| {
            error!(
                "Failed to read from {base_path} : {err}",
                base_path = base_path,
                err = err
            );
            err
        })?;
    let mut table: DataFrame = ctx.clone().table("listing_table").await?;
    if let Some(filter) = partition_filter_clause(partitions) {
        table = table.filter(filter)?;
    }
    let df_impl = DataFrame::new(ctx.state().clone(), table.into_optimized_plan().unwrap().clone());
    let provider = df_impl.into_view();
    ctx.register_table(table_name.as_str(), provider)?;
    ctx.clone().sql(&sql_query).await.err_into()
}

/// Prints each column of a struct array (giving a name to the struct array itself)
pub fn print_struct_schema(sa: &StructArray, name: &str) {
    for (i, column) in sa.columns().iter().enumerate() {
        trace!("{}[{}] = {:?}", name, i, column.data_type());
    }
}
