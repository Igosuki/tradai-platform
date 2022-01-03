use datafusion::arrow::array::StructArray;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::StructArrayExt;
use itertools::Itertools;
use std::sync::Arc;

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
