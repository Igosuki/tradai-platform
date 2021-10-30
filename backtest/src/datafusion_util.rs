use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::record_batch::RecordBatch;

pub fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name).unwrap().as_any().downcast_ref::<T>().unwrap()
}

pub fn to_struct_array(batch: &RecordBatch) -> StructArray {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|t| (t.0.clone(), t.1.clone()))
        .collect::<Vec<(Field, ArrayRef)>>()
        .into()
}
