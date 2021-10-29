use datafusion::arrow::array::StructArray;

pub fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name).unwrap().as_any().downcast_ref::<T>().unwrap()
}
