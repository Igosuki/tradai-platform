mod rocksdb;

use crate::error::*;

pub trait Storage {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn get<K>(&self, table: &str, key: K) -> Result<String>
    where
        K: AsRef<[u8]>;

    fn get_ranged<F>(&self, table: &str, from: F) -> Result<Vec<Box<[u8]>>>
    where
        F: AsRef<[u8]>;

    fn get_all(&self, table: &str) -> Result<Vec<(String, Box<[u8]>)>>;

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>;
}
