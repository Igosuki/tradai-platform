pub mod rocksdb;

use crate::error::*;
use crate::RocksDbStorage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

pub trait Storage: Send + Sync + Debug {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        Self: Sized,
        K: AsRef<[u8]>,
        V: Serialize;

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_ranged<F>(&self, table: &str, from: F) -> Result<Vec<Box<[u8]>>>
    where
        Self: Sized,
        F: AsRef<[u8]>;

    fn get_all(&self, table: &str) -> Result<Vec<(String, Box<[u8]>)>>;

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        Self: Sized,
        K: AsRef<[u8]>;

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        Self: Sized,
        K: AsRef<[u8]>;
}

pub fn get_or_create(path: &str, tables: Vec<String>) -> RocksDbStorage { RocksDbStorage::new(path, tables) }
