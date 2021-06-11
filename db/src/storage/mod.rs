pub mod mem;
pub mod rocksdb;

use crate::error::*;
use crate::storage::mem::MemoryKVStore;
use crate::RocksDbStorage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

pub trait Storage: Send + Sync + Debug {
    fn _put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>>;

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Box<[u8]>>>;

    /// TODO: this should return impl Iterator
    fn _get_all(&self, table: &str) -> Result<Vec<(String, Box<[u8]>)>>;

    fn _delete(&mut self, table: &str, key: &[u8]) -> Result<()>;

    fn _delete_range(&mut self, table: &str, from: &[u8], to: &[u8]) -> Result<()>;
}

pub trait StorageBincodeExt {
    fn put<K, V>(&mut self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize;

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_ranged<F>(&self, table: &str, from: F) -> Result<Vec<Box<[u8]>>>
    where
        F: AsRef<[u8]>;

    fn get_all<V>(&self, table: &str) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned;

    fn delete<K>(&mut self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn delete_range<K>(&mut self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>;
}

impl<T: Storage + ?Sized> StorageBincodeExt for T {
    fn put<K, V>(&mut self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
    {
        let serialized = bincode::serialize(&value)?;
        self._put(table, key.as_ref(), serialized.as_slice())
    }

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let record = self._get(table, key.as_ref())?;
        bincode::deserialize(record.as_slice()).map_err(|e| e.into())
    }

    fn get_ranged<F>(&self, table: &str, from: F) -> Result<Vec<Box<[u8]>>>
    where
        F: AsRef<[u8]>,
    {
        self._get_ranged(table, from.as_ref())
    }

    fn get_all<V>(&self, table: &str) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned,
    {
        let items = self._get_all(table)?;
        items
            .iter()
            .map(|(k, v)| {
                bincode::deserialize::<V>(v)
                    .map(|v| (k.clone(), v))
                    .map_err(|e| e.into())
            })
            .collect()
    }

    fn delete<K>(&mut self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._delete(table, key.as_ref())
    }

    fn delete_range<K>(&mut self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._delete_range(table, from.as_ref(), to.as_ref())
    }
}

pub fn get_or_create(path: &str, tables: Vec<String>) -> Box<dyn Storage> {
    Box::new(RocksDbStorage::new(path, tables))
}
