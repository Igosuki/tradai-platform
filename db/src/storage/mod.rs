pub mod mem;
pub mod rocksdb;

use crate::error::*;
use crate::RocksDbStorage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::path::Path;

pub trait Storage: Send + Sync + Debug {
    fn _put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>>;

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Box<[u8]>>>;

    /// TODO: this should return impl Iterator
    fn _get_all(&self, table: &str) -> Result<Vec<(String, Box<[u8]>)>>;

    fn _delete(&self, table: &str, key: &[u8]) -> Result<()>;

    fn _delete_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<()>;

    fn ensure_table(&self, name: &str) -> Result<()>;
}

pub trait StorageExt {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize;

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_all<V>(&self, table: &str) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned;

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>;
}

impl<T: Storage + ?Sized> StorageExt for T {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
    {
        let serialized = serde_json::to_vec::<V>(&value)?;
        self._put(table, key.as_ref(), serialized.as_slice())
    }

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let record = self._get(table, key.as_ref())?;
        serde_json::from_slice(record.as_slice()).map_err(|e| e.into())
    }

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let items = self._get_ranged(table, from.as_ref())?;
        items
            .iter()
            .map(|v| serde_json::from_slice(v).map_err(|e| e.into()))
            .collect()
    }

    fn get_all<V>(&self, table: &str) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned,
    {
        let items = self._get_all(table)?;
        items
            .iter()
            .map(|(k, v)| {
                serde_json::from_slice::<V>(v)
                    .map(|v| (k.clone(), v))
                    .map_err(|e| e.into())
            })
            .collect()
    }

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._delete(table, key.as_ref())
    }

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._delete_range(table, from.as_ref(), to.as_ref())
    }
}

pub fn get_or_create<S: AsRef<Path>>(path: S, tables: Vec<String>) -> Box<dyn Storage> {
    Box::new(RocksDbStorage::new(path, tables))
}
