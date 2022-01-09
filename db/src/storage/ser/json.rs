use crate::error::*;
use crate::storage::{BatchOperation, BatchOperationSer};
use crate::Storage;
use ext::ResultExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait JsonStorageExt {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize;

    fn batch<K>(&self, values: &[BatchOperationSer<'_, K>]) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_range<F, F2, V>(&self, table: &str, from: F, to: F2) -> Result<Vec<(String, V)>>
    where
        F: AsRef<[u8]>,
        F2: AsRef<[u8]>,
        V: DeserializeOwned;

    fn get_all<V>(&self, table: &str) -> Result<Vec<(Box<[u8]>, V)>>
    where
        V: DeserializeOwned;

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>;
}

impl<T: Storage + ?Sized> JsonStorageExt for T {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
    {
        let serialized = serde_json::to_vec::<V>(&value)?;
        self._put(table, key.as_ref(), serialized.as_slice())
    }

    fn batch<K>(&self, values: &[BatchOperationSer<K>]) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let vec: Vec<BatchOperation> = values
            .iter()
            .map(|(t, k, v)| {
                (
                    *t,
                    k.as_ref(),
                    v.as_ref().map(|v| serde_json::to_vec(v.as_ref()).unwrap()),
                )
            })
            .collect();
        self._batch(&vec)
    }

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let record = self._get(table, key.as_ref())?;
        Ok(serde_json::from_slice(record.as_slice())?)
    }

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let items = self._get_ranged(table, from.as_ref())?;
        items.iter().map(|v| serde_json::from_slice(v).err_into()).collect()
    }

    fn get_range<F, F2, V>(&self, table: &str, from: F, to: F2) -> Result<Vec<(String, V)>>
    where
        F: AsRef<[u8]>,
        F2: AsRef<[u8]>,
        V: DeserializeOwned,
    {
        let items = self._get_range(table, from.as_ref(), to.as_ref())?;
        items
            .into_iter()
            .map(|(k, v)| serde_json::from_slice(&v).err_into().map(|d| (k, d)))
            .collect()
    }

    fn get_all<V>(&self, table: &str) -> Result<Vec<(Box<[u8]>, V)>>
    where
        V: DeserializeOwned,
    {
        let items = self._get_all(table)?;
        items
            .into_iter()
            .map(|(k, v)| serde_json::from_slice::<V>(&v).map(|v| (k, v)).err_into())
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
