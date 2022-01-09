#![allow(unused_variables, dead_code)]

use crate::error::*;
use crate::Storage;
use anyhow::anyhow;
use bytecheck::CheckBytes;
use rkyv::ser::serializers::{AlignedSerializer, AllocSerializer, BufferScratch, CompositeSerializer};
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, Deserialize, Infallible, Serialize};

const RKYV_ALLOC: usize = 1024;

pub type BenchSerializer<'a> =
    CompositeSerializer<AlignedSerializer<&'a mut AlignedVec>, BufferScratch<&'a mut AlignedVec>, Infallible>;

pub trait RkyvStorageExt {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Archive + Serialize<AllocSerializer<RKYV_ALLOC>>;

    // fn batch<K>(&self, values: &[BatchOperationSer<'_, K>]) -> Result<()>
    // where
    //     K: AsRef<[u8]>;

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>;

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>;

    fn get_range<F, F2, V>(&self, table: &str, from: F, to: F2) -> Result<Vec<(String, V)>>
    where
        F: AsRef<[u8]>,
        F2: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>;

    fn get_all<V>(&self, table: &str) -> Result<Vec<(Box<[u8]>, V)>>
    where
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>;

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>;

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>;
}

impl<T: Storage + ?Sized> RkyvStorageExt for T {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Archive + Serialize<AllocSerializer<RKYV_ALLOC>>,
    {
        let serialized = rkyv::to_bytes::<V, RKYV_ALLOC>(&value).map_err(|e| Error::Rkyv(anyhow!("{}", e)))?;
        self._put(table, key.as_ref(), serialized.as_slice())
    }

    fn get<K, V>(&self, table: &str, key: K) -> Result<V>
    where
        K: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>,
    {
        let record = self._get(table, key.as_ref())?;
        let value = rkyv::check_archived_value::<V>(&record, 0).unwrap();
        value
            .deserialize(&mut Infallible)
            .map_err(|e| Error::Rkyv(anyhow!("{}", e)))
    }

    fn get_ranged<F, V>(&self, table: &str, from: F) -> Result<Vec<V>>
    where
        F: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>,
    {
        let items = self._get_ranged(table, from.as_ref())?;
        items
            .iter()
            .map(|v| {
                let value = rkyv::check_archived_value::<V>(v, 0).unwrap();
                value
                    .deserialize(&mut Infallible)
                    .map_err(|e| Error::Rkyv(anyhow!("{}", e)))
            })
            .collect()
    }

    fn get_range<F, F2, V>(&self, table: &str, from: F, to: F2) -> Result<Vec<(String, V)>>
    where
        F: AsRef<[u8]>,
        F2: AsRef<[u8]>,
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>,
    {
        let items = self._get_range(table, from.as_ref(), to.as_ref())?;
        items
            .into_iter()
            .map(|(k, v)| {
                let value = rkyv::check_archived_value::<V>(&v, 0).unwrap();
                value
                    .deserialize(&mut Infallible)
                    .map(|d| (k, d))
                    .map_err(|e| Error::Rkyv(anyhow!("{}", e)))
            })
            .collect()
    }

    fn get_all<V>(&self, table: &str) -> Result<Vec<(Box<[u8]>, V)>>
    where
        V: Archive,
        V::Archived: for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<V, Infallible>,
    {
        let items = self._get_all(table)?;
        items
            .into_iter()
            .map(|(k, v)| {
                let value = rkyv::check_archived_value::<V>(&v, 0).unwrap();
                value
                    .deserialize(&mut Infallible)
                    .map(|v| (k, v))
                    .map_err(|e| Error::Rkyv(anyhow!("{}", e)))
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

#[cfg(test)]
mod test {
    use crate::storage::ser::rkyv::RkyvStorageExt;
    use crate::{MemoryKVStore, Storage};

    #[test]
    fn roundtrip() {
        let store: Box<dyn Storage> = Box::new(MemoryKVStore::new());
        store.ensure_table("table").unwrap();
        RkyvStorageExt::put(store.as_ref(), "table", "kek", 1.0).unwrap();
        let result: crate::error::Result<f64> = RkyvStorageExt::get(store.as_ref(), "table", "kek");
        match result {
            Ok(v) => assert_eq!(v, 1.0),
            Err(e) => panic!("{}", e),
        }
    }
}
