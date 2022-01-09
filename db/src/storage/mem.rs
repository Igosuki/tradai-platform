use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::{BatchOperation, Bytes};
use crate::Storage;

type InMemoryTable = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug)]
pub struct MemoryKVStore {
    inner: RwLock<BTreeMap<Vec<u8>, InMemoryTable>>,
}

impl MemoryKVStore {
    #[must_use]
    pub fn new() -> Self {
        MemoryKVStore {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    fn with_table<F, R>(&self, table: &str, f: F) -> R
    where
        F: Fn(&mut InMemoryTable) -> R,
    {
        let mut writer = self.inner.write().unwrap();
        let column = match writer.get_mut(table.as_bytes()) {
            Some(r) => r,
            None => panic!("{}", format!("missing table {} tables = {:?}", table, writer)),
        };
        f(column)
    }
}

impl Default for MemoryKVStore {
    fn default() -> Self { Self::new() }
}

impl Storage for MemoryKVStore {
    fn _put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.with_table(table, |t| t.insert(key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn _batch(&self, values: &[BatchOperation]) -> Result<()> {
        for (table, k, v) in values {
            if let Some(v) = v {
                self._put(table, k, v)?;
            } else {
                self._delete(table, k)?;
            }
        }
        Ok(())
    }

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.with_table(table, |t| t.get(key).cloned())
            .ok_or_else(|| Error::NotFound(key.to_vec()))
    }

    fn _get_ranged(&self, _table: &str, _from: &[u8]) -> Result<Vec<Box<[u8]>>> { todo!() }

    fn _get_range(&self, _table: &str, _from: &[u8], _to: &[u8]) -> Result<Vec<(String, Box<[u8]>)>> { todo!() }

    fn _get_all(&self, table: &str) -> Result<Vec<(Bytes, Bytes)>> {
        let vec = self.with_table(table, |t| {
            t.iter()
                .map(|(k, v)| (k.clone().into_boxed_slice(), v.clone().into_boxed_slice()))
                .collect()
        });
        Ok(vec)
    }

    fn _delete(&self, table: &str, key: &[u8]) -> Result<()> {
        self.with_table(table, |t| t.remove(key));
        Ok(())
    }

    fn _delete_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<()> {
        self.with_table(table, |t| {
            t.drain_filter(|k, _v| *k.as_slice() > *from || *k.as_slice() < *to);
        });
        Ok(())
    }

    fn ensure_table(&self, name: &str) -> Result<()> {
        let mut r = self.inner.write().unwrap();
        if r.get(name.as_bytes()).is_none() {
            r.insert(name.as_bytes().to_vec(), InMemoryTable::new());
        }
        Ok(())
    }
}
