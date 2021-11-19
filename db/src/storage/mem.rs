use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::error::*;
use crate::Storage;

type InMemoryTable = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug)]
pub struct MemoryKVStore {
    inner: RwLock<BTreeMap<Vec<u8>, InMemoryTable>>,
}

impl MemoryKVStore {
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
        f(writer.get_mut(table.as_bytes()).unwrap())
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

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.with_table(table, |t| t.get(table.as_bytes()).cloned())
            .ok_or_else(|| Error::NotFound(format!("Could not find {}", std::str::from_utf8(key).unwrap())))
    }

    fn _get_ranged(&self, _table: &str, _from: &[u8]) -> Result<Vec<Box<[u8]>>> { todo!() }

    fn _get_range(&self, _table: &str, _from: &[u8], _to: &[u8]) -> Result<Vec<(String, Box<[u8]>)>> { todo!() }

    fn _get_all(&self, table: &str) -> Result<Vec<(String, Box<[u8]>)>> {
        let vec = self.with_table(table, |t| {
            t.iter()
                .map(|(k, v)| (String::from_utf8(k.clone()).unwrap(), v.clone().into_boxed_slice()))
                .collect()
        });
        Ok(vec)
    }

    fn _delete(&self, _table: &str, key: &[u8]) -> Result<()> {
        self.inner.write().unwrap().remove(key);
        Ok(())
    }

    fn _delete_range(&self, _table: &str, from: &[u8], to: &[u8]) -> Result<()> {
        self.inner
            .write()
            .unwrap()
            .drain_filter(|k, _v| k as &[u8] > from || k as &[u8] < to);
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
