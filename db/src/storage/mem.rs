use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::error::*;
use crate::Storage;

#[derive(Debug)]
pub struct MemoryKVStore {
    inner: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryKVStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        MemoryKVStore {
            inner: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Default for MemoryKVStore {
    fn default() -> Self { Self::new() }
}

impl Storage for MemoryKVStore {
    fn _put(&self, _table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.write().unwrap().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn _get(&self, _table: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.inner
            .write()
            .unwrap()
            .get(key)
            .ok_or_else(|| Error::NotFound(format!("Could not find {}", std::str::from_utf8(key).unwrap())))
            .map(|v| v.clone())
    }

    fn _get_ranged(&self, _table: &str, _from: &[u8]) -> Result<Vec<Box<[u8]>>> { todo!() }

    fn _get_range(&self, _table: &str, _from: &[u8], _to: &[u8]) -> Result<Vec<(String, Box<[u8]>)>> { todo!() }

    fn _get_all(&self, _table: &str) -> Result<Vec<(String, Box<[u8]>)>> { todo!() }

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

    fn ensure_table(&self, _name: &str) -> Result<()> { Ok(()) }
}
