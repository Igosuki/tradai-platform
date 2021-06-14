use crate::error::*;
use crate::Storage;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct MemoryKVStore {
    inner: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryKVStore {
    #[allow(dead_code)]
    pub fn new() -> Self { MemoryKVStore { inner: BTreeMap::new() } }
}

impl Storage for MemoryKVStore {
    fn _put(&mut self, _table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn _get(&self, _table: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.inner
            .get(key)
            .ok_or_else(|| Error::NotFound(format!("Could not find {}", std::str::from_utf8(key).unwrap())))
            .map(|v| v.clone())
    }

    fn _get_ranged(&self, _table: &str, _from: &[u8]) -> Result<Vec<Box<[u8]>>> { todo!() }

    fn _get_all(&self, _table: &str) -> Result<Vec<(String, Box<[u8]>)>> { todo!() }

    fn _delete(&mut self, _table: &str, key: &[u8]) -> Result<()> {
        self.inner.remove(key);
        Ok(())
    }

    fn _delete_range(&mut self, _table: &str, from: &[u8], to: &[u8]) -> Result<()> {
        self.inner.drain_filter(|k, _v| k as &[u8] > from || k as &[u8] < to);
        Ok(())
    }
}
