use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::{Storage, StorageExt};

use super::error::*;

static WAL_KEY_SEP: &str = "|";

pub trait WalCmp {
    fn is_before(&self, variant: &Self) -> bool;
}

#[derive(Debug)]
pub struct Wal {
    backend: Arc<dyn Storage>,
    table: String,
}

impl Wal {
    pub fn new(backend: Arc<dyn Storage>, table: String) -> Self {
        backend.ensure_table(&table).unwrap();
        Self { backend, table }
    }

    pub fn get_all_compacted<T: DeserializeOwned + WalCmp>(&self) -> Result<HashMap<String, T>> {
        let mut records: HashMap<String, T> = HashMap::new();
        self.backend
            .get_all::<serde_json::Value>(&self.table)?
            .into_iter()
            .for_each(|(k, v)| {
                serde_json::from_value(v)
                    .map(|v| {
                        let split: Vec<&str> = std::str::from_utf8(&*k).unwrap().split(WAL_KEY_SEP).collect();
                        if let [key, _ts] = split[..] {
                            let key_string = key.to_string();
                            match records.entry(key_string.clone()) {
                                Entry::Vacant(_) => {
                                    records.insert(key_string, v);
                                }
                                Entry::Occupied(entry) => {
                                    if entry.get().is_before(&v) {
                                        records.insert(key_string, v);
                                    }
                                }
                            }
                        }
                    })
                    .ok();
            });
        Ok(records)
    }

    pub fn get_all<T: DeserializeOwned>(&self) -> Result<Vec<(i64, (String, T))>> {
        let v = self.backend.get_all::<serde_json::Value>(&self.table)?;
        let res = v
            .into_iter()
            .filter_map(|(key, v)| {
                let (k, t): (String, i64) = Wal::wal_key(String::from_utf8(key.to_vec()).unwrap());
                serde_json::from_value(v).map(|vt| (t, (k, vt))).ok()
            })
            .collect();
        Ok(res)
    }

    fn wal_key(wal_key: String) -> (String, i64) {
        match wal_key.split_once(WAL_KEY_SEP) {
            Some((key, ts_str)) => (key.to_string(), ts_str.parse::<i64>().unwrap()),
            None => (wal_key, 0i64),
        }
    }

    /// Return all values for this key sorted by time
    pub fn get_all_k<T: DeserializeOwned>(&self, key: &str) -> Result<Vec<(i64, T)>> {
        let v = self.backend.get_range::<&str, &str, serde_json::Value>(
            &self.table,
            key,
            &format!("{}{}{}", key, WAL_KEY_SEP, i64::MAX),
        )?;
        let res = v
            .into_iter()
            .filter_map(|(key, v)| {
                let (_, t): (String, i64) = Wal::wal_key(key);
                serde_json::from_value(v).map(|vt| (t, vt)).ok()
            })
            .collect::<Vec<(i64, T)>>();
        Ok(res)
    }

    pub fn append<T: Serialize>(&self, k: &str, t: T) -> Result<()> {
        self.append_raw(k, Utc::now().timestamp_nanos(), t)
    }

    pub fn append_raw<T: Serialize>(&self, k: &str, ts: i64, t: T) -> Result<()> {
        let key = format!("{}{}{}", k, WAL_KEY_SEP, ts);
        Ok(self.backend.put(&self.table, &key, t)?)
    }
}
