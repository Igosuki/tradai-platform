use crate::error::*;
use chrono::Utc;
use db::{Storage, StorageExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

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
    pub fn new(backend: Arc<dyn Storage>, table: String) -> Self { Self { backend, table } }

    pub fn get_all_compacted<T: DeserializeOwned + WalCmp>(&self) -> Result<HashMap<String, T>> {
        let mut records: HashMap<String, T> = HashMap::new();
        self.backend
            .get_all::<serde_json::Value>(&self.table)?
            .into_iter()
            .for_each(|(k, v)| {
                serde_json::from_value(v)
                    .map(|v| {
                        let split: Vec<&str> = k.split(WAL_KEY_SEP).collect();
                        if let [_ts, key] = split[..] {
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
            .filter_map(|(k, v)| {
                let (t, k) = match k.split_once(WAL_KEY_SEP) {
                    Some((ts_str, key)) => (ts_str.parse::<i64>().unwrap(), key.to_string()),
                    None => (0i64, k),
                };
                serde_json::from_value(v).map(|vt| (t, (k, vt))).ok()
            })
            .collect();
        Ok(res)
    }

    pub fn append<T: Serialize>(&self, k: String, t: T) -> Result<()> {
        let key = format!("{}{}{}", Utc::now().timestamp_nanos(), WAL_KEY_SEP, k);
        Ok(self.backend.put(&self.table, &key, t)?)
    }
}
