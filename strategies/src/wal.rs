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
    backend: Arc<Box<dyn Storage>>,
    table: String,
}

impl Wal {
    pub fn new(backend: Arc<Box<dyn Storage>>, table: String) -> Self { Self { backend, table } }

    pub fn get_all_compacted<T: DeserializeOwned + WalCmp>(&self) -> Result<HashMap<String, T>> {
        let mut records: HashMap<String, T> = HashMap::new();
        self.backend.get_all::<T>(&self.table)?.into_iter().for_each(|(k, v)| {
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
        });
        Ok(records)
    }

    pub fn get_all<T: DeserializeOwned>(&self) -> Result<Vec<(i64, (String, T))>> {
        self.backend.get_all::<T>(&self.table).map_err(|e| e.into()).map(|v| {
            v.into_iter()
                .map(|(k, v)| {
                    let index = k.find(WAL_KEY_SEP);
                    let (t, k) = if let Some(i) = index {
                        let (ts_str, key) = k.split_at(i);
                        (ts_str.parse::<i64>().unwrap(), key.to_string())
                    } else {
                        (0i64, k)
                    };
                    (t, (k, v))
                })
                .collect()
        })
    }

    pub fn append<T: Serialize>(&self, k: String, t: T) -> Result<()> {
        let key = format!("{}{}{}", Utc::now().timestamp_nanos(), WAL_KEY_SEP, k);
        Ok(self.backend.put(&self.table, &key, t)?)
    }
}
