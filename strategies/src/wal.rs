use chrono::Utc;
use db::{Db, WriteResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

static WAL_KEY_SEP: &str = "|";

#[derive(Debug, Clone)]
pub struct Wal {
    backend: Db,
}

impl Wal {
    pub fn new(backend: Db) -> Self { Self { backend } }

    pub fn read_all<T: DeserializeOwned>(&self) -> HashMap<String, T> {
        let mut records: HashMap<String, T> = HashMap::new();
        let mut last_key_time: HashMap<String, i64> = HashMap::new();
        self.backend.read_all_json().into_iter().for_each(|(k, v)| {
            let split: Vec<&str> = k.split(WAL_KEY_SEP).collect();
            if let [ts, key] = split[..] {
                let key_string = key.to_string();
                match last_key_time.entry(key_string.clone()) {
                    Entry::Vacant(_) => {
                        records.insert(key_string, v);
                    }
                    Entry::Occupied(entry) => {
                        let new_time = ts.parse::<i64>().unwrap();
                        if new_time > *entry.get() {
                            last_key_time.insert(key_string.clone(), new_time);
                            records.insert(key_string, v);
                        }
                    }
                }
            }
        });
        records
    }

    pub fn append<T: Serialize>(&self, k: String, t: T) -> WriteResult {
        let key = format!("{}{}{}", Utc::now().timestamp_millis(), WAL_KEY_SEP, k);
        self.backend.put_json(&key, t)
    }
}
