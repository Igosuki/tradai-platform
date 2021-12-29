use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use ext::{ResultExt, ToAny};

use crate::error::Result;
use crate::storage::rocksdb::RocksDbOptions;
use crate::{MemoryKVStore, RocksDbStorage};

pub mod mem;
pub(crate) mod repo;
#[cfg(feature = "rkv-lmdb")]
pub mod rkv;
pub mod rocksdb;

pub type Bytes = Box<[u8]>;

pub trait Storage: Send + Sync + Debug + ToAny {
    fn _put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>>;

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Box<[u8]>>>;

    fn _get_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<Vec<(String, Box<[u8]>)>>;

    /// TODO: this should return impl Iterator
    fn _get_all(&self, table: &str) -> Result<Vec<(Bytes, Bytes)>>;

    fn _delete(&self, table: &str, key: &[u8]) -> Result<()>;

    fn _delete_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<()>;

    fn ensure_table(&self, name: &str) -> Result<()>;
}

pub trait StorageExt {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize;

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

impl<T: Storage + ?Sized> StorageExt for T {
    fn put<K, V>(&self, table: &str, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Serialize,
    {
        let serialized = serde_json::to_vec::<V>(&value)?;
        self._put(table, key.as_ref(), serialized.as_slice())
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

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbEngineOptions {
    RocksDb(RocksDbOptions),
    InMemory,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DbOptions<S: AsRef<Path>> {
    pub path: S,
    pub engine: DbEngineOptions,
}

impl<S: AsRef<Path>> DbOptions<S> {
    pub fn new(db_path: S) -> Self {
        Self {
            path: db_path,
            engine: DbEngineOptions::RocksDb(RocksDbOptions::default()),
        }
    }

    pub fn new_with_options(db_path: S, options: DbEngineOptions) -> Self {
        Self {
            path: db_path,
            engine: options,
        }
    }

    pub fn new_in_memory(db_path: S) -> Self {
        Self {
            path: db_path,
            engine: DbEngineOptions::InMemory,
        }
    }
}

/// Get or create a Key/Value storage
///
/// # Arguments
///
/// * `options`: The database options
/// * `path`: the path of this specific db
/// * `tables`: the tables that need to be created
///
/// returns: Arc<dyn Storage>
///
/// # Examples
///
/// ```
/// use db::{get_or_create, DbOptions, DbEngineOptions, RocksDbOptions};
/// get_or_create::<&str, &str>(&DbOptions{path: "/tmp/db", engine: DbEngineOptions::RocksDb(RocksDbOptions::default())}, "mydb", vec!["mytable".to_string()]);
/// ```
pub fn get_or_create<S: AsRef<Path>, S2: AsRef<Path>>(
    options: &DbOptions<S>,
    path: S2,
    tables: Vec<String>,
) -> Arc<dyn Storage> {
    match options.engine {
        DbEngineOptions::RocksDb(ref opt) => {
            let mut pb: PathBuf = PathBuf::from(options.path.as_ref());
            pb.push(path);
            Arc::new(RocksDbStorage::try_new(opt, pb, tables).unwrap())
        }
        DbEngineOptions::InMemory => Arc::new(MemoryKVStore::new()),
    }
}
