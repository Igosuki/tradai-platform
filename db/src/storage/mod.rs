use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ext::ToAny;

use crate::error::Result;
use crate::storage::rocksdb::RocksDbOptions;
use crate::{MemoryKVStore, RocksDbStorage};

pub mod mem;
pub(crate) mod repo;
#[cfg(feature = "rkv-lmdb")]
pub mod rkv;
pub mod rocksdb;
pub mod ser;

pub type Bytes = Box<[u8]>;

pub type BatchOperation<'a> = (&'a str, &'a [u8], Option<Vec<u8>>);

pub trait Storage: Send + Sync + Debug + ToAny {
    fn _put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn _batch(&self, values: &[BatchOperation]) -> Result<()>;

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>>;

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Box<[u8]>>>;

    fn _get_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<Vec<(String, Box<[u8]>)>>;

    /// TODO: this should return impl Iterator
    fn _get_all(&self, table: &str) -> Result<Vec<(Bytes, Bytes)>>;

    fn _delete(&self, table: &str, key: &[u8]) -> Result<()>;

    fn _delete_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<()>;

    fn ensure_table(&self, name: &str) -> Result<()>;
}

pub type BatchOperationSer<'a, K> = (&'a str, K, Option<Box<dyn erased_serde::Serialize>>);

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
#[allow(clippy::missing_panics_doc)]
pub fn get_or_create<S: AsRef<Path>, S2: AsRef<Path>>(
    options: &DbOptions<S>,
    path: S2,
    tables: Vec<String>,
) -> Arc<dyn Storage> {
    // TODO: this should obviously return a result
    match options.engine {
        DbEngineOptions::RocksDb(ref opt) => {
            let mut pb: PathBuf = PathBuf::from(options.path.as_ref());
            pb.push(path);
            Arc::new(RocksDbStorage::try_new(opt, pb, tables).unwrap())
        }
        DbEngineOptions::InMemory => Arc::new(MemoryKVStore::new()),
    }
}
