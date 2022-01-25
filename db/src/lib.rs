/*!
Common traits for persistent storage

# Overview

While this is called `db` it currently only gathers common behavior for key value based storage.
Currently `rkv`, `rocksdb` and `memory` can be used as backends.

 */

#![feature(test)]
#![feature(btree_drain_filter)]
#![feature(associated_type_defaults)]
#![allow(
    incomplete_features,
    clippy::wildcard_imports,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::let_underscore_drop
)]

///! Key/Value based storage interfaces
///! Main implementations are rkv-lmdb, rocksdb and in-memory

#[cfg(test)]
#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate serde;
#[cfg(feature = "rkv-lmdb")]
#[macro_use]
extern crate tracing;

pub use error::Error;
pub use storage::mem::MemoryKVStore;
#[cfg(feature = "rkv-lmdb")]
pub use storage::rkv;
pub use storage::rocksdb::{RocksDbOptions, RocksDbStorage};
pub use storage::ser::json::JsonStorageExt as StorageExt;
pub use storage::ser::json::JsonStorageExt;
pub use storage::ser::rkyv::RkyvStorageExt;
pub use storage::{get_or_create, repo::DefaultRepository, DbEngineOptions, DbOptions, Storage};

mod error;
mod storage;
