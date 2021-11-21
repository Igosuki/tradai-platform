#![feature(test)]
#![feature(btree_drain_filter)]
#![allow(incomplete_features)]

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
pub use storage::{get_or_create, DbEngineOptions, DbOptions, Storage, StorageExt};

mod error;
mod storage;
