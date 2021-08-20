#![feature(test)]
#![feature(btree_drain_filter)]
#![allow(incomplete_features)]

#[cfg(test)]
#[macro_use]
extern crate measure_time;
#[cfg(test)]
#[macro_use]
extern crate serde;

pub use error::Error;
#[cfg(feature = "rkv-lmdb")]
pub use storage::rkv;
pub use storage::rocksdb::RocksDbStorage;
pub use storage::{get_or_create, Storage, StorageExt};
mod error;
mod storage;
