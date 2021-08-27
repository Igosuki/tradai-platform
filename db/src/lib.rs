#![feature(test)]
#![feature(btree_drain_filter)]
#![allow(incomplete_features)]

#[cfg(test)]
#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate typed_builder;

mod error;
mod storage;

pub use error::Error;
#[cfg(feature = "rkv-lmdb")]
pub use storage::rkv;
pub use storage::rocksdb::{RocksDbOptions, RocksDbStorage};
pub use storage::{get_or_create, DbEngineOptions, DbOptions, Storage, StorageExt};
