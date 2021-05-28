use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("rkv error")]
    RocksDbError(#[from] rocksdb::Error),
    #[error("record not found {0}")]
    NotFoundError(String),
}

pub type Result<T> = core::result::Result<T, Error>;
