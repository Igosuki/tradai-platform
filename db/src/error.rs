use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bincode serde error {0}")]
    Bincode(#[from] bincode::Error),
    #[error("json serde error {0}")]
    Json(#[from] serde_json::Error),
    #[error("rocksdb error {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[cfg(feature = "rkyv")]
    #[error("rkyv error {0}")]
    Rkyv(#[from] anyhow::Error),
    #[error("record not found {0:?}")]
    NotFound(Vec<u8>),
}

pub type Result<T> = core::result::Result<T, Error>;
