use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("bincode serde error {0}")]
    Bincode(#[from] bincode::Error),
    #[error("json serde error {0}")]
    Json(#[from] serde_json::Error),
    #[error("rkv error")]
    RocksDb(#[from] rocksdb::Error),
    #[error("record not found {0}")]
    NotFound(String),
}

pub type Result<T> = core::result::Result<T, Error>;
