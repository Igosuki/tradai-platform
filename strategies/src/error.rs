use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IOError {0}")]
    IOError(#[from] std::io::Error),
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
    #[error("Db {0}")]
    Db(#[from] db::Error),
    #[error("model not loaded : {0}")]
    ModelLoadError(String),
}

pub type Result<T> = std::result::Result<T, Error>;
