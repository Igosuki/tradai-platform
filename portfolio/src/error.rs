use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
    #[error("db {0}")]
    Db(#[from] db::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
