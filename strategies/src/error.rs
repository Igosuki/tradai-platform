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
    #[error("no transaction found in operation")]
    NoTransactionInOperation,
    #[error("operation had to be restaged")]
    OperationRestaged,
    #[error("order manager mailbox was full")]
    OrderManagerMailboxError,
    #[error("order not found : {0}")]
    OrderNotFound(String),
    #[error("invalid position")]
    InvalidPosition,
}

pub type Result<T> = std::result::Result<T, Error>;
