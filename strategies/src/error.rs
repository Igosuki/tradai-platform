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
    #[error("ongoing operation status has not changed")]
    NoTransactionChange,
    #[error("operation had to be restaged")]
    OperationRestaged,
    #[error("order manager mailbox was full")]
    OrderManagerMailboxError,
    #[error("order not found : {0}")]
    OrderNotFound(String),
    #[error("invalid position")]
    InvalidPosition,
}

impl Error {
    pub(crate) fn short_name(&self) -> &'static str {
        match self {
            Error::IOError(_) => "io",
            Error::Coinnect(_) => "coinnect",
            Error::Db(_) => "db",
            Error::ModelLoadError(_) => "model_load",
            Error::NoTransactionChange => "no_transaction_change",
            Error::NoTransactionInOperation => "no_transaction_in_operation",
            Error::OperationRestaged => "operation_restaged",
            Error::OrderManagerMailboxError => "order_manager_mailbox",
            Error::OrderNotFound(_) => "order_not_found",
            Error::InvalidPosition => "invalid_position",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
