use thiserror::Error;
use trading::book::BookError;
use trading::order_manager;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io {0}")]
    IOError(#[from] std::io::Error),
    #[error("json {0}")]
    Json(#[from] serde_json::Error),
    #[error("coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
    #[error("trading {0}")]
    Trading(#[from] trading::error::Error),
    #[error("db {0}")]
    Db(#[from] db::Error),
    #[error("portfolio")]
    Portfolio(#[from] portfolio::Error),
    #[error("model not loaded : {0}")]
    ModelLoadError(String),
    #[error("there are pending operations")]
    PendingOperation,
    #[error("no transaction found in operation")]
    NoTransactionInOperation,
    #[error("ongoing operation status has not changed")]
    NoTransactionChange,
    #[error("operation had to be restaged")]
    OperationRejected,
    #[error("operation was a bad request")]
    OperationBadRequest,
    #[error("operation was cancelled")]
    OperationCancelled,
    #[error("missing order in operation : {0}")]
    OperationMissingOrder(String),
    #[error("order manager {0}")]
    OrderManager(#[from] order_manager::error::Error),
    #[error("invalid position")]
    InvalidPosition,
    #[error("invalid book position")]
    BookError(#[from] BookError),
    #[error("feature is not available yet for this")]
    FeatureNotImplemented,
    #[error("actor mailbox was full")]
    MailboxError(#[from] actix::MailboxError),
    #[cfg(feature = "python")]
    #[error("error running python code")]
    Python(#[from] pyo3::PyErr),
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
            Error::OperationRejected => "operation_restaged",
            Error::InvalidPosition => "invalid_position",
            Error::OperationCancelled => "operation_cancelled",
            Error::OperationBadRequest => "operation_bad_request",
            #[cfg(feature = "python")]
            Error::Python(_) => "python",
            Error::Trading(_) => "trading",
            Error::PendingOperation => "pending_operation",
            Error::FeatureNotImplemented => "feature_not_implemented",
            Error::Json(_) => "json",
            Error::OperationMissingOrder(_) => "operation_missing_order",
            Error::BookError(_) => "book_error",
            Error::OrderManager(_) => "order_manager",
            Error::MailboxError(_) => "mailbox_error",
            Error::Portfolio(_) => "portfolio",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
