use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IOError {0}")]
    IOError(#[from] std::io::Error),
    #[error("Json {0}")]
    Json(#[from] serde_json::Error),
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
    #[error("Db {0}")]
    Db(#[from] db::Error),
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
    #[error("order manager mailbox was full")]
    OrderManagerMailboxError,
    #[error("interest rate provider mailbox was full")]
    InterestRateProviderMailboxError,
    #[error("order not found : {0}")]
    OrderNotFound(String),
    #[error("staged order required")]
    StagedOrderRequired,
    #[error("invalid position")]
    InvalidPosition,
    #[error("invalid book position")]
    BookError(#[from] BookError),
    #[error("feature is not available yet for this")]
    FeatureNotImplemented,
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
            Error::OrderManagerMailboxError => "order_manager_mailbox",
            Error::OrderNotFound(_) => "order_not_found",
            Error::InvalidPosition => "invalid_position",
            Error::OperationCancelled => "operation_cancelled",
            Error::OperationBadRequest => "operation_bad_request",
            #[cfg(feature = "python")]
            Error::Python(_) => "python",
            Error::InterestRateProviderMailboxError => "interest_rate_provider_mailbox",
            Error::PendingOperation => "pending_operation",
            Error::FeatureNotImplemented => "feature_not_implemented",
            Error::StagedOrderRequired => "staged_order_required",
            Error::Json(_) => "json",
            Error::OperationMissingOrder(_) => "operation_missing_order",
            Error::BookError(_) => "book_error",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum BookError {
    #[error("at least one bid expected")]
    MissingBids,
    #[error("at least one ask expected")]
    MissingAsks,
}
