use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("order not found : {0}")]
    OrderNotFound(String),
    #[error("order manager mailbox was full")]
    OrderManagerMailboxError,
    #[error("staged order required")]
    StagedOrderRequired,
    #[error("Db {0}")]
    Db(#[from] db::Error),
    #[error("Coinnect {0}")]
    Coinnect(#[from] brokers::error::Error),
    #[error("enum parse error : {0}")]
    EnumParseError(#[from] strum::ParseError),
}

impl Error {
    pub fn short_name(&self) -> &'static str {
        match self {
            Error::Coinnect(_) => "coinnect",
            Error::Db(_) => "db",
            Error::OrderNotFound(_) => "order_not_found",
            Error::OrderManagerMailboxError => "order_mailbox",
            Error::StagedOrderRequired => "staged_order_required",
            Error::EnumParseError(_) => "enum_parse_error",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
