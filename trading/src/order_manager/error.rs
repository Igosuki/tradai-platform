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
    Coinnect(#[from] coinnect_rt::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
