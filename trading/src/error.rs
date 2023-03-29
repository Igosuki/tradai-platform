use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("interest rate provider mailbox was full")]
    InterestRateProviderMailboxError,
    #[error("Broker {0}")]
    Broker(#[from] brokers::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
