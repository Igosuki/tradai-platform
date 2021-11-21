use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("interest rate provider mailbox was full")]
    InterestRateProviderMailboxError,
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
