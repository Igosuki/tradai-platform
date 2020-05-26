use derive_more::Display;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IOError {0}")]
    IOError(#[from] std::io::Error),
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
}
