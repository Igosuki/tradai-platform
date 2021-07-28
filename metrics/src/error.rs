use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Prometheus(#[from] prometheus::Error),
}

pub type Result<T> = core::result::Result<T, Error>;
