use config::ConfigError;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("configuration error")]
    ConfError(#[from] ConfigError),
    #[error("datafusion error")]
    DataFusionError(#[from] DataFusionError),
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),
    #[error("io error")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
