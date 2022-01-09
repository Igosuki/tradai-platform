use config::ConfigError;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use trading::book::BookError;

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
    #[error("invalid book position")]
    BookError(#[from] BookError),
    #[error("other error")]
    AnyhowError(#[from] anyhow::Error),
    #[error("json error")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
