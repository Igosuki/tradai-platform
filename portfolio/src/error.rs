use coinnect_rt::types::TradeType;
use thiserror::Error;
use trading::position::PositionKind;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Coinnect {0}")]
    Coinnect(#[from] coinnect_rt::error::Error),
    #[error("db {0}")]
    Db(#[from] db::Error),
    #[error("no position found for exchange and pair")]
    NoPositionFound,
    #[error("position is locked or not opened")]
    PositionLocked,
    #[error("wrong order side ({2}) when trying to {0} {1} position")]
    BadSideForPosition(&'static str, PositionKind, TradeType),
    #[error("failed to parse uuid")]
    UuidParse(#[from] uuid::Error),
    #[error("no more lock existed for order")]
    NoLockForOrder,
}

pub type Result<T> = std::result::Result<T, Error>;
