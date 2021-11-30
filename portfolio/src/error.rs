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
    #[error("trading error")]
    Trading(#[from] trading::error::Error),
    #[error("order quantity was zero or negative")]
    ZeroOrNegativeOrderQty,
}

impl Error {
    pub fn short_name(&self) -> &'static str {
        match self {
            Error::Coinnect(_) => "coinnect",
            Error::Db(_) => "db",
            Error::Trading(_) => "trading",
            Error::NoPositionFound => "no_position_found",
            Error::PositionLocked => "position_locked",
            Error::BadSideForPosition(_, _, _) => "bad_side_for_pos",
            Error::UuidParse(_) => "uuid_parse",
            Error::NoLockForOrder => "no_lock_for_order",
            Error::ZeroOrNegativeOrderQty => "zero_or_negative_qty",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
