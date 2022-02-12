//! This module contains enum Error.
//! Error type represents all possible errors that can occur when dealing
//! with the generic or any dedicated-exchange API
use thiserror::Error;

use crate::exchange::Exchange;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Unhandled external exchange error: {0}")]
    ExchangeError(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ParseFloat(#[from] ::std::num::ParseFloatError),
    #[error(transparent)]
    ParseString(#[from] ::std::string::FromUtf8Error),
    #[error(transparent)]
    ParseUrl(#[from] ::url::ParseError),
    #[error(transparent)]
    HttpClient(#[from] reqwest::Error),
    #[error(transparent)]
    DataDecoding(#[from] data_encoding::DecodeError),
    #[error(transparent)]
    Io(#[from] ::std::io::Error),
    #[error("Received an event but didn't know how to handle it.")]
    UnhandledEventType,
    #[error("The response could not be parsed.")]
    BadParse,
    #[error("Host could not be reached: {0}.")]
    ServiceUnavailable(String),
    #[error("The informations provided do not allow authentication.")]
    BadCredentials,
    #[error("The credentials could not be found for account {0}")]
    MissingCredentials(String),
    #[error("API call rate limit exceeded.")]
    RateLimitExceeded,
    #[error("This pair is not supported.")]
    PairUnsupported,
    #[error("This symbol could not be converted into a pair.")]
    SymbolPairConversion(String),
    #[error("Exchange not loaded in the pair registry.")]
    ExchangeNotInPairRegistry,
    #[error("Exchange not loaded.")]
    ExchangeNotLoaded,
    #[error("Arguments passed do not conform to the protocol.")]
    InvalidArguments,
    #[error("Exchange error: {0}")]
    ExchangeSpecificError(String),
    #[error("Fail to initialize TLS client.")]
    TlsError,
    #[error("Fail to parse field \"{value}\".")]
    InvalidFieldFormat {
        value: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("Invalid value for field \"{value}\".")]
    InvalidFieldValue {
        value: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("Missing field \"{0}\".")]
    MissingField(String),
    #[error("You haven't enough founds.")]
    InsufficientFunds,
    #[error("Your order is not big enough.")]
    InsufficientOrderSize,
    #[error("No price specified.")]
    MissingPrice,
    #[error("Invalid config: \nExpected: {expected:?}\nFind: {find:?}")]
    InvalidConfigType { expected: Exchange, find: Exchange },
    #[error("Invalid exchange: \"{0}\"")]
    InvalidExchange(String),
    #[error("Invalid nonce")]
    InvalidNonce,
    #[error("The operation cannot be done with the provided credentials")]
    PermissionDenied,
    #[error("Unable to connect to websocket {0}")]
    WsError(String),
    #[error("Unable to connect, last error : {0}")]
    BackoffConnectionTimeout(String),
    #[error("Unable to send into channel : {0}")]
    ChannelCanceled(String),
    #[error("{0}")]
    Msg(&'static str),
    // Order Errors
    #[error("Pair was empty.")]
    EmptyPair,
    #[error("Quantity was negative or null.")]
    InvalidQty,
    #[error("Invalid price")]
    InvalidPrice,
    #[error("Not found")]
    NotFound,
    #[error("Unsupported account type")]
    UnsupportedAccountType,
    #[error("Feature is not implemented for this exchange")]
    ExchangeFeatureNotImplemented,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool { std::mem::discriminant(self) == std::mem::discriminant(other) }
}

pub type Result<T> = core::result::Result<T, Error>;
