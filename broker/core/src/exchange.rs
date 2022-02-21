use crate::error::*;
use serde::{de, Deserialize, Deserializer};
use std::str::FromStr;

#[derive(Debug, Display, PartialEq, Clone, Copy, Eq, Hash, Deserialize, Serialize, PartialOrd, Ord, AsRefStr)]
#[serde(from = "String")]
pub enum Exchange {
    #[strum(serialize = "bitstamp")]
    Bitstamp,
    #[strum(serialize = "kraken")]
    Kraken,
    #[strum(serialize = "poloniex")]
    Poloniex,
    #[strum(serialize = "bittrex")]
    Bittrex,
    #[strum(serialize = "coinbase")]
    Coinbase,
    #[strum(serialize = "binance")]
    Binance,
}

impl Exchange {
    #[must_use]
    pub fn default_fees() -> f64 { 0.001 }

    #[must_use]
    pub fn capitalized(&self) -> String {
        let mut c = self.as_ref().chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        }
    }
}

impl Default for Exchange {
    fn default() -> Self { Self::Binance }
}

pub trait DeserializeWith: Sized {
    fn deserialize_with<'de, D>(de: D) -> ::std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>;
}

impl DeserializeWith for Exchange {
    fn deserialize_with<'de, D>(dez: D) -> ::std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(dez)?;

        let e: Self = Exchange::from_str(s.as_str()).map_err(|e| de::Error::custom(format!("{:?}", e)))?;
        Ok(e)
    }
}

impl From<Exchange> for String {
    fn from(exchange: Exchange) -> Self {
        match exchange {
            Exchange::Bitstamp => "Bitstamp".to_string(),
            Exchange::Kraken => "Kraken".to_string(),
            Exchange::Poloniex => "Poloniex".to_string(),
            Exchange::Bittrex => "Bittrex".to_string(),
            Exchange::Coinbase => "Coinbase".to_string(),
            Exchange::Binance => "Binance".to_string(),
        }
    }
}

impl From<String> for Exchange {
    fn from(s: String) -> Self { Self::from_str(s.as_ref()).unwrap() }
}

impl FromStr for Exchange {
    type Err = Error;

    fn from_str(input: &str) -> ::std::result::Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "bitstamp" => Ok(Exchange::Bitstamp),
            "kraken" => Ok(Exchange::Kraken),
            "poloniex" => Ok(Exchange::Poloniex),
            "bittrex" => Ok(Exchange::Bittrex),
            "coinbase" => Ok(Exchange::Coinbase),
            "binance" => Ok(Exchange::Binance),
            _ => Err(Error::InvalidExchange(input.to_string())),
        }
    }
}
