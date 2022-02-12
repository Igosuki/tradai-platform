use serde_json::value::Map;
use serde_json::Value;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;

/// Return the name associated to the pair used by Poloniex
/// If the Pair is not supported, None is returned.
pub fn get_symbol(pair: &Pair) -> Result<Symbol> { broker_core::pair::pair_to_symbol(&Exchange::Poloniex, pair) }

/// Return the Pair associated to the string used by Poloniex
/// If the Pair is not supported, None is returned.
#[allow(dead_code)]
pub fn get_pair(symbol: &str) -> Result<Pair> {
    broker_core::pair::symbol_to_pair(&Exchange::Poloniex, &Symbol::from(symbol))
}

/// If error array is null, return the result (encoded in a json object)
/// else return the error string found in array
pub fn parse_result(response: &Map<String, Value>) -> Result<Map<String, Value>> {
    let error_msg = match response.get("error") {
        Some(error) => error.as_str().ok_or_else(|| Error::InvalidFieldFormat {
            value: "error".to_string(),
            source: anyhow!("expected a string"),
        })?,
        None => return Ok(response.clone()),
    };

    match error_msg {
        "Invalid command." => Err(Error::InvalidArguments),
        "Invalid API key/secret pair." => Err(Error::BadCredentials),
        "Total must be at least 0.0001." => Err(Error::InsufficientOrderSize),
        other => Err(Error::ExchangeSpecificError(other.to_string())),
    }
}

/// Return the currency enum associated with the
/// string used by Poloniex. If no currency is found,
/// return None
/// # Examples
///
/// ```
/// use crate::coinnect::poloniex::utils::get_currency_enum;
/// use crate::coinnect::types::Currency;
///
/// let currency = get_currency_enum("BTC").unwrap();
/// assert_eq!(currency, Currency::BTC);
/// ```
#[allow(clippy::unnecessary_wraps)]
pub fn get_currency_enum(currency: &str) -> Option<Asset> { Some(currency.to_uppercase().into()) }
