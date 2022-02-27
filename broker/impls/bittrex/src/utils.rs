use serde_json::value::Map;
use serde_json::Value;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;

/// Return the name associated to the pair used by Bittrex
/// If the Pair is not supported, None is returned.
pub fn get_pair_string(pair: &Pair) -> Result<MarketSymbol> {
    broker_core::pair::pair_to_symbol(&Exchange::Bittrex, pair)
}

/// Return the Pair associated to the string used by Bittrex
/// If the Pair is not supported, None is returned.
pub fn get_pair_enum(symbol: &str) -> Result<Pair> {
    broker_core::pair::symbol_to_pair(&Exchange::Bittrex, &MarketSymbol::from(symbol))
}

/// If error array is null, return the result (which can be an array, object or null)
/// else return the error string found in array
pub fn parse_result(response: &Map<String, Value>) -> Result<Value> {
    let is_success = match response["success"].as_bool() {
        Some(is_success) => is_success,
        None => return Err(Error::BadParse),
    };

    if is_success {
        Ok(response.get("result").unwrap().clone())
    } else {
        let error_message = response
            .get("message")
            .ok_or_else(|| Error::MissingField("message".to_string()))?
            .as_str()
            .ok_or_else(|| Error::InvalidFieldFormat {
                value: "message".to_string(),
                source: anyhow!("expected a string"),
            })?;

        match error_message {
            "MIN_TRADE_REQUIREMENT_NOT_MET" => Err(Error::InsufficientOrderSize),
            "INVALID_PERMISSION" => Err(Error::PermissionDenied),
            _ => Err(Error::ExchangeSpecificError(error_message.to_string())),
        }
    }
}

/// Return the Asset associated with the
/// string used by Bittrex.
/// # Examples
/// ```
/// use crate::coinnect::bittrex::utils::get_asset;
///
/// let currency = get_asset("1ST");
/// ```
#[allow(clippy::unnecessary_wraps)]
pub fn get_asset(currency: &str) -> Option<Asset> { Some(currency.into()) }
