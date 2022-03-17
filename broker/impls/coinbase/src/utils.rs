use hmac::{Hmac, Mac};
use serde_json::value::Map;
use serde_json::Value;
use sha2::Sha256;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::{Asset, MarketSymbol, Pair};

/// Return the name associated to the pair used by Gdax
/// If the Pair is not supported, None is returned.
pub fn get_symbol(pair: &Pair) -> Result<MarketSymbol> { broker_core::pair::pair_to_symbol(&Exchange::Coinbase, pair) }

/// Return the Pair associated to the string used by Gdax
/// If the Pair is not supported, None is returned.
#[allow(dead_code)]
pub fn get_pair(symbol: &str) -> Result<Pair> {
    broker_core::pair::symbol_to_pair(&Exchange::Coinbase, &MarketSymbol::from(symbol))
}

pub fn build_signature(nonce: &str, passphrase: &str, api_key: &str, api_secret: &str) -> Result<String> {
    const C: &[u8] = b"0123456789ABCDEF";

    let message = nonce.to_owned() + passphrase + api_key;

    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).expect("HMAC can take key of any size");

    mac.update(message.as_bytes());
    let result = mac.finalize();

    let array = result.into_bytes();
    let raw_signature = array.as_slice();
    let len = raw_signature.len();
    let mut signature = Vec::with_capacity(len * 2);
    for &byte in raw_signature {
        signature.push(C[(byte >> 4) as usize]);
        signature.push(C[(byte & 0xf) as usize]);
    }
    Ok(String::from_utf8(signature)?)
}

pub fn generate_nonce(fixed_nonce: Option<String>) -> String {
    match fixed_nonce {
        Some(v) => v,
        None => get_unix_timestamp_ms().to_string(),
    }
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
/// string used by Bitstamp. If no currency is found,
/// return None
/// # Examples
///
/// ```
/// use crate::coinnect::coinbase::utils::get_currency_enum;
/// use crate::coinnect::types::Currency;
///
/// let currency = get_currency_enum("usd_balance");
/// assert_eq!(Some(Currency::USD), currency);
/// ```
#[allow(clippy::unnecessary_wraps)]
pub fn get_currency_enum(currency: &str) -> Option<Asset> {
    Some(currency.replace("_balance", "").to_uppercase().into())
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn should_create_a_fixed_nonce_when_requested() {
        assert_eq!(super::generate_nonce(Some("1".to_string())), "1");
    }

    #[tokio::test]
    async fn should_create_a_nonce_bigger_than_2017() {
        assert!(super::generate_nonce(None).parse::<i64>().unwrap() > 1_483_228_800);
    }

    #[tokio::test]
    async fn should_create_a_correct_signature() {
        let nonce = "1483228800";
        let passphrase = "123456";
        let api_key = "1234567890ABCDEF1234567890ABCDEF";
        let api_secret = "1234567890ABCDEF1234567890ABCDEF";
        let expected_signature = "7D7C4168D49CBC2620A45EF00EAA228C1287561F1C1F94172272E1231A8ADF6B".to_string();
        assert_eq!(
            super::build_signature(nonce, passphrase, api_key, api_secret).unwrap(),
            expected_signature
        );
    }
}
